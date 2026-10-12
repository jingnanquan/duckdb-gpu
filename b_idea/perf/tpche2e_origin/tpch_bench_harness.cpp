// tpch_bench_harness.cpp
//
// TPC-H e2e 计时 harness —— 通过 DuckDB C API 链接 libduckdb.so 执行。
//
// 设计要点 (解决旧脚本的两大问题):
//   1) 冷启动: 全部 22 条查询都在 *同一个进程 / 同一个 connection* 内执行,
//      只有进程启动时加载一次 .so, 每条查询不再 fork subprocess。
//      因此 avg 反映的是查询本身的开销, 而非 Python/subprocess 调度开销。
//   2) 计时精度: 用 std::chrono::steady_clock 紧贴 conn.Query()+Materialize(),
//      排除 CLI 初始化/打印/进程退出等噪声, 比 "wall-clock around subprocess" 精确得多。
//
// 官方版与 bitmap 版使用 *同一个* 二进制, 仅通过 LD_LIBRARY_PATH 决定加载哪个
// libduckdb.so, 并通过 --bitmap 决定是否下发 bitmap_join_load / open_bitmap_join。
// 这样两遍在同一机器、同一进程模型下测量, 偏差可控。
//
// 用法:
//   LD_LIBRARY_PATH=<dir-with-libduckdb.so> ./tpch_bench_harness \
//       --queries queries_run.sql --data-dir <parquet dir> \
//       [--meta bitmap_join_meta.json] [--bitmap] \
//       [--warmup 1] [--runs 3]
//
// 查询文件格式 (由 Python 驱动拼接): 以单独一行 "--QUERY Qnn" 作为分隔符,
// 其后的内容 (直到下一个 "--QUERY" 或文件尾) 即为该查询的 SQL 文本。
// 这样可原样容纳含引号/换行的任意 SQL, 无需 JSON 转义。
//
// 输出: 仅把 JSON 结果写到 stdout; 所有日志/错误写到 stderr。

#include <duckdb.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

namespace {

// ---------- 配置 ----------
struct Config {
    std::string queries_file;
    std::string data_dir;
    std::string meta;       // 仅 bitmap 模式使用
    bool bitmap = false;
    int warmup = 1;
    int runs = 3;
};

void die(const std::string &msg) {
    fprintf(stderr, "[harness ERROR] %s\n", msg.c_str());
    exit(2);
}

// 读取整个文件
std::string read_file(const std::string &path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) die("cannot open queries file: " + path);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

// 按 "--QUERY Qnn" 分隔符解析出 name -> sql
std::map<std::string, std::string> parse_queries(const std::string &content) {
    std::map<std::string, std::string> out;
    std::vector<std::pair<size_t, std::string>> markers;  // (pos, name)
    const std::string prefix = "--QUERY ";
    size_t pos = 0;
    while ((pos = content.find(prefix, pos)) != std::string::npos) {
        size_t line_start = pos;
        size_t eol = content.find('\n', pos);
        std::string line = (eol == std::string::npos)
                               ? content.substr(pos)
                               : content.substr(pos, eol - pos);
        std::string name = line.substr(prefix.size());
        // 去掉尾部 \r / 空白
        while (!name.empty() && (name.back() == '\r' || name.back() == ' '))
            name.pop_back();
        markers.emplace_back(line_start, name);
        pos = (eol == std::string::npos) ? std::string::npos : eol + 1;
    }
    if (markers.empty()) die("no '--QUERY Qxx' markers found in queries file");

    for (size_t i = 0; i < markers.size(); ++i) {
        size_t start = markers[i].first;
        size_t end = (i + 1 < markers.size()) ? markers[i + 1].first : content.size();
        // 跳过 marker 这一行本身 (从第一个 '\n' 之后开始取)
        size_t body = content.find('\n', start);
        if (body == std::string::npos) body = end;  // 理论不会发生
        else body += 1;
        std::string sql = content.substr(body, end - body);
        // 去掉结尾可能的 "--END" / 空白
        while (!sql.empty() &&
               (sql.back() == '\n' || sql.back() == '\r' || sql.back() == ' '))
            sql.pop_back();
        if (!sql.empty() && sql.rfind("--END", 0) == sql.size() - 5) {
            sql = sql.substr(0, sql.size() - 5);
            while (!sql.empty() &&
                   (sql.back() == '\n' || sql.back() == '\r' || sql.back() == ' '))
                sql.pop_back();
        }
        out[markers[i].second] = sql;
    }
    return out;
}

// ---------- DuckDB 执行 ----------
struct DB {
    duckdb_database db = nullptr;
    duckdb_connection con = nullptr;

    void open(bool bitmap, const std::string &data_dir, const std::string &meta) {
        if (duckdb_open(nullptr, &db) != DuckDBSuccess)
            die("duckdb_open failed");
        if (duckdb_connect(db, &con) != DuckDBSuccess)
            die("duckdb_connect failed");

        // 关闭扩展自动安装/加载 (与原脚本一致)
        exec("SET autoinstall_known_extensions=false;");
        exec("SET autoload_known_extensions=false;");

        // 建 8 张表 -> parquet view
        const char *tables[] = {"region", "nation", "customer", "orders",
                                "part", "partsupp", "supplier", "lineitem"};
        for (const char *t : tables) {
            std::string sql = "CREATE OR REPLACE VIEW " + std::string(t) +
                              " AS SELECT * FROM read_parquet('" + data_dir + "/" +
                              t + ".parquet');";
            exec(sql.c_str());
        }

        if (bitmap) {
            if (meta.empty())
                die("--bitmap 需要 --meta <bitmap_join_meta.json>");
            std::string load = "PRAGMA bitmap_join_load('" + meta + "');";
            exec(load.c_str());
            exec("SET open_bitmap_join=true;");
            fprintf(stderr, "[harness] bitmap 模式已启用 (meta=%s)\n", meta.c_str());
        } else {
            fprintf(stderr, "[harness] 官方模式 (无 bitmap join)\n");
        }
    }

    void exec(const char *sql) {
        duckdb_result res;
        if (duckdb_query(con, sql, &res) != DuckDBSuccess) {
            const char *err = duckdb_result_error(&res);
            std::string e = err ? err : "unknown error";
            duckdb_destroy_result(&res);
            die(std::string("SQL 执行失败: ") + e + "\n  SQL: " + sql);
        }
        duckdb_destroy_result(&res);
    }

    // 执行一条查询并返回 (耗时秒, 行数)。报错则抛出异常用 die 处理。
    std::pair<double, idx_t> run_timed(const std::string &sql) {
        auto t0 = std::chrono::steady_clock::now();
        duckdb_result res;
        if (duckdb_query(con, sql.c_str(), &res) != DuckDBSuccess) {
            const char *err = duckdb_result_error(&res);
            std::string e = err ? err : "unknown error";
            duckdb_destroy_result(&res);
            die(std::string("QUERY 执行失败: ") + e);
        }
        // duckdb_query 在 C API 中返回的是已完整物化的 duckdb_result,
        // 因此计时区域已经覆盖完整执行/输出开销。
        auto t1 = std::chrono::steady_clock::now();
        idx_t rows = duckdb_row_count(&res);
        duckdb_destroy_result(&res);
        double secs =
            std::chrono::duration<double>(t1 - t0).count();
        return {secs, rows};
    }

    ~DB() {
        if (con) duckdb_disconnect(&con);
        if (db) duckdb_close(&db);
    }
};

void usage() {
    fprintf(stderr,
            "usage: tpch_bench_harness --queries f.sql --data-dir D "
            "[--meta m.json] [--bitmap] [--warmup N] [--runs N]\n");
}

}  // namespace

int main(int argc, char **argv) {
    Config cfg;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto need = [&](const char *name) -> std::string {
            if (i + 1 >= argc) { usage(); die(std::string("missing value for ") + name); }
            return argv[++i];
        };
        if (a == "--queries") cfg.queries_file = need("--queries");
        else if (a == "--data-dir") cfg.data_dir = need("--data-dir");
        else if (a == "--meta") cfg.meta = need("--meta");
        else if (a == "--bitmap") cfg.bitmap = true;
        else if (a == "--warmup") cfg.warmup = std::atoi(need("--warmup").c_str());
        else if (a == "--runs") cfg.runs = std::atoi(need("--runs").c_str());
        else { usage(); die("unknown arg: " + a); }
    }
    if (cfg.queries_file.empty() || cfg.data_dir.empty())
        die("--queries 和 --data-dir 为必填");
    if (cfg.warmup < 0) cfg.warmup = 0;
    if (cfg.runs < 1) cfg.runs = 1;

    // 解析查询
    std::string content = read_file(cfg.queries_file);
    auto queries = parse_queries(content);
    fprintf(stderr, "[harness] 解析到 %zu 条查询\n", queries.size());

    // 打开 DB (单进程 / 单连接)
    DB db;
    db.open(cfg.bitmap, cfg.data_dir, cfg.meta);

    // 执行
    std::map<std::string, std::map<std::string, std::string>> results;
    for (auto &kv : queries) {
        const std::string &name = kv.first;
        const std::string &sql = kv.second;

        // warmup (仅预热, 不计入均值)
        double w = 0.0;
        idx_t wrows = 0;
        for (int i = 0; i < cfg.warmup; ++i) {
            auto r = db.run_timed(sql);
            w = r.first;
            wrows = r.second;
        }
        // measured runs
        std::vector<double> runs;
        runs.reserve(cfg.runs);
        idx_t rows = wrows;
        for (int i = 0; i < cfg.runs; ++i) {
            auto r = db.run_timed(sql);
            runs.push_back(r.first);
            rows = r.second;
        }
        double sum = 0.0, mn = runs[0], mx = runs[0];
        for (double v : runs) { sum += v; if (v < mn) mn = v; if (v > mx) mx = v; }
        double avg = sum / runs.size();

        std::string runs_str;
        for (size_t i = 0; i < runs.size(); ++i) {
            if (i) runs_str += ",";
            char buf[64];
            snprintf(buf, sizeof(buf), "%.6f", runs[i]);
            runs_str += buf;
        }
        results[name] = {
            {"warmup", (cfg.warmup > 0) ? std::to_string(w) : std::string("0")},
            {"avg", std::to_string(avg)},
            {"min", std::to_string(mn)},
            {"max", std::to_string(mx)},
            {"runs", runs_str},
            {"rows", std::to_string((long long)rows)},
        };
        fprintf(stderr, "  %s: warmup=%.4fs avg=%.4fs (min=%.4f max=%.4f) rows=%llu\n",
                name.c_str(), w, avg, mn, mx, (unsigned long long)rows);
    }

    // 输出 JSON 到 stdout (供 Python 解析)。顺序按 Q 编号。
    std::string out = "{";
    bool first = true;
    for (auto &kv : results) {
        if (!first) out += ",";
        first = false;
        out += "\"" + kv.first + "\":{";
        out += "\"warmup\":" + kv.second["warmup"] + ",";
        out += "\"avg\":" + kv.second["avg"] + ",";
        out += "\"min\":" + kv.second["min"] + ",";
        out += "\"max\":" + kv.second["max"] + ",";
        out += "\"runs\":[" + kv.second["runs"] + "],";
        out += "\"rows\":" + kv.second["rows"];
        out += "}";
    }
    out += "}";
    printf("%s\n", out.c_str());
    fflush(stdout);
    return 0;
}
