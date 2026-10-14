// tpch_bench_harness.cpp
//
// TPC-H e2e 计时 harness —— 通过 DuckDB C API 链接 libduckdb.so 执行。
//
// 设计要点 (解决旧脚本的两大问题):
//   1) 冷启动: 整个 benchmark 只启动一个进程、只加载一次 .so。DB 实例的生命周期
//      由 --db-mode 控制:
//        per-query : 每条查询新建独立 DB 实例 / 独立 connection, 从空 buffer pool
//                    起步 (消除跨查询缓存污染与内存淘汰干扰, OS page cache 仍共享)
//        reuse     : 全程一个 DB 实例, 建表/物化只做一次
//      默认 auto: --load-mode view -> per-query, --load-mode mem -> reuse。
//   2) 计时精度: 用 std::chrono::steady_clock 紧贴 duckdb_query() (C API 已
//      完整物化结果), 排除 CLI 初始化/打印/进程退出等噪声。
//
// 加载模式 (新增, 用于与 "数据已驻留内存" 的执行引擎对齐口径):
//   --load-mode view (默认): CREATE OR REPLACE VIEW t AS SELECT * FROM read_parquet(...)
//      只是磁盘 parquet 的映射, 每次查询都要重新解码 parquet —— 与"含 IO/解码"
//      的端到端口径对应。
//   --load-mode mem: CREATE OR REPLACE TABLE t AS SELECT * FROM read_parquet(...)
//      把 8 张表 CTAS 物化进 DuckDB 自身列存 (纯内存库), 之后查询不再触碰
//      parquet —— 与"数据已在内存列存、只统计纯查询"的口径对应。
//   两种模式下建表/物化的耗时都单独计时并输出为 load_ms, 不计入 query 时间,
//      与 fascalsql 的 load/query 分段语义一致。
//
// official_baseline / project_baseline / project_bitmap 使用 *同一个* 二进制,
// 仅通过 LD_LIBRARY_PATH 决定加载哪个 libduckdb.so, 并通过 --bitmap 决定
// 是否下发 bitmap_join_load / open_bitmap_join。这样三遍在同一机器、同一
// 进程模型下测量, 偏差可控。
//
// 用法:
//   LD_LIBRARY_PATH=<dir-with-libduckdb.so> ./tpch_bench_harness \
//       --queries queries_run.sql --data-dir <parquet dir> \
//       [--meta bitmap_join_meta.json] [--bitmap] [--set-bitmap-off] \
//       [--load-mode view|mem] [--db-mode auto|per-query|reuse] \
//       [--memory-limit 24GB] \
//       [--warmup 1] [--runs 3]
//
// 查询文件格式 (由 Python 驱动拼接): 以单独一行 "--QUERY Qnn" 作为分隔符,
// 其后的内容 (直到下一个 "--QUERY" 或文件尾) 即为该查询的 SQL 文本。
// 这样可原样容纳含引号/换行的任意 SQL, 无需 JSON 转义。
//
// 输出: 仅把 JSON 结果写到 stdout; 所有日志/错误写到 stderr。
// JSON 结构:
//   {"config": {...},
//    "load": {"mode": "...", "total_ms": x, "by_query": {"Q01": x, ...}},
//    "queries": {"Q01": {"warmup","avg","min","max","runs","rows"}, ...}}
//   by_query 仅在 per-query 模式下非空 (每条查询各自建表一次);
//   reuse 模式下建表只发生一次, 用 load.total_ms 表示。

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
    bool set_bitmap_off = false;  // 仅项目 baseline 使用，官方 lib 无此配置项
    std::string load_mode = "view";   // view | mem
    std::string db_mode = "auto";     // auto | per-query | reuse
    std::string memory_limit;         // 可选, 如 "24GB"; 为空则沿用 DuckDB 默认
    int warmup = 1;
    int runs = 3;
};

void die(const std::string &msg) {
    fprintf(stderr, "[harness ERROR] %s\n", msg.c_str());
    exit(2);
}

static double elapsed_ms(std::chrono::steady_clock::time_point t0) {
    return std::chrono::duration<double, std::milli>(
               std::chrono::steady_clock::now() - t0).count();
}

static std::string fmt(double v, const char *spec) {
    char buf[64];
    snprintf(buf, sizeof(buf), spec, v);
    return std::string(buf);
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
    double load_ms = 0.0;   // 建表/物化耗时 (对齐 fascalsql 的 load)

    void open(const Config &cfg) {
        if (duckdb_open(nullptr, &db) != DuckDBSuccess)
            die("duckdb_open failed");
        if (duckdb_connect(db, &con) != DuckDBSuccess)
            die("duckdb_connect failed");

        // 关闭扩展自动安装/加载 (与原脚本一致)
        exec("SET autoinstall_known_extensions=false;");
        exec("SET autoload_known_extensions=false;");
        if (!cfg.memory_limit.empty())
            exec(("SET memory_limit='" + cfg.memory_limit + "';").c_str());

        // ---- 建 8 张表 (这一段单独计时, 不计入 query) ----
        //   view: parquet 映射, 每次查询都要重新解码
        //   mem : CTAS 物化进 DuckDB 自身列存 (纯内存库), 之后不再触碰 parquet
        auto t_load = std::chrono::steady_clock::now();
        const char *tables[] = {"region", "nation", "customer", "orders",
                                "part", "partsupp", "supplier", "lineitem"};
        for (const char *t : tables) {
            std::string src = "read_parquet('" + cfg.data_dir + "/" + t + ".parquet')";
            std::string sql;
            if (cfg.load_mode == "mem") {
                sql = "CREATE OR REPLACE TABLE " + std::string(t) +
                      " AS SELECT * FROM " + src + ";";
            } else {
                sql = "CREATE OR REPLACE VIEW " + std::string(t) +
                      " AS SELECT * FROM " + src + ";";
            }
            exec(sql.c_str());
        }
        load_ms = elapsed_ms(t_load);
        fprintf(stderr, "[harness] load_mode=%s 建表完成: %.1f ms\n",
                cfg.load_mode.c_str(), load_ms);

        if (cfg.bitmap) {
            if (cfg.meta.empty())
                die("--bitmap 需要 --meta <bitmap_join_meta.json>");
            std::string load = "PRAGMA bitmap_join_load('" + cfg.meta + "');";
            exec(load.c_str());
            exec("SET open_bitmap_join=true;");
            fprintf(stderr, "[harness] bitmap 模式已启用 (meta=%s)\n", cfg.meta.c_str());
        } else if (cfg.set_bitmap_off) {
            exec("SET open_bitmap_join=false;");
            fprintf(stderr, "[harness] project baseline 模式 (open_bitmap_join=false)\n");
        } else {
            fprintf(stderr, "[harness] official baseline 模式 (不设置 open_bitmap_join)\n");
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

typedef std::map<std::string, std::map<std::string, std::string>> ResultMap;

// 在给定 DB 上跑完一条查询的 warmup + measured runs, 写入 results
void run_one(DB &db, const std::string &name, const std::string &sql,
             const Config &cfg, ResultMap &results) {
    // warmup (仅预热, 不计入均值, 在本查询自己的 buffer pool 内)
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
        runs_str += fmt(runs[i], "%.6f");
    }
    results[name] = {
        {"warmup", (cfg.warmup > 0) ? fmt(w, "%.6f") : std::string("0")},
        {"avg", fmt(avg, "%.6f")},
        {"min", fmt(mn, "%.6f")},
        {"max", fmt(mx, "%.6f")},
        {"runs", runs_str},
        {"rows", std::to_string((long long)rows)},
    };
    fprintf(stderr, "  %s: warmup=%.4fs avg=%.4fs (min=%.4f max=%.4f) rows=%llu\n",
            name.c_str(), w, avg, mn, mx, (unsigned long long)rows);
}

void usage() {
    fprintf(stderr,
            "usage: tpch_bench_harness --queries f.sql --data-dir D "
            "[--meta m.json] [--bitmap] [--set-bitmap-off] "
            "[--load-mode view|mem] [--db-mode auto|per-query|reuse] "
            "[--memory-limit 24GB] [--warmup N] [--runs N]\n");
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
        else if (a == "--set-bitmap-off") cfg.set_bitmap_off = true;
        else if (a == "--load-mode") cfg.load_mode = need("--load-mode");
        else if (a == "--db-mode") cfg.db_mode = need("--db-mode");
        else if (a == "--memory-limit") cfg.memory_limit = need("--memory-limit");
        else if (a == "--warmup") cfg.warmup = std::atoi(need("--warmup").c_str());
        else if (a == "--runs") cfg.runs = std::atoi(need("--runs").c_str());
        else { usage(); die("unknown arg: " + a); }
    }
    if (cfg.queries_file.empty() || cfg.data_dir.empty())
        die("--queries 和 --data-dir 为必填");
    if (cfg.warmup < 0) cfg.warmup = 0;
    if (cfg.runs < 1) cfg.runs = 1;
    if (cfg.load_mode != "view" && cfg.load_mode != "mem")
        die("--load-mode 只能是 view|mem, 收到: " + cfg.load_mode);

    // 解析 db_mode: auto -> mem 用 reuse (物化只做一次), view 用 per-query
    std::string db_mode = cfg.db_mode;
    if (db_mode.empty() || db_mode == "auto")
        db_mode = (cfg.load_mode == "mem") ? "reuse" : "per-query";
    if (db_mode != "per-query" && db_mode != "reuse")
        die("--db-mode 只能是 auto|per-query|reuse, 收到: " + cfg.db_mode);
    if (cfg.load_mode == "mem" && db_mode == "per-query")
        fprintf(stderr,
                "[harness WARN] load_mode=mem + db_mode=per-query: "
                "每条查询都会重建 8 张表 (CTAS), 开销很大, 通常应使用 reuse\n");

    // 解析查询
    std::string content = read_file(cfg.queries_file);
    auto queries = parse_queries(content);
    fprintf(stderr, "[harness] 解析到 %zu 条查询  load_mode=%s db_mode=%s\n",
            queries.size(), cfg.load_mode.c_str(), db_mode.c_str());

    // 执行
    ResultMap results;
    std::map<std::string, double> load_by_query;  // 仅 per-query 模式填充
    double load_total = 0.0;

    if (db_mode == "reuse") {
        // 全程一个 DB 实例: 建表/物化只做一次, 之后所有查询都打在同一份数据上
        DB db;
        db.open(cfg);
        load_total = db.load_ms;
        for (auto &kv : queries)
            run_one(db, kv.first, kv.second, cfg, results);
    } else {
        // 每条查询独立实例 / 独立 connection / 独立 buffer manager
        for (auto &kv : queries) {
            DB db;
            db.open(cfg);
            load_by_query[kv.first] = db.load_ms;
            load_total += db.load_ms;
            run_one(db, kv.first, kv.second, cfg, results);
        }
    }

    // 输出 JSON 到 stdout (供 Python 解析)。顺序按 Q 编号。
    std::string out = "{";
    out += "\"config\":{";
    out += "\"load_mode\":\"" + cfg.load_mode + "\",";
    out += "\"db_mode\":\"" + db_mode + "\",";
    out += "\"warmup\":" + std::to_string(cfg.warmup) + ",";
    out += "\"runs\":" + std::to_string(cfg.runs);
    out += "},";

    out += "\"load\":{";
    out += "\"mode\":\"" + cfg.load_mode + "\",";
    out += "\"total_ms\":" + fmt(load_total, "%.4f") + ",";
    out += "\"by_query\":{";
    {
        bool first = true;
        for (auto &kv : load_by_query) {
            if (!first) out += ",";
            first = false;
            out += "\"" + kv.first + "\":" + fmt(kv.second, "%.4f");
        }
    }
    out += "}},";

    out += "\"queries\":{";
    {
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
    }
    out += "}}";

    printf("%s\n", out.c_str());
    fflush(stdout);
    return 0;
}
