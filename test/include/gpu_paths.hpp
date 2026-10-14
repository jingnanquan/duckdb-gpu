//===----------------------------------------------------------------------===//
// gpu_paths.hpp — duckdb-gpu 统一路径配置 (C++ 测试侧)
//===----------------------------------------------------------------------===//
// 以前散落在测试里的硬编码绝对路径 (例如 "/home/featurize/workspace/duckdb-cuda")
// 现在统一由仓库根目录的 gpu_paths.conf 决定:
//
//     base_dir=/home/yx/code/bishe/duckdb-gpu
//
// 换机器时只改 gpu_paths.conf 那一行, 无需重新编译。
//
// 查找顺序 (BaseDir()):
//   1. 环境变量 DUCKDB_GPU_BASE_DIR     —— 直接覆盖, 用于临时切换
//   2. gpu_paths.conf 里的 base_dir, 配置文件按以下顺序定位:
//        a. 环境变量 DUCKDB_GPU_PATHS_CONF 指定的文件
//        b. DUCKDB_ROOT_DIRECTORY (CMake 传入的工程源码目录) 下
//        c. 当前工作目录向上逐级查找 (unittest 一般在仓库根目录启动)
//   3. 兜底: DUCKDB_ROOT_DIRECTORY / 当前工作目录
//
// 用法:
//   #include "gpu_paths.hpp"
//   auto dir = duckdb::gpu_paths::Join("data/tpch_sf5_bitmap");
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"

#include <cstdlib>
#include <fstream>
#include <string>

namespace duckdb {
namespace gpu_paths {

inline std::string Env(const char *name) {
	const char *value = std::getenv(name);
	return value ? std::string(value) : std::string();
}

inline std::string Trim(const std::string &s) {
	const char *ws = " \t\r\n";
	auto first = s.find_first_not_of(ws);
	if (first == std::string::npos) {
		return std::string();
	}
	auto last = s.find_last_not_of(ws);
	return s.substr(first, last - first + 1);
}

//! 去掉末尾多余的 '/' (根目录 '/' 除外)
inline std::string StripTrailingSlash(const std::string &s) {
	std::string out = Trim(s);
	while (out.size() > 1 && out[out.size() - 1] == '/') {
		out.pop_back();
	}
	return out;
}

inline bool FileExists(const std::string &path) {
	std::ifstream in(path.c_str());
	return in.good();
}

inline std::string ParentDirectory(const std::string &path) {
	auto pos = path.find_last_of('/');
	if (pos == std::string::npos || pos == 0) {
		return std::string();
	}
	return path.substr(0, pos);
}

//! 从 key=value 形式的配置文件里取值 (忽略 '#' 注释, 值可带引号)
inline bool ParseConfFile(const std::string &path, const std::string &key, std::string &value) {
	std::ifstream in(path.c_str());
	if (!in.is_open()) {
		return false;
	}
	std::string line;
	while (std::getline(in, line)) {
		auto hash = line.find('#');
		if (hash != std::string::npos) {
			line = line.substr(0, hash);
		}
		auto eq = line.find('=');
		if (eq == std::string::npos) {
			continue;
		}
		if (Trim(line.substr(0, eq)) != key) {
			continue;
		}
		std::string parsed = Trim(line.substr(eq + 1));
		if (parsed.size() >= 2 &&
		    ((parsed.front() == '"' && parsed.back() == '"') || (parsed.front() == '\'' && parsed.back() == '\''))) {
			parsed = parsed.substr(1, parsed.size() - 2);
		}
		if (parsed.empty()) {
			continue;
		}
		value = parsed;
		return true;
	}
	return false;
}

inline std::string FindConfFile() {
	// (a) 环境变量直接指定配置文件
	std::string env_conf = Env("DUCKDB_GPU_PATHS_CONF");
	if (!env_conf.empty() && FileExists(env_conf)) {
		return env_conf;
	}
	// (b) 工程源码目录 (CMake: DUCKDB_ROOT_DIRECTORY)
#ifdef DUCKDB_ROOT_DIRECTORY
	{
		std::string candidate = std::string(DUCKDB_ROOT_DIRECTORY) + "/gpu_paths.conf";
		if (FileExists(candidate)) {
			return candidate;
		}
	}
#endif
	// (c) 当前工作目录向上逐级查找
	std::string dir = FileSystem::GetWorkingDirectory();
	while (!dir.empty()) {
		std::string candidate = dir + "/gpu_paths.conf";
		if (FileExists(candidate)) {
			return candidate;
		}
		std::string parent = ParentDirectory(dir);
		if (parent.empty() || parent == dir) {
			break;
		}
		dir = parent;
	}
	return std::string();
}

//! gpu_paths.conf 中的任意配置项
inline std::string ConfValue(const std::string &key, const std::string &default_value = std::string()) {
	std::string value;
	std::string conf = FindConfFile();
	if (!conf.empty() && ParseConfFile(conf, key, value)) {
		return value;
	}
	return default_value;
}

inline std::string ComputeBaseDir() {
	// 1. 环境变量优先
	std::string env_base = Env("DUCKDB_GPU_BASE_DIR");
	if (!env_base.empty()) {
		return StripTrailingSlash(env_base);
	}
	// 2. gpu_paths.conf
	std::string value;
	std::string conf = FindConfFile();
	if (!conf.empty() && ParseConfFile(conf, "base_dir", value)) {
		return StripTrailingSlash(value);
	}
	// 3. 兜底
#ifdef DUCKDB_ROOT_DIRECTORY
	return StripTrailingSlash(std::string(DUCKDB_ROOT_DIRECTORY));
#else
	return StripTrailingSlash(FileSystem::GetWorkingDirectory());
#endif
}

//! 仓库根目录 (base_dir), 惰性初始化 + 缓存
inline const std::string &BaseDir() {
	static const std::string base_dir = ComputeBaseDir();
	return base_dir;
}

//! base_dir 下的相对路径 -> 绝对路径 (传入绝对路径则原样返回)
inline std::string Join(const std::string &relative) {
	const std::string &base = BaseDir();
	if (relative.empty()) {
		return base;
	}
	if (relative[0] == '/') {
		return relative;
	}
	return base + "/" + relative;
}

} // namespace gpu_paths
} // namespace duckdb
