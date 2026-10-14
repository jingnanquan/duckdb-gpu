#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gpu_paths.py — duckdb-gpu 统一路径配置加载器 (Python 侧)

所有脚本里的绝对路径都由仓库根目录的 gpu_paths.conf 决定:

    base_dir=/home/yx/code/bishe/duckdb-gpu

换机器时只改 gpu_paths.conf 那一行即可。

优先级:
  1. 环境变量 DUCKDB_GPU_BASE_DIR   (临时覆盖, 不落盘)
  2. gpu_paths.conf 里的 base_dir
  3. 本文件所在目录 (即仓库根目录)

用法 (仓库任意子目录下的脚本):

    import os, sys
    _root = os.path.dirname(os.path.abspath(__file__))
    while not os.path.isfile(os.path.join(_root, "gpu_paths.py")) and \\
            os.path.dirname(_root) != _root:
        _root = os.path.dirname(_root)
    sys.path.insert(0, _root)
    from gpu_paths import BASE_DIR, path

    DATA_DIR = path("data", "tpch_sf5_bitmap")

命令行:
    python3 gpu_paths.py            # 打印 base_dir
    python3 gpu_paths.py data/tpch_sf5_bitmap
"""

import os
import sys

CONF_NAME = "gpu_paths.conf"
ENV_BASE_DIR = "DUCKDB_GPU_BASE_DIR"

# 仓库根目录 = 本文件所在目录
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def _strip_quotes(value):
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        value = value[1:-1]
    return value.rstrip("/") or "/"


def _parse_conf(conf_path):
    values = {}
    try:
        with open(conf_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.split("#", 1)[0].strip()
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                value = _strip_quotes(value)
                if value:
                    values[key.strip()] = value
    except OSError:
        pass
    return values


def _load():
    values = _parse_conf(os.path.join(ROOT_DIR, CONF_NAME))
    env_base = os.environ.get(ENV_BASE_DIR)
    if env_base:
        values["base_dir"] = _strip_quotes(env_base)
    values.setdefault("base_dir", ROOT_DIR)
    return values


VALUES = _load()

#: 仓库根目录 (绝对路径, 末尾无斜杠)
BASE_DIR = VALUES["base_dir"]

#: gpu_paths.conf 的完整路径
CONF_PATH = os.path.join(ROOT_DIR, CONF_NAME)


def get(key, default=None):
    """读取 gpu_paths.conf 中的任意键 (默认返回 default)。"""
    return VALUES.get(key, default)


def path(*parts):
    """把 base_dir 与相对路径拼接成绝对路径。"""
    return os.path.join(BASE_DIR, *parts)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        print(path(*sys.argv[1:]))
    else:
        print(BASE_DIR)
