#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
add_bitmap_columns.py
=====================

详细设计 §6.1 模块 M-S：表预处理脚本。

将 TPC-H parquet 原表升级为 BHJ（Bitmap HashJoin）友好版本：

1. 对每张主表 T：
   - 若 T.pk_col 已经是稠密整数 [base, base+N)（base ∈ {0,1}），则直接复用 pk_col
     作为 rowid，仅记录 rowid_offset=base；不新增列，节省存储与改写成本。
   - 否则追加 `_rowid` 列（UBIGINT，从 0 起递增）。
2. 对每张外键表 F：对每个外键列 fk_col 引用 T.pk_col：
   - 追加 `<fk_col>_ref` 列（UBIGINT，可空），值 = T 中对应行的 rowid
     （即 pk_value - rowid_offset，若 PK 复用；否则通过 join 取 rowid）。
3. 输出 bitmap_join_meta.json，列出所有 PK / FK 绑定，供 DuckDB 启动时加载。

设计要点（与 §6.1 / §18 对齐）：
- 仅追加列，不修改/删除原列；输出到独立目录（默认 <input>_bitmap），原数据保留。
- 使用 DuckDB 自身完成 parquet I/O，避免引入 pandas/pyarrow 依赖；
  纯 SQL 路径（COPY ... TO ... PARQUET）是 DuckDB 内的零拷贝列式读写。
- 幂等：如果目标 parquet 已存在且已包含 rowid/_ref 列，则跳过。
- 使用 --force 强制重做；使用 --dry-run 仅打印计划不写文件。
- 对每个 (pk_table, pk_col) 只处理一次（多个 FK 表共享同一 PK 不重复扫主表）。

依赖：
- python >= 3.8
- duckdb python 包（pip install duckdb）

CLI 示例：
    python b_idea/scripts/add_bitmap_columns.py \
        --input  data/tpch_sf10/ \
        --output data/tpch_sf10_bitmap/ \
        --constraints b_idea/scripts/tpch_constraints.csv \
        --meta-out   data/tpch_sf10_bitmap/bitmap_join_meta.json
"""
from __future__ import annotations

import argparse
import csv
import dataclasses
import json
import logging
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    import duckdb  # type: ignore
except ImportError:  # pragma: no cover
    sys.stderr.write(
        "[FATAL] python `duckdb` package not found in: " + sys.executable + "\n"
        "  Install it for the current interpreter, e.g.:\n"
        "    " + sys.executable + " -m pip install duckdb\n"
        "  Or, if you have a dedicated python (e.g. /root/.local/python-3.11.10):\n"
        "    /root/.local/python-3.11.10/bin/pip3.11 install duckdb\n"
        "  Then re-run this script with that interpreter.\n"
    )
    raise

# ---------------------------------------------------------------------------
# 常量与日志
# ---------------------------------------------------------------------------

ROWID_COLUMN_NAME = "_rowid"   # 当 PK 不是稠密整数时新增的列名
REF_COLUMN_SUFFIX = "_ref"     # FK 表新增的列名后缀
META_VERSION = 1

logger = logging.getLogger("add_bitmap_columns")


def _setup_logger(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


# ---------------------------------------------------------------------------
# 数据结构
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class Constraint:
    """一行 tpch_constraints.csv：pk_table, pk_col, fk_table, fk_col"""
    pk_table: str
    pk_col: str
    fk_table: str
    fk_col: str


@dataclasses.dataclass
class PKBinding:
    """与 §6.2 BitmapJoinPKBinding 对齐"""
    pk_table: str
    pk_column: str
    rowid_column: str        # 实际承担 rowid 角色的列名（可能 == pk_column）
    rowid_offset: int        # 原 PK 值减去 offset = rowid。仅当复用 pk_col 时非 0
    row_count: int

    def to_meta(self) -> dict:
        return {
            "pk_table": self.pk_table,
            "pk_column": self.pk_column,
            "rowid_column": self.rowid_column,
            "rowid_offset": self.rowid_offset,
            "row_count": self.row_count,
        }


@dataclasses.dataclass
class FKBinding:
    """与 §6.2 BitmapJoinFKBinding 对齐"""
    fk_table: str
    fk_column: str
    ref_column: str
    pk_table: str
    pk_column: str

    def to_meta(self) -> dict:
        return {
            "fk_table": self.fk_table,
            "fk_column": self.fk_column,
            "ref_column": self.ref_column,
            "pk_table": self.pk_table,
            "pk_column": self.pk_column,
        }


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------


def _quote_ident(name: str) -> str:
    """把列名 / 表名安全包成 SQL 标识符（双引号，处理内嵌双引号）"""
    return '"' + name.replace('"', '""') + '"'


def _quote_sql_string(value: str) -> str:
    """把字符串安全包成 SQL 字面量（单引号，处理内嵌单引号）"""
    return "'" + value.replace("'", "''") + "'"


def _parquet_path(dir_: Path, table: str) -> Path:
    return dir_ / f"{table}.parquet"


def load_constraints(csv_path: Path) -> List[Constraint]:
    constraints: List[Constraint] = []
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for raw in reader:
            if not raw:
                continue
            # 跳过注释 / 空行 / 表头
            first = raw[0].strip()
            if not first or first.startswith("#"):
                continue
            if first.lower() == "pk_table":
                continue
            if len(raw) < 4:
                raise ValueError(f"bad constraint row: {raw!r}")
            constraints.append(
                Constraint(
                    pk_table=raw[0].strip(),
                    pk_col=raw[1].strip(),
                    fk_table=raw[2].strip(),
                    fk_col=raw[3].strip(),
                )
            )
    if not constraints:
        raise ValueError(f"no constraints found in {csv_path}")
    return constraints


def get_columns(con: duckdb.DuckDBPyConnection, parquet_path: Path) -> List[Tuple[str, str]]:
    """返回 parquet 文件的 (column_name, column_type) 列表，保持 schema 顺序。

    使用 DESCRIBE SELECT * FROM read_parquet(...)，列顺序与 parquet 物理列顺序一致。
    返回结构：[(name, type), ...]，DESCRIBE 输出的前两列分别是 column_name 与 column_type。
    """
    rows = con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)", [str(parquet_path)]
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def has_column(cols: List[Tuple[str, str]], name: str) -> bool:
    return any(c[0] == name for c in cols)


def detect_dense_int_pk(
    con: duckdb.DuckDBPyConnection,
    parquet_path: Path,
    pk_col: str,
) -> Tuple[bool, int, int]:
    """
    判断 pk_col 是否是「稠密整数」：count == max-min+1，且没有 NULL。
    返回 (is_dense, base, row_count)。base = min(pk)，row_count = count。
    若不是整数类型，返回 (False, 0, count)。
    """
    qcol = _quote_ident(pk_col)
    qpath = str(parquet_path)
    # 先看类型
    type_row = con.execute(
        f"SELECT typeof({qcol}) FROM read_parquet(?) LIMIT 1", [qpath]
    ).fetchone()
    if type_row is None:
        return False, 0, 0
    typ = (type_row[0] or "").upper()
    integer_types = (
        "TINYINT", "UTINYINT", "SMALLINT", "USMALLINT",
        "INTEGER", "UINTEGER", "BIGINT", "UBIGINT", "HUGEINT",
    )
    is_int = any(t in typ for t in integer_types)

    cnt_row = con.execute(
        f"SELECT COUNT(*), COUNT({qcol}) FROM read_parquet(?)", [qpath]
    ).fetchone()
    total, non_null = int(cnt_row[0]), int(cnt_row[1])
    if total == 0:
        return False, 0, 0

    if not is_int or non_null != total:
        return False, 0, total

    mm_row = con.execute(
        f"SELECT MIN({qcol}), MAX({qcol}), COUNT(DISTINCT {qcol}) "
        f"FROM read_parquet(?)",
        [qpath],
    ).fetchone()
    pk_min, pk_max, distinct = int(mm_row[0]), int(mm_row[1]), int(mm_row[2])
    is_dense = (
        distinct == total
        and (pk_max - pk_min + 1) == total
        and pk_min in (0, 1)  # 仅认 0 起或 1 起；其他 base 视为非稠密以保守
    )
    return is_dense, pk_min, total


# ---------------------------------------------------------------------------
# 核心处理流程
# ---------------------------------------------------------------------------


class BitmapPreprocessor:
    def __init__(
        self,
        input_dir: Path,
        output_dir: Path,
        constraints: List[Constraint],
        force: bool,
        dry_run: bool,
        threads: int,
    ) -> None:
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.constraints = constraints
        self.force = force
        self.dry_run = dry_run
        self.threads = threads

        # 已处理过的 PK / FK，键为 (table, col)，避免重复
        self.pk_done: Dict[Tuple[str, str], PKBinding] = {}
        self.fk_done: Dict[Tuple[str, str, str, str], FKBinding] = {}

        # 当前所需的所有表（input dir 里必须存在）
        all_tables = set()
        for c in constraints:
            all_tables.add(c.pk_table)
            all_tables.add(c.fk_table)
        self.all_tables = sorted(all_tables)

        # DuckDB 内存连接；所有 SQL 通过它跑，不持久化
        self.con = duckdb.connect()
        if threads > 0:
            self.con.execute(f"PRAGMA threads={threads}")

    # ---------- 路径与拷贝 ----------

    def _input_parquet(self, table: str) -> Path:
        p = _parquet_path(self.input_dir, table)
        if not p.exists():
            raise FileNotFoundError(f"input parquet not found: {p}")
        return p

    def _output_parquet(self, table: str) -> Path:
        return _parquet_path(self.output_dir, table)

    def _ensure_output_copy(self, table: str) -> Path:
        """
        把 input parquet 拷贝（或符号链接，按需）到 output 目录，作为「待加列」基础。
        如果 output 已存在且 not force，则原样使用（用于幂等增量）。
        """
        src = self._input_parquet(table)
        dst = self._output_parquet(table)
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists() and not self.force:
            logger.debug("[%s] reuse existing output parquet: %s", table, dst)
            return dst
        if self.dry_run:
            logger.info("[dry-run] would copy %s -> %s", src, dst)
            return dst
        logger.info("[%s] copy %s -> %s", table, src, dst)
        shutil.copy2(src, dst)
        return dst

    # ---------- 处理 PK 表 ----------

    def process_pk_table(
        self, pk_table: str, pk_col: str
    ) -> PKBinding:
        key = (pk_table, pk_col)
        if key in self.pk_done:
            return self.pk_done[key]

        out_path = self._ensure_output_copy(pk_table)
        cols = get_columns(self.con, out_path) if out_path.exists() else []

        # 检测 pk_col 是否稠密
        src_for_detect = out_path if out_path.exists() else self._input_parquet(pk_table)
        is_dense, base, row_count = detect_dense_int_pk(
            self.con, src_for_detect, pk_col
        )

        if is_dense:
            logger.info(
                "[%s.%s] PK is dense int (base=%d, count=%d), reuse as rowid",
                pk_table, pk_col, base, row_count,
            )
            binding = PKBinding(
                pk_table=pk_table,
                pk_column=pk_col,
                rowid_column=pk_col,
                rowid_offset=base,
                row_count=row_count,
            )
            self.pk_done[key] = binding
            return binding

        # 非稠密：需要追加 _rowid 列
        if has_column(cols, ROWID_COLUMN_NAME) and not self.force:
            logger.info(
                "[%s] %s already exists, skip (use --force to overwrite)",
                pk_table, ROWID_COLUMN_NAME,
            )
            # 仍然需要 row_count
            row_count = int(
                self.con.execute(
                    "SELECT COUNT(*) FROM read_parquet(?)", [str(out_path)]
                ).fetchone()[0]
            )
        else:
            self._append_rowid_column(out_path, pk_table)
            row_count = int(
                self.con.execute(
                    "SELECT COUNT(*) FROM read_parquet(?)", [str(out_path)]
                ).fetchone()[0]
            )

        binding = PKBinding(
            pk_table=pk_table,
            pk_column=pk_col,
            rowid_column=ROWID_COLUMN_NAME,
            rowid_offset=0,
            row_count=row_count,
        )
        self.pk_done[key] = binding
        return binding

    def _append_rowid_column(self, parquet_path: Path, table: str) -> None:
        """
        给 parquet_path 追加 `_rowid` UBIGINT 列。
        DuckDB 不支持 in-place 修改 parquet，做法：
          1. 读 parquet 为关系；用 row_number() OVER () - 1 作为 rowid；
          2. 写到临时文件；
          3. 原子替换。
        注意：parquet 没有「原始文件行号」概念，但 row_number 顺序与 read_parquet 的
        scan 顺序一致。所有 FK 表 ref 列后续基于 (pk_value -> rowid) 的映射生成，
        与物理顺序无关。
        """
        if self.dry_run:
            logger.info("[dry-run] would append %s to %s", ROWID_COLUMN_NAME, parquet_path)
            return
        tmp_path = parquet_path.with_suffix(".parquet.tmp")
        qcol = _quote_ident(ROWID_COLUMN_NAME)
        qtmp = _quote_sql_string(str(tmp_path))
        sql = (
            f"COPY (SELECT *, "
            f"   CAST(row_number() OVER () - 1 AS UBIGINT) AS {qcol} "
            f"FROM read_parquet(?)) TO {qtmp} (FORMAT PARQUET)"
        )
        t0 = time.time()
        self.con.execute(sql, [str(parquet_path)])
        os.replace(tmp_path, parquet_path)
        logger.info(
            "[%s] appended %s to %s (took %.2fs)",
            table, ROWID_COLUMN_NAME, parquet_path, time.time() - t0,
        )

    # ---------- 处理 FK 表 ----------

    def process_fk(
        self,
        constraint: Constraint,
        pk_binding: PKBinding,
    ) -> FKBinding:
        key = (
            constraint.fk_table,
            constraint.fk_col,
            constraint.pk_table,
            constraint.pk_col,
        )
        if key in self.fk_done:
            return self.fk_done[key]

        ref_col = constraint.fk_col + REF_COLUMN_SUFFIX
        out_path = self._ensure_output_copy(constraint.fk_table)
        cols = get_columns(self.con, out_path) if out_path.exists() else []

        if has_column(cols, ref_col) and not self.force:
            logger.info(
                "[%s] %s already exists, skip",
                constraint.fk_table, ref_col,
            )
        else:
            self._append_ref_column(
                fk_path=out_path,
                fk_table=constraint.fk_table,
                fk_col=constraint.fk_col,
                ref_col=ref_col,
                pk_binding=pk_binding,
            )

        binding = FKBinding(
            fk_table=constraint.fk_table,
            fk_column=constraint.fk_col,
            ref_column=ref_col,
            pk_table=constraint.pk_table,
            pk_column=constraint.pk_col,
        )
        self.fk_done[key] = binding
        return binding

    def _append_ref_column(
        self,
        fk_path: Path,
        fk_table: str,
        fk_col: str,
        ref_col: str,
        pk_binding: PKBinding,
    ) -> None:
        """
        给 fk_path 追加 `<fk_col>_ref` UBIGINT 列。
        - 若 PK 复用稠密整数列：ref = fk_col - rowid_offset（NULL → NULL）；
          这是一个纯 row-wise 的算术映射，不需要 join。
        - 否则：ref = (fk_col → pk.<rowid_col>) 的 LEFT JOIN 结果；
          构建临时哈希表（DuckDB 自身的 hashjoin 即可）。
        """
        if self.dry_run:
            logger.info(
                "[dry-run] would append %s to %s (ref of %s.%s)",
                ref_col, fk_path, pk_binding.pk_table, pk_binding.pk_column,
            )
            return

        tmp_path = fk_path.with_suffix(".parquet.tmp")
        qfk = _quote_ident(fk_col)
        qref = _quote_ident(ref_col)
        qtmp = _quote_sql_string(str(tmp_path))
        t0 = time.time()

        if pk_binding.rowid_column == pk_binding.pk_column:
            # PK 已稠密，纯算术映射
            offset = pk_binding.rowid_offset
            # 用 CASE 处理 NULL；CAST 到 UBIGINT
            # 注意：fk_col 可能是有符号整数；做减法不会溢出（min ≥ offset）
            expr = (
                f"CASE WHEN {qfk} IS NULL THEN NULL "
                f"ELSE CAST({qfk} - {offset} AS UBIGINT) END AS {qref}"
            )
            sql = (
                f"COPY (SELECT *, {expr} FROM read_parquet(?)) "
                f"TO {qtmp} (FORMAT PARQUET)"
            )
            self.con.execute(sql, [str(fk_path)])
        else:
            # 非稠密：LEFT JOIN PK 表
            pk_path = self._output_parquet(pk_binding.pk_table)
            qpk_col = _quote_ident(pk_binding.pk_column)
            qpk_rowid = _quote_ident(pk_binding.rowid_column)
            sql = (
                f"COPY ("
                f"  SELECT f.*, "
                f"         CAST(p.{qpk_rowid} AS UBIGINT) AS {qref} "
                f"  FROM read_parquet(?) AS f "
                f"  LEFT JOIN read_parquet(?) AS p "
                f"    ON f.{qfk} = p.{qpk_col}"
                f") TO {qtmp} (FORMAT PARQUET)"
            )
            self.con.execute(
                sql, [str(fk_path), str(pk_path)]
            )

        os.replace(tmp_path, fk_path)
        logger.info(
            "[%s] appended %s -> %s.%s (took %.2fs)",
            fk_table, ref_col, pk_binding.pk_table, pk_binding.rowid_column,
            time.time() - t0,
        )

    # ---------- 校验 ----------

    def validate_fk(self, fk_path: Path, ref_col: str, bitmap_size: int) -> None:
        """断言 ref 列：要么 NULL，要么 0 ≤ ref < bitmap_size，且每个 ref 至多对应一个 PK 行（语义上）"""
        if self.dry_run:
            return
        qref = _quote_ident(ref_col)
        row = self.con.execute(
            f"SELECT "
            f"  COUNT(*) FILTER (WHERE {qref} IS NOT NULL AND "
            f"                        ({qref} < 0 OR {qref} >= {bitmap_size})) "
            f"FROM read_parquet(?)",
            [str(fk_path)],
        ).fetchone()
        bad = int(row[0])
        if bad > 0:
            raise RuntimeError(
                f"[VALIDATE-FAIL] {fk_path}.{ref_col} has {bad} out-of-range refs "
                f"(bitmap_size={bitmap_size})"
            )
        logger.debug("[validate] %s.%s ok", fk_path.name, ref_col)

    # ---------- 主入口 ----------

    def run(self) -> Tuple[List[PKBinding], List[FKBinding]]:
        # 1) 先处理所有涉及到的 PK 表
        seen_pk = set()
        for c in self.constraints:
            key = (c.pk_table, c.pk_col)
            if key in seen_pk:
                continue
            seen_pk.add(key)
            self.process_pk_table(c.pk_table, c.pk_col)

        # 2) 再为非 PK 表（仅 FK 角色）确保 output 拷贝存在
        pk_tables = {c.pk_table for c in self.constraints}
        for c in self.constraints:
            if c.fk_table not in pk_tables:
                self._ensure_output_copy(c.fk_table)

        # 3) 处理所有 FK
        for c in self.constraints:
            pk_binding = self.pk_done[(c.pk_table, c.pk_col)]
            fk_binding = self.process_fk(c, pk_binding)
            # 校验
            self.validate_fk(
                fk_path=self._output_parquet(c.fk_table),
                ref_col=fk_binding.ref_column,
                bitmap_size=pk_binding.row_count,
            )

        return list(self.pk_done.values()), list(self.fk_done.values())


# ---------------------------------------------------------------------------
# meta JSON 输出
# ---------------------------------------------------------------------------


def write_meta_json(
    pk_bindings: List[PKBinding],
    fk_bindings: List[FKBinding],
    meta_path: Path,
    input_dir: Path,
    output_dir: Path,
) -> None:
    meta = {
        "version": META_VERSION,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "rowid_column_default": ROWID_COLUMN_NAME,
        "ref_column_suffix": REF_COLUMN_SUFFIX,
        "pk_bindings": [b.to_meta() for b in pk_bindings],
        "fk_bindings": [b.to_meta() for b in fk_bindings],
    }
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    with meta_path.open("w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
    logger.info("wrote meta json: %s (%d PK, %d FK)",
                meta_path, len(pk_bindings), len(fk_bindings))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Add rowid / *_ref columns to TPC-H parquet for BHJ.",
    )
    p.add_argument(
        "--input", required=True, type=Path,
        help="input parquet directory containing {customer,orders,...}.parquet",
    )
    p.add_argument(
        "--output", required=True, type=Path,
        help="output directory for enhanced parquet (will be created)",
    )
    p.add_argument(
        "--constraints", required=True, type=Path,
        help="csv file describing PK/FK constraints",
    )
    p.add_argument(
        "--meta-out", type=Path, default=None,
        help="path for output bitmap_join_meta.json "
             "(default: <output>/bitmap_join_meta.json)",
    )
    p.add_argument(
        "--force", action="store_true",
        help="overwrite existing rowid/_ref columns",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="print plan only; do not write files",
    )
    p.add_argument(
        "--threads", type=int, default=0,
        help="duckdb threads (0 = default)",
    )
    p.add_argument(
        "-v", "--verbose", action="store_true",
        help="enable debug logging",
    )
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    _setup_logger(args.verbose)

    input_dir: Path = args.input.resolve()
    output_dir: Path = args.output.resolve()
    constraints_path: Path = args.constraints.resolve()
    meta_path: Path = (
        args.meta_out.resolve()
        if args.meta_out is not None
        else output_dir / "bitmap_join_meta.json"
    )

    if not input_dir.is_dir():
        logger.error("input dir not found: %s", input_dir)
        return 2
    if not constraints_path.is_file():
        logger.error("constraints file not found: %s", constraints_path)
        return 2

    constraints = load_constraints(constraints_path)
    logger.info(
        "loaded %d constraints from %s", len(constraints), constraints_path
    )
    for c in constraints:
        logger.debug("  %s.%s <- %s.%s",
                     c.pk_table, c.pk_col, c.fk_table, c.fk_col)

    output_dir.mkdir(parents=True, exist_ok=True)

    pre = BitmapPreprocessor(
        input_dir=input_dir,
        output_dir=output_dir,
        constraints=constraints,
        force=args.force,
        dry_run=args.dry_run,
        threads=args.threads,
    )
    t0 = time.time()
    pk_bindings, fk_bindings = pre.run()
    logger.info("preprocess done in %.2fs", time.time() - t0)

    if not args.dry_run:
        write_meta_json(
            pk_bindings=pk_bindings,
            fk_bindings=fk_bindings,
            meta_path=meta_path,
            input_dir=input_dir,
            output_dir=output_dir,
        )
    else:
        logger.info("[dry-run] would write meta json to %s", meta_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
