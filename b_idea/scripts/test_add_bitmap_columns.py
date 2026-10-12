#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_add_bitmap_columns.py
==========================

针对 add_bitmap_columns.py 的最小可复现自检脚本。

不依赖 TPC-H 数据，构造两组 parquet 表：

A) 稠密 1 起整数 PK（模拟 TPC-H customer / orders）：
   - dim_customer(c_custkey BIGINT PRIMARY KEY=1..N, c_name)
   - fact_orders(o_orderkey, o_custkey)
   断言：
     - dim_customer.parquet 不会新增 _rowid（复用 c_custkey，offset=1）
     - fact_orders.parquet 新增 o_custkey_ref = o_custkey - 1
     - meta 中 rowid_column == "c_custkey", rowid_offset == 1

B) 字符串 PK（模拟 a_idea 中 teacher.name）：
   - dim_teacher(t_name VARCHAR PRIMARY KEY, dept)
   - fact_course(c_id, t_name)
   断言：
     - dim_teacher.parquet 新增 _rowid 列（0 起递增）
     - fact_course.parquet 新增 t_name_ref，值与 PK 表 _rowid 对齐
     - 越界 / NULL 行为正确（NULL→NULL）

运行：
    python b_idea/scripts/test_add_bitmap_columns.py
退出码 0 表示通过。
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

try:
    import duckdb  # type: ignore
except ImportError:
    sys.stderr.write(
        "[FATAL] python `duckdb` package not found in: " + sys.executable + "\n"
        "  Install it for the current interpreter, e.g.:\n"
        "    " + sys.executable + " -m pip install duckdb\n"
        "  Or, if you have a dedicated python (e.g. /root/.local/python-3.11.10):\n"
        "    /root/.local/python-3.11.10/bin/pip3.11 install duckdb\n"
        "  Then re-run this script with that interpreter.\n"
    )
    raise

HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "add_bitmap_columns.py"


def _build_case_a(work: Path) -> Path:
    """稠密整数 PK 用例。返回 input dir。"""
    in_dir = work / "case_a_in"
    in_dir.mkdir(parents=True)
    con = duckdb.connect()

    # dim_customer: c_custkey 1..1000 稠密
    con.execute(
        f"""
        COPY (
          SELECT
            CAST(i AS BIGINT) AS c_custkey,
            'name_' || i      AS c_name
          FROM range(1, 1001) t(i)
        ) TO '{in_dir}/dim_customer.parquet' (FORMAT PARQUET);
        """
    )

    # fact_orders: o_custkey 在 1..1000 之间，外加少量 NULL
    con.execute(
        f"""
        COPY (
          SELECT
            CAST(i AS BIGINT) AS o_orderkey,
            CASE WHEN i % 137 = 0 THEN NULL
                 ELSE CAST(((i * 7) % 1000) + 1 AS BIGINT) END AS o_custkey
          FROM range(1, 5001) t(i)
        ) TO '{in_dir}/fact_orders.parquet' (FORMAT PARQUET);
        """
    )
    con.close()
    return in_dir


def _build_case_b(work: Path) -> Path:
    """字符串 PK 用例。返回 input dir。"""
    in_dir = work / "case_b_in"
    in_dir.mkdir(parents=True)
    con = duckdb.connect()

    # dim_teacher: t_name 字符串 PK，乱序
    con.execute(
        f"""
        COPY (
          SELECT
            'teacher_' || lpad(CAST(i AS VARCHAR), 4, '0') AS t_name,
            'dept_' || (i % 7) AS dept
          FROM range(0, 200) t(i)
          ORDER BY hash(i)
        ) TO '{in_dir}/dim_teacher.parquet' (FORMAT PARQUET);
        """
    )

    # fact_course: 引用 dim_teacher.t_name；混入 NULL
    con.execute(
        f"""
        COPY (
          SELECT
            CAST(i AS BIGINT) AS c_id,
            CASE WHEN i % 50 = 0 THEN NULL
                 ELSE 'teacher_' || lpad(CAST(((i * 11) % 200) AS VARCHAR), 4, '0')
            END AS t_name
          FROM range(0, 1000) t(i)
        ) TO '{in_dir}/fact_course.parquet' (FORMAT PARQUET);
        """
    )
    con.close()
    return in_dir


def _write_constraints(in_dir: Path, mapping: list) -> Path:
    p = in_dir.parent / f"{in_dir.name}_constraints.csv"
    with p.open("w", encoding="utf-8") as f:
        f.write("pk_table,pk_col,fk_table,fk_col\n")
        for row in mapping:
            f.write(",".join(row) + "\n")
    return p


def _run(cmd: list) -> None:
    print("[exec]", " ".join(map(str, cmd)))
    subprocess.run(cmd, check=True)


def _check(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(f"[FAIL] {msg}")
    print(f"[OK]   {msg}")


def case_a(work: Path) -> None:
    in_dir = _build_case_a(work)
    out_dir = work / "case_a_out"
    constraints = _write_constraints(
        in_dir, [("dim_customer", "c_custkey", "fact_orders", "o_custkey")]
    )

    _run([
        sys.executable, str(SCRIPT),
        "--input", str(in_dir),
        "--output", str(out_dir),
        "--constraints", str(constraints),
        "-v",
    ])

    con = duckdb.connect()
    cust_cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{out_dir}/dim_customer.parquet')"
    ).fetchall()]
    _check("_rowid" not in cust_cols,
           "dim_customer should NOT add _rowid (dense pk reused)")
    _check({"c_custkey", "c_name"}.issubset(set(cust_cols)),
           "dim_customer columns preserved")

    ord_cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{out_dir}/fact_orders.parquet')"
    ).fetchall()]
    _check("o_custkey_ref" in ord_cols, "fact_orders has o_custkey_ref")

    # ref = custkey - 1 处校验
    bad = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out_dir}/fact_orders.parquet') "
        f"WHERE o_custkey IS NOT NULL "
        f"  AND o_custkey_ref IS DISTINCT FROM CAST(o_custkey - 1 AS UBIGINT)"
    ).fetchone()[0]
    _check(bad == 0, "fact_orders.o_custkey_ref == o_custkey - 1")

    null_match = con.execute(
        f"SELECT "
        f"  COUNT(*) FILTER (WHERE o_custkey IS NULL AND o_custkey_ref IS NOT NULL), "
        f"  COUNT(*) FILTER (WHERE o_custkey IS NOT NULL AND o_custkey_ref IS NULL) "
        f"FROM read_parquet('{out_dir}/fact_orders.parquet')"
    ).fetchone()
    _check(null_match[0] == 0 and null_match[1] == 0,
           "fact_orders NULL alignment between o_custkey and o_custkey_ref")

    # meta
    meta = json.loads((out_dir / "bitmap_join_meta.json").read_text())
    pk = meta["pk_bindings"][0]
    _check(pk["rowid_column"] == "c_custkey", "meta: rowid_column = c_custkey")
    _check(pk["rowid_offset"] == 1, "meta: rowid_offset = 1")
    _check(pk["row_count"] == 1000, "meta: row_count = 1000")

    fk = meta["fk_bindings"][0]
    _check(fk["ref_column"] == "o_custkey_ref", "meta: ref_column = o_custkey_ref")
    con.close()


def case_b(work: Path) -> None:
    in_dir = _build_case_b(work)
    out_dir = work / "case_b_out"
    constraints = _write_constraints(
        in_dir, [("dim_teacher", "t_name", "fact_course", "t_name")]
    )

    _run([
        sys.executable, str(SCRIPT),
        "--input", str(in_dir),
        "--output", str(out_dir),
        "--constraints", str(constraints),
        "-v",
    ])

    con = duckdb.connect()
    teach_cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{out_dir}/dim_teacher.parquet')"
    ).fetchall()]
    _check("_rowid" in teach_cols, "dim_teacher should add _rowid (string pk)")

    rid_check = con.execute(
        f"SELECT MIN(_rowid), MAX(_rowid), COUNT(*), COUNT(DISTINCT _rowid) "
        f"FROM read_parquet('{out_dir}/dim_teacher.parquet')"
    ).fetchone()
    _check(
        rid_check[0] == 0 and rid_check[1] == 199 and rid_check[2] == 200
        and rid_check[3] == 200,
        "dim_teacher _rowid is 0..199 dense unique",
    )

    course_cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{out_dir}/fact_course.parquet')"
    ).fetchall()]
    _check("t_name_ref" in course_cols, "fact_course has t_name_ref")

    # 一致性校验：ref 指向的 _rowid 在 teacher 里 t_name 必须等于 course.t_name
    bad = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out_dir}/fact_course.parquet') c "
        f"LEFT JOIN read_parquet('{out_dir}/dim_teacher.parquet') t "
        f"  ON c.t_name_ref = t._rowid "
        f"WHERE c.t_name IS NOT NULL "
        f"  AND (t.t_name IS NULL OR t.t_name <> c.t_name)"
    ).fetchone()[0]
    _check(bad == 0, "fact_course.t_name_ref correctly points to dim_teacher._rowid")

    null_align = con.execute(
        f"SELECT "
        f"  COUNT(*) FILTER (WHERE t_name IS NULL AND t_name_ref IS NOT NULL), "
        f"  COUNT(*) FILTER (WHERE t_name IS NOT NULL AND t_name_ref IS NULL) "
        f"FROM read_parquet('{out_dir}/fact_course.parquet')"
    ).fetchone()
    _check(null_align[0] == 0 and null_align[1] == 0,
           "fact_course NULL alignment between t_name and t_name_ref")

    meta = json.loads((out_dir / "bitmap_join_meta.json").read_text())
    pk = meta["pk_bindings"][0]
    _check(pk["rowid_column"] == "_rowid", "meta: rowid_column = _rowid")
    _check(pk["rowid_offset"] == 0, "meta: rowid_offset = 0")
    _check(pk["row_count"] == 200, "meta: row_count = 200")
    con.close()


def case_idempotent(work: Path) -> None:
    """重复运行不应失败，且不引入额外列。"""
    in_dir = _build_case_a(work / "idem")
    out_dir = work / "idem_out"
    constraints = _write_constraints(
        in_dir, [("dim_customer", "c_custkey", "fact_orders", "o_custkey")]
    )

    cmd = [
        sys.executable, str(SCRIPT),
        "--input", str(in_dir),
        "--output", str(out_dir),
        "--constraints", str(constraints),
    ]
    _run(cmd)
    _run(cmd)  # 第二次

    con = duckdb.connect()
    cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{out_dir}/fact_orders.parquet')"
    ).fetchall()]
    _check(cols.count("o_custkey_ref") == 1,
           "idempotent: o_custkey_ref appears exactly once after rerun")
    con.close()


def main() -> int:
    work = Path(tempfile.mkdtemp(prefix="bhj_test_"))
    print(f"[test] workdir = {work}")
    try:
        case_a(work)
        case_b(work)
        case_idempotent(work)
        print("\n[ALL PASS]")
        return 0
    finally:
        if os.environ.get("BHJ_KEEP_WORKDIR") != "1":
            shutil.rmtree(work, ignore_errors=True)
            print(f"[test] cleaned {work}")
        else:
            print(f"[test] kept workdir at {work}")


if __name__ == "__main__":
    sys.exit(main())
