埋点与输出
scan（物化 chunk）/ build（多线程 sink+combine）/ finalize / probe 四段 wall-clock，另给 join_only（不含 scan）便于横向对比；同时输出 build/probe/matched 行数、probe 吞吐、BHJ 的 payload 模式与 bitmap 大小。正确性用独立算出的期望行数 + 跨算法 payload 校验和双重校验。

可外部自定义
key 形态：UNIQUE_DENSE（PK/rowid）、UNIQUE_SPARSE（宽域）、DUPLICATE_KEYS（重复 key）、HASH_COLLISION（重复 hash）、CONSTANT_KEY、RUNS_OF_SAME_KEY
重复 hash是真构造出来的：DuckDB 的 MurmurHash64 在 mod 2⁶⁴ 上可逆，框架求逆后把目标 hash 写成 [16位salt][组内序号][桶号]，于是同组 key 的 hash & bitmask 与 salt 都相同 → 线性探测链最长
规模：build_rows / probe_rows / chunk_size / pk_row_count（PK 域大小，控制 bitmap 密度）
命中行为：probe_hit_ratio、probe_hot_fraction（倾斜）
并发：build_threads / probe_threads 各自独立（每个线程一段连续 chunk）
其它：key 类型、payload 列数、key_offset（1-based PK）、verify
运行
bash
./build/release/test/unittest "Join micro-bench: *"      # 全部场景
./build/release/test/unittest "Join micro-bench: dense unique PK (hash vs perfect vs bitmap)"
用例带 [.]，默认 make unittest 不跑。

实测（build 500K / probe 8M / 4+4 线程）
code
hash-join:         scan=188.68 build=9.64 finalize=12.61 probe=99.02 join_only=121.27 ms
perfect-hash-join: scan=188.68 build=8.79 finalize=27.11 probe=22.93 join_only=58.84  ms
bitmap-join:       scan=188.68 build=4.03 finalize=0.00  probe=27.67 join_only=31.69  ms
线程扩展性（1M build / 4M probe）：hash probe 161ms(1线程) → 50ms(8线程)；BHJ probe 21.6ms → 7.8ms。