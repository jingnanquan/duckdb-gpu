//===----------------------------------------------------------------------===//
//
// duckdb/execution/unchained_hash_table.hpp
//
// Unchained Hash Table —— 复现 DaMoN'24
//   "Simple, Efficient, and Robust Hash Tables for Join Processing"
//   (Michael Kaufmann, Amir Shaikhha, Dan Olteanu, et al.)
//
//===----------------------------------------------------------------------===//
//
// 与 DuckDB 默认 JoinHashTable 的本质区别
// ------------------------------------
// DuckDB 默认实现是「链式线性探测」:
//   * 一张 pointer table, 每项是 8 字节 ht_entry_t (高 16 位 salt + 低 48 位行指针);
//   * 所有 hash 落入同一桶的行用行内 8 字节字段 (pointer_offset) 串成单链表;
//   * probe 时先线性探测找到桶, 再沿链逐跳比较, 每一跳都是一次随机访存。
//
// 论文的做法是「无链 + 桶排序 + 寄存器阻塞 Bloom 过滤器」:
//   * directory: 每个 hash 前缀 (slot) 一项 8 字节, 高 48 位存该 slot 在 adjacency
//     区域的**上界**, 低 16 位存一个 4-bit 阻塞 Bloom tag;
//   * adjacency: 所有行按 slot 排序后**连续**存放, slot 的区间 = [directory[s-1], directory[s]);
//   * probe 时先做一次 andn 的 Bloom 判定, 再顺序扫描一段连续内存 —— 无链、无随机访存。
//
// 构建分 3 步 (论文 §3.3 / Fig.10):
//   1) Counting  : 统计每个 slot 的行数, 同时把 Bloom tag OR 进去 (无同步);
//   2) Prefix sum: 把计数变成 adjacency 下标, 每个 slot 区间化;
//   3) Copy      : 把行指针 + hash 写进 adjacency, 同样无同步。
//
// 与论文不一致、需要在报告里说明的 4 处适配
// ------------------------------------------
//   (A) 分区复用 DuckDB 自己的 radix 分区, 因此 slot 取 hash 的 bit [48-K, 47]
//       (论文取最高 K 位)。原因是 DuckDB 的 RadixPartitioning::Shift(radix_bits)
//       = 48 - radix_bits, 即分区号恰好是 bit [48-R, 47]; 这样
//       partition == slot >> (K - R), 分区与 slot 区间天然对齐, 可以直接复用
//       sink_collection 的物理分区做零同步并行, 不需要自己再拷贝一遍行。
//       代价是 hash 的最高 16 位被浪费 (DuckDB 自己留作 salt)。
//   (B) Bloom tag 的取样位从论文的「低 32 位的最高 11 位」改为 hash 的最低 11 位
//       bit [0, 10]。因为 (A) 之后 slot 占了 bit [48-K, 47], 必须让 tag 位与 slot
//       位不相交, 否则同一 slot 内所有 tag 相同、过滤器完全失效。
//       这要求 K < 38, 因此 MAX_SLOT_BITS = 34 (directory 最大 16G 项 / 128GB)。
//   (C) adjacency 存的是 {row_ptr, hash} 16 字节, 而不是把整个 tuple 拷进来。
//       DuckDB 的 build 侧行已经物化在 TupleDataCollection 里, 再拷一遍代价太大;
//       存 row_ptr (8B) 就够 Gather。额外 8B 的 hash 让 Bloom 判定命中后可以先
//       比 hash 再决定是否要解引用行(随机访存), 这是论文的 Bloom 过滤器在
//       「行不连续」场景下等价的替代品。
//       副作用: 总内存 = 9n (directory) + 16n (adjacency) = 25n, 略高于 DuckDB 的
//       16n (capacity = 2n 的 8 字节 pointer table)。
//   (D) 并行度上限 = DuckDB 的 radix 分区数 (INITIAL_RADIX_BITS = 4, 即 16),
//       论文是自己分区所以可以随便选 R。这里刻意不去改 INITIAL_RADIX_BITS,
//       否则 build 侧的 sink 代价会变, 和其它路径的对比就不公平了。
//
// 与执行器的集成方式
// ------------------
// 本类只负责「查表结构」本身: 构建 (Build) 与探针 (GetRowPointers / AdvancePointers)。
// ScanStructure 的结果收集、谓词解析、各 join 类型语义全部复用 DuckDB 原有代码 ——
// JoinHashTable::Probe 与 ScanStructure::AdvancePointers 在 unchained 打开时分派到
// 这里, 其余 (NextInnerJoin / NextLeftJoin / ScanFullOuter / ...) 一行都不改。
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/types/vector.hpp"
// 单向依赖: join_hashtable.hpp 只前向声明 UnchainedHashTable, 不 include 本文件
#include "duckdb/execution/join_hashtable.hpp"

#include <array>

namespace duckdb {

//===--------------------------------------------------------------------===//
// directory 表项: 高 48 位 = adjacency 下标(该 slot 区间的上界), 低 16 位 = tag
//===--------------------------------------------------------------------===//
struct unchained_entry_t {
	static constexpr const uint64_t OFFSET_SHIFT = 16;
	static constexpr const uint64_t TAG_MASK = 0x000000000000FFFFULL;
	//! 48 位下标, 支持 2^48 条 adjacency 记录
	static constexpr const uint64_t MAX_OFFSET = 0x0000FFFFFFFFFFFFULL;

	static inline uint64_t ExtractOffset(const uint64_t entry) {
		return entry >> OFFSET_SHIFT;
	}
	static inline uint16_t ExtractTag(const uint64_t entry) {
		return static_cast<uint16_t>(entry & TAG_MASK);
	}
	static inline uint64_t IncrementOffset(const uint64_t entry, const uint64_t by) {
		// 只动高 48 位, tag 保持不变
		return entry + (by << OFFSET_SHIFT);
	}
};

//===--------------------------------------------------------------------===//
// adjacency 表项
//===--------------------------------------------------------------------===//
struct unchained_adjacency_t {
	data_ptr_t row;
	hash_t hash;
};

//===--------------------------------------------------------------------===//
// UnchainedHashTable
//===--------------------------------------------------------------------===//
class UnchainedHashTable {
public:
	//=== 论文参数 ===//
	//! directory 大小 = 1.125 * n (论文 §3.1 的 m = 1.125n), 再向上取到 2 的幂
	static constexpr double SLOT_LOAD_FACTOR = 1.125;
	//! m 的下界 (和 DuckDB 的 MINIMUM_CAPACITY 保持一致)
	static constexpr idx_t MIN_SLOT_BITS = 14;
	//! m 的上界: 见适配说明 (B), 必须让 slot 位 [48-K, 47] 与 tag 位 [0,10] 不相交
	static constexpr idx_t MAX_SLOT_BITS = 34;
	//! Bloom tag: 4 bit of 16 -> C(16,4) = 1820, 补到 2^11 = 2048 项
	static constexpr idx_t TAG_INDEX_BITS = 11;
	static constexpr idx_t TAG_TABLE_SIZE = idx_t(1) << TAG_INDEX_BITS;
	static constexpr idx_t TAG_INDEX_MASK = TAG_TABLE_SIZE - 1;
	static constexpr uint16_t TAG_FULL = 0xFFFF;

	UnchainedHashTable();
	~UnchainedHashTable();

	//! m = 1.125 * count 向上取到 2 的幂, 返回其位数 (夹紧到 [MIN_SLOT_BITS, MAX_SLOT_BITS])
	static idx_t SlotBitsForCount(idx_t count);

	//===------------------------------------------------------------------===//
	// Build
	//===------------------------------------------------------------------===//
	//! 从 JoinHashTable 的 radix 分区构建 directory + adjacency。
	//! 必须在 ht.Unpartition() **之前** 调用(需要 sink_collection 的物理分区);
	//! TupleDataCollection::Combine 只是搬移 segment 指针, 行地址在随后的
	//! Unpartition() 之后依然有效。
	//! partition_offsets[p] = 前 p 个分区的行数 (size = 分区数 + 1)。
	void Build(JoinHashTable &ht, const vector<idx_t> &partition_offsets, idx_t num_threads);

	bool IsBuilt() const {
		return built;
	}
	idx_t Count() const {
		return count;
	}
	idx_t SlotCount() const {
		return slot_count;
	}
	idx_t SlotBits() const {
		return slot_bits;
	}
	idx_t MaxSlotCount() const {
		return max_slot_count;
	}
	idx_t SizeInBytes() const {
		return directory_size + adjacency_size;
	}
	idx_t DirectorySizeInBytes() const {
		return directory_size;
	}
	idx_t AdjacencySizeInBytes() const {
		return adjacency_size;
	}

	//===------------------------------------------------------------------===//
	// Probe
	//===------------------------------------------------------------------===//
	inline idx_t Slot(const hash_t hash) const {
		return (hash >> slot_shift) & slot_mask;
	}
	//! 论文的 5 条指令 andn 判定: !(tag & ~entry_tag)
	inline bool CouldContain(const uint64_t entry, const hash_t hash) const {
		const auto tag = tags[hash & TAG_INDEX_MASK];
		return !(tag & ~unchained_entry_t::ExtractTag(entry));
	}

	//! 探针入口: 填充 ScanStructure 的 pointers / sel_vector / count, 并建立游标
	void GetRowPointers(JoinHashTable::ScanStructure &scan_structure, const SelectionVector *sel, idx_t count,
	                    Vector &hashes_v, Vector &pointers_result_v, SelectionVector &match_sel);
	//! 推进游标到同一 slot 内下一条 hash 相同的记录 (对应 ScanStructure::AdvancePointers)
	void AdvancePointers(JoinHashTable::ScanStructure &scan_structure, const SelectionVector &sel, idx_t sel_count);

	//! 生成 tag 表 (C(16,4) = 1820 个「16 位里恰好 4 位为 1」的模式, 补齐到 2048)
	static const uint16_t *TagTable();

private:
	//! Counting: 统计每个 slot 的行数并 OR 进 Bloom tag
	void CountPhase(JoinHashTable &ht, idx_t partition_idx_from, idx_t partition_idx_to);
	//! Prefix sum: 计数 -> adjacency 下标
	void PrefixSumPhase(idx_t partition_idx_from, idx_t partition_idx_to);
	//! Copy: 写 adjacency
	void CopyPhase(JoinHashTable &ht, idx_t partition_idx_from, idx_t partition_idx_to);
	//! 检测同一 slot 内是否有两条 hash 相同的记录 (决定是否需要精确的 key 比较)
	void DetectDuplicateHashes(idx_t partition_idx_from, idx_t partition_idx_to);

private:
	bool built = false;
	idx_t count = 0;
	idx_t slot_bits = 0;
	idx_t slot_count = 0;
	uint64_t slot_shift = 0;
	uint64_t slot_mask = 0;
	idx_t max_slot_count = 0;
	//! 同一 slot 内是否出现过两条 hash 相同的记录
	atomic<bool> has_duplicate_hashes {false};

	AllocatedData directory_data;
	AllocatedData adjacency_data;
	//! 指向 directory_data 的第 1 项, 因此 directory[-1] 合法 (= adjacency 起点 0)
	uint64_t *directory = nullptr;
	unchained_adjacency_t *adjacency = nullptr;
	idx_t directory_size = 0;
	idx_t adjacency_size = 0;

	const uint16_t *tags = nullptr;

	//! 构建期临时: 每个分区的起始 adjacency 下标
	vector<idx_t> partition_starts;
	//! 构建期临时: 每个分区内 slot 的最大记录数
	vector<idx_t> partition_max_counts;
	idx_t radix_bits = 0;
	idx_t partition_count = 0;
};

} // namespace duckdb
