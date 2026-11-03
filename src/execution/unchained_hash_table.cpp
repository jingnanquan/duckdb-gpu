#include "duckdb/execution/unchained_hash_table.hpp"

#include "duckdb/common/row_operations/row_matcher.hpp"
#include "duckdb/common/types/row/partitioned_tuple_data.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"
#include "duckdb/execution/join_hashtable.hpp"

#include <algorithm>
#include <thread>

namespace duckdb {

//===--------------------------------------------------------------------===//
// 每 probe 行的扫描游标
//===--------------------------------------------------------------------===//
UnchainedScanState::UnchainedScanState()
    : cursors(make_unsafe_uniq_array_uninitialized<idx_t>(STANDARD_VECTOR_SIZE)),
      ends(make_unsafe_uniq_array_uninitialized<idx_t>(STANDARD_VECTOR_SIZE)),
      hashes(make_unsafe_uniq_array_uninitialized<hash_t>(STANDARD_VECTOR_SIZE)) {
}

//===--------------------------------------------------------------------===//
// 4-bit 阻塞 Bloom 过滤器的 tag 表
//===--------------------------------------------------------------------===//
const uint16_t *UnchainedHashTable::TagTable() {
	// C(16,4) = 1820 个「16 位里恰好 4 位为 1」的模式, 补齐到 2048 项 (论文 §3.2 / Fig.7)
	static const std::array<uint16_t, TAG_TABLE_SIZE> table = []() -> std::array<uint16_t, TAG_TABLE_SIZE> {
		static constexpr idx_t VALID_TAG_COUNT = 1820;
		std::array<uint16_t, TAG_TABLE_SIZE> result {};
		idx_t found = 0;
		for (uint32_t candidate = 0; candidate < 0x10000 && found < VALID_TAG_COUNT; candidate++) {
			// 手写 popcount, 避免依赖编译器内建
			uint32_t bits = 0;
			for (uint32_t v = candidate; v != 0; v >>= 1) {
				bits += v & 1;
			}
			if (bits == 4) {
				result[found++] = static_cast<uint16_t>(candidate);
			}
		}
		// 补齐: 循环复用合法模式, 保证 padding 也不会退化成 "全放行"
		for (idx_t i = found; i < TAG_TABLE_SIZE; i++) {
			result[i] = result[i % VALID_TAG_COUNT];
		}
		return result;
	}();
	return table.data();
}

UnchainedHashTable::UnchainedHashTable() : tags(TagTable()) {
}

UnchainedHashTable::~UnchainedHashTable() {
}

idx_t UnchainedHashTable::SlotBitsForCount(idx_t count) {
	const auto target_slots = static_cast<idx_t>(static_cast<double>(count) * SLOT_LOAD_FACTOR) + 1;
	idx_t bits = MIN_SLOT_BITS;
	while (bits < MAX_SLOT_BITS && (idx_t(1) << bits) < target_slots) {
		bits++;
	}
	return bits;
}

//===--------------------------------------------------------------------===//
// 并行: 把 [0, partition_count) 切成 num_threads 段
//===--------------------------------------------------------------------===//
namespace {
template <class FUNC>
void RunOnPartitions(idx_t partition_count, idx_t num_threads, FUNC &&func) {
	if (partition_count == 0) {
		return;
	}
	const auto thread_count = MaxValue<idx_t>(MinValue<idx_t>(num_threads, partition_count), 1);
	if (thread_count == 1) {
		func(0, partition_count);
		return;
	}
	vector<std::thread> threads;
	threads.reserve(thread_count);
	const idx_t per_thread = (partition_count + thread_count - 1) / thread_count;
	for (idx_t t = 0; t < thread_count; t++) {
		const auto from = MinValue<idx_t>(t * per_thread, partition_count);
		const auto to = MinValue<idx_t>(from + per_thread, partition_count);
		threads.emplace_back([&func, from, to]() { func(from, to); });
	}
	for (auto &thread : threads) {
		thread.join();
	}
}
} // namespace

//===--------------------------------------------------------------------===//
// Build
//===--------------------------------------------------------------------===//
void UnchainedHashTable::Build(JoinHashTable &ht, const vector<idx_t> &partition_offsets, idx_t num_threads) {
	D_ASSERT(!built);
	auto &sink_collection = ht.GetSinkCollection();
	auto &partitions = sink_collection.GetPartitions();

	count = sink_collection.Count();
	radix_bits = ht.GetRadixBits();
	partition_count = partitions.size();

	if (count == 0 || partition_count == 0 || partition_offsets.size() != partition_count + 1) {
		built = true;
		return;
	}
	D_ASSERT(radix_bits <= MIN_SLOT_BITS);

	//=== directory 大小: m = 1.125 * n, 向上取到 2 的幂 (论文 §3.1) ===//
	slot_bits = SlotBitsForCount(count);
	slot_count = idx_t(1) << slot_bits;
	slot_shift = 48 - slot_bits;
	slot_mask = slot_count - 1;

	//=== 分配 ===//
	auto &allocator = ht.buffer_manager.GetBufferAllocator();
	directory_size = (slot_count + 1) * sizeof(uint64_t);
	adjacency_size = count * sizeof(unchained_adjacency_t);
	directory_data = allocator.Allocate(directory_size);
	adjacency_data = allocator.Allocate(adjacency_size);

	// directory[-1] 必须合法且为 0 (slot 0 的区间下界)
	memset(directory_data.get(), 0, directory_size);
	directory = reinterpret_cast<uint64_t *>(directory_data.get()) + 1;
	adjacency = reinterpret_cast<unchained_adjacency_t *>(adjacency_data.get());

	// 分区 p 的 adjacency 起点 = 它的行起点 (分区按 slot 递增排列, 且 slot 区间互不重叠)
	partition_starts = partition_offsets;

	//=== 1) Counting (论文 Fig.10 第一步) ===//
	RunOnPartitions(partition_count, num_threads, [&](idx_t from, idx_t to) { CountPhase(ht, from, to); });

	//=== 2) Prefix sum (论文 Fig.10 第二步) ===//
	partition_max_counts.resize(partition_count, 0);
	RunOnPartitions(partition_count, num_threads, [&](idx_t from, idx_t to) { PrefixSumPhase(from, to); });

	//=== 3) Copy (论文 Fig.10 第三步) ===//
	RunOnPartitions(partition_count, num_threads, [&](idx_t from, idx_t to) { CopyPhase(ht, from, to); });

	//=== 附加: 检测同一 slot 内是否存在「hash 相同但 key 可能不同」的记录 ===//
	// 论文的 probe 是 cur->key == key; 这里用「64 位 hash 相等」代替 key 比较,
	// 只有真正发生 64 位 hash 碰撞时才会出错。为了不放弃这个优化, 这里扫一遍:
	// 只要某个 slot 内有两条记录的 hash 相同, 就退回用 RowMatcher 做精确的 key 比较。
	RunOnPartitions(partition_count, num_threads, [&](idx_t from, idx_t to) { DetectDuplicateHashes(from, to); });

	max_slot_count = 0;
	for (auto slot_max : partition_max_counts) {
		max_slot_count = MaxValue<idx_t>(max_slot_count, slot_max);
	}

	//=== 与 DuckDB ScanStructure 对接的两个标志位 ===//
	// chains_longer_than_one 在 DuckDB 里的含义是"一条链上可能有多条记录, 所以需要
	// NextInnerJoin 的 compaction 慢路径"。对 unchained 来说, 一条 probe 行最多能产出
	// 多少条结果取决于「同一 slot 里有多少条 hash 相同的记录」—— 这正是
	// has_duplicate_hashes (重复 key 必然落在这里)。没有重复 hash 时每条 probe 行最多
	// 一条结果, 可以放心走 NextInnerJoin 的 fast path。
	ht.chains_longer_than_one = has_duplicate_hashes;
	// needs_chain_matcher: 只有可能出错(hash 碰撞)时才打开精确比较
	if (has_duplicate_hashes && !ht.row_matcher_probe) {
		ht.row_matcher_probe = unique_ptr<RowMatcher>(new RowMatcher());
		ht.row_matcher_probe->Initialize(false, *ht.layout_ptr, ht.equality_predicates, ht.equality_predicate_columns);
		ht.row_matcher_probe_no_match_sel = unique_ptr<RowMatcher>(new RowMatcher());
		ht.row_matcher_probe_no_match_sel->Initialize(true, *ht.layout_ptr, ht.equality_predicates,
		                                              ht.equality_predicate_columns);
	}
	ht.needs_chain_matcher = ht.needs_chain_matcher || has_duplicate_hashes;

	built = true;
}

void UnchainedHashTable::CountPhase(JoinHashTable &ht, idx_t partition_idx_from, idx_t partition_idx_to) {
	auto &partitions = ht.GetSinkCollection().GetPartitions();
	const auto pointer_offset = ht.pointer_offset;
	const auto shift = slot_shift;
	const auto mask = slot_mask;
	const uint16_t *tag_table = tags;

	for (idx_t p = partition_idx_from; p < partition_idx_to; p++) {
		auto &collection = *partitions[p];
		if (collection.Count() == 0) {
			continue;
		}
		TupleDataChunkIterator iterator(collection, TupleDataPinProperties::KEEP_EVERYTHING_PINNED, false);
		const auto row_locations = iterator.GetRowLocations();
		do {
			const auto row_count = iterator.GetCurrentChunkCount();
			for (idx_t i = 0; i < row_count; i++) {
				const auto hash = Load<hash_t>(row_locations[i] + pointer_offset);
				const auto slot = (hash >> shift) & mask;
				const auto tag = tag_table[hash & TAG_INDEX_MASK];
				// directory[slot] += 1 << 16;  directory[slot] |= tag;   (论文 Fig.10)
				auto &entry = directory[slot];
				entry = unchained_entry_t::IncrementOffset(entry, 1);
				entry |= tag;
			}
		} while (iterator.Next());
	}
}

void UnchainedHashTable::PrefixSumPhase(idx_t partition_idx_from, idx_t partition_idx_to) {
	// 分区 p 覆盖 slot [p << (slot_bits - radix_bits), (p+1) << ...)  (论文 Fig.10 的 start/end)
	const idx_t slots_per_partition = slot_count >> radix_bits;
	D_ASSERT(slots_per_partition > 0);

	for (idx_t p = partition_idx_from; p < partition_idx_to; p++) {
		const idx_t slot_begin = p * slots_per_partition;
		const idx_t slot_end = (p + 1) * slots_per_partition;
		idx_t current = partition_starts[p];
		idx_t local_max = 0;
		for (idx_t slot = slot_begin; slot < slot_end; slot++) {
			const auto entry = directory[slot];
			const auto slot_count_here = unchained_entry_t::ExtractOffset(entry);
			if (slot_count_here > local_max) {
				local_max = slot_count_here;
			}
			directory[slot] = (current << unchained_entry_t::OFFSET_SHIFT) | unchained_entry_t::ExtractTag(entry);
			current += slot_count_here;
		}
		partition_max_counts[p] = local_max;
	}
}

void UnchainedHashTable::CopyPhase(JoinHashTable &ht, idx_t partition_idx_from, idx_t partition_idx_to) {
	auto &partitions = ht.GetSinkCollection().GetPartitions();
	const auto pointer_offset = ht.pointer_offset;
	const auto shift = slot_shift;
	const auto mask = slot_mask;

	for (idx_t p = partition_idx_from; p < partition_idx_to; p++) {
		auto &collection = *partitions[p];
		if (collection.Count() == 0) {
			continue;
		}
		TupleDataChunkIterator iterator(collection, TupleDataPinProperties::KEEP_EVERYTHING_PINNED, false);
		const auto row_locations = iterator.GetRowLocations();
		do {
			const auto row_count = iterator.GetCurrentChunkCount();
			for (idx_t i = 0; i < row_count; i++) {
				const auto row = row_locations[i];
				const auto hash = Load<hash_t>(row + pointer_offset);
				const auto slot = (hash >> shift) & mask;
				auto &entry = directory[slot];
				const auto target = unchained_entry_t::ExtractOffset(entry);
				entry = unchained_entry_t::IncrementOffset(entry, 1);
				adjacency[target].row = row;
				adjacency[target].hash = hash;
			}
		} while (iterator.Next());
	}
}

void UnchainedHashTable::DetectDuplicateHashes(idx_t partition_idx_from, idx_t partition_idx_to) {
	// 一个 slot 内的记录在 adjacency 中是连续的: [directory[slot-1], directory[slot])
	// slot 一般只有 1~几条记录, 所以两两比较的期望代价是 O(1); 极端倾斜时超过阈值就直接
	// 保守地认为"有重复 hash", 退回精确比较 (牺牲性能换正确性)。
	static constexpr idx_t DUP_SCAN_LIMIT = 32;
	const idx_t slots_per_partition = slot_count >> radix_bits;

	for (idx_t p = partition_idx_from; p < partition_idx_to; p++) {
		const idx_t slot_begin = p * slots_per_partition;
		const idx_t slot_end = (p + 1) * slots_per_partition;
		for (idx_t slot = slot_begin; slot < slot_end; slot++) {
			const auto begin = unchained_entry_t::ExtractOffset(directory[slot - 1]);
			const auto end = unchained_entry_t::ExtractOffset(directory[slot]);
			const auto slot_count_here = end - begin;
			if (slot_count_here < 2) {
				continue;
			}
			if (slot_count_here > DUP_SCAN_LIMIT) {
				has_duplicate_hashes = true;
				continue;
			}
			for (idx_t i = begin; i < end && !has_duplicate_hashes; i++) {
				for (idx_t j = i + 1; j < end; j++) {
					if (adjacency[i].hash == adjacency[j].hash) {
						has_duplicate_hashes = true;
						break;
					}
				}
			}
		}
	}
}

//===--------------------------------------------------------------------===//
// Probe
//===--------------------------------------------------------------------===//
void UnchainedHashTable::GetRowPointers(JoinHashTable::ScanStructure &scan_structure, const SelectionVector *sel,
                                        idx_t count, Vector &hashes_v, Vector &pointers_result_v,
                                        SelectionVector &match_sel) {
	if (!scan_structure.unchained_state) {
		scan_structure.unchained_state = make_uniq<UnchainedScanState>();
	}
	auto &state = *scan_structure.unchained_state;
	const auto hash_data = FlatVector::GetData<hash_t>(hashes_v);
	auto pointers = FlatVector::GetData<data_ptr_t>(pointers_result_v);

	idx_t match_count = 0;
	for (idx_t i = 0; i < count; i++) {
		const auto row_index = sel->get_index(i);
		const auto hash = hash_data[i];
		const auto slot = Slot(hash);
		const auto entry = directory[slot];

		// 论文 Fig.6 的 5 条指令 andn 判定; 命中不了就直接跳过整个 slot
		if (!CouldContain(entry, hash)) {
			continue;
		}

		const auto begin = unchained_entry_t::ExtractOffset(directory[slot - 1]);
		const auto end = unchained_entry_t::ExtractOffset(entry);
		state.ends[row_index] = end;
		state.hashes[row_index] = hash;

		idx_t current = begin;
		while (current < end) {
			const auto &candidate = adjacency[current++];
			if (candidate.hash == hash) {
				pointers[row_index] = candidate.row;
				state.cursors[row_index] = current;
				match_sel.set_index(match_count++, row_index);
				break;
			}
		}
	}
	scan_structure.count = match_count;
}

void UnchainedHashTable::AdvancePointers(JoinHashTable::ScanStructure &scan_structure, const SelectionVector &sel,
                                         idx_t sel_count) {
	auto &state = *scan_structure.unchained_state;
	auto pointers = FlatVector::GetData<data_ptr_t>(scan_structure.pointers);

	idx_t new_count = 0;
	for (idx_t i = 0; i < sel_count; i++) {
		const auto row_index = sel.get_index(i);
		auto &current = state.cursors[row_index];
		const auto end = state.ends[row_index];
		const auto hash = state.hashes[row_index];
		while (current < end) {
			const auto &candidate = adjacency[current++];
			if (candidate.hash == hash) {
				pointers[row_index] = candidate.row;
				scan_structure.sel_vector.set_index(new_count++, row_index);
				break;
			}
		}
	}
	scan_structure.count = new_count;
}

void JoinHashTable::ScanStructure::AdvanceUnchainedPointers(const SelectionVector &sel, const idx_t sel_count) {
	ht.unchained->AdvancePointers(*this, sel, sel_count);
}

} // namespace duckdb
