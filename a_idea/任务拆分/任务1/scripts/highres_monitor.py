#!/usr/bin/env python3
"""
高精度系统监控模块 (High-Resolution System Monitor)

通过独立进程从 /proc/stat 和 /proc/diskstats 采集 CPU 和 IO 数据，
采样间隔可低至 1~5ms，远超 iostat/mpstat 的 1s 最小间隔。

用法:
    # 作为模块导入
    from highres_monitor import HighResMonitor
    monitor = HighResMonitor(interval_ms=2)
    monitor.start()
    # ... 执行查询 ...
    monitor.stop()
    monitor.save_csv("output.csv")
    monitor.plot("output.png", title="TPCH Q01")

    # 独立运行测试
    python3 highres_monitor.py --test

依赖:
    pip install matplotlib  (画图用，采集本身无外部依赖)
"""

import os
import sys
import time
import csv
import json
import signal
from multiprocessing import Process, Value, Array
from ctypes import c_double, c_int, c_bool
import struct
import mmap

# ============================================================
# /proc 文件系统解析
# ============================================================

def read_cpu_stats():
    """
    从 /proc/stat 读取 CPU 使用情况
    返回: (total_jiffies, idle_jiffies) 的元组
    
    /proc/stat 第一行格式:
    cpu  user nice system idle iowait irq softirq steal guest guest_nice
    """
    with open('/proc/stat', 'r') as f:
        line = f.readline()  # 只读第一行 (汇总所有 CPU)
    
    parts = line.split()
    # parts[0] = "cpu", parts[1:] = 各时间片
    values = [int(x) for x in parts[1:]]
    
    # idle = idle + iowait
    idle = values[3] + values[4]  # idle + iowait
    total = sum(values)
    
    return total, idle


def read_cpu_per_core():
    """
    从 /proc/stat 读取每个 CPU 核心的使用情况
    返回: [(total, idle), ...] 列表
    """
    cores = []
    with open('/proc/stat', 'r') as f:
        f.readline()  # 跳过汇总行
        for line in f:
            if not line.startswith('cpu'):
                break
            parts = line.split()
            values = [int(x) for x in parts[1:]]
            idle = values[3] + values[4]
            total = sum(values)
            cores.append((total, idle))
    return cores


def read_disk_stats(target_devices=None):
    """
    从 /proc/diskstats 读取磁盘 IO 统计
    
    /proc/diskstats 格式 (kernel 4.18+):
    major minor name rd_ios rd_merges rd_sectors rd_ticks 
                     wr_ios wr_merges wr_sectors wr_ticks
                     ios_in_progress io_ticks weighted_io_ticks
                     [discard_ios discard_merges discard_sectors discard_ticks]
    
    返回: {device: {rd_ios, rd_sectors, wr_ios, wr_sectors, io_ticks}} 字典
    """
    stats = {}
    with open('/proc/diskstats', 'r') as f:
        for line in f:
            parts = line.split()
            if len(parts) < 14:
                continue
            device = parts[2]
            
            # 跳过 loop/ram 设备
            if device.startswith(('loop', 'ram')):
                continue
            
            # 如果指定了目标设备，只采集这些
            if target_devices and device not in target_devices:
                continue
            
            stats[device] = {
                'rd_ios': int(parts[3]),
                'rd_sectors': int(parts[5]),
                'wr_ios': int(parts[7]),
                'wr_sectors': int(parts[9]),
                'io_ticks': int(parts[12]),  # 花在 IO 上的毫秒数
            }
    
    return stats


def read_iowait_pct():
    """
    从 /proc/stat 读取 iowait 占比
    返回当前累计的 iowait jiffies 和 total jiffies
    """
    with open('/proc/stat', 'r') as f:
        line = f.readline()
    parts = line.split()
    values = [int(x) for x in parts[1:]]
    iowait = values[4]
    total = sum(values)
    return total, iowait


def detect_data_disk():
    """
    自动检测数据所在的磁盘设备
    通过查看 /data 挂载点对应的设备
    """
    try:
        with open('/proc/mounts', 'r') as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2 and parts[1] == '/data':
                    # /dev/vdb -> vdb
                    dev = parts[0].split('/')[-1]
                    return dev
        # 如果没有 /data 挂载点，返回所有 vd* 设备
        devices = []
        with open('/proc/diskstats', 'r') as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 3:
                    dev = parts[2]
                    # 只要主设备（如 vda, vdb），不要分区（如 vda1）
                    if dev.startswith('vd') and not any(c.isdigit() for c in dev[2:]):
                        devices.append(dev)
        return devices if devices else None
    except Exception:
        return None


# ============================================================
# 高精度监控器
# ============================================================

class HighResMonitor:
    """
    高精度系统监控器
    
    在独立进程中以毫秒级间隔采集 CPU 和 IO 数据。
    使用共享内存进行进程间通信，开销极低。
    
    参数:
        interval_ms: 采样间隔（毫秒），建议 2~5ms
        target_disks: 要监控的磁盘设备列表，None 则自动检测
    """
    
    def __init__(self, interval_ms=2, target_disks=None):
        self.interval_ms = interval_ms
        self.interval_s = interval_ms / 1000.0
        self.target_disks = target_disks
        
        # 自动检测磁盘
        if self.target_disks is None:
            detected = detect_data_disk()
            if isinstance(detected, list):
                self.target_disks = detected
            elif detected:
                self.target_disks = [detected]
            else:
                self.target_disks = []
        
        # 采集结果存储
        self.samples = []
        self._running = Value(c_bool, False)
        self._process = None
        
        # 使用临时文件进行数据传输（避免共享内存大小限制）
        self._data_file = '/tmp/highres_monitor_{}.bin'.format(os.getpid())
    
    def start(self):
        """启动采集进程"""
        self._running.value = True
        self._process = Process(
            target=self._sample_loop,
            args=(self._running, self._data_file, self.interval_s, self.target_disks)
        )
        self._process.daemon = True
        self._process.start()
    
    def stop(self):
        """停止采集进程并收集数据"""
        if self._process and self._process.is_alive():
            self._running.value = False
            self._process.join(timeout=5)
            if self._process.is_alive():
                self._process.terminate()
        
        # 读取采集数据
        self._load_samples()
    
    def _load_samples(self):
        """从临时文件加载采集数据"""
        self.samples = []
        if not os.path.exists(self._data_file):
            return
        
        try:
            with open(self._data_file, 'r') as f:
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) >= 5:
                        self.samples.append({
                            'timestamp_s': float(parts[0]),
                            'cpu_pct': float(parts[1]),
                            'iowait_pct': float(parts[2]),
                            'disk_read_mbps': float(parts[3]),
                            'disk_write_mbps': float(parts[4]),
                            'disk_util_pct': float(parts[5]) if len(parts) > 5 else 0.0,
                        })
        except (IOError, ValueError):
            pass
        finally:
            # 清理临时文件
            try:
                os.unlink(self._data_file)
            except OSError:
                pass
    
    @staticmethod
    def _sample_loop(running, data_file, interval_s, target_disks):
        """采集循环（在独立进程中运行）"""
        # 初始化基准值
        prev_cpu_total, prev_cpu_idle = read_cpu_stats()
        prev_cpu_total_iow, prev_iowait = read_iowait_pct()
        prev_disk = read_disk_stats(target_disks)
        prev_time = time.time()
        
        start_time = prev_time
        
        # 打开输出文件
        f = open(data_file, 'w')
        
        # 等待一个采样间隔让差值有意义
        time.sleep(interval_s)
        
        try:
            while running.value:
                now = time.time()
                elapsed = now - prev_time
                
                if elapsed < interval_s * 0.5:
                    # 还没到采样时间，短暂 sleep
                    time.sleep(interval_s * 0.1)
                    continue
                
                # === CPU 利用率 ===
                cpu_total, cpu_idle = read_cpu_stats()
                d_total = cpu_total - prev_cpu_total
                d_idle = cpu_idle - prev_cpu_idle
                cpu_pct = ((d_total - d_idle) / d_total * 100.0) if d_total > 0 else 0.0
                
                # === IO Wait ===
                cpu_total_iow, iowait = read_iowait_pct()
                d_total_iow = cpu_total_iow - prev_cpu_total_iow
                d_iowait = iowait - prev_iowait
                iowait_pct = (d_iowait / d_total_iow * 100.0) if d_total_iow > 0 else 0.0
                
                # === 磁盘 IO ===
                curr_disk = read_disk_stats(target_disks)
                disk_read_mbps = 0.0
                disk_write_mbps = 0.0
                disk_util_pct = 0.0
                
                for dev in target_disks:
                    if dev in curr_disk and dev in prev_disk:
                        d_rd_sectors = curr_disk[dev]['rd_sectors'] - prev_disk[dev]['rd_sectors']
                        d_wr_sectors = curr_disk[dev]['wr_sectors'] - prev_disk[dev]['wr_sectors']
                        d_io_ticks = curr_disk[dev]['io_ticks'] - prev_disk[dev]['io_ticks']
                        
                        # sectors 通常是 512 字节
                        disk_read_mbps += d_rd_sectors * 512.0 / (1024 * 1024) / elapsed
                        disk_write_mbps += d_wr_sectors * 512.0 / (1024 * 1024) / elapsed
                        # io_ticks 是毫秒，elapsed 转毫秒
                        disk_util_pct += min(100.0, d_io_ticks / (elapsed * 1000.0) * 100.0)
                
                # 写入数据行
                relative_time = now - start_time
                f.write('{:.6f},{:.2f},{:.2f},{:.2f},{:.2f},{:.2f}\n'.format(
                    relative_time, cpu_pct, iowait_pct,
                    disk_read_mbps, disk_write_mbps, disk_util_pct
                ))
                
                # 更新前值
                prev_cpu_total = cpu_total
                prev_cpu_idle = cpu_idle
                prev_cpu_total_iow = cpu_total_iow
                prev_iowait = iowait
                prev_disk = curr_disk
                prev_time = now
                
                # 精确 sleep
                next_sample = now + interval_s
                sleep_time = next_sample - time.time()
                if sleep_time > 0:
                    time.sleep(sleep_time)
        
        finally:
            f.flush()
            f.close()
    
    def save_csv(self, output_path):
        """保存采集数据为 CSV 文件"""
        if not self.samples:
            return
        
        with open(output_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'timestamp_s', 'cpu_pct', 'iowait_pct',
                'disk_read_mbps', 'disk_write_mbps', 'disk_util_pct'
            ])
            writer.writeheader()
            writer.writerows(self.samples)
    
    def save_json(self, output_path):
        """保存采集数据为 JSON 文件"""
        if not self.samples:
            return
        
        with open(output_path, 'w') as f:
            json.dump({
                'interval_ms': self.interval_ms,
                'target_disks': self.target_disks,
                'num_samples': len(self.samples),
                'duration_s': self.samples[-1]['timestamp_s'] if self.samples else 0,
                'samples': self.samples
            }, f, indent=2)
    
    def get_summary(self):
        """获取采集数据的统计摘要"""
        if not self.samples:
            return None
        
        cpu_values = [s['cpu_pct'] for s in self.samples]
        iowait_values = [s['iowait_pct'] for s in self.samples]
        read_values = [s['disk_read_mbps'] for s in self.samples]
        write_values = [s['disk_write_mbps'] for s in self.samples]
        util_values = [s['disk_util_pct'] for s in self.samples]
        
        def stats(values):
            if not values:
                return {'avg': 0, 'max': 0, 'min': 0, 'p50': 0, 'p95': 0, 'p99': 0}
            sorted_v = sorted(values)
            n = len(sorted_v)
            return {
                'avg': sum(values) / n,
                'max': max(values),
                'min': min(values),
                'p50': sorted_v[n // 2],
                'p95': sorted_v[int(n * 0.95)],
                'p99': sorted_v[int(n * 0.99)],
            }
        
        return {
            'num_samples': len(self.samples),
            'duration_s': self.samples[-1]['timestamp_s'],
            'actual_interval_ms': (self.samples[-1]['timestamp_s'] / len(self.samples) * 1000)
                                   if len(self.samples) > 1 else 0,
            'cpu_pct': stats(cpu_values),
            'iowait_pct': stats(iowait_values),
            'disk_read_mbps': stats(read_values),
            'disk_write_mbps': stats(write_values),
            'disk_util_pct': stats(util_values),
        }
    
    def plot(self, output_path, title="System Metrics", figsize=(14, 10)):
        """
        生成 CPU + IO 负载时序图
        
        图表包含 4 个子图:
        1. CPU 利用率 (%)
        2. IO Wait (%)
        3. 磁盘吞吐量 (MB/s)
        4. 磁盘利用率 (%)
        """
        try:
            import matplotlib
            matplotlib.use('Agg')  # 无头模式
            import matplotlib.pyplot as plt
            from matplotlib.ticker import MaxNLocator
        except ImportError:
            print("  ⚠️ matplotlib 未安装，跳过画图")
            print("  安装: pip install matplotlib")
            return False
        
        if not self.samples:
            print("  ⚠️ 无采集数据，跳过画图")
            return False
        
        timestamps = [s['timestamp_s'] * 1000 for s in self.samples]  # 转为毫秒
        cpu_pct = [s['cpu_pct'] for s in self.samples]
        iowait_pct = [s['iowait_pct'] for s in self.samples]
        read_mbps = [s['disk_read_mbps'] for s in self.samples]
        write_mbps = [s['disk_write_mbps'] for s in self.samples]
        disk_util = [s['disk_util_pct'] for s in self.samples]
        
        fig, axes = plt.subplots(4, 1, figsize=figsize, sharex=True)
        fig.suptitle(title, fontsize=14, fontweight='bold')
        
        # --- 子图1: CPU 利用率 ---
        ax1 = axes[0]
        ax1.fill_between(timestamps, cpu_pct, alpha=0.3, color='#2196F3')
        ax1.plot(timestamps, cpu_pct, linewidth=0.8, color='#1565C0')
        ax1.set_ylabel('CPU Usage (%)')
        ax1.set_ylim(0, 105)
        ax1.axhline(y=100, color='red', linestyle='--', alpha=0.3, linewidth=0.5)
        ax1.grid(True, alpha=0.3)
        # 标注平均值
        avg_cpu = sum(cpu_pct) / len(cpu_pct) if cpu_pct else 0
        ax1.axhline(y=avg_cpu, color='#1565C0', linestyle=':', alpha=0.5)
        ax1.text(timestamps[-1] * 0.98, avg_cpu + 3, 'avg={:.1f}%'.format(avg_cpu),
                 ha='right', fontsize=8, color='#1565C0')
        
        # --- 子图2: IO Wait ---
        ax2 = axes[1]
        ax2.fill_between(timestamps, iowait_pct, alpha=0.3, color='#FF9800')
        ax2.plot(timestamps, iowait_pct, linewidth=0.8, color='#E65100')
        ax2.set_ylabel('IO Wait (%)')
        ax2.set_ylim(0, max(max(iowait_pct) * 1.2, 10))
        ax2.grid(True, alpha=0.3)
        avg_iow = sum(iowait_pct) / len(iowait_pct) if iowait_pct else 0
        ax2.axhline(y=avg_iow, color='#E65100', linestyle=':', alpha=0.5)
        ax2.text(timestamps[-1] * 0.98, avg_iow + 1, 'avg={:.1f}%'.format(avg_iow),
                 ha='right', fontsize=8, color='#E65100')
        
        # --- 子图3: 磁盘吞吐量 ---
        ax3 = axes[2]
        ax3.fill_between(timestamps, read_mbps, alpha=0.3, color='#4CAF50', label='Read')
        ax3.fill_between(timestamps, write_mbps, alpha=0.3, color='#F44336', label='Write')
        ax3.plot(timestamps, read_mbps, linewidth=0.8, color='#2E7D32')
        ax3.plot(timestamps, write_mbps, linewidth=0.8, color='#C62828')
        ax3.set_ylabel('Throughput (MB/s)')
        ax3.legend(loc='upper right', fontsize=8)
        ax3.grid(True, alpha=0.3)
        
        # --- 子图4: 磁盘利用率 ---
        ax4 = axes[3]
        ax4.fill_between(timestamps, disk_util, alpha=0.3, color='#9C27B0')
        ax4.plot(timestamps, disk_util, linewidth=0.8, color='#6A1B9A')
        ax4.set_ylabel('Disk Util (%)')
        ax4.set_xlabel('Time (ms)')
        ax4.set_ylim(0, 105)
        ax4.axhline(y=100, color='red', linestyle='--', alpha=0.3, linewidth=0.5)
        ax4.grid(True, alpha=0.3)
        avg_util = sum(disk_util) / len(disk_util) if disk_util else 0
        ax4.axhline(y=avg_util, color='#6A1B9A', linestyle=':', alpha=0.5)
        ax4.text(timestamps[-1] * 0.98, avg_util + 3, 'avg={:.1f}%'.format(avg_util),
                 ha='right', fontsize=8, color='#6A1B9A')
        
        # 添加采样信息
        info_text = 'Samples: {}  |  Interval: {:.1f}ms  |  Duration: {:.1f}ms  |  Disks: {}'.format(
            len(self.samples),
            self.samples[-1]['timestamp_s'] / len(self.samples) * 1000 if len(self.samples) > 1 else 0,
            timestamps[-1],
            ', '.join(self.target_disks) if self.target_disks else 'auto'
        )
        fig.text(0.5, 0.01, info_text, ha='center', fontsize=8, color='gray')
        
        plt.tight_layout(rect=[0, 0.03, 1, 0.96])
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        plt.close()
        
        return True


# ============================================================
# 便捷函数：用于集成到 run_profiling.py
# ============================================================

def monitor_query_execution(duckdb_bin, setup_sql, query_sql, profile_json_path,
                            output_prefix, interval_ms=2, warmup=True):
    """
    一站式函数：监控查询执行期间的系统指标
    
    参数:
        duckdb_bin: DuckDB 二进制路径
        setup_sql: 建表/视图 SQL
        query_sql: 要执行的查询 SQL
        profile_json_path: DuckDB profile JSON 输出路径
        output_prefix: 输出文件前缀（会生成 _metrics.csv, _metrics.png）
        interval_ms: 采样间隔（毫秒）
        warmup: 是否先预热一次
    
    返回:
        {
            'wall_time_s': 查询执行时间,
            'monitor_summary': 监控统计摘要,
            'csv_path': CSV 数据文件路径,
            'plot_path': 图表文件路径,
            'error': 错误信息（如果有）
        }
    """
    import subprocess
    
    # 预热
    if warmup:
        warmup_sql = "{}\n{}\n".format(setup_sql, query_sql)
        subprocess.run(
            [duckdb_bin, "-c", warmup_sql],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, timeout=600
        )
    
    # 构造带 profiling 的完整 SQL
    full_sql = """{setup}
PRAGMA threads=4;
PRAGMA enable_profiling='json';
PRAGMA profiling_mode='detailed';
PRAGMA profiling_output='{profile}';
{query}
""".format(setup=setup_sql, profile=profile_json_path, query=query_sql)
    
    # 启动监控
    monitor = HighResMonitor(interval_ms=interval_ms)
    monitor.start()
    
    # 等待监控进程稳定
    time.sleep(0.01)  # 10ms
    
    # 执行查询
    start = time.time()
    result = subprocess.run(
        [duckdb_bin, "-c", full_sql],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        universal_newlines=True, timeout=600
    )
    elapsed = time.time() - start
    
    # 停止监控
    time.sleep(0.01)  # 等最后几个采样点
    monitor.stop()
    
    # 检查执行结果
    error = None
    if result.returncode != 0:
        error = result.stderr[:500]
    
    # 保存数据
    csv_path = output_prefix + '_metrics.csv'
    plot_path = output_prefix + '_metrics.png'
    json_path = output_prefix + '_metrics.json'
    
    monitor.save_csv(csv_path)
    
    # 画图
    plot_ok = monitor.plot(plot_path, title=os.path.basename(output_prefix))
    
    # 获取摘要
    summary = monitor.get_summary()
    
    # 保存完整 JSON（含摘要）
    with open(json_path, 'w') as f:
        json.dump({
            'wall_time_s': elapsed,
            'interval_ms': interval_ms,
            'summary': summary,
            'error': error,
        }, f, indent=2)
    
    return {
        'wall_time_s': elapsed,
        'monitor_summary': summary,
        'csv_path': csv_path,
        'plot_path': plot_path if plot_ok else None,
        'json_path': json_path,
        'error': error,
    }


# ============================================================
# 自测
# ============================================================

def self_test():
    """自测：采集 2 秒的系统指标"""
    print("=" * 60)
    print("高精度系统监控器 - 自测")
    print("=" * 60)
    
    # 检测磁盘
    disks = detect_data_disk()
    print("\n检测到的磁盘设备: {}".format(disks))
    
    # 启动监控
    print("\n启动监控 (interval=2ms, duration=2s)...")
    monitor = HighResMonitor(interval_ms=2)
    monitor.start()
    
    # 模拟负载：读取一些文件
    time.sleep(0.5)
    print("  [0.5s] 开始模拟 IO 负载...")
    for i in range(100):
        with open('/proc/stat', 'r') as f:
            f.read()
    time.sleep(1.0)
    print("  [1.5s] IO 负载结束")
    time.sleep(0.5)
    
    # 停止
    monitor.stop()
    
    print("\n采集结果:")
    print("  采样点数: {}".format(len(monitor.samples)))
    if monitor.samples:
        duration = monitor.samples[-1]['timestamp_s']
        actual_interval = duration / len(monitor.samples) * 1000
        print("  持续时间: {:.3f}s".format(duration))
        print("  实际采样间隔: {:.2f}ms".format(actual_interval))
        
        summary = monitor.get_summary()
        print("\n  CPU 利用率: avg={:.1f}%, max={:.1f}%, p95={:.1f}%".format(
            summary['cpu_pct']['avg'], summary['cpu_pct']['max'], summary['cpu_pct']['p95']))
        print("  IO Wait:    avg={:.1f}%, max={:.1f}%, p95={:.1f}%".format(
            summary['iowait_pct']['avg'], summary['iowait_pct']['max'], summary['iowait_pct']['p95']))
        print("  Disk Read:  avg={:.1f} MB/s, max={:.1f} MB/s".format(
            summary['disk_read_mbps']['avg'], summary['disk_read_mbps']['max']))
        print("  Disk Write: avg={:.1f} MB/s, max={:.1f} MB/s".format(
            summary['disk_write_mbps']['avg'], summary['disk_write_mbps']['max']))
        print("  Disk Util:  avg={:.1f}%, max={:.1f}%".format(
            summary['disk_util_pct']['avg'], summary['disk_util_pct']['max']))
    
    # 保存 CSV
    test_csv = '/tmp/highres_monitor_test.csv'
    monitor.save_csv(test_csv)
    print("\n  CSV 已保存: {} ({} 行)".format(test_csv, len(monitor.samples)))
    
    # 尝试画图
    test_png = '/tmp/highres_monitor_test.png'
    if monitor.plot(test_png, title="Self-Test (2s)"):
        print("  图表已保存: {}".format(test_png))
    
    print("\n✅ 自测完成")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        self_test()
    else:
        print("用法: python3 {} --test".format(sys.argv[0]))
        print("\n作为模块使用:")
        print("  from highres_monitor import HighResMonitor, monitor_query_execution")
