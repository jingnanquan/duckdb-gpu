"tpch_Q05": {
    "wall_time_s": 0.4173870086669922,
    "profile_total_time_s": 1.350087823000002,
    "scan_ratio_pct": 85.60819624546735,
    "type_summary": {
      "TABLE_SCAN": {
        "time_s": 1.1557858329999995,
        "pct": 85.60819624546735,
        "count": 6
      },
      "HASH_JOIN": {
        "time_s": 0.187715503000002,
        "pct": 13.903947565639346,
        "count": 5
      },
      "HASH_GROUP_BY": {
        "time_s": 0.00414584,
        "pct": 0.30707928250086836,
        "count": 1
      },
      "PROJECTION": {
        "time_s": 0.0022922999999999997,
        "pct": 0.16978895453677434,
        "count": 2
      },
      "ORDER_BY": {
        "time_s": 0.00014834699999999998,
        "pct": 0.01098795185563271,
        "count": 1
      }
    },}

"tpch_Q09": {
    "wall_time_s": 0.8770058155059814,
    "profile_total_time_s": 3.0064480950000014,
    "scan_ratio_pct": 48.07231435006696,
    "type_summary": {
      "TABLE_SCAN": {
        "time_s": 1.4452691790000005,
        "pct": 48.07231435006696,
        "count": 6
      },
      "HASH_JOIN": {
        "time_s": 1.4050183600000001,
        "pct": 46.73349798842941,
        "count": 5
      },
      "HASH_GROUP_BY": {
        "time_s": 0.0948813790000002,
        "pct": 3.1559293891618023,
        "count": 1
      },
      "PROJECTION": {
        "time_s": 0.060995375000000046,
        "pct": 2.028818495201728,
        "count": 9
      },
      "ORDER_BY": {
        "time_s": 0.000283802,
        "pct": 0.009439777140073987,
        "count": 1
      }
    },}

后续问题：
1. 我需要拿到10的执行计划，并分析一下为什么3个hashjoin只有一个命中bitmapjoin。同理我也需要5和9的执行计划，并查看为什么5和9命中率也只有3/5。需要判断无法bitmapjoin是否是因为无解（"多列等值条件"、聚合算子）的情况，还是实际可以优化的情况（中间join落到left侧）
2. 需要对5，9，10这几个查询，profling出每个hashjoin实际耗费的时间，即统计hashjoin、perfecthashjoin和bitmapjoin的绝对时间。这个很重要，查看优化效果不大的原因是执行计划上还是执行operator中
3. 需要优化“PK 表在中间 join 里落在了 LEFT 侧”这种情况，这意味着上层的join，本来可以走bitmapjoin优化，但是因为下层join的原因不得不放弃。应该有较好的方法，当下层join左侧传播破坏位置时能够通知到上层join去更新位置。或者，整个绑定的过程直接自下而上这样上层join在binding隐藏列时，下层join的隐藏列已经固化了。并且这一类问题，应该需要在test_bitmap_join_chain.cpp中测试出来
4. RIGHT_SEMI 这条代码路径，完全没有被自己写的测试跑过一次，也需要测出来