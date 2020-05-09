# VLDB 2021 Evelyn Paxos

This directory contains benchmark scripts for our VLDB 2021 submission on
Evelyn Paxos. Our submission includes plots generated from 7 experiments.

- `e1_lt_surprise`. We generate a latency-throughput curve for Evelyn Paxos
  with an all-writes workload and an all-reads workload. We then ask the reader
  to guess what the curve will look like for a workload with 50% reads and 50%
  writes. We show a naive guess and the correct answer. The point is that the
  performance behavior is counter-intuitive.
- `e2_no_scale`. After we introduce the theory behind why the system is
  performing counter-intuitively, we show analytic plots. We then show real
  plots that resemble them. We show the throughput vs the number of replicas
  (for various write percentages) and throughput vs the write percentage (for
  various number of replicas). The two plots are like transposes of one
  another.
- `e3_scale`. We show throughput vs number of replicas for various fixed write
  loads. Now, the graph scales nicely and as expected.
- `e4_lt_misc`. We show latency-throughput curves for Evelyn Paxos with f=1,
  f=2, eventually consistent reads, sequentially consistent reads, and with
  batching for 10 replicas. From now on, we can use batching.
- `e5_craq`. We show throughput vs data skew. CRAQ should perform poorly with
  high skew. Evelyn Paxos should be completely unaffected.
- `e6_harmonia`. We show a latency-throughput curve for Evelyn Paxos with and
  without early reads. The two should look similar, showing that waiting is not
  a big deal, something that Harmonia helps avoid.
- `e7_data_size`. We show throughput vs data size. Larger data should lead to
  lower throughput.
