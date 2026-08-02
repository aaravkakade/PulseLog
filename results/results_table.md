## Environment

| Property | Value |
|---|---|
| OS | Darwin 23.5.0 |
| Architecture | arm64 |
| CPU | Apple M2 (8 logical cores) |
| Memory | 16.0 GiB |
| Compiler | clang 15.0.0 |
| Event loop | kqueue |
| Checksum | hardware-arm64 |
| Load model | closed |
| Latency scope | per produce request (a batch), not per record |

## Results

Median of all trials per scenario. Latency is per produce request; with batching a request carries many records.

| Scenario | Config | records/s (median) | spread | MiB/s | p50 | p99 | p99.9 | err |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 01-single-producer-no-replication | `1p/1part/b100/128B/leader` | 1,036,382 | 823,340-1,157,141 | 154.2 | 37 us | 871 us | 8,798 us | 0 |
| 02-multi-producer-one-partition | `4p/1part/b100/128B/leader` | 2,490,528 | 1,403,758-2,794,517 | 370.5 | 76 us | 2,427 us | 10,723 us | 0 |
| 03-multi-producer-multi-partition | `4p/8part/b100/128B/leader` | 1,142,777 | 932,877-1,870,111 | 170.0 | 84 us | 3,478 us | 11,346 us | 0 |
| 04-concurrent-producers-consumers | `4p/4part/b100/128B/leader` | 1,286,244 | 1,127,381-1,665,872 | 191.4 | 87 us | 3,049 us | 3,869 us | 0 |
| 05-leader-ack | `4p/4part/b100/128B/leader` | 2,160,036 | 1,887,207-2,712,154 | 321.4 | 63 us | 3,181 us | 5,632 us | 0 |
| 06-quorum-ack | `4p/4part/b100/128B/quorum` | 27,761 | 22,909-39,611 | 4.1 | 13,730 us | 22,495 us | 24,003 us | 0 |
| 07-replication-under-load | `4p/6part/b100/128B/leader` | 416,873 | 368,874-511,590 | 62.0 | 169 us | 16,728 us | 25,182 us | 0 |
| 08-no-batching | `4p/4part/b1/128B/leader` | 59,631 | 51,307-82,331 | 8.9 | 39 us | 129 us | 9,224 us | 0 |
| 09-small-message | `4p/4part/b200/16B/leader` | 4,009,087 | 3,820,558-5,821,184 | 164.4 | 59 us | 3,052 us | 3,281 us | 0 |
| 10-large-message | `4p/4part/b4/65536B/leader` | 2,846 | 1,538-6,333 | 177.9 | 596 us | 84,083 us | 115,081 us | 0 |
| 11-acks-none | `4p/4part/b100/128B/none` | 663,092 | 503,750-868,535 | 98.7 | 131 us | 12,755 us | 17,465 us | 0 |
| 12-fsync-full-vs-data | `4p/4part/b100/128B/leader` | 739,356 | 596,289-982,057 | 110.0 | 110 us | 14,041 us | 17,302 us | 0 |
| 13-baseline-mutex-queue | `4p/1part/b1/128B/leader` | 4,896,785 | 3,326,252-5,369,854 | 597.8 | 0 us | 14 us | 39 us | 0 |
