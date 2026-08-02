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
| 01-single-producer-no-replication | `1p/1part/b100/128B/leader` | 861,344 | 784,807-1,017,269 | 128.1 | 39 us | 349 us | 15,278 us | 0 |
| 02-multi-producer-one-partition | `4p/1part/b100/128B/leader` | 1,850,569 | 871,593-2,392,495 | 275.3 | 84 us | 2,646 us | 20,808 us | 0 |
| 03-multi-producer-multi-partition | `4p/8part/b100/128B/leader` | 971,961 | 866,114-1,150,219 | 144.6 | 72 us | 7,287 us | 10,641 us | 0 |
| 04-concurrent-producers-consumers | `4p/4part/b100/128B/leader` | 1,197,498 | 951,462-1,627,036 | 178.2 | 89 us | 6,140 us | 8,008 us | 0 |
| 05-leader-ack | `4p/4part/b100/128B/leader` | 1,301,128 | 1,191,211-2,013,511 | 193.6 | 87 us | 8,495 us | 9,675 us | 0 |
| 06-quorum-ack | `4p/4part/b100/128B/quorum` | 26,147 | 20,315-61,532 | 3.9 | 15,385 us | 19,890 us | 20,382 us | 0 |
| 07-replication-under-load | `4p/6part/b100/128B/leader` | 398,581 | 131,668-500,937 | 59.3 | 164 us | 14,574 us | 20,939 us | 0 |
| 08-no-batching | `4p/4part/b1/128B/leader` | 83,865 | 78,908-85,945 | 12.5 | 38 us | 113 us | 2,259 us | 0 |
| 09-small-message | `4p/4part/b200/16B/leader` | 3,119,652 | 2,691,146-3,256,432 | 127.9 | 87 us | 3,592 us | 6,332 us | 0 |
| 10-large-message | `4p/4part/b4/65536B/leader` | 6,619 | 3,728-10,227 | 413.9 | 514 us | 28,017 us | 49,611 us | 0 |
| 11-acks-none | `4p/4part/b100/128B/none` | 600,651 | 409,202-976,624 | 89.4 | 77 us | 19,661 us | 31,031 us | 0 |
| 12-fsync-full-vs-data | `4p/4part/b100/128B/leader` | 2,529,461 | 2,080,046-3,141,181 | 376.3 | 60 us | 3,260 us | 6,443 us | 0 |
| 13-baseline-mutex-queue | `4p/1part/b1/128B/leader` | 5,225,821 | 4,466,670-5,341,729 | 637.9 | 0 us | 11 us | 24 us | 0 |
