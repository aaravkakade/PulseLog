# Security

## Threat model

**PulseLog v1 has no authentication, no authorisation and no transport
encryption.** Any process that can open a TCP connection to a broker port can:

* publish to and read from every topic;
* create and delete topics;
* impersonate a follower and receive the full replication stream;
* impersonate a leader and push arbitrary records into a follower's log;
* read and modify any consumer group's committed offsets.

It is designed to run on a trusted network — a single host, a private subnet,
or a container network that nothing untrusted can reach. Treat a broker port
exactly as you would an unauthenticated database port.

## What *is* hardened

Corruption and a hostile client produce the same bytes, so the parsing path is
written defensively regardless of the trust assumption:

* Frame headers carry their own CRC-32C, validated **before** any other header
  field is trusted. A corrupt `payload_len` cannot desynchronise the stream or
  drive an allocation.
* `payload_len` is checked against a configurable ceiling before any buffer is
  sized.
* Array counts are bounded by the bytes actually remaining in the payload, so a
  claimed count of a billion elements in a 20-byte request is rejected before
  any `reserve`.
* Every record length is validated structurally — minimum size, maximum size,
  and key/value lengths must exactly fill the declared record — so a corrupt
  varint cannot produce a span pointing outside the buffer.
* Connections have a hard output ceiling and a connection-count limit, so a
  peer cannot drive the broker out of memory or out of descriptors.
* `tests/unit/test_frame.cc` and `test_messages.cc` include randomised
  malformed-input runs asserting nothing is ever accepted spuriously and no
  read goes out of bounds; CI runs them under ASan and UBSan.

## Deployment guidance

* Bind to a private interface. `--net.listen=127.0.0.1:9092` for single-host
  use; do not expose 0.0.0.0 to an untrusted network.
* Put brokers on an isolated network segment; use the container network in the
  provided compose topology rather than host networking.
* Terminate TLS at a proxy if you need encryption in transit. There is no
  in-process TLS.
* The metrics endpoint (default 9644) exposes topic names, partition sizes and
  consumer group names. Treat it as sensitive and keep it internal.
* Run as an unprivileged user. The Docker image runs as uid 10001 and owns only
  its data directory.

## Reporting a vulnerability

This is a portfolio project, not production software, and there is no security
response process. If you find something interesting, open a GitHub issue.
Please do not report it as though a deployment depends on it — if one does,
that is the more urgent problem.
