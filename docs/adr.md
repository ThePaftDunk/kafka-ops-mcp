# ADR: read-only Kafka tools with exact diagnostics

Status: accepted for the portfolio server.

AI clients need observed data, but broad admin commands and payload sampling add risk and make authorization difficult. The inspector interface includes only topic metadata and group offsets. The MCP layer exposes four fixed schemas. Exact allowlists are checked before a selected broker operation; live mode requires explicit policy. Kafka ACLs must independently constrain the service principal.

All offsets are strings at the transport boundary and `BigInt` internally. For each partition, a valid commit in `[logStart, logEnd]` gives `logEnd - committed`. Missing commits and out-of-range offsets retain their diagnostic meaning rather than being coerced to zero. If one partition is unknown, total lag is unknown; the known subtotal is separately labeled.

The recovery tool provides inspection steps only. A lag snapshot does not identify a failed database, stopped consumer, retention change, or slow workload. Automatic offset changes would need state reconciliation, an audited execution path, and a separate authorization model. That extension is intentionally outside this server's tool surface.

The fixture implements the same interface as KafkaJS so domain tests stay deterministic. The protocol integration test still uses a real SDK client and a separate server process, which catches errors that direct function tests miss. Live broker compatibility, ACL behavior, and certificate configuration require an integration environment.
