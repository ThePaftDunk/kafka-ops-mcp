# ADR: read-only Kafka tools with exact diagnostics

Status: accepted for the portfolio server.

AI clients need observed data, but broad admin commands and payload sampling add risk and make authorization difficult. The inspector interface includes only topic metadata and group offsets. The MCP layer exposes four fixed schemas. Exact allowlists are checked before a selected broker operation; live mode requires explicit policy. Kafka ACLs must independently constrain the service principal.

All offsets are strings at the transport boundary and `BigInt` internally. For each partition, a valid commit in `[logStart, logEnd]` gives `logEnd - committed`. Missing commits and out-of-range offsets retain their diagnostic meaning rather than being coerced to zero. If one partition is unknown, total lag is unknown; the known subtotal is separately labeled.

## Unmatched partition observations — 18 October 2026

Bounds and committed offsets come from independent calls. Previously the calculation iterated only over bounds, silently dropping commits with other partition IDs and potentially claiming a complete total. Preserve those unmatched entries in `unobservedCommittedPartitions`, withhold the total and mark the observation incomplete. Retain the known subtotal for comparable partitions. Validate nonnegative safe integer partition identities and all committed offsets, including unmatched entries, before interpreting them.

This decision favors useful partial evidence over either a guessed healthy total or a generic failure that hides the mismatch. Automatically retrying until the IDs match can conceal uncertainty and still cannot provide an atomic broker snapshot. A third topic-metadata read may aid investigation but cannot make the existing reads atomic. Recovery guidance therefore asks the operator to re-read and check topic identity without diagnosing retention loss or prescribing a fresh snapshot solely from an unmatched partition set.

Matching IDs do not prove that reads refer to the same instant or topic incarnation. The live broker path, concurrent topic changes and ACL behavior still need integration validation. This change establishes a deterministic interpretation of the evidence already returned by the inspector; it introduces no broker mutation or background polling.

The recovery tool provides inspection steps only. A lag snapshot does not identify a failed database, stopped consumer, retention change, or slow workload. Automatic offset changes would need state reconciliation, an audited execution path, and a separate authorization model. That extension is intentionally outside this server's tool surface.

The fixture implements the same interface as KafkaJS so domain tests stay deterministic. The protocol integration test still uses a real SDK client and a separate server process, which catches errors that direct function tests miss. Live broker compatibility, ACL behavior, and certificate configuration require an integration environment.
