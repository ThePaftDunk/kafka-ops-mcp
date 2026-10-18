# Development session: incomplete partition observations

Date: 18 October 2026. Branch: `portfolio/incomplete-partition-observations`.

## Reproduced problem

The lag calculator iterated over observed bounds and ignored committed-offset entries for other partition IDs. With bounds for partition 0 ending at 100, its commit at 75, and an additional commit for partition 1, the original code returned `complete: true` and `totalLag: "25"`. It silently omitted partition 1. That claim could mislead an operator or an AI client even though the two reads did not provide comparable evidence for every reported partition.

## Change and decision

Return unmatched commits in `unobservedCommittedPartitions`, sorted by ID, preserving exact offset strings and representing the `-1` uncommitted sentinel as null. Any unmatched entry makes `complete` false and `totalLag` null. `knownPartitionLag` remains the subtotal for partitions with valid comparable offsets. Missing commits on bounded partitions retain their existing `uncommitted` status. Invalid partition identities and malformed offsets fail validation even when they would previously have been ignored.

Recovery guidance asks for a fresh metadata/bounds/commits observation and topic-identity checks. It does not infer retention loss or suggest a new snapshot merely because partition reads are unmatched. The [ADR](adr.md) compares partial evidence with generic failure, automatic retry and an extra metadata read.

## Validation observed

- Reproduced the false-complete result by compiling and invoking the original domain code before the fix.
- `npm test`: 12 tests pass locally on Node.js v25.6.1 with the pinned TypeScript/MCP dependencies. Ten domain tests and two real stdio protocol tests cover the existing contract plus unmatched partition evidence, very large exact offsets, invalid partition identities and malformed offsets on otherwise unmatched entries.
- The new protocol test starts a separate server with a temporary fixture containing a committed partition absent from bounds. Both `consumer_lag` and `recovery_plan` return an incomplete observation and preserve the offset above JavaScript's safe integer range; recovery still has no automatic actions.
- `npm run demo`: all four tools are discovered and the normal fixture still reports total lag 500 with an empty unmatched-partition array.

CI targets Node.js 22 and supplies separate verification after publication. No live Kafka/ACL test or benchmark is claimed. Docker's CLI was available, but the configured daemon socket was absent, so the live PostgreSQL/Debezium/Kafka experiment could not run in this session.

## Walkthrough for Shubham

1. Run `npm ci`, `npm test` and `npm run demo`.
2. Inspect the new protocol fixture setup: it adds partition 7's commit while the bounds still cover partitions 0 and 1.
3. Explain why summing only the comparable rows gives a known subtotal rather than a complete group total.
4. Explain why retries or a third metadata call cannot establish atomicity across the existing broker reads.
5. Show that the recovery plan preserves uncertainty and exposes no offset-reset action.

## Next experiment

Run a disposable Kafka broker with a least-privilege principal, validate the adapter's metadata/offset shapes and ACL denial behavior, and capture observations around a controlled partition change. Keep that live experiment separate from this fixture-backed correctness result.
