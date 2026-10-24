# ADR: exercise the read-only boundary against a disposable broker

Status: accepted for the integration harness, 24 October 2026.

## Problem

Fixture and stdio tests validate domain calculations, tool schemas and local policy. They do not establish that KafkaJS observes the expected broker offset shapes, that TLS/SASL authentication works, or that the broker independently denies mutations by the MCP principal.

## Decision

Keep the fast default suite and add `npm run test:live` plus a separate CI job. The harness owns a UUID-named Kafka container, generated certificate/keystore, random passwords and synthetic data. Pin the Kafka 4.0.2 image by manifest digest. Publish only a random host loopback port. Copy TLS files into the container rather than requiring host filesystem mounts, which also supports Docker running in an isolated VM.

A bootstrap administrator creates two customer partitions and commits known offsets. The server receives a distinct `reader` identity with only `Describe` on the customer topic and two permitted groups. The setup administrator remains outside the production MCP tool surface. Internal broker/controller listeners are not published; the internal anonymous superuser is confined to this disposable fixture.

Use a real SDK client and a separate live-mode server process for tool checks. Independently attempt produce, create, delete and offset-reset operations as the reader and require broker authorization failures. Then use the setup administrator to verify that records, topic existence and offsets remain unchanged. A refusal caused only by network failure or timeout does not count as an ACL pass.

Generate and trust a test certificate through the runtime's normal CA path; never disable certificate verification. Separate adapter startup probes must fail with an incorrect password and with the test CA omitted. Errors returned by tool calls and startup probes must remain redacted.

## Alternatives and tradeoffs

- More fixture cases remain useful for exact offsets and domain edge cases but cannot prove broker authorization behavior.
- An external shared cluster would complicate reproducibility, cleanup, credentials and attribution of failures. This harness never accepts a target-cluster address.
- A multi-broker secure cluster would cover more failure modes, at higher setup and CI cost. Start with one broker and keep the unverified boundaries explicit.
- KafkaJS's `admin.setOffsets` helper internally runs a consumer. In this experiment its denied path did not settle, so it is not suitable as the negative-test oracle. Use Kafka's command-line admin client for the rejected reset, require a group authorization error and verify the stored offsets independently.

Broker metadata readiness does not imply that a new group coordinator is ready. Bootstrap retries only recognized coordinator-startup errors within a deadline. The test itself and CI job also have execution limits.

## Boundaries

This establishes the tested KafkaJS 2.2.4 / Kafka 4.0.2 path with SASL/PLAIN over TLS and selected ACL denials. It does not establish SCRAM rotation, production PKI compatibility, multi-broker failover, record-count lag semantics, rebalances, or CDC snapshot handoff. Large-offset precision and retention/truncation remain fixture-backed checks.

The setup follows Apache's [SASL/PLAIN configuration](https://kafka.apache.org/40/security/authentication-using-sasl/) and [StandardAuthorizer/ACL model](https://kafka.apache.org/40/security/authorization-and-acls/). The permissive setup principal and generated certificate belong only to the owned disposable broker.
