# Development session: live Kafka, TLS and broker ACLs

Date: 24 October 2026. Branch: `portfolio/live-broker-acl-validation`, based on `main`. The pending partition-observation PR remains separate.

## Goal and environment

Close the gap between a compiling live adapter and evidence from a real broker. The default Docker daemon was absent. Colima was installed with no existing instances, so the session created a dedicated `portfolio-lab` VM with 2 CPUs, 3 GiB RAM, no host mounts and no active-context change. Docker was selected explicitly through `DOCKER_CONTEXT=colima-portfolio-lab`.

Local runtime: Node.js v25.6.1 and OpenJDK 24.0.2 on macOS/Apple silicon. Broker: `apache/kafka:4.0.2@sha256:836cafdad9f4825880d7cf1d5a21202915ae2527bd0ef1c3600c526ed7814d1f`. CI targets Ubuntu, Node.js 22 and JDK 21 separately.

## Observed results

`npm test`: all six baseline tests pass. `npm run test:live`: all thirteen live checks pass:

1. A real MCP stdio handshake discovers exactly four read-only tools.
2. Real topic metadata is filtered by broker ACLs and the tool allowlist.
3. The broker has partition ends 10 and 6 and committed offsets 7 and 2, producing lag 3 + 4 = 7.
4. A group without commits remains uncommitted, has no total lag, and receives guidance with no automatic actions.
5. An unlisted topic is refused by application policy.
6. Allowlisted but broker-denied topic/group access returns a generic diagnostic error.
7. The reader cannot produce records.
8. The reader cannot create a topic.
9. The reader cannot delete a topic.
10. The reader cannot reset committed offsets through Kafka's native admin client.
11. Independent administrator reads confirm that denied attempts did not change topics, record ends or committed offsets.
12. An invalid SASL password fails closed with a redacted startup error.
13. Omitting the generated CA causes TLS trust failure; certificate verification is enabled.

The harness uses generated ephemeral credentials and synthetic messages. It removes the owned broker and TLS directory; the dedicated VM is stopped after local validation. Its base disk and image cache remain available for future sessions. KafkaJS emits `TimeoutNegativeWarning` on local Node.js 25; assertions still pass. The Node.js 22 CI result is reported through PR checks rather than inferred from this local run.

## What the live experiment changed

The first bootstrap reached the broker before a new group coordinator was ready. Bounded retries now distinguish that startup condition from authorization or other failures. The negative offset-reset experiment also exposed a KafkaJS helper that did not settle on the denied path. The final test uses the native admin client, requires a specific group authorization error, and checks stored state afterward. These are harness decisions; production tools still expose no write operation.

The [ADR](adr-live-broker-validation.md) explains the separate setup principal, transport trust, alternatives and remaining validation limits.

## Walkthrough for Shubham

Run `npm ci`, `npm test` and `npm run test:live` with a Docker daemon and `keytool` available. Explain why tool annotations and application allowlists do not replace broker ACLs. Show the distinction between an authentication failure, a policy refusal and a broker authorization denial. Explain why the rejected reset is checked against durable offsets rather than treating any exception as success.

## Next experiment

Test credential rotation or a second broker as a separate slice. The CDC source/snapshot handoff, SCRAM, real PKI and deployment failover remain unverified by this run.
