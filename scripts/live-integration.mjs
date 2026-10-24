// Disposable Kafka integration: synthetic data, generated TLS/credentials, no external cluster.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Kafka, logLevel } from "kafkajs";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";

const image = "apache/kafka:4.0.2@sha256:836cafdad9f4825880d7cf1d5a21202915ae2527bd0ef1c3600c526ed7814d1f";
const name = `kafka-ops-test-${randomUUID()}`;
const directory = await mkdtemp(join(tmpdir(), "kafka-ops-tls-"));
const storePassword = randomBytes(18).toString("hex");
const adminPassword = randomBytes(18).toString("hex");
const readerPassword = randomBytes(18).toString("hex");
const resources = [];
let container;
let client;
let checks = 0;
let cleanupPromise;

function command(binary, args, timeout = 120000) {
  const result = spawnSync(binary, args, { encoding: "utf8", timeout });
  if (result.status !== 0) {
    // Do not include argv: generated credentials occur in container environment arguments.
    throw new Error(`${binary} failed (${result.status ?? result.error?.code}): ${result.stderr.slice(-2000)}`);
  }
  return result.stdout.trim();
}
const docker = (...args) => command("docker", args);
const pass = label => { checks++; console.log(`PASS: ${label}`); };
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
function authorizationFailure(error) {
  if (!error || typeof error !== "object") return false;
  if (["TOPIC_AUTHORIZATION_FAILED", "GROUP_AUTHORIZATION_FAILED", "CLUSTER_AUTHORIZATION_FAILED"].includes(error.type)) return true;
  return [error.cause, error.originalError, ...(error.errors ?? [])].some(authorizationFailure);
}
async function freePort() {
  const socket = createServer();
  await new Promise((resolve, reject) => { socket.once("error", reject); socket.listen(0, "127.0.0.1", resolve); });
  const port = socket.address().port;
  await new Promise(resolve => socket.close(resolve));
  return port;
}
function cleanup() {
  return cleanupPromise ??= (async () => {
    await Promise.allSettled([client?.close()]);
    await Promise.allSettled(resources.reverse().map(resource => resource.disconnect()));
    try { if (container) docker("rm", "--force", "--volumes", container); }
    finally { await rm(directory, { recursive: true, force: true }); }
  })();
}
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.once(signal, () => { void cleanup().finally(() => process.exit(130)); });
}

try {
  docker("info", "--format", "{{.ServerVersion}}");
  await chmod(directory, 0o755);
  const keystore = join(directory, "broker.p12"), certificate = join(directory, "broker.pem");
  command("keytool", ["-genkeypair", "-alias", "broker", "-keyalg", "RSA", "-keysize", "2048", "-storetype", "PKCS12",
    "-keystore", keystore, "-storepass", storePassword, "-keypass", storePassword, "-validity", "2",
    "-dname", "CN=localhost", "-ext", "SAN=dns:localhost,ip:127.0.0.1", "-noprompt"]);
  command("keytool", ["-exportcert", "-rfc", "-alias", "broker", "-keystore", keystore, "-storepass", storePassword, "-file", certificate]);
  const port = await freePort();
  const broker = `127.0.0.1:${port}`;
  await writeFile(join(directory, "reader.properties"), [
    "security.protocol=SASL_SSL", "sasl.mechanism=PLAIN",
    `sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="reader" password="${readerPassword}";`,
    "ssl.truststore.location=/tls/broker.p12", "ssl.truststore.type=PKCS12", `ssl.truststore.password=${storePassword}`,
    "request.timeout.ms=5000", "default.api.timeout.ms=10000"
  ].join("\n"));
  const environment = {
    KAFKA_NODE_ID: "1", KAFKA_PROCESS_ROLES: "broker,controller",
    KAFKA_LISTENERS: `CLIENT://:${port},INTERNAL://:19092,CONTROLLER://:9093`,
    KAFKA_ADVERTISED_LISTENERS: `CLIENT://${broker},INTERNAL://127.0.0.1:19092`,
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CLIENT:SASL_SSL,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
    KAFKA_INTER_BROKER_LISTENER_NAME: "INTERNAL", KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER",
    KAFKA_CONTROLLER_QUORUM_VOTERS: "1@127.0.0.1:9093",
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1", KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS: "1", KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1",
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1", KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0",
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false", KAFKA_LOG_DIRS: "/tmp/kafka-data",
    KAFKA_AUTHORIZER_CLASS_NAME: "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
    KAFKA_SUPER_USERS: "User:admin;User:ANONYMOUS", KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: "false",
    KAFKA_SASL_ENABLED_MECHANISMS: "PLAIN",
    KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG: `org.apache.kafka.common.security.plain.PlainLoginModule required user_admin="${adminPassword}" user_reader="${readerPassword}";`,
    KAFKA_SSL_KEYSTORE_LOCATION: "/tls/broker.p12", KAFKA_SSL_KEYSTORE_TYPE: "PKCS12",
    KAFKA_SSL_KEYSTORE_PASSWORD: storePassword, KAFKA_SSL_KEY_PASSWORD: storePassword, KAFKA_SSL_CLIENT_AUTH: "none",
    KAFKA_HEAP_OPTS: "-Xms256m -Xmx512m", CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
  };
  const args = ["create", "--name", name, "--publish", `127.0.0.1:${port}:${port}`];
  for (const [key, value] of Object.entries(environment)) args.push("--env", `${key}=${value}`);
  args.push(image);
  container = command("docker", args, 240000);
  docker("cp", directory, `${container}:/tls`);
  docker("start", container);
  const readyBy = Date.now() + 120000;
  while (true) {
    const ready = spawnSync("docker", ["exec", container, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "127.0.0.1:19092", "--list"], { encoding: "utf8", timeout: 10000 });
    if (ready.status === 0) break;
    if (Date.now() >= readyBy || docker("inspect", "--format", "{{.State.Running}}", container) !== "true") {
      throw new Error(`Kafka did not become ready: ${docker("logs", "--tail", "40", container)}`);
    }
    await wait(1000);
  }
  console.log(`Broker: ${image}; image ID: ${docker("inspect", "--format", "{{.Image}}", container)}`);
  const ca = await readFile(certificate, "utf8");
  const kafka = username => new Kafka({ clientId: `integration-${username}`, brokers: [broker], ssl: { ca: [ca] },
    sasl: { mechanism: "plain", username, password: username === "admin" ? adminPassword : readerPassword },
    logLevel: logLevel.NOTHING, retry: { retries: 0 }, connectionTimeout: 3000, requestTimeout: 5000 });
  const admin = kafka("admin").admin(); resources.push(admin); await admin.connect();
  await admin.createTopics({ topics: [{ topic: "customers", numPartitions: 2, replicationFactor: 1 }, { topic: "restricted", numPartitions: 1, replicationFactor: 1 }] });
  const producer = kafka("admin").producer(); resources.push(producer); await producer.connect();
  await producer.send({ topic: "customers", messages: [
    ...Array.from({ length: 10 }, (_, i) => ({ partition: 0, value: `synthetic-a-${i}` })),
    ...Array.from({ length: 6 }, (_, i) => ({ partition: 1, value: `synthetic-b-${i}` }))
  ] });
  const groupReadyBy = Date.now() + 30000;
  while (true) {
    try {
      await admin.setOffsets({ groupId: "observed-group", topic: "customers", partitions: [{ partition: 0, offset: "7" }, { partition: 1, offset: "2" }] });
      break;
    } catch (error) {
      const starting = error.name === "KafkaJSGroupCoordinatorNotFound" || ["COORDINATOR_NOT_AVAILABLE", "COORDINATOR_LOAD_IN_PROGRESS", "NOT_COORDINATOR"].includes(error.type);
      if (!starting || Date.now() >= groupReadyBy) throw error;
      await wait(500);
    }
  }
  for (const resource of [["--topic", "customers"], ["--group", "observed-group"], ["--group", "uncommitted-group"]]) {
    docker("exec", container, "/opt/kafka/bin/kafka-acls.sh", "--bootstrap-server", "127.0.0.1:19092", "--add", "--allow-principal", "User:reader", "--operation", "Describe", ...resource);
  }

  const serverEnv = { KAFKA_MODE: "live", KAFKA_BROKERS: broker, KAFKA_TLS: "true", KAFKA_SASL_MECHANISM: "plain",
    KAFKA_USERNAME: "reader", KAFKA_PASSWORD: readerPassword, NODE_EXTRA_CA_CERTS: certificate,
    ALLOWED_TOPICS: "customers,restricted", ALLOWED_GROUPS: "observed-group,uncommitted-group,blocked-group" };
  client = new Client({ name: "live-kafka-test", version: "1.0.0" });
  await client.connect(new StdioClientTransport({ command: process.execPath, args: ["dist/server.js"], env: serverEnv }));
  const tools = await client.listTools();
  assert.equal(tools.tools.length, 4);
  assert.ok(tools.tools.every(tool => tool.annotations?.readOnlyHint && !tool.annotations?.destructiveHint));
  pass("live MCP handshake exposes only the four read-only tools");
  const call = (name, args = {}) => client.callTool({ name, arguments: args });
  const result = reply => { assert.equal(reply.isError, undefined); const body = JSON.parse(reply.content[0].text); assert.equal(body.mode, "live"); return body.result; };
  assert.deepEqual(result(await call("list_topics")).topics, ["customers"]);
  assert.deepEqual(result(await call("describe_topic", { topic: "customers" })).partitions.map(p => p.partition).sort(), [0, 1]);
  pass("broker ACLs and tool allowlists filter real topic metadata");
  const lag = result(await call("consumer_lag", { group: "observed-group", topic: "customers" }));
  assert.equal(lag.totalLag, "7"); assert.equal(lag.complete, true);
  assert.deepEqual(lag.partitions.map(p => [p.partition, p.committedOffset, p.logEndOffset, p.lag]).sort(), [[0, "7", "10", "3"], [1, "2", "6", "4"]]);
  pass("real committed/beginning/end offsets yield lag 3 + 4 = 7");
  const missing = result(await call("consumer_lag", { group: "uncommitted-group", topic: "customers" }));
  assert.equal(missing.totalLag, null); assert.ok(missing.partitions.every(p => p.status === "uncommitted"));
  const recovery = result(await call("recovery_plan", { group: "uncommitted-group", topic: "customers" }));
  assert.deepEqual(recovery.automaticActions, []); assert.equal(recovery.warnings.length, 2);
  pass("uncommitted real group remains unknown and recovery performs no actions");
  const denied = await call("describe_topic", { topic: "outside-policy" });
  assert.equal(denied.isError, true); assert.match(denied.content[0].text, /configured allowlist/);
  pass("tool policy denies an unlisted topic");
  for (const [tool, args] of [["describe_topic", { topic: "restricted" }], ["consumer_lag", { group: "blocked-group", topic: "customers" }]]) {
    const reply = await call(tool, args);
    assert.equal(reply.isError, true);
    assert.equal(reply.content[0].text, "Diagnostic failed; check server connectivity and configuration");
  }
  pass("topic/group ACL denial is returned as a redacted diagnostic failure");

  const reader = kafka("reader").admin(); resources.push(reader); await reader.connect();
  const writer = kafka("reader").producer(); resources.push(writer); await writer.connect();
  for (const [label, action] of [
    ["produce", () => writer.send({ topic: "customers", messages: [{ partition: 0, value: "must-not-exist" }] })],
    ["create topic", () => reader.createTopics({ topics: [{ topic: "must-not-exist", numPartitions: 1, replicationFactor: 1 }] })],
    ["delete topic", () => reader.deleteTopics({ topics: ["customers"] })]
  ]) {
    await assert.rejects(action, authorizationFailure, `${label} must fail with a broker authorization error`);
    pass(`least-privilege broker principal cannot ${label}`);
  }
  const reset = spawnSync("docker", ["exec", container, "/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server", broker,
    "--command-config", "/tls/reader.properties", "--group", "observed-group", "--topic", "customers:0", "--reset-offsets", "--to-offset", "0", "--execute"],
    { encoding: "utf8", timeout: 20000 });
  assert.equal(reset.error, undefined, "reset attempt must finish rather than timing out");
  assert.match(reset.stdout + reset.stderr, /GroupAuthorizationException|GROUP_AUTHORIZATION_FAILED/);
  pass("least-privilege broker principal cannot commit/reset offsets");
  assert.deepEqual((await admin.fetchTopicOffsets("customers")).map(p => [p.partition, p.high]).sort(), [[0, "10"], [1, "6"]]);
  assert.equal((await admin.fetchOffsets({ groupId: "observed-group", topics: ["customers"] }))[0].partitions.find(p => p.partition === 0).offset, "7");
  assert.ok(!(await admin.listTopics()).includes("must-not-exist"));
  pass("denied writes leave topics, records and committed offsets unchanged");

  // Separate Node processes verify the adapter's startup failures using its actual TLS path.
  const probe = `import {kafkaInspector} from './dist/kafka.js'; try { const k=await kafkaInspector(process.env); await k.close(); process.exitCode=2; } catch(e) { console.log(e.message); }`;
  for (const [label, patch] of [["wrong SASL password", { KAFKA_PASSWORD: "intentionally-invalid" }], ["untrusted TLS certificate", { NODE_EXTRA_CA_CERTS: "" }]]) {
    const check = spawnSync(process.execPath, ["--input-type=module", "-e", probe], { env: { ...process.env, ...serverEnv, ...patch }, encoding: "utf8", timeout: 20000 });
    assert.equal(check.status, 0, label);
    assert.equal(check.stdout.trim(), "Kafka connection failed; check broker access and credentials", label);
    assert.ok(!check.stderr.includes(readerPassword) && !check.stdout.includes(readerPassword));
    pass(`${label} fails closed without exposing credentials`);
  }
  console.log(`PASS: ${checks} live Kafka/MCP checks on ${process.version}`);
} finally {
  await cleanup();
}
