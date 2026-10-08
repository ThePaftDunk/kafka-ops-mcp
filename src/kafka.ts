import { Kafka, logLevel, type SASLOptions } from "kafkajs";
import type { Inspector } from "./domain.js";
export async function kafkaInspector(env: NodeJS.ProcessEnv): Promise<Inspector> {
  const brokers = env.KAFKA_BROKERS?.split(",").map(x => x.trim()).filter(Boolean);
  if (!brokers?.length) throw new Error("KAFKA_BROKERS is required for live mode");
  let sasl: SASLOptions | undefined;
  if (env.KAFKA_USERNAME || env.KAFKA_PASSWORD) {
    const mechanism = env.KAFKA_SASL_MECHANISM ?? "scram-sha-512";
    if (!env.KAFKA_USERNAME || !env.KAFKA_PASSWORD || !["plain", "scram-sha-256", "scram-sha-512"].includes(mechanism)) throw new Error("Incomplete or unsupported SASL configuration");
    if (env.KAFKA_TLS !== "true") throw new Error("SASL credentials require TLS");
    sasl = { mechanism: mechanism as "plain" | "scram-sha-256" | "scram-sha-512", username: env.KAFKA_USERNAME, password: env.KAFKA_PASSWORD };
  }
  const kafka = new Kafka({ clientId: "kafka-ops-mcp", brokers, ssl: env.KAFKA_TLS === "true", sasl, logLevel: logLevel.NOTHING, connectionTimeout: 3000, requestTimeout: 5000, retry: { retries: 1 } });
  const admin = kafka.admin();
  try { await admin.connect(); } catch { await admin.disconnect().catch(() => {}); throw new Error("Kafka connection failed; check broker access and credentials"); }
  return {
    async listTopics() { return admin.listTopics(); },
    async describeTopic(topic) {
      const result = await admin.fetchTopicMetadata({ topics: [topic] });
      const found = result.topics.find(t => t.name === topic); if (!found) throw new Error("Unknown topic");
      return { name: found.name, partitions: found.partitions.map(p => ({ partition: p.partitionId, leader: p.leader, replicas: p.replicas, isr: p.isr })) };
    },
    async bounds(topic) { return (await admin.fetchTopicOffsets(topic)).map(p => ({ partition: p.partition, low: p.low, high: p.high })); },
    async committed(group, topic) { return (await admin.fetchOffsets({ groupId: group, topics: [topic] })).find(t => t.topic === topic)?.partitions.map(p => ({ partition: p.partition, offset: p.offset })) ?? []; },
    async close() { await admin.disconnect(); }
  };
}
