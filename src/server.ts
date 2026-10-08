import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { AccessPolicy, Diagnostics } from "./domain.js";
import { fixtureInspector } from "./fixture.js";
import { kafkaInspector } from "./kafka.js";
const mode = process.env.KAFKA_MODE ?? "fixture";
if (!["fixture", "live"].includes(mode)) throw new Error("KAFKA_MODE must be fixture or live");
const defaults = mode === "fixture";
const entries = (value: string | undefined) => (value ?? "").split(",").map(x => x.trim()).filter(Boolean);
// In live mode an omitted policy fails closed, before connecting to the broker.
const policy = new AccessPolicy(entries(process.env.ALLOWED_TOPICS ?? (defaults ? "customers" : undefined)), entries(process.env.ALLOWED_GROUPS ?? (defaults ? "cdc-recovery-lab" : undefined)));
const inspector = defaults ? await fixtureInspector(process.env.KAFKA_FIXTURE ?? "fixtures/cluster.json") : await kafkaInspector(process.env);
const diagnostics = new Diagnostics(inspector, policy);
const server = new McpServer({ name: "kafka-ops-mcp", version: "1.0.0" });
const annotations = { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false };
const name = z.string().min(1).max(249);
async function reply(action: () => Promise<unknown>) {
  try {
    const result = await action();
    return { content: [{ type: "text" as const, text: JSON.stringify({ mode, result }, null, 2) }] };
  } catch (error) {
    // Only known local policy messages are returned; arbitrary broker errors may contain infrastructure details.
    const message = error instanceof Error && error.message.includes("configured allowlist") ? error.message : "Diagnostic failed; check server connectivity and configuration";
    return { isError: true, content: [{ type: "text" as const, text: message }] };
  }
}
server.registerTool("list_topics", { description: "List accessible topic names within the explicit allowlist. No messages are read.", inputSchema: {}, annotations }, () => reply(() => diagnostics.topics()));
server.registerTool("describe_topic", { description: "Read partition leaders, replicas and ISR for an allowlisted topic.", inputSchema: { topic: name }, annotations }, ({ topic }) => reply(() => diagnostics.describe(topic)));
server.registerTool("consumer_lag", { description: "Measure offset distance per partition with exact integer arithmetic. Missing commits and retention loss are explicit; no offsets are changed.", inputSchema: { group: name, topic: name }, annotations }, ({ group, topic }) => reply(() => diagnostics.lag(group, topic)));
server.registerTool("recovery_plan", { description: "Generate conservative diagnostic steps from observed offsets. This is guidance, not a diagnosis or an action; it cannot reset offsets.", inputSchema: { group: name, topic: name }, annotations }, ({ group, topic }) => reply(() => diagnostics.recoveryPlan(group, topic)));
await server.connect(new StdioServerTransport());
let closing = false;
async function close() { if (closing) return; closing = true; await server.close(); await inspector.close(); }
process.once("SIGINT", () => { void close(); });
process.once("SIGTERM", () => { void close(); });
process.stdin.once("end", () => { void close(); });
