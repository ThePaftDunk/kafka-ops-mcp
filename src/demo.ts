import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
const client = new Client({ name: "portfolio-demo", version: "1.0.0" });
await client.connect(new StdioClientTransport({ command: process.execPath, args: ["dist/server.js"], env: { KAFKA_MODE: "fixture" } }));
try {
  const tools = await client.listTools();
  console.log("Available read-only tools:", tools.tools.map(t => t.name).join(", "));
  for (const [name, args] of [["list_topics", {}], ["consumer_lag", { group: "cdc-recovery-lab", topic: "customers" }], ["recovery_plan", { group: "cdc-recovery-lab", topic: "customers" }]] as const) {
    const result = await client.callTool({ name, arguments: args });
    console.log(`\n${name}:\n${JSON.stringify(result, null, 2)}`);
    if (result.isError) throw new Error("Demo tool call failed");
  }
} finally { await client.close(); }
