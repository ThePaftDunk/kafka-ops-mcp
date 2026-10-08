import test from "node:test";
import assert from "node:assert/strict";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
test("real stdio MCP handshake, tool discovery, permissions and results", async () => {
  const client = new Client({ name: "integration-test", version: "1.0.0" });
  await client.connect(new StdioClientTransport({ command: process.execPath, args: ["dist/server.js"], env: { KAFKA_MODE: "fixture" } }));
  try {
    const tools = await client.listTools(); assert.equal(tools.tools.length, 4);
    assert.ok(tools.tools.every(t => t.annotations?.readOnlyHint && !t.annotations?.destructiveHint));
    const result = await client.callTool({ name: "consumer_lag", arguments: { group: "cdc-recovery-lab", topic: "customers" } });
    assert.equal(result.isError, undefined);
    const content = result.content as {type: string; text: string}[];
    const body = JSON.parse(content[0]!.text);
    assert.equal(body.mode, "fixture"); assert.equal(body.result.totalLag, "500");
    const denied = await client.callTool({ name: "describe_topic", arguments: { topic: "private-payments" } });
    assert.equal(denied.isError, true);
    const invalid = await client.callTool({ name: "consumer_lag", arguments: { topic: "customers" } });
    assert.equal(invalid.isError, true);
  } finally { await client.close(); }
});
