import test from "node:test";
import assert from "node:assert/strict";
import { AccessPolicy, calculateLag, Diagnostics, type Inspector } from "../domain.js";
test("offsets above Number.MAX_SAFE_INTEGER are exact", () => {
  const result = calculateLag([{ partition: 0, low: "9007199254740993", high: "9007199254741993" }], [{ partition: 0, offset: "9007199254741743" }]);
  assert.equal(result.totalLag, "250"); assert.equal(result.partitions[0]?.committedOffset, "9007199254741743");
  assert.deepEqual(result.unobservedCommittedPartitions, []);
});
test("missing commits do not silently look like zero lag", () => {
  const result = calculateLag([{ partition: 0, low: "100", high: "150" }], [{ partition: 0, offset: "-1" }]);
  assert.equal(result.totalLag, null); assert.equal(result.complete, false); assert.equal(result.partitions[0]?.status, "uncommitted");
});
test("retention loss and truncation are explicit", () => {
  const bounds = [{ partition: 0, low: "100", high: "200" }, { partition: 1, low: "0", high: "10" }];
  const result = calculateLag(bounds, [{ partition: 0, offset: "50" }, { partition: 1, offset: "20" }]);
  assert.equal(result.partitions[0]?.status, "behind_retention"); assert.equal(result.partitions[1]?.status, "ahead_of_log"); assert.equal(result.totalLag, null);
});
test("invalid broker offsets fail", () => {
  assert.throws(() => calculateLag([], []));
  assert.throws(() => calculateLag([{ partition: 0, low: "0", high: "1" }, { partition: 0, low: "0", high: "1" }], []));
  assert.throws(() => calculateLag([{ partition: 0, low: "3", high: "2" }], []));
  assert.throws(() => calculateLag([{ partition: 0, low: "0", high: "NaN" }], []));
});
test("policy fails closed before any broker operation", async () => {
  assert.throws(() => new AccessPolicy([], [])); assert.throws(() => new AccessPolicy(["*"], ["g"]));
  let calls = 0;
  const inspector: Inspector = { async listTopics() { calls++; return ["customers", "private"]; }, async describeTopic() { calls++; throw new Error(); }, async bounds() { calls++; return []; }, async committed() { calls++; return []; }, async close() {} };
  const diagnostics = new Diagnostics(inspector, new AccessPolicy(["customers"], ["group"]));
  await assert.rejects(diagnostics.describe("private"), /allowlist/);
  await assert.rejects(diagnostics.lag("outside", "customers"), /allowlist/); assert.equal(calls, 0);
  assert.deepEqual(await diagnostics.topics(), { topics: ["customers"] });
});
test("committed partitions without bounds make the observation incomplete", () => {
  const result = calculateLag([{ partition: 0, low: "0", high: "100" }], [
    { partition: 2, offset: "9007199254741743" },
    { partition: 0, offset: "75" },
    { partition: 1, offset: "-1" }
  ]);
  assert.equal(result.complete, false);
  assert.equal(result.totalLag, null);
  assert.equal(result.knownPartitionLag, "25");
  assert.deepEqual(result.unobservedCommittedPartitions, [
    { partition: 1, committedOffset: null },
    { partition: 2, committedOffset: "9007199254741743" }
  ]);
  assert.equal(result.partitions[0]?.status, "ok");
});
test("missing commits retain their meaning alongside unmatched committed partitions", () => {
  const result = calculateLag([{ partition: 0, low: "10", high: "20" }], [{ partition: 1, offset: "5" }]);
  assert.equal(result.partitions[0]?.status, "uncommitted");
  assert.equal(result.totalLag, null);
  assert.equal(result.knownPartitionLag, "0");
  assert.deepEqual(result.unobservedCommittedPartitions, [{ partition: 1, committedOffset: "5" }]);
});
test("invalid partition identities fail on either read", () => {
  for (const partition of [-1, .5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
    assert.throws(() => calculateLag([{ partition, low: "0", high: "1" }], []), /partition/i);
    assert.throws(() => calculateLag([{ partition: 0, low: "0", high: "1" }], [{ partition, offset: "0" }]), /partition/i);
  }
});
test("unmatched committed partitions cannot hide malformed offsets", () => {
  for (const bad of ["NaN", "-2"]) {
    assert.throws(() => calculateLag([{ partition: 0, low: "0", high: "1" }], [{ partition: 1, offset: bad }]), /offset/i);
  }
});
test("recovery guidance treats unmatched reads as uncertainty rather than retention loss", async () => {
  const inspector: Inspector = {
    async listTopics() { return ["customers"]; },
    async describeTopic() { return { name: "customers", partitions: [] }; },
    async bounds() { return [{ partition: 0, low: "0", high: "100" }]; },
    async committed() { return [{ partition: 0, offset: "100" }, { partition: 1, offset: "20" }]; },
    async close() {}
  };
  const result = await new Diagnostics(inspector, new AccessPolicy(["customers"], ["group"])).recoveryPlan("group", "customers");
  assert.equal(result.totalLag, null);
  assert.equal(result.complete, false);
  assert.ok(result.warnings.some(warning => /without observed bounds: 1/.test(warning)));
  assert.ok(result.steps.some(step => /Re-read topic metadata, bounds and commits/.test(step)));
  assert.ok(result.steps.every(step => !step.includes("fresh snapshot")));
  assert.deepEqual(result.automaticActions, []);
});
