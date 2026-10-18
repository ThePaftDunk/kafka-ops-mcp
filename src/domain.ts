export type Bounds = { partition: number; low: string; high: string };
export type Committed = { partition: number; offset: string };
export type TopicInfo = { name: string; partitions: { partition: number; leader: number; replicas: number[]; isr: number[] }[] };
export interface Inspector {
  listTopics(): Promise<string[]>;
  describeTopic(topic: string): Promise<TopicInfo>;
  bounds(topic: string): Promise<Bounds[]>;
  committed(group: string, topic: string): Promise<Committed[]>;
  close(): Promise<void>;
}
export class AccessPolicy {
  readonly topics: ReadonlySet<string>;
  readonly groups: ReadonlySet<string>;
  constructor(topics: string[], groups: string[]) {
    if (!topics.length || !groups.length) throw new Error("Explicit topic and group allowlists are required");
    for (const name of [...topics, ...groups]) if (!/^[A-Za-z0-9._-]{1,249}$/.test(name) || name === "." || name === "..") throw new Error("Invalid allowlist entry");
    this.topics = new Set(topics); this.groups = new Set(groups);
  }
  topic(name: string): void { if (!this.topics.has(name)) throw new Error("Topic is outside the configured allowlist"); }
  group(name: string): void { if (!this.groups.has(name)) throw new Error("Consumer group is outside the configured allowlist"); }
}
function offset(value: string): bigint {
  if (!/^\d+$/.test(value)) throw new Error("Broker returned an invalid offset");
  return BigInt(value);
}
function validatePartition(value: number): void {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error("Broker returned an invalid partition identity");
}
export function calculateLag(bounds: Bounds[], commits: Committed[]) {
  if (!bounds.length) throw new Error("Topic has no observable partitions");
  for (const value of bounds) validatePartition(value.partition);
  for (const value of commits) {
    validatePartition(value.partition);
    if (value.offset !== "-1") offset(value.offset);
  }
  if (new Set(bounds.map(p => p.partition)).size !== bounds.length || new Set(commits.map(p => p.partition)).size !== commits.length) throw new Error("Duplicate partition metadata");
  const byPartition = new Map(commits.map(value => [value.partition, value.offset]));
  const observedPartitions = new Set(bounds.map(value => value.partition));
  const unobservedCommittedPartitions = commits.filter(value => !observedPartitions.has(value.partition))
    .map(value => ({ partition: value.partition, committedOffset: value.offset === "-1" ? null : offset(value.offset).toString() }))
    .sort((left, right) => left.partition - right.partition);
  const partitions = bounds.map(value => {
    const low = offset(value.low), high = offset(value.high);
    if (high < low) throw new Error("Broker offset bounds are inconsistent");
    const committed = byPartition.get(value.partition);
    const base = { partition: value.partition, logStartOffset: low.toString(), logEndOffset: high.toString() };
    if (committed === undefined || committed === "-1") return { ...base, committedOffset: null, lag: null, status: "uncommitted" as const, availableOffsetSpan: (high-low).toString() };
    const next = offset(committed);
    if (next < low) return { ...base, committedOffset: next.toString(), lag: null, status: "behind_retention" as const, missingOffsetSpan: (low-next).toString() };
    if (next > high) return { ...base, committedOffset: next.toString(), lag: null, status: "ahead_of_log" as const };
    return { ...base, committedOffset: next.toString(), lag: (high-next).toString(), status: "ok" as const };
  });
  const knownTotal = partitions.reduce((sum, item) => sum + BigInt(item.lag ?? "0"), 0n);
  const complete = unobservedCommittedPartitions.length === 0 && partitions.every(item => item.status === "ok");
  return { partitions, unobservedCommittedPartitions, totalLag: complete ? knownTotal.toString() : null, knownPartitionLag: knownTotal.toString(), complete, units: "offset distance; may differ from record count on compacted/transactional topics" };
}
export class Diagnostics {
  constructor(readonly inspector: Inspector, readonly policy: AccessPolicy) {}
  async topics() {
    return { topics: (await this.inspector.listTopics()).filter(topic => this.policy.topics.has(topic)).sort() };
  }
  async describe(topic: string) { this.policy.topic(topic); return this.inspector.describeTopic(topic); }
  async lag(group: string, topic: string) {
    this.policy.topic(topic); this.policy.group(group);
    const [bounds, commits] = await Promise.all([this.inspector.bounds(topic), this.inspector.committed(group, topic)]);
    return { group, topic, observedAt: new Date().toISOString(), ...calculateLag(bounds, commits), consistency: "Bounds and commits are separate broker reads; this is an observational snapshot." };
  }
  async recoveryPlan(group: string, topic: string) {
    const lag = await this.lag(group, topic);
    const partitionWarnings = lag.partitions.filter(p => p.status !== "ok").map(p => `Partition ${p.partition}: ${p.status}`);
    const unmatched = lag.unobservedCommittedPartitions;
    const warnings = [...partitionWarnings, ...(unmatched.length
      ? [`Committed offsets reference partitions without observed bounds: ${unmatched.map(p => p.partition).join(", ")}.`] : [])];
    return { ...lag, warnings, steps: [
      "Compare the sink's durable checkpoint with the Kafka committed next offset before changing anything.",
      "Check consumer assignment, recent failures and sink availability; high lag alone does not establish the root cause.",
      ...(unmatched.length ? ["Re-read topic metadata, bounds and commits; verify topic identity and the partition set before interpreting lag. Unmatched reads alone do not prove retention loss or corruption."] : []),
      ...(partitionWarnings.length ? ["Investigate retention/truncation or missing commits. A fresh snapshot may be required; do not blindly reset offsets."] : unmatched.length ? [] : ["Measure lag over time and verify throughput before deciding whether to scale consumers."]),
      "Rehearse recovery in a separate environment and verify target idempotency. This tool performs no recovery actions."
    ], automaticActions: [] };
  }
}
