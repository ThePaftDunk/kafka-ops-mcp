import { readFile } from "node:fs/promises";
import { z } from "zod";
import type { Inspector } from "./domain.js";
const numeric = z.string().regex(/^\d+$/);
const fixtureSchema = z.object({
  topics: z.array(z.object({ name: z.string(), partitions: z.array(z.object({ partition: z.number().int().nonnegative(), leader: z.number().int(), replicas: z.array(z.number().int()), isr: z.array(z.number().int()) })) })),
  bounds: z.record(z.array(z.object({ partition: z.number().int().nonnegative(), low: numeric, high: numeric }))),
  groups: z.record(z.record(z.array(z.object({ partition: z.number().int().nonnegative(), offset: z.string().regex(/^-1$|^\d+$/) }))))
});
export async function fixtureInspector(path: string): Promise<Inspector> {
  const data = fixtureSchema.parse(JSON.parse(await readFile(path, "utf8")));
  return {
    async listTopics() { return data.topics.map(t => t.name); },
    async describeTopic(topic) { const found = data.topics.find(t => t.name === topic); if (!found) throw new Error("Unknown topic"); return found; },
    async bounds(topic) { const found = data.bounds[topic]; if (!found) throw new Error("Unknown topic"); return found; },
    async committed(group, topic) { return data.groups[group]?.[topic] ?? []; },
    async close() {}
  };
}
