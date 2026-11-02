import type { Int64 } from "../api/types";

export interface PartitionLookupRequest {
  specId: Int64;
  values: (string | null)[];
  snapshot?: Int64;
}

const MAX_INT64 = 9_223_372_036_854_775_807n;

function id(raw: string, name: string, positive: boolean): Int64 {
  if (!/^\d+$/.test(raw) || raw.length > 19 || BigInt(raw) > MAX_INT64 ||
    (positive && BigInt(raw) === 0n)) {
    throw new Error(`${name} must be ${positive ? "a positive" : "a non-negative"} int64.`);
  }
  return BigInt(raw).toString();
}

export function parsePartitionLookupInput(
  spec: string,
  tuple: string,
  snapshot: string,
): PartitionLookupRequest {
  const specId = id(spec.trim(), "Spec ID", true);
  const at = snapshot.trim() === "" ? undefined : id(snapshot.trim(), "Snapshot", false);
  if (tuple.length > 65_536) throw new Error("The tuple is limited to 65,536 characters.");
  let values: unknown;
  try {
    values = JSON.parse(tuple);
  } catch {
    throw new Error("Enter the complete tuple as a JSON array of strings or nulls.");
  }
  if (!Array.isArray(values) || values.length < 1 || values.length > 64 ||
    values.some((value) => value !== null && typeof value !== "string")) {
    throw new Error("Enter a JSON array containing 1–64 strings or nulls.");
  }
  for (const value of values as (string | null)[]) {
    if (value === null) continue;
    if (value.length > 8192) throw new Error("Each partition value is limited to 8,192 characters.");
    for (let i = 0; i < value.length; i++) {
      const code = value.charCodeAt(i);
      if (code === 0 || (code >= 0xdc00 && code <= 0xdfff)) {
        throw new Error("Partition values must be valid Unicode without NUL.");
      }
      if (code >= 0xd800 && code <= 0xdbff) {
        const next = value.charCodeAt(++i);
        if (!(next >= 0xdc00 && next <= 0xdfff)) {
          throw new Error("Partition values must be valid Unicode without NUL.");
        }
      }
    }
  }
  return { specId, values: values as (string | null)[], snapshot: at };
}

export function readPartitionLookupRequest(params: URLSearchParams): {
  request: PartitionLookupRequest | null;
  error: string | null;
} {
  try {
    const snapshotNames = params.get("lookup_at") === "head" ? ["lookup_snapshot"] : ["snapshot", "lookup_snapshot"];
    for (const name of ["lookup_spec", "lookup_values", "lookup_at", ...snapshotNames]) {
      if (params.getAll(name).length > 1) throw new Error(`The link repeats ${name}; enter one value.`);
    }
    for (const name of snapshotNames) {
      const value = params.get(name);
      if (value !== null) id(value, "Snapshot", false);
    }
    const intent = params.get("lookup_at") ?? params.get("snapshot");
    if (intent !== null && intent !== "head") id(intent, "Snapshot", false);
    const snapshot = params.get("lookup_snapshot") ?? (intent === "head" ? null : intent);
    if (!params.has("lookup_spec") && !params.has("lookup_values")) return { request: null, error: null };
    const spec = params.get("lookup_spec");
    const tuple = params.get("lookup_values");
    if (spec === null || tuple === null) throw new Error("The link must include both a spec ID and a complete tuple.");
    id(spec, "Spec ID", true);
    return { request: parsePartitionLookupInput(spec, tuple, snapshot ?? ""), error: null };
  } catch (error) {
    return { request: null, error: error instanceof Error ? error.message : String(error) };
  }
}

export function partitionLookupParams(params: URLSearchParams, request: PartitionLookupRequest): URLSearchParams {
  const next = new URLSearchParams(params);
  next.set("tab", "partitions");
  next.set("lookup", "1");
  next.set("lookup_spec", request.specId);
  next.set("lookup_values", JSON.stringify(request.values));
  if (request.snapshot === undefined) next.delete("lookup_snapshot");
  else next.set("lookup_snapshot", request.snapshot);
  return next;
}
