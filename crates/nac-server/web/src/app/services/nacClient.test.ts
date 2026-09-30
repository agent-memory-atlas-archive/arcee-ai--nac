import { describe, expect, it, vi } from "vitest";

import {
  ApiError,
  createNacClient,
  NacClientConfigurationError,
  NacVersionMismatchError,
} from "@/app/services/nacClient";

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

const readiness = {
  status: "ok",
  managed: false,
  version: "0.1.4",
  schema_version: 20,
  product_version: "0.1.4",
  build_id: "dev",
  build_track: "dev",
  source_revision: "b216e148",
  supported_schema_version: 20,
  minimum_migratable_schema_version: 1,
  opened_schema_version: 20,
  migration_state: "current",
  migration_failure: null,
  maintenance_state: "serving",
  checks: [],
};

describe("endpoint and credential policy", () => {
  it("requires an explicit policy for an absolute endpoint", () => {
    expect(() => createNacClient({ endpoint: "https://nac.example" })).toThrow(
      NacClientConfigurationError,
    );
  });

  it("centralizes endpoint, auth, launch headers, request ids, and credentials", async () => {
    const fetch = vi.fn().mockResolvedValue(json(readiness));
    const client = createNacClient({
      endpoint: "https://nac.example/runtime/v1",
      credentials: "include",
      authorization: { kind: "bearer", token: "launch-token" },
      headers: { "X-NAC-Launch": "launch-1" },
      requestId: () => "request-1",
      fetch,
    });

    await client.getReadiness();

    expect(fetch).toHaveBeenCalledExactlyOnceWith(
      "https://nac.example/runtime/v1/readyz",
      expect.objectContaining({
        method: "GET",
        credentials: "include",
        headers: {
          authorization: "Bearer launch-token",
          "x-nac-launch": "launch-1",
          "x-nac-request-id": "request-1",
        },
      }),
    );
    expect(() => client.transport.eventSourceInit()).toThrow(/cannot send bearer authorization/);
  });

  it("does not silently drop launch headers on native EventSource", () => {
    const client = createNacClient({ headers: { "X-NAC-Launch": "launch-1" } });
    expect(() => client.transport.eventSourceInit()).toThrow(/cannot send launch headers/);
  });

  it("decodes structured errors with the correlated request id", async () => {
    const client = createNacClient({
      fetch: vi.fn().mockResolvedValue(json({ title: "Request denied" }, 403)),
      requestId: () => "request-denied",
    });

    await expect(client.getReadiness()).rejects.toEqual(
      expect.objectContaining({
        name: "ApiError",
        status: 403,
        requestId: "request-denied",
        message: "Request denied (HTTP 403)",
      }),
    );
  });
});

describe("build compatibility", () => {
  it("accepts the exact tested product version and source revision", async () => {
    const client = createNacClient({
      fetch: vi.fn().mockResolvedValue(json(readiness)),
      version: { productVersion: "0.1.4", sourceRevision: "b216e148" },
    });
    await expect(client.checkCompatibility()).resolves.toMatchObject(readiness);
  });

  it("returns an actionable mismatch with expected and actual identities", async () => {
    const client = createNacClient({
      fetch: vi.fn().mockResolvedValue(json(readiness)),
      version: { productVersion: "0.2.0" },
    });
    const error = await client.checkCompatibility().catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(NacVersionMismatchError);
    expect(error).toEqual(
      expect.objectContaining({
        name: "NacVersionMismatchError",
        message: "NAC 0.1.4 is incompatible with client target 0.2.0.",
      }),
    );
  });
});

describe("command admission", () => {
  it("never retries a response-lost prompt and reports uncertain admission", async () => {
    const fetch = vi.fn().mockRejectedValue(new TypeError("connection reset"));
    const client = createNacClient({ fetch, requestId: () => "command-1" });

    await expect(client.submitPrompt("session", "hello")).resolves.toEqual({
      status: "uncertain",
      requestId: "command-1",
      error: expect.any(TypeError),
    });
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("distinguishes a pre-aborted command that was definitely not sent", async () => {
    const fetch = vi.fn();
    const client = createNacClient({ fetch, requestId: () => "command-2" });
    const controller = new AbortController();
    controller.abort();

    await expect(client.submitPrompt("session", "hello", controller.signal)).resolves.toEqual({
      status: "not-sent",
      requestId: "command-2",
      reason: "aborted",
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("keeps HTTP rejection distinct from network uncertainty", async () => {
    const client = createNacClient({
      fetch: vi.fn().mockResolvedValue(json({ error: "busy" }, 409)),
      requestId: () => "command-3",
    });
    await expect(client.submitPrompt("session", "hello")).rejects.toBeInstanceOf(ApiError);
  });
});

describe("snapshot plus cursor replay", () => {
  it("captures a pre-snapshot cursor, removes duplicates, and drains replay pages", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(json({ boundary: { epoch_id: "epoch", sequence_id: 2 }, events: [] }))
      .mockResolvedValueOnce(json({ summary: { session_id: "session" } }))
      .mockResolvedValueOnce(
        json({
          boundary: { epoch_id: "epoch", sequence_id: 5 },
          events: [3, 3, 4].map((sequence_id) => ({
            session_id: "session",
            epoch_id: "epoch",
            sequence_id,
            event: { type: "run_failed", message: "failed" },
          })),
        }),
      )
      .mockResolvedValueOnce(
        json({
          boundary: { epoch_id: "epoch", sequence_id: 5 },
          events: [
            {
              session_id: "session",
              epoch_id: "epoch",
              sequence_id: 5,
              event: { type: "run_failed", message: "failed" },
            },
          ],
        }),
      );
    const client = createNacClient({ fetch });

    const capture = await client.captureSessionSnapshot("session", {}, { pageSize: 3 });

    expect(capture.baseline).toEqual({ epoch_id: "epoch", sequence_id: 2 });
    expect(capture.replay).toMatchObject({
      status: "complete",
      cursor: { epoch_id: "epoch", sequence_id: 5 },
      duplicates: 1,
    });
    expect(capture.replay.events.map((event) => event.sequence_id)).toEqual([3, 4, 5]);
  });

  it("makes a missing sequence explicit", async () => {
    const client = createNacClient({
      fetch: vi.fn().mockResolvedValue(
        json({
          boundary: { epoch_id: "epoch", sequence_id: 4 },
          events: [
            {
              session_id: "session",
              epoch_id: "epoch",
              sequence_id: 4,
              event: { type: "run_failed", message: "failed" },
            },
          ],
        }),
      ),
    });

    await expect(
      client.replaySessionEvents("session", { epoch_id: "epoch", sequence_id: 2 }),
    ).resolves.toMatchObject({ status: "gap", missing: { from: 3, to: 3 } });
  });

  it("stops at the configured replay budget instead of hiding backpressure", async () => {
    const client = createNacClient({
      fetch: vi.fn().mockResolvedValue(
        json({
          boundary: { epoch_id: "epoch", sequence_id: 8 },
          events: [3, 4].map((sequence_id) => ({
            session_id: "session",
            epoch_id: "epoch",
            sequence_id,
            event: { type: "run_failed", message: "failed" },
          })),
        }),
      ),
    });

    await expect(
      client.replaySessionEvents(
        "session",
        { epoch_id: "epoch", sequence_id: 2 },
        { maxEvents: 2 },
      ),
    ).resolves.toMatchObject({
      status: "backpressure",
      cursor: { epoch_id: "epoch", sequence_id: 4 },
      boundary: { epoch_id: "epoch", sequence_id: 8 },
    });
  });
});
