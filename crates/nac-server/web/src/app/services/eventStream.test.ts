/** @vitest-environment jsdom */

import { afterEach, beforeEach, expect, it, vi } from "vitest";

import { subscribeToSessionEvents } from "@/app/services/eventStream";
import { createNacClient } from "@/app/services/nacClient";

// The real api module is import-safe (its eventStreamUrl is a pure function)
// and the real perfDebug is inert unless enabled, so the only fake the stream
// needs is the EventSource global, which jsdom does not implement.
class FakeEventSource {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSED = 2;
  static instances: FakeEventSource[] = [];

  readonly url: string;
  readonly init: EventSourceInit | undefined;
  readyState = FakeEventSource.CONNECTING;
  onopen: (() => void) | null = null;
  onerror: (() => void) | null = null;
  private listeners = new Map<string, (event: MessageEvent<string>) => void>();

  constructor(url: string, init?: EventSourceInit) {
    this.url = url;
    this.init = init;
    FakeEventSource.instances.push(this);
  }

  addEventListener(name: string, listener: EventListenerOrEventListenerObject) {
    // SAFETY: the fake only ever emits MessageEvents, so a listener registered
    // for one is invoked with exactly that shape.
    this.listeners.set(name, listener as (event: MessageEvent<string>) => void);
  }

  emit<T>(name: string, value: T) {
    const event = new MessageEvent<string>(name, {
      data: JSON.stringify(value),
    });
    this.listeners.get(name)?.(event);
  }

  close() {
    this.readyState = FakeEventSource.CLOSED;
  }
}

beforeEach(() => {
  vi.useFakeTimers();
  FakeEventSource.instances = [];
  vi.stubGlobal("EventSource", FakeEventSource);
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

it("uses an explicit remote endpoint and cookie credential policy", () => {
  const client = createNacClient({
    endpoint: "https://nac.example/runtime/v1",
    credentials: "include",
  });
  const dispose = subscribeToSessionEvents("session:a", { onEnvelope: vi.fn() }, { client });

  expect(FakeEventSource.instances[0]).toMatchObject({
    url: "https://nac.example/runtime/v1/sessions/session%3Aa/events/stream",
    init: { withCredentials: true },
  });
  dispose();
});

it("passes bearer and launch context to an injected stream adapter", async () => {
  const client = createNacClient({
    endpoint: "https://nac.example/runtime/v1",
    credentials: "omit",
    authorization: { kind: "bearer", token: "gateway-token" },
    headers: async () => ({ "X-NAC-Launch": "launch-1" }),
    requestId: () => "stream-request-1",
  });
  const eventSource = vi.fn((url: string, init: EventSourceInit) => {
    return new FakeEventSource(url, init) as unknown as EventSource;
  });

  const dispose = subscribeToSessionEvents(
    "session-a",
    { onEnvelope: vi.fn() },
    { client, eventSource },
  );

  await vi.waitFor(() => expect(eventSource).toHaveBeenCalledOnce());
  expect(eventSource).toHaveBeenCalledWith(
    "https://nac.example/runtime/v1/sessions/session-a/events/stream",
    { withCredentials: false },
    {
      credentials: "omit",
      headers: {
        authorization: "Bearer gateway-token",
        "x-nac-launch": "launch-1",
        "x-nac-request-id": "stream-request-1",
      },
      requestId: "stream-request-1",
    },
  );
  dispose();
});

it("reconnects with the epoch and sequence as one cursor", async () => {
  const dispose = subscribeToSessionEvents("session-a", {
    onEnvelope: vi.fn(),
  });
  const first = FakeEventSource.instances[0];
  first.readyState = FakeEventSource.OPEN;
  first.onopen?.();
  first.emit("replay_boundary", {
    epoch_id: "epoch-a",
    replay_boundary_sequence_id: 0,
  });
  first.emit("session_event", {
    session_id: "session-a",
    epoch_id: "epoch-a",
    sequence_id: 7,
    event: { type: "run_failed", message: "failed" },
  });
  await Promise.resolve();

  first.onerror?.();
  expect(first.readyState).toBe(FakeEventSource.CLOSED);
  await vi.advanceTimersByTimeAsync(1_000);

  expect(FakeEventSource.instances).toHaveLength(2);
  expect(FakeEventSource.instances[1].url).toBe(
    "/sessions/session-a/events/stream?after_epoch_id=epoch-a&after_sequence_id=7",
  );
  dispose();
});

it("replaces an old-epoch cursor with the new replay boundary", async () => {
  const dispose = subscribeToSessionEvents("session-a", {
    onEnvelope: vi.fn(),
  });
  const first = FakeEventSource.instances[0];
  first.emit("session_event", {
    session_id: "session-a",
    epoch_id: "epoch-a",
    sequence_id: 7,
    event: { type: "run_failed", message: "failed" },
  });
  await Promise.resolve();
  first.emit("replay_boundary", {
    epoch_id: "epoch-b",
    replay_boundary_sequence_id: 2,
  });

  first.onerror?.();
  await vi.advanceTimersByTimeAsync(1_000);

  expect(FakeEventSource.instances[1].url).toBe(
    "/sessions/session-a/events/stream?after_epoch_id=epoch-b&after_sequence_id=2",
  );
  dispose();
});

it("does not advance a same-epoch cursor past replayed events", async () => {
  const dispose = subscribeToSessionEvents("session-a", {
    onEnvelope: vi.fn(),
  });
  const first = FakeEventSource.instances[0];
  first.emit("session_event", {
    session_id: "session-a",
    epoch_id: "epoch-a",
    sequence_id: 3,
    event: { type: "run_failed", message: "failed" },
  });
  await Promise.resolve();
  first.emit("replay_boundary", {
    epoch_id: "epoch-a",
    replay_boundary_sequence_id: 7,
  });

  first.onerror?.();
  await vi.advanceTimersByTimeAsync(1_000);

  expect(FakeEventSource.instances[1].url).toBe(
    "/sessions/session-a/events/stream?after_epoch_id=epoch-a&after_sequence_id=3",
  );
  dispose();
});

it("keeps the cursor absent when no event has been observed", async () => {
  const dispose = subscribeToSessionEvents("session-a", {
    onEnvelope: vi.fn(),
  });
  const first = FakeEventSource.instances[0];
  first.emit("replay_boundary", {
    epoch_id: "epoch-a",
    replay_boundary_sequence_id: 7,
  });

  first.onerror?.();
  await vi.advanceTimersByTimeAsync(1_000);

  expect(FakeEventSource.instances[1].url).toBe("/sessions/session-a/events/stream");
  dispose();
});

it("drops a duplicate without delivering it twice", async () => {
  const onEnvelope = vi.fn();
  const onDuplicate = vi.fn();
  const dispose = subscribeToSessionEvents("session-a", { onEnvelope, onDuplicate });
  const source = FakeEventSource.instances[0];
  const envelope = {
    session_id: "session-a",
    epoch_id: "epoch-a",
    sequence_id: 1,
    event: { type: "run_failed", message: "failed" } as const,
  };

  source.emit("session_event", envelope);
  await Promise.resolve();
  source.emit("session_event", envelope);
  await Promise.resolve();

  expect(onEnvelope).toHaveBeenCalledTimes(1);
  expect(onDuplicate).toHaveBeenCalledExactlyOnceWith(envelope);
  dispose();
});

it("reconnects from the last delivered cursor when it observes a sequence gap", async () => {
  const onSequenceGap = vi.fn();
  const dispose = subscribeToSessionEvents("session-a", {
    onEnvelope: vi.fn(),
    onSequenceGap,
  });
  const source = FakeEventSource.instances[0];
  source.emit("session_event", {
    session_id: "session-a",
    epoch_id: "epoch-a",
    sequence_id: 1,
    event: { type: "run_failed", message: "failed" },
  });
  await Promise.resolve();
  source.emit("session_event", {
    session_id: "session-a",
    epoch_id: "epoch-a",
    sequence_id: 3,
    event: { type: "run_failed", message: "failed" },
  });
  await vi.advanceTimersByTimeAsync(500);

  expect(onSequenceGap).toHaveBeenCalledWith({
    epochId: "epoch-a",
    expectedSequenceId: 2,
    receivedSequenceId: 3,
  });
  expect(FakeEventSource.instances[1].url).toBe(
    "/sessions/session-a/events/stream?after_epoch_id=epoch-a&after_sequence_id=1",
  );
  dispose();
});

it("bounds a slow handler and replays from the last accepted boundary", async () => {
  let release: (() => void) | undefined;
  const blocked = new Promise<void>((resolve) => {
    release = resolve;
  });
  let delivery = 0;
  const onEnvelope = vi.fn(() => {
    delivery += 1;
    return delivery === 1 ? blocked : undefined;
  });
  const onBackpressure = vi.fn();
  const dispose = subscribeToSessionEvents(
    "session-a",
    { onEnvelope, onBackpressure },
    { maxPendingEvents: 1 },
  );
  const source = FakeEventSource.instances[0];
  source.emit("replay_boundary", {
    epoch_id: "epoch-a",
    replay_boundary_sequence_id: 0,
  });
  source.emit("session_event", {
    session_id: "session-a",
    epoch_id: "epoch-a",
    sequence_id: 1,
    event: { type: "run_failed", message: "failed" },
  });
  source.emit("session_event", {
    session_id: "session-a",
    epoch_id: "epoch-a",
    sequence_id: 2,
    event: { type: "run_failed", message: "failed" },
  });
  await vi.advanceTimersByTimeAsync(500);

  expect(onBackpressure).toHaveBeenCalledWith({
    reason: "queue-overflow",
    maxPendingEvents: 1,
  });
  expect(FakeEventSource.instances[1].url).toBe(
    "/sessions/session-a/events/stream?after_epoch_id=epoch-a&after_sequence_id=0",
  );
  release?.();
  await Promise.resolve();
  await Promise.resolve();
  FakeEventSource.instances[1].onopen?.();
  FakeEventSource.instances[1].emit("session_event", {
    session_id: "session-a",
    epoch_id: "epoch-a",
    sequence_id: 1,
    event: { type: "run_failed", message: "failed" },
  });
  await Promise.resolve();
  FakeEventSource.instances[1].onerror?.();
  await vi.advanceTimersByTimeAsync(500);
  expect(FakeEventSource.instances[2].url).toBe(
    "/sessions/session-a/events/stream?after_epoch_id=epoch-a&after_sequence_id=1",
  );
  dispose();
});
