/**
 * End-to-end idempotency tests that drive the REAL triggerSessionTurn dispatch
 * path (not just the extracted helpers) for a fresh async virtual trigger, using
 * the REAL idempotency-store + async-trigger-store (temp SESSION_DATA_DIR).
 * Boundaries (lark client / session-store / worker-pool) are mocked so we can
 * make forkWorker throw and assert the barrier / fork-fault convergence codex
 * asked for: a synchronous dispatch throw must record a durable failed
 * (dispatch_unknown), close, and NOT leave the caller polling `running`; a
 * same-key retry must reuse (never double-fork).
 *
 * Run:  pnpm vitest run test/trigger-session-idempotency-e2e.test.ts
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { mkdtempSync, rmSync, mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import type { BotConfig } from '../src/bot-registry.js';
import { handleA2A } from '../src/dashboard/a2a-api.js';
import { resolveAsyncTriggerState } from '../src/services/async-trigger-state.js';
import { buildSessionMessagePreview } from '../src/core/session-message-preview.js';
import type { TriggerRequest } from '../src/services/trigger-types.js';

let tempDir: string;
let prevDataDir: string | undefined;

vi.mock('../src/utils/logger.js', () => ({
  logger: { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));

// Lark client — no real chat for a fresh async virtual trigger, but imported.
vi.mock('../src/im/lark/client.js', () => ({
  getMessageChatId: vi.fn(),
  getChatMode: vi.fn(async () => 'group'),
  sendMessage: vi.fn(async () => 'om_x'),
  replyMessage: vi.fn(async () => 'om_x'),
  listChatBotMembers: vi.fn(async () => []),
}));

const mockGetBot = vi.fn(() => ({ config: { cliId: 'codex-app', apiOnly: true } }));
vi.mock('../src/bot-registry.js', () => ({
  getBot: (...a: any[]) => mockGetBot(...a),
  effectiveDefaultWorkingDir: vi.fn(() => '/tmp'),
}));

vi.mock('../src/services/groups-store.js', () => ({ isInChat: vi.fn(async () => true) }));
vi.mock('../src/services/oncall-store.js', () => ({ getOncallStatus: vi.fn(() => undefined) }));

let sessionSeq = 0;
const createdSessions: any[] = [];
vi.mock('../src/services/session-store.js', () => ({
  createSession: vi.fn((chatId: string, anchor: string, title: string) => {
    const s = { sessionId: `sess-${++sessionSeq}`, chatId, rootMessageId: anchor, title, scope: 'chat', status: 'active', createdAt: '2026-06-01T00:00:00.000Z' };
    createdSessions.push(s);
    return s;
  }),
  updateSession: vi.fn(),
  getSession: vi.fn((id: string) => createdSessions.find(s => s.sessionId === id)),
  getOwnedSession: vi.fn((id: string) => createdSessions.find(s => s.sessionId === id)),
  registerSessionBridgeSendMarkerCleanupFence: vi.fn(),
  cleanupSessionBridgeSendMarkers: vi.fn(),
  cleanupSessionBridgeSendMarkersNow: vi.fn(),
}));

vi.mock('../src/services/message-queue.js', () => ({ ensureQueue: vi.fn() }));
vi.mock('../src/core/session-manager.js', () => ({
  buildFollowUpContent: vi.fn((p: string) => p),
  buildFollowUpCliInput: vi.fn((p: string) => ({ content: p })),
  buildNewTopicPrompt: vi.fn((p: string) => p),
  buildNewTopicCliInput: vi.fn((p: string) => ({ content: p })),
  ensureSessionWhiteboard: vi.fn(),
  getAvailableBots: vi.fn(async () => []),
  rememberLastCliInput: vi.fn(),
}));
vi.mock('../src/services/default-worktree.js', () => ({ botAutoWorktreeEnabled: vi.fn(() => false) }));
vi.mock('../src/im/lark/card-handler.js', () => ({ runAutoWorktreeCommit: vi.fn(async () => {}) }));

// worker-pool: forkWorker is the dispatch side effect we make throw on demand.
let forkShouldThrow = false;
const mockForkWorker = vi.fn(() => { if (forkShouldThrow) throw new Error('injected fork failure'); });
const mockCloseSession = vi.fn(async () => ({ ok: true, outcome: 'closed', alreadyClosed: false, known: true }));
vi.mock('../src/core/worker-pool.js', () => ({
  forkWorker: (...a: any[]) => mockForkWorker(...a),
  sendWorkerInput: vi.fn(() => true),
  getCurrentCliVersion: vi.fn(() => 'test'),
  setActiveSessionIfActive: (map: Map<string, any>, key: string, ds: any) => { map.set(key, ds); return true; },
  closeSession: (...a: any[]) => mockCloseSession(...a),
  closeSessionForBackgroundCleanup: (...a: any[]) => mockCloseSession(a[0]),
  getDaemonBootId: () => 'boot-CURRENT',
  // master refactor: trigger-session now takes the active-session key lock and
  // checks the queued-activation admission gate. The lock just runs the action;
  // no queued-activation gate in these fresh-async-virtual tests.
  withActiveSessionKeyLock: (_map: any, _key: string, action: () => any) => action(),
  hasQueuedActivationAdmissionGate: () => false,
}));

import { triggerSessionTurn, convergeIdempotentAsyncTurnOnWorkerExit, buildExternalEventDataContext } from '../src/core/trigger-session.js';
import * as asyncTriggerStore from '../src/services/async-trigger-store.js';
import * as idempotencyStore from '../src/services/idempotency-store.js';
import { acquireDeviceIsolationFreeze, releaseDeviceIsolationFreeze, resetDeviceIsolationActivationForTest } from '../src/core/device-isolation-activation.js';

const APP = 'local_riff';
function freshAsyncReq(idempotencyKey: string, instruction = 'do the thing'): TriggerRequest {
  return {
    source: { type: 'webhook', sourceName: 'riff' } as any,
    target: { kind: 'turn', botId: APP },
    envelope: { format: 'text', sourceName: 'riff', trusted: false },
    instruction,
    options: { asyncReturnSessionId: true, idempotencyKey },
  };
}

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'trig-idem-e2e-'));
  prevDataDir = process.env.SESSION_DATA_DIR;
  process.env.SESSION_DATA_DIR = tempDir;
  sessionSeq = 0; createdSessions.length = 0;
  forkShouldThrow = false;
  mockForkWorker.mockClear(); mockCloseSession.mockClear();
});
afterEach(() => {
  resetDeviceIsolationActivationForTest(); // clear any freeze lease a test acquired
  if (prevDataDir === undefined) delete process.env.SESSION_DATA_DIR; else process.env.SESSION_DATA_DIR = prevDataDir;
  rmSync(tempDir, { recursive: true, force: true });
});

describe('triggerSessionTurn — idempotency dispatch (real stores)', () => {
  it('A2A HTTP retries reuse real persisted tasks across first messages and follow-ups', async () => {
    const activeSessions = new Map<string, any>();
    const priorToken = process.env.TEST_A2A_INTEGRATION_TOKEN;
    process.env.TEST_A2A_INTEGRATION_TOKEN = 'integration-token';
    const deps = {
      loadBotConfigs: () => [{ larkAppId: APP, a2a: { enabled: true, tokenEnv: 'TEST_A2A_INTEGRATION_TOKEN' } } as BotConfig],
      proxyToDaemon: async (_bot: string, path: string, init: RequestInit) => {
        if (path === '/api/trigger') return Response.json(await triggerSessionTurn(JSON.parse(String(init.body)), { larkAppId: APP, activeSessions }));
        const url = new URL(path, 'http://localhost');
        const sessionId = url.pathname.split('/')[3];
        const triggerId = url.searchParams.get('triggerId')!;
        const result = resolveAsyncTriggerState({ sessionId, requestedTriggerId: triggerId, liveActive: false,
          persisted: asyncTriggerStore.lookup(sessionId, triggerId), storedStatus: 'open' });
        return Response.json(result, { status: result.errorCode === 'bad_request' ? 400 : 200 });
      },
    };
    const server = createServer((req, res) => { void handleA2A(req, res, new URL(req.url!, 'http://localhost'), deps).catch(error => { res.writeHead(500); res.end(String(error)); }); });
    try {
      await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
      const call = async (method: string, params: unknown) => {
        const response = await fetch(`http://127.0.0.1:${(server.address() as AddressInfo).port}/a2a/${APP}`, {
          method: 'POST', headers: { authorization: 'Bearer integration-token', 'a2a-version': '1.0', 'content-type': 'application/json' },
          body: JSON.stringify({ jsonrpc: '2.0', id: 'http-attempt', method, params }),
        });
        return await response.json() as any;
      };
      const firstMessage = { message: { messageId: 'first-message', role: 'ROLE_USER', parts: [{ text: 'first task' }] }, configuration: { returnImmediately: true } };
      const first = (await call('SendMessage', firstMessage)).result.task;
      expect(mockForkWorker).toHaveBeenCalledTimes(1);
      expect((await call('GetTask', { id: first.id })).result.status.state).toBe('TASK_STATE_WORKING');
      // This fixture has no live Worker: a failed retry receipt must not become working.
      expect((await call('SendMessage', firstMessage)).result.task.status.state).toBe('TASK_STATE_FAILED');
      const firstTrigger = first.id.split(':')[1];
      asyncTriggerStore.recordCompleted(first.contextId, firstTrigger, 'first answer', Date.now(), APP);
      const previewSession = { ...createdSessions[0], larkAppId: APP };
      expect(buildSessionMessagePreview(previewSession)).toMatchObject({ previewBotFullText: 'first answer', previewBotState: 'replied' });
      // The first receipt could have been lost: sending it again must read the original result.
      const recovered = (await call('SendMessage', firstMessage)).result.task;
      expect(recovered.id).toBe(first.id);
      expect(recovered.status.state).toBe('TASK_STATE_COMPLETED');
      expect(mockForkWorker).toHaveBeenCalledTimes(1);
      const followupMessage = { ...firstMessage, message: { ...firstMessage.message, messageId: 'followup', contextId: first.contextId, parts: [{ text: 'next task' }] } };
      const followup = (await call('SendMessage', followupMessage)).result.task;
      expect(followup.contextId).toBe(first.contextId); expect(followup.id).not.toBe(first.id);
      expect(buildSessionMessagePreview(previewSession).previewBotFullText).toBeUndefined();
      const completedAt = Date.now();
      asyncTriggerStore.recordCompleted(followup.contextId, followup.id.split(':')[1], 'second answer\n第二行 🧪', completedAt, APP);
      expect(buildSessionMessagePreview(previewSession).previewBotFullText).toBe('second answer\n第二行 🧪');
      expect(buildSessionMessagePreview({ ...previewSession, larkAppId: 'other-bot' }).previewBotFullText).toBeUndefined();
      expect(buildSessionMessagePreview({ ...previewSession, status: 'closed' }).previewBotFullText).toBeNull();
      mkdirSync(join(tempDir, 'turn-sends'));
      const sendFile = join(tempDir, 'turn-sends', `${first.contextId}.jsonl`);
      writeFileSync(sendFile, JSON.stringify({ previewText: 'older chat answer', sentAtMs: completedAt - 1000 }) + '\n');
      expect(buildSessionMessagePreview(previewSession).previewBotFullText).toBe('second answer\n第二行 🧪');
      writeFileSync(sendFile, JSON.stringify({ previewText: 'newer chat answer', sentAtMs: completedAt + 1000 }) + '\n');
      expect(buildSessionMessagePreview(previewSession).previewBotFullText).toBe('newer chat answer');
      expect((await call('SendMessage', followupMessage)).result.task.id).toBe(followup.id);
      expect(mockForkWorker).toHaveBeenCalledTimes(2);
      expect((await call('GetTask', { id: first.id })).result.artifacts[0].parts[0].text).toBe('first answer');
      expect((await call('GetTask', { id: `${first.contextId}:trg_missing` })).error.code).toBe(-32001);
      const conflict = await call('SendMessage', { ...followupMessage, message: { ...followupMessage.message, parts: [{ text: 'changed task' }] } });
      expect(conflict.error).toBeDefined(); expect(mockForkWorker).toHaveBeenCalledTimes(2);
      for (const contextId of [undefined, first.contextId]) {
        const message = { ...firstMessage, message: { ...firstMessage.message, messageId: `interrupt-${contextId ?? 'new'}`, contextId } };
        const task = (await call('SendMessage', message)).result.task;
        const dispatched = mockForkWorker.mock.calls.length;
        asyncTriggerStore.recordInterruptedStrict(task.contextId, task.id.split(':')[1], Date.now(), APP);
        const canceled = (await call('GetTask', { id: task.id })).result;
        expect(canceled).toMatchObject({ id: task.id, contextId: task.contextId, status: { state: 'TASK_STATE_CANCELED' } });
        expect((await call('SendMessage', message)).result.task).toEqual(canceled);
        expect(mockForkWorker).toHaveBeenCalledTimes(dispatched);
      }
    } finally {
      await new Promise<void>(resolve => server.close(() => resolve()));
      if (priorToken === undefined) delete process.env.TEST_A2A_INTEGRATION_TOKEN; else process.env.TEST_A2A_INTEGRATION_TOKEN = priorToken;
    }
  });

  it('first call: forks once, writes a reserved→attempting lease, returns idempotent:false', async () => {
    const res = await triggerSessionTurn(freshAsyncReq('k-1'), { larkAppId: APP, activeSessions: new Map() });
    expect(res.ok).toBe(true);
    expect(res.idempotent).toBe(false);
    expect(mockForkWorker).toHaveBeenCalledTimes(1);
    const lease = idempotencyStore.lookup(APP, 'k-1');
    expect(lease?.state).toBe('attempting'); // barrier crossed before fork
    expect(lease?.sessionId).toBe(res.target?.sessionId);
  });

  it('same key + same payload retry: reuses, does NOT fork again', async () => {
    const first = await triggerSessionTurn(freshAsyncReq('k-2'), { larkAppId: APP, activeSessions: new Map() });
    expect(mockForkWorker).toHaveBeenCalledTimes(1);
    const second = await triggerSessionTurn(freshAsyncReq('k-2'), { larkAppId: APP, activeSessions: new Map() });
    expect(second.idempotent).toBe(true);
    expect(second.target?.sessionId).toBe(first.target?.sessionId);
    expect(mockForkWorker).toHaveBeenCalledTimes(1); // still ONE fork
  });

  it('same key + DIFFERENT payload → 409 idempotency_conflict, no second fork', async () => {
    await triggerSessionTurn(freshAsyncReq('k-3', 'payload A'), { larkAppId: APP, activeSessions: new Map() });
    const conflict = await triggerSessionTurn(freshAsyncReq('k-3', 'payload B'), { larkAppId: APP, activeSessions: new Map() });
    expect(conflict.ok).toBe(false);
    expect(conflict.errorCode).toBe('idempotency_conflict');
    expect(mockForkWorker).toHaveBeenCalledTimes(1);
  });

  it('same key, same instruction, but DIFFERENT options.status → 409 (requestHash covers full options)', async () => {
    // codex #776 round-4: status firing→resolved changes the rendered prompt; the
    // hash must change too, else the resolved event silently reuses the firing turn.
    const firing: TriggerRequest = { ...freshAsyncReq('k-status'), options: { asyncReturnSessionId: true, idempotencyKey: 'k-status', status: 'firing' } };
    const resolved: TriggerRequest = { ...freshAsyncReq('k-status'), options: { asyncReturnSessionId: true, idempotencyKey: 'k-status', status: 'resolved' } };
    await triggerSessionTurn(firing, { larkAppId: APP, activeSessions: new Map() });
    const res = await triggerSessionTurn(resolved, { larkAppId: APP, activeSessions: new Map() });
    expect(res.ok).toBe(false);
    expect(res.errorCode).toBe('idempotency_conflict');
    expect(mockForkWorker).toHaveBeenCalledTimes(1);
  });

  it('fork throw AFTER the barrier → durable async failed(dispatch_unknown) + close, retry does NOT re-run', async () => {
    forkShouldThrow = true;
    const res = await triggerSessionTurn(freshAsyncReq('k-4'), { larkAppId: APP, activeSessions: new Map() });
    // The HTTP result reports a terminal failed (at-most-once), not queued.
    expect(res.state).toBe('failed');
    expect(res.errorCode).toBe('no_output');
    const sid = res.target!.sessionId!;
    // Authoritative durable failed evidence is written (so trigger-result converges).
    expect(asyncTriggerStore.lookup(sid, res.triggerId!)?.result.status).toBe('failed');
    expect(asyncTriggerStore.lookup(sid, res.triggerId!)?.result.reason).toBe('dispatch_unknown');
    expect(mockCloseSession).toHaveBeenCalledWith(sid);
    // Retry with the same key must NOT dispatch again — it resolves terminal.
    forkShouldThrow = false;
    const forkBefore = mockForkWorker.mock.calls.length;
    const retry = await triggerSessionTurn(freshAsyncReq('k-4'), { larkAppId: APP, activeSessions: new Map() });
    expect(retry.state).toBe('failed');
    expect(mockForkWorker.mock.calls.length).toBe(forkBefore); // no new fork
  });

  it('DOUBLE failure (fork throw + terminal write throw) → 5xx trigger_failed, NOT a phantom state:failed', async () => {
    forkShouldThrow = true;
    // Make recordFailedStrict's durable write fail: pre-create the async-triggers
    // target path for the next session (sess-1) as a DIRECTORY, so the atomic
    // rename onto it fails. The dispatch throws AND the terminal write throws →
    // we must NOT claim state:failed (the caller could never observe it).
    const asyncDir = join(tempDir, 'async-triggers');
    mkdirSync(join(asyncDir, 'sess-1.json'), { recursive: true }); // path is a dir → write fails
    const res = await triggerSessionTurn(freshAsyncReq('k-dbl'), { larkAppId: APP, activeSessions: new Map() });
    expect(res.ok).toBe(false);
    expect(res.state).not.toBe('failed');       // no phantom terminal
    expect(res.errorCode).toBe('trigger_failed'); // honest 5xx-class hard error
  });

  // ── codex #776 finding #1: attempt-barrier (transition reserved→attempting)
  //    fault. The barrier-fail release must genuinely CONVERGE the lease — not
  //    swallow a `changed`/EIO — so a same-boot retry does the right thing.
  //    Two disk states codex named: pre-rename (disk still reserved) and
  //    post-rename (disk landed attempting, then fsync threw).

  it('barrier PRE-rename fault (transition throws, disk still reserved) → 5xx; same-key retry starts FRESH (re-forks once)', async () => {
    const shared = new Map(); // real daemon shares ONE activeSessions map across calls
    // First call: make the barrier transition throw WITHOUT mutating disk (the
    // lease stays `reserved`). Barrier-fail release must cleanly compareAndRemove
    // it so the retry is not blocked by a same-boot reserved orphan.
    const spy = vi.spyOn(idempotencyStore, 'transition').mockImplementationOnce(() => { throw new Error('injected pre-rename barrier fault'); });
    const first = await triggerSessionTurn(freshAsyncReq('k-bpre'), { larkAppId: APP, activeSessions: shared });
    expect(first.ok).toBe(false);
    expect(first.errorCode).toBe('trigger_failed');
    expect(mockForkWorker).not.toHaveBeenCalled(); // barrier failed before fork
    // Lease was released (clean reserved removal) — no leftover blocking the key.
    expect(idempotencyStore.lookup(APP, 'k-bpre')).toBeUndefined();
    spy.mockRestore();
    // Retry: real transition now works → fresh claim + one fork + attempting lease.
    const retry = await triggerSessionTurn(freshAsyncReq('k-bpre'), { larkAppId: APP, activeSessions: shared });
    expect(retry.ok).toBe(true);
    expect(retry.idempotent).toBe(false);
    expect(mockForkWorker).toHaveBeenCalledTimes(1); // exactly one fork total
    expect(idempotencyStore.lookup(APP, 'k-bpre')?.state).toBe('attempting');
  });

  it('barrier POST-rename fault (disk landed attempting, then throw) → observable durable failed; same-key retry does NOT re-fork', async () => {
    // Simulate the rename landing (disk becomes attempting) THEN a post-rename
    // fsync throw: advance the real on-disk lease to attempting, then throw. The
    // barrier-fail release sees compareAndRemove→changed(attempting) and must
    // durably terminalize (never delete the crossed fence) AND report an
    // observable terminal (state:failed with the sessionId), not a bare 5xx.
    const realTransition = idempotencyStore.transition;
    const spy = vi.spyOn(idempotencyStore, 'transition').mockImplementationOnce((owner: any, key: any, from: any, patch: any) => {
      realTransition(owner, key, from, patch); // rename lands: disk now attempting
      throw new Error('injected post-rename barrier fault (fsync)');
    });
    const first = await triggerSessionTurn(freshAsyncReq('k-bpost'), { larkAppId: APP, activeSessions: new Map() });
    spy.mockRestore();
    expect(first.ok).toBe(false);
    expect(first.state).toBe('failed');            // observable terminal, not bare 5xx
    expect(first.errorCode).toBe('no_output');
    expect(mockForkWorker).not.toHaveBeenCalled(); // threw before fork
    const sid = first.target!.sessionId!;
    // The crossed fence was durably terminalized (dispatch_unknown), NOT deleted.
    expect(asyncTriggerStore.lookup(sid, first.triggerId!)?.result.status).toBe('failed');
    expect(asyncTriggerStore.lookup(sid, first.triggerId!)?.result.reason).toBe('dispatch_unknown');
    // Retry with the same key resolves TERMINAL (at-most-once) — never re-forks.
    const retry = await triggerSessionTurn(freshAsyncReq('k-bpost'), { larkAppId: APP, activeSessions: new Map() });
    expect(retry.state).toBe('failed');
    expect(mockForkWorker).not.toHaveBeenCalled(); // still zero forks
  });

  it('barrier release compareAndRemove EIO (unprovable) → 5xx; retry stays TERMINAL (not-live orphan, no reuse-forever)', async () => {
    // Barrier transition throws pre-rename (disk still reserved), but the release
    // compareAndRemove ALSO throws (EIO) → the lease state is unprovable. We must
    // NOT reuse-forever: resolveIdempotencyHit's not-live guard makes the retry
    // terminal (the still-reserved same-boot lease has no live worker in the
    // persistent map, which evicts on closeSession — modeled here by a fresh map).
    const tSpy = vi.spyOn(idempotencyStore, 'transition').mockImplementationOnce(() => { throw new Error('injected barrier fault'); });
    const rSpy = vi.spyOn(idempotencyStore, 'compareAndRemove').mockImplementationOnce(() => { throw new Error('injected EIO on release unlink'); });
    const first = await triggerSessionTurn(freshAsyncReq('k-beio'), { larkAppId: APP, activeSessions: new Map() });
    tSpy.mockRestore(); rSpy.mockRestore();
    expect(first.ok).toBe(false);
    expect(first.errorCode).toBe('trigger_failed');
    expect(mockForkWorker).not.toHaveBeenCalled();
    // The reserved lease is still on disk (release couldn't prove removal)…
    expect(idempotencyStore.lookup(APP, 'k-beio')?.state).toBe('reserved');
    // …but the session is not live (closed + persistent map evicts) → retry is
    // TERMINAL, never reused-forever.
    const retry = await triggerSessionTurn(freshAsyncReq('k-beio'), { larkAppId: APP, activeSessions: new Map() });
    expect(retry.state).toBe('failed');
    expect(retry.idempotent).toBe(true);
    expect(mockForkWorker).not.toHaveBeenCalled(); // no dispatch on the orphan
  });

  // ── codex #776 round-6 finding #1: worker exits with NO final_output. The
  //    dispatched turn stamps ds.idempotentAsyncTurn; the worker-exit handler
  //    must converge it to a durable dispatch_unknown so a same-key retry AND
  //    trigger-result both resolve `failed`, never re-forking / polling forever.
  it('worker exit with no final_output → durable dispatch_unknown; retry + poll both failed, no 2nd fork', async () => {
    const shared = new Map();
    const first = await triggerSessionTurn(freshAsyncReq('k-wx'), { larkAppId: APP, activeSessions: shared });
    expect(first.ok).toBe(true);
    expect(mockForkWorker).toHaveBeenCalledTimes(1);
    const sid = first.target!.sessionId!;
    // Locate the dispatched DaemonSession + its stamped generation.
    const ds = [...shared.values()].find((d: any) => d.session.sessionId === sid) as any;
    expect(ds?.idempotentAsyncTurns?.get(first.triggerId!)).toBeDefined();
    const gen = ds.idempotentAsyncTurns.get(first.triggerId!).workerGeneration;
    // Simulate the real worker-exit handler: worker=null (dead), then converge.
    ds.worker = null;
    convergeIdempotentAsyncTurnOnWorkerExit(ds, gen);
    // Durable authoritative terminal is written…
    expect(asyncTriggerStore.lookup(sid, first.triggerId!)?.result.status).toBe('failed');
    expect(asyncTriggerStore.lookup(sid, first.triggerId!)?.result.reason).toBe('dispatch_unknown');
    // …the entry is dropped (idempotent: a later gen exit is a no-op)…
    expect(ds.idempotentAsyncTurns?.get(first.triggerId!)).toBeUndefined();
    // …and a same-key retry resolves TERMINAL without a second fork.
    const retry = await triggerSessionTurn(freshAsyncReq('k-wx'), { larkAppId: APP, activeSessions: shared });
    expect(retry.state).toBe('failed');
    expect(retry.idempotent).toBe(true);
    expect(mockForkWorker).toHaveBeenCalledTimes(1); // still ONE fork
  });

  it('worker-exit convergence ignores a NON-matching generation (no retro-fail of a later turn)', async () => {
    const shared = new Map();
    const first = await triggerSessionTurn(freshAsyncReq('k-wxgen'), { larkAppId: APP, activeSessions: shared });
    const sid = first.target!.sessionId!;
    const ds = [...shared.values()].find((d: any) => d.session.sessionId === sid) as any;
    const gen = ds.idempotentAsyncTurns.get(first.triggerId!).workerGeneration;
    // A DIFFERENT (older) generation exits → must NOT converge this turn.
    convergeIdempotentAsyncTurnOnWorkerExit(ds, gen - 1);
    expect(asyncTriggerStore.lookup(sid, first.triggerId!)?.result.status).toBe('pending'); // untouched
    expect(ds.idempotentAsyncTurns?.get(first.triggerId!)).toBeDefined(); // entry intact
  });

  it('FINDING #3: a FOREIGN completed on the same sessionId does NOT clear the exit-convergence stamp', async () => {
    // codex round-7 #3: async-trigger-store is keyed by sessionId; a foreign bot's
    // completed on the same sessionId/triggerId must NOT be treated as OUR
    // completion and clear our only exit-convergence stamp — else onCliExit leaves
    // no stamp, resolveIdempotencyHit reuses the attempting lease (foreign outcome
    // ignored + liveWorker), and the later onWorkerExit — stampless — can never
    // converge → permanent running. Convergence must still write OUR durable failed
    // (recordFailedStrict is owner-proofed and would throw on a real foreign-owned
    // file; here the foreign record is under a DIFFERENT session file, so our write
    // to our own session succeeds).
    const shared = new Map();
    const first = await triggerSessionTurn(freshAsyncReq('k-fc3'), { larkAppId: APP, activeSessions: shared });
    const sid = first.target!.sessionId!;
    const ds = [...shared.values()].find((d: any) => d.session.sessionId === sid) as any;
    const gen = ds.idempotentAsyncTurns.get(first.triggerId!).workerGeneration;
    // Foreign bot writes a completed on OUR sessionId/triggerId (adversarial /
    // sessionId collision). ownerLarkAppId != our APP.
    asyncTriggerStore.recordCompleted(sid, first.triggerId!, 'B answer', 100, 'cli_OTHER_BOT');
    ds.worker = null;
    convergeIdempotentAsyncTurnOnWorkerExit(ds, gen);
    // The foreign completed did NOT count as our completion: convergence attempted
    // our durable failed. recordFailedStrict is completed-wins + owner-proofed, so
    // the on-disk foreign completed stays (owner mismatch → our write threw inside,
    // entry intact for reconcile). The KEY invariant: the entry was NOT silently
    // cleared by the foreign completed.
    const rec = asyncTriggerStore.lookup(sid, first.triggerId!);
    expect(rec?.ownerLarkAppId).toBe('cli_OTHER_BOT'); // foreign evidence untouched (owner-proof)
    expect(ds.idempotentAsyncTurns?.get(first.triggerId!)).toBeDefined();  // entry NOT cleared by foreign completed
  });

  it('FINDING #1: keyed at-most-once turn forks with atMostOnce so the worker never replays it after CLI exit', async () => {
    // codex round-7 #1: the fork must carry atMostOnce so the worker excludes the
    // input from BOTH inflight carry-over and pendingMessages on CLI exit. Assert
    // the daemon side passes it (the worker-side no-replay is unit-tested in
    // inflight-input-tracker + worker restart integration).
    mockForkWorker.mockClear();
    await triggerSessionTurn(freshAsyncReq('k-amo'), { larkAppId: APP, activeSessions: new Map() });
    expect(mockForkWorker).toHaveBeenCalledTimes(1);
    const forkArg = mockForkWorker.mock.calls[0][2]; // third arg = resumeOrTurnId
    expect(typeof forkArg).toBe('object');
    expect(forkArg.atMostOnce).toBe(true);
    expect(forkArg.turnId).toBeTruthy();
  });

  // ── codex #776 round-6 finding #4: the raw idempotencyKey must NOT leak into
  //    the rendered prompt, or trim-equivalent keys ('k' vs ' k ') would produce
  //    a different prompt while the hash (which excludes the key) matches — a
  //    silent reuse flagged as `prompt differs`. The renderer strips the key too.
  it('idempotencyKey is stripped from the rendered event prompt (normalized-key seam)', () => {
    const withKey = { ...freshAsyncReq('  spaced-key  '), options: { asyncReturnSessionId: true, idempotencyKey: '  spaced-key  ', status: 'firing' } } as any;
    const prompt = buildExternalEventDataContext(withKey, 'trg_x');
    expect(prompt).not.toContain('spaced-key');   // raw key never rendered
    expect(prompt).not.toContain('idempotencyKey'); // field itself stripped
    expect(prompt).toContain('"status": "firing"'); // other options still rendered
  });

  it('trim-equivalent keys reuse the SAME lease and do NOT 409 (prompt is identical after strip)', async () => {
    const a = await triggerSessionTurn({ ...freshAsyncReq('k-trim'), options: { asyncReturnSessionId: true, idempotencyKey: 'k-trim' } } as any, { larkAppId: APP, activeSessions: new Map() });
    expect(mockForkWorker).toHaveBeenCalledTimes(1);
    // A retry with a whitespace-padded key trims to the same lookup key; because
    // the renderer strips the key, optionsForHash AND the rendered prompt are
    // identical → legitimate reuse, NOT a 409 idempotency_conflict.
    const b = await triggerSessionTurn({ ...freshAsyncReq('k-trim'), options: { asyncReturnSessionId: true, idempotencyKey: '  k-trim  ' } } as any, { larkAppId: APP, activeSessions: new Map() });
    expect(b.errorCode).not.toBe('idempotency_conflict');
    expect(b.idempotent).toBe(true);
    expect(b.target?.sessionId).toBe(a.target?.sessionId);
    expect(mockForkWorker).toHaveBeenCalledTimes(1); // no second fork
  });

  // ── codex #776 round-8 finding #1: forkWorker defers (returns without forking)
  //    during a device-isolation freeze. A keyed dispatch must NOT cross the
  //    barrier + report queued in that window, or a retry sees failed while the
  //    deferred fork later runs. Refuse up-front, retryable, nothing dispatched.
  it('keyed dispatch during a device-isolation freeze → retryable error, NO lease/async/fork', async () => {
    const acq = acquireDeviceIsolationFreeze({ nonce: 'n1', inventoryGeneration: 'g1', leaseMs: 30_000 });
    expect(acq.ok).toBe(true);
    const res = await triggerSessionTurn(freshAsyncReq('k-freeze'), { larkAppId: APP, activeSessions: new Map() });
    expect(res.ok).toBe(false);
    expect(res.errorCode).toBe('trigger_failed');
    expect(res.state).not.toBe('failed');           // NOT a terminal — retryable
    expect(mockForkWorker).not.toHaveBeenCalled();   // nothing dispatched/deferred
    expect(idempotencyStore.lookup(APP, 'k-freeze')).toBeUndefined(); // no lease claimed
    // After release, the same key dispatches cleanly (exactly once).
    if (acq.ok) releaseDeviceIsolationFreeze({ nonce: 'n1', leaseId: acq.lease.leaseId });
    const ok = await triggerSessionTurn(freshAsyncReq('k-freeze'), { larkAppId: APP, activeSessions: new Map() });
    expect(ok.ok).toBe(true);
    expect(ok.idempotent).toBe(false);
    expect(mockForkWorker).toHaveBeenCalledTimes(1);
    expect(idempotencyStore.lookup(APP, 'k-freeze')?.state).toBe('attempting');
  });

  // ── codex #776 round-8 finding #2: if the exit-convergence durable write FAILS
  //    (EIO/ENOSPC), the helper must report write_failed so the daemon fail-closes
  //    the session (observable terminal), not strand the poller on running.
  it('worker-exit convergence write failure → returns write_failed (daemon fail-closes)', async () => {
    const shared = new Map();
    const first = await triggerSessionTurn(freshAsyncReq('k-wf'), { larkAppId: APP, activeSessions: shared });
    const sid = first.target!.sessionId!;
    const ds = [...shared.values()].find((d: any) => d.session.sessionId === sid) as any;
    const gen = ds.idempotentAsyncTurns.get(first.triggerId!).workerGeneration;
    // Inject an EIO/ENOSPC-class failure on the durable dispatch_unknown write.
    const wSpy = vi.spyOn(asyncTriggerStore, 'recordFailedStrict').mockImplementationOnce(() => { throw new Error('injected ENOSPC on durable write'); });
    ds.worker = null;
    const outcome = convergeIdempotentAsyncTurnOnWorkerExit(ds, gen);
    wSpy.mockRestore();
    expect(outcome).toBe('write_failed');
    // Entry is KEPT (not dropped) so a later retry / reconcile can still converge.
    expect(ds.idempotentAsyncTurns?.get(first.triggerId!)).toBeDefined();
    // The durable failed was NOT written (write threw) — async record stays pending;
    // the daemon wrapper (failCloseIdempotentTurnIfConvergenceWriteFailed) is what
    // closes the session on this write_failed signal (asserted via source-lock).
    expect(asyncTriggerStore.lookup(sid, first.triggerId!)?.result.status).toBe('pending');
  });
});
