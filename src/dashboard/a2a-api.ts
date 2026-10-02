import type { IncomingMessage, ServerResponse } from 'node:http';
import { createHash, timingSafeEqual } from 'node:crypto';
import { AgentCard, Message, Role, Task, TaskState } from '@a2a-js/sdk';
import { JsonRpcTransportHandler, ServerCallContext, validateVersion, type A2ARequestHandler } from '@a2a-js/sdk/server';
import { ContentTypeNotSupportedError, InvalidAgentResponseError, PushNotificationNotSupportedError, RequestMalformedError, TaskNotFoundError, UnsupportedOperationError, toJsonRpcError } from '@a2a-js/sdk/errors';
import type { BotConfig } from '../bot-registry.js';
import type { TriggerResponse } from '../services/trigger-types.js';
import { dispatchTriggerRequest, readJsonBodyWithLimit, type TriggerApiDeps } from './trigger-api.js';
import { jsonRes } from './http.js';

const digest = (value: string) => createHash('sha256').update(value).digest();
const unsupported = (): never => { throw new UnsupportedOperationError(); };
const noPush = (): never => { throw new PushNotificationNotSupportedError(); };

function taskResult(result: TriggerResponse, sessionId = result.target?.sessionId, triggerId = result.triggerId): Task {
  if (result.state === 'not_found' || (result.errorCode === 'bad_request' && result.message === 'requested triggerId not found for this session')) throw new TaskNotFoundError();
  if (!result.ok && result.state !== 'failed') {
    if (['bad_request', 'idempotency_conflict', 'session_not_found'].includes(result.errorCode ?? '')) throw new RequestMalformedError(result.error);
    throw new Error(result.error ?? 'Botmux request failed');
  }
  const state = result.state === 'interrupted' ? 'canceled' : result.state ?? (result.ok && result.action === 'queued' ? 'submitted' : undefined);
  if (!sessionId || !triggerId) throw new InvalidAgentResponseError('Botmux did not return a task ID');
  if (!state || !['submitted', 'running', 'completed', 'failed', 'canceled'].includes(state)) throw new InvalidAgentResponseError('Unknown Botmux task state');
  if (result.output?.content !== undefined && typeof result.output.content !== 'string') throw new InvalidAgentResponseError('Invalid Botmux text result');
  const id = `${sessionId}:${triggerId}`;
  return Task.fromJSON({ id, contextId: sessionId,
    status: { state: `TASK_STATE_${state === 'running' ? 'WORKING' : state.toUpperCase()}`, timestamp: result.finishedAt,
      message: state === 'failed' ? { messageId: `${id}:status`, role: 'ROLE_AGENT', parts: [{ text: result.error ?? 'Task failed' }] } : undefined },
    artifacts: state === 'completed' && result.output?.content !== undefined
      ? [{ artifactId: `${id}:answer`, parts: [{ text: result.output.content }] }] : [],
  });
}

export async function handleA2A(req: IncomingMessage, res: ServerResponse, url: URL, deps: TriggerApiDeps & { loadBotConfigs(): BotConfig[] }): Promise<void> {
  const match = /^\/a2a\/([^/]+)(\/agent-card\.json)?$/.exec(url.pathname);
  const bot = match && deps.loadBotConfigs().find(b => encodeURIComponent(b.larkAppId) === match[1]);
  if (!bot?.a2a?.enabled) return jsonRes(res, 404, { error: 'A2A is not enabled' });
  const token = process.env[bot.a2a.tokenEnv];
  if (!token || !timingSafeEqual(digest(req.headers.authorization ?? ''), digest(`Bearer ${token}`))) {
    res.setHeader('WWW-Authenticate', 'Bearer');
    return jsonRes(res, 401, { error: 'Invalid A2A token' });
  }
  const endpoint = new URL(`/a2a/${encodeURIComponent(bot.larkAppId)}`, url);
  if (req.headers['x-forwarded-proto'] === 'https') endpoint.protocol = 'https:';
  const card = AgentCard.fromJSON({ name: bot.name ?? bot.larkAppId, description: 'Botmux text tasks; send with returnImmediately=true, then query the returned task.', version: '1',
    supportedInterfaces: [{ url: endpoint.href, protocolBinding: 'JSONRPC', protocolVersion: '1.0' }],
    capabilities: { streaming: false, pushNotifications: false }, defaultInputModes: ['text/plain'], defaultOutputModes: ['text/plain'],
    skills: [{ id: 'chat', name: 'Chat', description: 'Text tasks in a Botmux session', tags: ['chat'] }],
    securitySchemes: { bearer: { httpAuthSecurityScheme: { scheme: 'bearer' } } }, securityRequirements: [{ schemes: { bearer: { list: [] } } }],
  });
  if (match?.[2] && req.method === 'GET') return jsonRes(res, 200, AgentCard.toJSON(card));
  if (match?.[2] || req.method !== 'POST') return jsonRes(res, 405, { error: 'Method not allowed' });
  const getTask = async ({ id }: { id: string }): Promise<Task> => {
    if (!/^[\w-]+:[\w-]+$/.test(id)) throw new RequestMalformedError('Invalid task ID');
    const [sessionId, triggerId] = id.split(':');
    const response = await deps.proxyToDaemon(bot.larkAppId, `/api/sessions/${sessionId}/trigger-result?triggerId=${triggerId}`, { method: 'GET' });
    const result = await response.json() as TriggerResponse;
    if (!response.ok && result.ok) throw new Error(`Botmux query failed (${response.status})`);
    return taskResult(result, sessionId, triggerId);
  };
  const handler: A2ARequestHandler = {
    getAgentCard: async () => card, getTask,
    async sendMessage({ message, configuration }) {
      if (!message?.messageId.trim() || message.role !== Role.ROLE_USER || message.taskId || (message.contextId && !/^[\w-]+$/.test(message.contextId))) throw new RequestMalformedError('Use a new messageId and the returned contextId; omit taskId');
      if (!configuration?.returnImmediately) throw new RequestMalformedError('Set configuration.returnImmediately=true');
      if (configuration.taskPushNotificationConfig) noPush();
      if (configuration.acceptedOutputModes.length && !configuration.acceptedOutputModes.some(m => ['text/plain', 'text/*', '*/*'].includes(m))) throw new ContentTypeNotSupportedError();
      if (!message.parts.length || message.parts.some(p => p.content?.$case !== 'text')) throw new ContentTypeNotSupportedError('Only text parts are supported');
      const text = message.parts.map(p => p.content?.value as string).join('\n');
      if (!text.trim()) throw new RequestMalformedError('Message text must not be empty');
      const key = `a2a:${digest(message.messageId).toString('hex')}`;
      // The same message must produce the same trigger payload on every HTTP retry.
      const { body: result, status } = await dispatchTriggerRequest({
        source: { type: 'headless', connectorId: 'a2a', requestId: key },
        target: { kind: 'turn', botId: bot.larkAppId, ...(message.contextId ? { sessionId: message.contextId } : {}) },
        instruction: text, envelope: { format: 'botmux.a2a.v1', sourceName: 'A2A', trusted: false, payload: Message.toJSON(message) },
        presentation: { title: `A2A: ${Array.from(text).slice(0, 180).join('')}`, topicMessage: null },
        options: { asyncReturnSessionId: true, ...(message.contextId ? { turnIdempotencyKey: key } : { idempotencyKey: key }) },
      }, deps);
      if (status >= 400 && result.ok) throw new InvalidAgentResponseError(`Botmux submit failed (${status})`);
      const task = taskResult(result);
      if (!result.idempotent) return task;
      const current = await getTask({ id: task.id });
      // Keep failed receipts terminal unless the task confirms cancellation.
      return result.state === 'failed' && current.status?.state !== TaskState.TASK_STATE_CANCELED ? task : current;
    },
    getAuthenticatedExtendedAgentCard: unsupported, sendMessageStream: unsupported, resubscribe: unsupported,
    cancelTask: unsupported, listTasks: unsupported,
    createTaskPushNotificationConfig: noPush, getTaskPushNotificationConfig: noPush,
    listTaskPushNotificationConfigs: noPush, deleteTaskPushNotificationConfig: noPush,
  };
  let body: Record<string, unknown> | undefined;
  try {
    body = await readJsonBodyWithLimit<Record<string, unknown>>(req);
    const context = new ServerCallContext({ requestedVersion: String(req.headers['a2a-version'] ?? '0.3') });
    validateVersion(context.requestedVersion, card, 'JSONRPC');
    return jsonRes(res, 200, await new JsonRpcTransportHandler(handler).handle(body, context));
  } catch (error) {
    const failure = error instanceof SyntaxError || (error as Error).message === 'body_too_large' ? new RequestMalformedError('Invalid JSON or request body too large') : error;
    return jsonRes(res, 200, { jsonrpc: '2.0', id: body?.id ?? null, error: toJsonRpcError(failure) });
  }
}
