# A2A 文本任务

外部 Agent 可以给已有 Botmux 机器人发消息，再按任务编号查询最终回答。执行、会话、重试去重和结果保存都复用现有 trigger 链路。

## 配置

在目标机器人的 `bots.json` 条目中添加：

```json
"a2a": { "enabled": true, "tokenEnv": "BOTMUX_A2A_TOKEN" }
```

在 Dashboard 进程的环境中设置 `BOTMUX_A2A_TOKEN`，然后重启 Dashboard。每个机器人可以引用不同的环境变量。调用方需要一个可访问的 Dashboard HTTPS 地址；反向代理须保留 Authorization 和 A2A-Version 请求头，HTTPS 终止代理应传入 `X-Forwarded-Proto: https`。

- 能力说明：`GET /a2a/{botId}/agent-card.json`
- JSON-RPC 调用：`POST /a2a/{botId}`
- 两者均使用 `Authorization: Bearer <token>`；JSON-RPC 请求带 `A2A-Version: 1.0`。
- token 授权访问该机器人的会话和任务；共享 token 的调用方之间没有额外权限隔离。

## 提交与查询

首次发送不带 `contextId`。下面是 POST 的 JSON 请求体；`id` 是本次 HTTP 交互的流水号，`messageId` 标识这条消息。

```json
{
  "jsonrpc": "2.0",
  "id": "request-1",
  "method": "SendMessage",
  "params": {
    "message": {
      "messageId": "02edf671-c4cf-4fde-9086-8a36fef64855",
      "role": "ROLE_USER",
      "parts": [{ "text": "请检查项目并总结结果" }]
    },
    "configuration": { "returnImmediately": true }
  }
}
```

保存响应里的 `result.task.id` 和 `result.task.contextId`。以后查询使用完整任务编号：

```json
{
  "jsonrpc": "2.0",
  "id": "query-1",
  "method": "GetTask",
  "params": { "id": "<完整任务编号>" }
}
```

查询结果位于 `result`：`status.state` 为 `TASK_STATE_WORKING` 时继续等待，完成后从 `artifacts[].parts[].text` 读取文字。失败返回 `TASK_STATE_FAILED` 和原因。在 Botmux 侧中断任务后，返回 `TASK_STATE_CANCELED`；原样重发仍返回原任务的已取消状态，不会重新执行。本次查询断线或请求错误不代表任务失败。

## 追问与重试

- 双方保留各自的会话编号。调用方保存“本地对话、目标机器人、分支 → contextId”的对应关系。
- 本轮结束后追问，带回 `message.contextId`，生成新的 `messageId`，不带已经完成的 `taskId`。下一轮有新的任务编号，旧任务仍可查询。
- 已拿到任务编号时，只查询该任务。提交后没收到回执，才原样重发同一消息；保留原 `messageId`、内容和会话参数，HTTP 流水号可以改变。
- 原消息已完成时，重发会返回原任务的当前状态和回答。明确想重新执行，使用新的 `messageId`。
- 一个会话同一时间只提交一条执行消息；多个独立任务各开会话。会话隔离不等于文件目录隔离。

## 能力范围

本版支持文字、异步提交和结果查询。慢任务不受原同步等待的 120 秒限制；调用方自行决定查询间隔和总等待时长。当前查询结果没有中间文字，也不提供取消、推送或流式订阅。停止查询不会取消任务。

Dashboard 的会话卡片和“最近问答”读取已保存的异步任务回答，保留文字换行；与聊天回答混用时按时间显示最近一条。续问完成后显示新回答。终端链接通过 Dashboard 同源入口进行登录校验。

## 验证

`test/trigger-session-idempotency-e2e.test.ts` 中的 A2A 集成测试串联真实 HTTP、派发逻辑和临时结果存储，覆盖提交、查询、续问、重发及旧轮次结果查询，并检查 Worker 启动次数。Worker 本身使用可控测试实现。在仓库目录运行：

```bash
node_modules/.bin/vitest run test/trigger-session-idempotency-e2e.test.ts -t 'A2A HTTP retries'
```

2026-09-25 使用真实 Codex App CLI 和 tmux 完成提交、查询、续问、重发、并发独立会话及服务重启测试。130 秒等待任务约 149 秒后取得结果，重发后的执行计数仍为 1；14 个异常请求没有启动 Worker。查询不存在轮次的错误映射已在实测中修复。

Dashboard 已用真实 Codex App CLI 和浏览器验证首轮回答、续问更新及中文换行；只读终端已连接并显示实际输出。原有集成流程补充了回答预览、机器人归属和聊天记录混用检查。Claude Code 补充实测遇到模型服务 ConnectionRefused，回答展示未完成该 CLI 的实测。其他操作系统和公网反向代理尚未验证。
