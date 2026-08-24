# dsh-plugin-continue

为 DeepSeek Harness 增加一个直接续跑按钮。

当会话的最近一轮因为模型请求错误、输出 token 上限、进程崩溃恢复，或 Host 停止而没有正常完成时，按钮会出现在输入栏右侧。可继续的 subagent 也使用同一按钮，但只显示父会话在线时最近一个异常中断的 continuable subagent；已完成、one-shot 和更早的中断 subagent 不显示。点击按钮会让 Agent 直接开始下一轮；续跑操作本身不会把“继续”作为新的用户消息发送给模型。

## 工作方式

- Host 从持久化 Session 的最后一个 `turn/start` 或 `turn/end` 判断是否存在可续跑状态，并按 `seedLength` 重放冷 Session 的待处理输入。
- 对 subagent，Host 只接受 Session header 标记为 `origin: subagent` 的 continuable 子代理；普通 fork 即使有 `parentSession` 也不会被当作 subagent。多个子代理同时存在时，先按各自 seed 之后的最新异常边界排序，再检查唯一最新目标是否空闲；它正在运行时不会退回续跑更早的子代理。
- 续跑请求携带失败轮次和事件序号；状态变化或已有输入时，旧按钮请求会被拒绝，避免重复执行。
- 冷 Session 先通过 `sessionPersistence.list/inspect` 读取，再用 `agents.resume({ signal, setup })` 恢复日志记录的 preset 和模型选择。临时模型选择器只负责首个续跑请求，随后立即卸载；本次续跑完全空闲后释放冷 Agent，后续 Web 操作由标准恢复路径接管。读取、准备和发布提交点都响应插件关闭信号。
- 续跑使用当前 AgentLoop 的空闲驱动边界。普通会话首个 `preStep` 只接收一个内部流程哨兵；subagent 的 follow-up 进入其 FIFO inbox 后，插件会在首个 `preStep` 过滤插件专属空 marker。两条路径都会报告需要进入 step，但迭代结果为空，因此模型会继续处理已有 Session 历史，同时不会创建续跑专属的 `user/message`。AgentLoop 自身产生的动态上下文仍按其正常规则处理。
- subagent marker 进入首个 `preStep` 时会再次核对原失败边界和全部同级子代理；目标先执行了其他轮次，或其他子代理成为更新的异常目标时，本次续跑会取消。marker 在通过这道边界后才向客户端返回成功。
- Host 崩溃后留下的 marker 会与恢复时的新 marker 合并，只开启一轮模型执行。过滤同时依据 marker 的稳定消息 ID，因此其他 `preStep` 扩展改写其内容或来源也不会把它变成用户消息。入口快照之后到达的 marker 会进入插件自己的下一轮控制队列，并在当前 Agent 空闲后自动唤醒；期间出现普通输入时，本次续跑立即取消，普通输入独立执行。
- 插件只在目标 Agent 处于真实 idle 且没有普通收件箱输入时启动续跑。停止或更新时先关闭 RPC，通过运行期关闭信号结束插件自己的请求，清理队列 marker，并让正在执行的旧 wrapper 丢弃已 claim 的控制 marker，最后同步恢复 Agent 边界；卸载过程不会启动或等待模型轮次。
- 插件升级时会清理旧版普通会话遗留的空 marker；subagent 的持久 marker 由首个 `preStep` 合并消费。普通用户消息始终保留。
- 客户端只对普通会话，或 `continuable` 且父会话在线的 subagent 请求状态；one-shot 和父会话离线的 subagent 继续保持只读。
- 插件通过 `subagents.registerContinuableSetup` 在子 Agent 发布前安装同一边界，覆盖新建和冷恢复；如果 AgentLoop 不兼容，子 Agent 创建会回滚，不会退化为普通用户消息。

## 安装

把包加入 DSH Web profile 的 `dependencies`，再把 `dsh-plugin-continue` 加入 `dsh.profile.bundles`。包会通过自己的 `cordis.patch.yml` 插入唯一的 `dsh-continue` 行；不要再把同一行复制到 profile 的 `cordis.patch.yml`，否则启动会报 `duplicate loader entry id: dsh-continue`。

完成后重启 DSH Web 进程。仅刷新浏览器不会重新加载 Host 实现。

## 开发

```text
pnpm install
pnpm test
pnpm run pack:check
```

插件面向 DSH `0.1.1-rc.1` 的 Host/Client 合约构建。
