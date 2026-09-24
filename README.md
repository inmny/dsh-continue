# dsh-continue

为 DeepSeek Harness 增加一个直接续跑按钮。当会话异常结束时，可以从现有上下文继续执行，无需再次输入提示词。

## 特色

- **零提示词续跑**：直接沿用现有会话上下文，续跑过程不生成“继续”或其他额外用户消息。
- **覆盖常见异常结束**：支持模型请求失败、重试耗尽、输出 token 达到上限、进程崩溃、Host 中断、用户主动停止和未正常关闭的轮次。
- **精确选择子代理**：只允许继续最新且符合条件的异常 `continuable` subagent；已完成、`one-shot`、更早中断和父会话离线的子代理保持只读。
- **保留原有运行配置**：会话当前未加载时，会恢复原来的 Agent preset、模型、供应商和推理强度，再从中断位置继续。
- **防止重复执行**：自动处理重复点击、过期按钮、并发输入和崩溃遗留状态；普通输入到达时优先执行普通输入。
- **按状态显示按钮**：按钮只在当前会话确实可以继续时出现，正常完成的会话不会显示。

## 安装

要求：

- DeepSeek Harness `0.1.7-rc.1`
- Node.js 24 或更高版本
- 本机已经配置 GitHub SSH 访问

安装到 DSH Web profile：

```bash
dsh plugin --profile web add "git+ssh://git@github.com/inmny/dsh-continue.git#main"
```

这条命令会安装依赖，并自动把 `dsh-plugin-continue` 加入 Web profile 的 bundle 列表。然后停止并重新启动 DSH Web：

```bash
dsh web
```

仅刷新浏览器不会重新加载 Host 插件。

插件通过自带的 `cordis.patch.yml` 注册 `dsh-continue`。Web profile 的 `cordis.patch.yml` 无需添加同名插件行，否则会产生重复注册。

## 更新

```bash
dsh plugin --profile web update dsh-plugin-continue
dsh web
```

更新后需要重新启动 DSH Web。

## 卸载

```bash
dsh plugin --profile web remove dsh-plugin-continue
```

这条命令会同时从 Web profile 的 bundle 列表移除插件。随后重新启动 DSH Web。

## 开发

```bash
pnpm install
pnpm test
pnpm run pack:check
```

## 许可证

[MIT](./LICENSE)
