# inference-model-warmup-state-validator

模型实例 warmup（预热）状态离线校验器。

模型实例**加载权重、初始化 kernel 与通过健康检查分属不同阶段**：健康探针可能在
shard 尚未加载完、权重摘要未校验或 kernel 未就绪时就提前返回成功，此时把实例接入
流量是不安全的。本工具消费一段事件日志，离线重放每个实例的 warmup 过程，计算它
**真正可以安全接入流量的时间点**，并标出探针假成功、提前接入和版本回滚。

## 不连接推理引擎

本项目是一个**纯离线、纯函数式的事件日志分析器**：

- 不启动、不连接任何推理引擎、模型服务、GPU runtime 或注册中心；
- 不发起任何网络请求，不读取真实实例状态；
- 唯一输入是 JSON/JSONL 事件文件，唯一输出是分析结果（终端文本或 JSON）；
- 所有时间均来自事件自身的 `ts` 字段（毫秒时间戳或 ISO 字符串）。

运行环境：Node.js 24.16.0（仅使用内置 `node:test`、`node:assert`、`node:fs`，无第三方依赖）。

## Warmup 门控模型

一个实例只有同时满足以下条件，才算 warmup 完成，其安全接入点
`safeReadyAt` 取所有门控完成时间的最大值（最后完成的那个阶段）：

1. **Shard 门控**：`manifest.totalShards` 声明的全部 shard 加载成功，且没有处于失败状态的 shard；
2. **权重门控**：收到 `weight_loaded`，且摘要 `weightDigest` 与 `manifest.expectedWeight` 一致；
3. **Kernel 门控**：收到 `kernel_init` 且 `ready=true`；
4. **未回滚**：没有 `rollback` 事件。回滚会撤销该版本的安全接入点（`safeReadyAt=null`）。

探针 `probe` **不是** warmup 完成的充分条件。在 warmup 完成之前出现的健康探针会被
记入 `gates.probe.earlySuccesses`（探针假成功）；在此之前的 `route_decision=admit`
会被记入 `routing.unsafeAdmissions`，并计算不安全暴露窗口。

## 事件格式

每个事件包含 `type`、`instanceId`、`ts`，按字段区分类型：

| type | 关键字段 | 含义 |
| --- | --- | --- |
| `manifest` | `version`, `totalShards`, `expectedWeight` | 声明 shard 总数与期望权重摘要 |
| `shard_loaded` | `shardId` | 单个 shard 加载成功（失败后重传成功会解除失败标记） |
| `shard_failed` | `shardId`, `reason?` | 单个 shard 加载失败 |
| `weight_loaded` | `version?`, `weightDigest` | 权重加载完成，自动与期望摘要比对 |
| `weight_mismatch` | `actual?`, `expected?` | 显式上报权重不匹配 |
| `kernel_init` | `ready` | kernel 初始化结果 |
| `probe` | `healthy` | 健康探针结果 |
| `route_decision` | `action`（`admit`/`hold`） | 流量路由决策 |
| `rollback` | `version?` | 版本回滚，撤销安全接入点 |

事件允许乱序，分析前会按 `ts` 排序。

## 输出说明

每个实例输出：

- `status`：`ready` / `blocked` / `rolled_back`；
- `reasons`：阻断原因列表，代码包括
  `INCOMPLETE_SHARDS`、`SHARD_FAILED`、`WEIGHT_MISSING`、`WEIGHT_MISMATCH`、
  `KERNEL_NOT_READY`、`PROBE_UNHEALTHY`、`ROLLED_BACK`；
- `gates`：shard / 权重 / kernel / 探针四个门控的明细，探针中含 `earlySuccesses` 假成功时间；
- `warmupCompleteAt`：结构上 warmup 全部完成的时间（未完成则为 `null`）；
- `safeReadyAt`：**安全接入流量的时间点**；回滚或未完成时为 `null`；
- `rollback`：回滚状态；
- `routing.unsafeAdmissions`：不安全接入记录，`phase` 取值：
  - `early_admission`：warmup 完成前接入，窗口在 `safeReadyAt` 关闭；
  - `warmup_incomplete`：warmup 始终未完成，暴露窗口不关闭（`closedAt=null`）；
  - `pre_rollback` / `after_rollback`：回滚前/后的接入，回滚即关闭窗口。

## 使用方式

```bash
# 文本报告
node src/cli.js examples/events.json

# JSON 输出（便于管道处理）
node src/cli.js --json examples/events.json

# 也支持 JSONL（每行一个事件）
node src/cli.js examples/events.jsonl
```

在代码中调用：

```js
import { analyzeWarmup } from './src/warmup.js';

const result = analyzeWarmup(events);
for (const instance of result.instances) {
  console.log(instance.instanceId, instance.status, instance.safeReadyAt);
}
```

## 测试

```bash
node --test
# 或
npm test
```

测试覆盖：完整 warmup 安全接入点、**shard 失败与部分 shard**、shard 重试恢复、
**探针提前成功（假成功）与提前接入暴露窗口**、**版本回滚**、权重摘要不匹配、
kernel 未就绪、乱序事件归一化以及非法输入校验。
