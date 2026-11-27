/**
 * 离线 warmup 状态校验器。
 *
 * 只消费事件日志（shard 加载、权重摘要、kernel 初始化、探针、路由、回滚），
 * 不连接任何推理引擎、模型服务或网络。
 */

export const EVENT_TYPES = Object.freeze({
  SHARD_LOADED: 'shard_loaded',
  SHARD_FAILED: 'shard_failed',
  MANIFEST: 'manifest',
  WEIGHT_LOADED: 'weight_loaded',
  WEIGHT_MISMATCH: 'weight_mismatch',
  KERNEL_INIT: 'kernel_init',
  PROBE: 'probe',
  ROUTE_DECISION: 'route_decision',
  ROLLBACK: 'rollback',
});

export const BLOCK_REASONS = Object.freeze({
  INCOMPLETE_SHARDS: 'INCOMPLETE_SHARDS',
  SHARD_FAILED: 'SHARD_FAILED',
  WEIGHT_MISSING: 'WEIGHT_MISSING',
  WEIGHT_MISMATCH: 'WEIGHT_MISMATCH',
  KERNEL_NOT_READY: 'KERNEL_NOT_READY',
  PROBE_UNHEALTHY: 'PROBE_UNHEALTHY',
  ROLLED_BACK: 'ROLLED_BACK',
});

const VALID_TYPES = new Set(Object.values(EVENT_TYPES));

function toTimestamp(value, label) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === 'string') {
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  throw new TypeError(`${label} 的 ts 必须是毫秒时间戳或 ISO 时间字符串，收到: ${String(value)}`);
}

function createInstanceState(instanceId) {
  return {
    instanceId,
    manifest: null,
    loadedShards: new Set(),
    failedShards: new Map(),
    lastShardTs: null,
    weight: null,
    weightMismatched: false,
    weightTs: null,
    kernelReady: false,
    kernelTs: null,
    probes: [],
    routes: [],
    rollback: null,
  };
}

function validateEvent(event, index) {
  if (event === null || typeof event !== 'object' || Array.isArray(event)) {
    throw new TypeError(`events[${index}] 必须是事件对象`);
  }
  if (!VALID_TYPES.has(event.type)) {
    throw new TypeError(`events[${index}] 的 type 未知: ${String(event.type)}`);
  }
  if (typeof event.instanceId !== 'string' || event.instanceId.length === 0) {
    throw new TypeError(`events[${index}] 缺少 instanceId`);
  }
}

function applyEvent(state, event, ts) {
  switch (event.type) {
    case EVENT_TYPES.MANIFEST:
      state.manifest = {
        version: event.version ?? state.manifest?.version ?? null,
        totalShards: event.totalShards ?? state.manifest?.totalShards ?? null,
        expectedWeight: event.expectedWeight ?? state.manifest?.expectedWeight ?? null,
        at: ts,
      };
      break;

    case EVENT_TYPES.SHARD_LOADED:
      if (typeof event.shardId !== 'string' && typeof event.shardId !== 'number') {
        throw new TypeError(`${state.instanceId} 的 shard_loaded 事件缺少 shardId`);
      }
      state.loadedShards.add(String(event.shardId));
      state.failedShards.delete(String(event.shardId));
      state.lastShardTs = ts;
      break;

    case EVENT_TYPES.SHARD_FAILED:
      if (typeof event.shardId !== 'string' && typeof event.shardId !== 'number') {
        throw new TypeError(`${state.instanceId} 的 shard_failed 事件缺少 shardId`);
      }
      state.failedShards.set(String(event.shardId), { at: ts, reason: event.reason ?? null });
      state.lastShardTs = ts;
      break;

    case EVENT_TYPES.WEIGHT_LOADED: {
      const digest = event.weightDigest ?? null;
      const expected = state.manifest?.expectedWeight ?? event.expectedWeight ?? null;
      const mismatched = expected != null && digest != null && digest !== expected;
      state.weight = { version: event.version ?? null, digest, expectedDigest: expected };
      state.weightMismatched = mismatched;
      state.weightTs = ts;
      break;
    }

    case EVENT_TYPES.WEIGHT_MISMATCH:
      state.weightMismatched = true;
      if (state.weight) {
        state.weight.expectedDigest = event.expected ?? state.weight.expectedDigest;
        state.weight.digest = event.actual ?? state.weight.digest;
      }
      break;

    case EVENT_TYPES.KERNEL_INIT:
      state.kernelReady = event.ready !== false;
      state.kernelTs = ts;
      break;

    case EVENT_TYPES.PROBE:
      state.probes.push({ at: ts, healthy: event.healthy !== false });
      break;

    case EVENT_TYPES.ROUTE_DECISION:
      if (event.action !== 'admit' && event.action !== 'hold') {
        throw new TypeError(
          `${state.instanceId} 的 route_decision.action 必须是 admit 或 hold，收到: ${String(event.action)}`,
        );
      }
      state.routes.push({ at: ts, action: event.action });
      break;

    case EVENT_TYPES.ROLLBACK:
      state.rollback = { at: ts, version: event.version ?? state.manifest?.version ?? null };
      break;

    default:
      throw new TypeError(`不支持的事件类型: ${event.type}`);
  }
}

function buildReasons(state, gates) {
  const reasons = [];

  if (state.failedShards.size > 0) {
    const firstFailure = [...state.failedShards.values()].sort((a, b) => a.at - b.at)[0];
    reasons.push({
      code: BLOCK_REASONS.SHARD_FAILED,
      at: firstFailure.at,
      detail: { failedShards: [...state.failedShards.keys()] },
    });
  }

  if (!gates.shards.complete) {
    reasons.push({
      code: BLOCK_REASONS.INCOMPLETE_SHARDS,
      at: state.lastShardTs,
      detail: { loaded: gates.shards.loaded, total: gates.shards.total },
    });
  }

  if (!state.weight) {
    reasons.push({ code: BLOCK_REASONS.WEIGHT_MISSING, at: null, detail: null });
  } else if (state.weightMismatched) {
    reasons.push({
      code: BLOCK_REASONS.WEIGHT_MISMATCH,
      at: state.weightTs,
      detail: {
        actualDigest: state.weight.digest,
        expectedDigest: state.weight.expectedDigest,
      },
    });
  }

  if (!state.kernelReady) {
    reasons.push({
      code: BLOCK_REASONS.KERNEL_NOT_READY,
      at: state.kernelTs,
      detail: state.kernelTs == null ? '未收到 kernel_init 就绪事件' : '最近一次 kernel_init 未就绪',
    });
  }

  const lastProbe = state.probes[state.probes.length - 1];
  if (lastProbe && !lastProbe.healthy) {
    reasons.push({
      code: BLOCK_REASONS.PROBE_UNHEALTHY,
      at: lastProbe.at,
      detail: '最近一次探针结果为不健康',
    });
  }

  if (state.rollback) {
    reasons.push({
      code: BLOCK_REASONS.ROLLED_BACK,
      at: state.rollback.at,
      detail: { version: state.rollback.version },
    });
  }

  return reasons;
}

function buildUnsafeAdmissions(state, safeReadyAt) {
  return state.routes
    .filter((route) => route.action === 'admit')
    .map((route) => {
      let closedAt = null;
      let phase;
      if (state.rollback && route.at >= state.rollback.at) {
        phase = 'after_rollback';
        closedAt = route.at;
      } else if (state.rollback) {
        phase = 'pre_rollback';
        closedAt = state.rollback.at;
      } else if (safeReadyAt == null) {
        phase = 'warmup_incomplete';
      } else if (route.at < safeReadyAt) {
        phase = 'early_admission';
        closedAt = safeReadyAt;
      } else {
        return null;
      }
      return {
        at: route.at,
        phase,
        closedAt,
        exposureMs: closedAt == null ? null : closedAt - route.at,
      };
    })
    .filter((item) => item !== null);
}

function summarize(state) {
  const totalShards = state.manifest?.totalShards ?? null;
  const shardsComplete =
    totalShards != null &&
    state.failedShards.size === 0 &&
    state.loadedShards.size >= totalShards;

  const weightOk = state.weight != null && !state.weightMismatched;
  const gateTimes = [
    shardsComplete ? state.lastShardTs : null,
    weightOk ? state.weightTs : null,
    state.kernelReady ? state.kernelTs : null,
  ].filter((ts) => ts != null);

  const structuralComplete = shardsComplete && weightOk && state.kernelReady;
  const warmupCompleteAt = structuralComplete ? Math.max(...gateTimes) : null;
  const rolledBack = state.rollback != null;
  const safeReadyAt = structuralComplete && !rolledBack ? warmupCompleteAt : null;

  const gates = {
    shards: {
      loaded: state.loadedShards.size,
      total: totalShards,
      complete: shardsComplete,
      failedShards: [...state.failedShards.entries()].map(([shardId, info]) => ({
        shardId,
        at: info.at,
        reason: info.reason,
      })),
    },
    weight: {
      loaded: state.weight != null,
      digest: state.weight?.digest ?? null,
      expectedDigest: state.weight?.expectedDigest ?? null,
      ok: weightOk,
      mismatched: state.weightMismatched,
    },
    kernel: { ready: state.kernelReady, at: state.kernelTs },
    probe: {
      lastHealthy: state.probes.length ? state.probes[state.probes.length - 1].healthy : null,
      at: state.probes.length ? state.probes[state.probes.length - 1].at : null,
      earlySuccesses: state.probes
        .filter((probe) => probe.healthy)
        .filter((probe) => warmupCompleteAt == null || probe.at < warmupCompleteAt)
        .map((probe) => probe.at),
    },
  };

  const reasons = buildReasons(state, gates);
  const status = rolledBack ? 'rolled_back' : reasons.length === 0 ? 'ready' : 'blocked';
  const unsafeAdmissions = buildUnsafeAdmissions(state, safeReadyAt);

  return {
    instanceId: state.instanceId,
    version: state.manifest?.version ?? state.weight?.version ?? null,
    status,
    reasons,
    gates,
    warmupCompleteAt,
    safeReadyAt,
    rollback: state.rollback
      ? { rolledBack: true, at: state.rollback.at, version: state.rollback.version }
      : { rolledBack: false, at: null, version: null },
    routing: {
      admitted: state.routes.filter((route) => route.action === 'admit').length,
      held: state.routes.filter((route) => route.action === 'hold').length,
      unsafeAdmissions,
    },
  };
}

/**
 * 分析 warmup 事件流，输出每个实例的状态、阻断原因与安全接入时间。
 *
 * @param {Array<object>} events 按时间发生的事件（乱序也可以，内部会按 ts 排序）
 * @returns {{generatedAt: number, instances: Array<object>, summary: object}}
 */
export function analyzeWarmup(events) {
  if (!Array.isArray(events)) {
    throw new TypeError('events 必须是事件对象数组');
  }

  const states = new Map();
  const order = [];

  const normalized = events
    .map((event, index) => {
      validateEvent(event, index);
      return { event, ts: toTimestamp(event.ts, `events[${index}]`) };
    })
    .sort((a, b) => a.ts - b.ts);

  for (const { event, ts } of normalized) {
    let state = states.get(event.instanceId);
    if (!state) {
      state = createInstanceState(event.instanceId);
      states.set(event.instanceId, state);
      order.push(event.instanceId);
    }
    applyEvent(state, event, ts);
  }

  const instances = order.map((instanceId) => summarize(states.get(instanceId)));
  const summary = {
    totalInstances: instances.length,
    ready: instances.filter((item) => item.status === 'ready').length,
    blocked: instances.filter((item) => item.status === 'blocked').length,
    rolledBack: instances.filter((item) => item.status === 'rolled_back').length,
    instancesWithEarlyProbe: instances.filter(
      (item) => item.gates.probe.earlySuccesses.length > 0,
    ).length,
    totalUnsafeAdmissions: instances.reduce(
      (count, item) => count + item.routing.unsafeAdmissions.length,
      0,
    ),
  };

  return { generatedAt: Date.now(), instances, summary };
}
