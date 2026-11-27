import assert from 'node:assert/strict';
import { test } from 'node:test';
import { analyzeWarmup, BLOCK_REASONS } from '../src/warmup.js';

const manifest = (instanceId, overrides = {}) => ({
  type: 'manifest',
  instanceId,
  ts: 1000,
  version: 'v3.1.0',
  totalShards: 3,
  expectedWeight: 'sha256:ok',
  ...overrides,
});

function healthyWarmupEvents(instanceId, { probeBeforeReady = false, admitEarly = false } = {}) {
  const events = [
    manifest(instanceId),
    { type: 'shard_loaded', instanceId, ts: 1100, shardId: 's0' },
    { type: 'shard_loaded', instanceId, ts: 1200, shardId: 's1' },
    { type: 'shard_loaded', instanceId, ts: 1300, shardId: 's2' },
    {
      type: 'weight_loaded',
      instanceId,
      ts: 1400,
      version: 'v3.1.0',
      weightDigest: 'sha256:ok',
    },
    { type: 'kernel_init', instanceId, ts: 1600, ready: true },
  ];
  if (probeBeforeReady) {
    events.push({ type: 'probe', instanceId, ts: 1250, healthy: true });
  }
  if (admitEarly) {
    events.push({ type: 'route_decision', instanceId, ts: 1260, action: 'admit' });
  }
  events.push({ type: 'probe', instanceId, ts: 1650, healthy: true });
  events.push({ type: 'route_decision', instanceId, ts: 1700, action: 'admit' });
  return events;
}

function byId(result, instanceId) {
  const found = result.instances.find((item) => item.instanceId === instanceId);
  assert.ok(found, `未找到实例 ${instanceId}`);
  return found;
}

test('完整 warmup 的实例在 kernel 就绪时间获得安全接入点', () => {
  const result = analyzeWarmup(healthyWarmupEvents('inst-ok'));
  const instance = byId(result, 'inst-ok');

  assert.equal(instance.status, 'ready');
  assert.deepEqual(instance.reasons, []);
  assert.equal(instance.warmupCompleteAt, 1600);
  assert.equal(instance.safeReadyAt, 1600);
  assert.equal(instance.gates.shards.complete, true);
  assert.equal(instance.gates.weight.ok, true);
  assert.equal(instance.gates.kernel.ready, true);
  assert.equal(instance.routing.unsafeAdmissions.length, 0);
  assert.equal(result.summary.ready, 1);
});

test('shard 失败会永久阻断，即使探针成功并已接入流量', () => {
  const events = [
    manifest('inst-shard-fail'),
    { type: 'shard_loaded', instanceId: 'inst-shard-fail', ts: 1100, shardId: 's0' },
    {
      type: 'shard_failed',
      instanceId: 'inst-shard-fail',
      ts: 1200,
      shardId: 's1',
      reason: 'disk io error',
    },
    { type: 'probe', instanceId: 'inst-shard-fail', ts: 1300, healthy: true },
    { type: 'route_decision', instanceId: 'inst-shard-fail', ts: 1310, action: 'admit' },
  ];

  const result = analyzeWarmup(events);
  const instance = byId(result, 'inst-shard-fail');

  assert.equal(instance.status, 'blocked');
  assert.equal(instance.safeReadyAt, null);
  const codes = instance.reasons.map((reason) => reason.code);
  assert.ok(codes.includes(BLOCK_REASONS.SHARD_FAILED));
  assert.ok(codes.includes(BLOCK_REASONS.INCOMPLETE_SHARDS));
  assert.deepEqual(instance.gates.shards.failedShards[0].shardId, 's1');

  assert.equal(instance.gates.probe.earlySuccesses.length, 1);
  assert.equal(instance.routing.unsafeAdmissions.length, 1);
  assert.equal(instance.routing.unsafeAdmissions[0].phase, 'warmup_incomplete');
  assert.equal(instance.routing.unsafeAdmissions[0].closedAt, null);
});

test('shard 失败后重新加载成功可以解除 SHARD_FAILED 阻断', () => {
  const events = [
    manifest('inst-retry'),
    { type: 'shard_loaded', instanceId: 'inst-retry', ts: 1100, shardId: 's0' },
    { type: 'shard_failed', instanceId: 'inst-retry', ts: 1200, shardId: 's1' },
    { type: 'shard_loaded', instanceId: 'inst-retry', ts: 1300, shardId: 's1' },
    { type: 'shard_loaded', instanceId: 'inst-retry', ts: 1400, shardId: 's2' },
    {
      type: 'weight_loaded',
      instanceId: 'inst-retry',
      ts: 1500,
      weightDigest: 'sha256:ok',
    },
    { type: 'kernel_init', instanceId: 'inst-retry', ts: 1700, ready: true },
    { type: 'probe', instanceId: 'inst-retry', ts: 1750, healthy: true },
  ];

  const instance = byId(analyzeWarmup(events), 'inst-retry');
  assert.equal(instance.status, 'ready');
  assert.equal(instance.gates.shards.complete, true);
  assert.equal(instance.gates.shards.failedShards.length, 0);
  assert.equal(instance.safeReadyAt, 1700);
});

test('探针在 warmup 完成前成功属于假成功，提前接入被标记为不安全', () => {
  const events = healthyWarmupEvents('inst-early', {
    probeBeforeReady: true,
    admitEarly: true,
  });

  const instance = byId(analyzeWarmup(events), 'inst-early');
  assert.equal(instance.status, 'ready');
  assert.deepEqual(instance.gates.probe.earlySuccesses, [1250]);

  const early = instance.routing.unsafeAdmissions.find((item) => item.phase === 'early_admission');
  assert.ok(early, '应当记录提前接入窗口');
  assert.equal(early.at, 1260);
  assert.equal(early.closedAt, 1600);
  assert.equal(early.exposureMs, 340);

  const safeAdmit = instance.routing.unsafeAdmissions.filter((item) => item.at === 1700);
  assert.equal(safeAdmit.length, 0);
});

test('版本回滚撤销安全接入点并标记回滚后的接入', () => {
  const events = [
    ...healthyWarmupEvents('inst-rollback'),
    { type: 'rollback', instanceId: 'inst-rollback', ts: 1800, version: 'v3.0.9' },
    { type: 'route_decision', instanceId: 'inst-rollback', ts: 1900, action: 'admit' },
  ];

  const instance = byId(analyzeWarmup(events), 'inst-rollback');
  assert.equal(instance.status, 'rolled_back');
  assert.equal(instance.safeReadyAt, null);
  assert.equal(instance.rollback.rolledBack, true);
  assert.equal(instance.rollback.version, 'v3.0.9');

  const codes = instance.reasons.map((reason) => reason.code);
  assert.deepEqual(codes, [BLOCK_REASONS.ROLLED_BACK]);

  const before = instance.routing.unsafeAdmissions.find((item) => item.phase === 'pre_rollback');
  assert.ok(before);
  assert.equal(before.at, 1700);
  assert.equal(before.closedAt, 1800);
  assert.equal(before.exposureMs, 100);

  const after = instance.routing.unsafeAdmissions.find((item) => item.phase === 'after_rollback');
  assert.ok(after);
  assert.equal(after.at, 1900);
  assert.equal(after.exposureMs, 0);
});

test('权重摘要与期望值不匹配时阻断接入', () => {
  const events = [
    manifest('inst-weight'),
    { type: 'shard_loaded', instanceId: 'inst-weight', ts: 1100, shardId: 's0' },
    { type: 'shard_loaded', instanceId: 'inst-weight', ts: 1200, shardId: 's1' },
    { type: 'shard_loaded', instanceId: 'inst-weight', ts: 1300, shardId: 's2' },
    {
      type: 'weight_loaded',
      instanceId: 'inst-weight',
      ts: 1400,
      weightDigest: 'sha256:wrong',
    },
    { type: 'kernel_init', instanceId: 'inst-weight', ts: 1500, ready: true },
    { type: 'probe', instanceId: 'inst-weight', ts: 1550, healthy: true },
  ];

  const instance = byId(analyzeWarmup(events), 'inst-weight');
  assert.equal(instance.status, 'blocked');
  assert.equal(instance.safeReadyAt, null);
  const mismatch = instance.reasons.find((reason) => reason.code === BLOCK_REASONS.WEIGHT_MISMATCH);
  assert.ok(mismatch);
  assert.equal(mismatch.detail.actualDigest, 'sha256:wrong');
  assert.equal(mismatch.detail.expectedDigest, 'sha256:ok');
});

test('显式 weight_mismatch 事件也会阻断', () => {
  const events = [
    manifest('inst-weight-event'),
    {
      type: 'weight_loaded',
      instanceId: 'inst-weight-event',
      ts: 1100,
      weightDigest: 'sha256:ok',
    },
    {
      type: 'weight_mismatch',
      instanceId: 'inst-weight-event',
      ts: 1200,
      actual: 'sha256:changed',
      expected: 'sha256:ok',
    },
  ];

  const instance = byId(analyzeWarmup(events), 'inst-weight-event');
  assert.equal(instance.gates.weight.mismatched, true);
  assert.ok(
    instance.reasons.some((reason) => reason.code === BLOCK_REASONS.WEIGHT_MISMATCH),
  );
});

test('kernel 未就绪时即使探针健康也不能接入', () => {
  const events = [
    manifest('inst-kernel'),
    { type: 'shard_loaded', instanceId: 'inst-kernel', ts: 1100, shardId: 's0' },
    { type: 'shard_loaded', instanceId: 'inst-kernel', ts: 1200, shardId: 's1' },
    { type: 'shard_loaded', instanceId: 'inst-kernel', ts: 1300, shardId: 's2' },
    {
      type: 'weight_loaded',
      instanceId: 'inst-kernel',
      ts: 1400,
      weightDigest: 'sha256:ok',
    },
    { type: 'kernel_init', instanceId: 'inst-kernel', ts: 1500, ready: false },
    { type: 'probe', instanceId: 'inst-kernel', ts: 1550, healthy: true },
  ];

  const instance = byId(analyzeWarmup(events), 'inst-kernel');
  assert.equal(instance.status, 'blocked');
  assert.ok(
    instance.reasons.some((reason) => reason.code === BLOCK_REASONS.KERNEL_NOT_READY),
  );
  assert.equal(instance.safeReadyAt, null);
});

test('部分 shard 加载会被识别为 INCOMPLETE_SHARDS', () => {
  const events = [
    manifest('inst-partial'),
    { type: 'shard_loaded', instanceId: 'inst-partial', ts: 1100, shardId: 's0' },
    { type: 'probe', instanceId: 'inst-partial', ts: 1200, healthy: true },
  ];

  const instance = byId(analyzeWarmup(events), 'inst-partial');
  const reason = instance.reasons.find((item) => item.code === BLOCK_REASONS.INCOMPLETE_SHARDS);
  assert.ok(reason);
  assert.deepEqual(reason.detail, { loaded: 1, total: 3 });
});

test('乱序事件按 ts 归一化后结果一致', () => {
  const ordered = healthyWarmupEvents('inst-order');
  const shuffled = [...ordered].reverse();
  assert.deepEqual(
    analyzeWarmup(shuffled).instances[0],
    analyzeWarmup(ordered).instances[0],
  );
});

test('非法事件输入抛出明确错误', () => {
  assert.throws(() => analyzeWarmup('not-an-array'), /events 必须是事件对象数组/);
  assert.throws(
    () => analyzeWarmup([{ type: 'unknown', instanceId: 'x', ts: 1 }]),
    /type 未知/,
  );
  assert.throws(
    () => analyzeWarmup([{ type: 'probe', instanceId: 'x', ts: 'not-a-time' }]),
    /ts 必须是/,
  );
});
