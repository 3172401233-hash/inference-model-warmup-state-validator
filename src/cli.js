#!/usr/bin/env node

import { readFileSync } from 'node:fs';
import { analyzeWarmup } from './warmup.js';

function loadEvents(path) {
  const raw = readFileSync(path, 'utf8');
  if (path.endsWith('.jsonl')) {
    return raw
      .split('\n')
      .map((line) => line.trim())
      .filter((line) => line.length > 0)
      .map((line) => JSON.parse(line));
  }
  const parsed = JSON.parse(raw);
  if (Array.isArray(parsed)) {
    return parsed;
  }
  if (Array.isArray(parsed.events)) {
    return parsed.events;
  }
  throw new Error(`${path} 必须是事件数组或包含 events 字段的 JSON 文件`);
}

function printText(result) {
  const { summary } = result;
  console.log('Warmup 校验结果（离线分析，未连接推理引擎）');
  console.log(
    `实例总数=${summary.totalInstances} ready=${summary.ready} blocked=${summary.blocked} ` +
      `rolled_back=${summary.rolledBack} 探针假成功实例=${summary.instancesWithEarlyProbe} ` +
      `不安全接入=${summary.totalUnsafeAdmissions}`,
  );

  for (const item of result.instances) {
    console.log('');
    console.log(`- 实例 ${item.instanceId}（version=${item.version ?? 'unknown'}）状态=${item.status}`);
    console.log(
      `  shard: ${item.gates.shards.loaded}/${item.gates.shards.total ?? '?'} ` +
        `complete=${item.gates.shards.complete}`,
    );
    if (item.gates.shards.failedShards.length > 0) {
      for (const failure of item.gates.shards.failedShards) {
        console.log(`    shard ${failure.shardId} 失败 @${failure.at} (${failure.reason ?? 'no-reason'})`);
      }
    }
    console.log(
      `  权重: loaded=${item.gates.weight.loaded} ok=${item.gates.weight.ok} ` +
        `digest=${item.gates.weight.digest ?? '-'} expected=${item.gates.weight.expectedDigest ?? '-'}`,
    );
    console.log(`  kernel: ready=${item.gates.kernel.ready}`);
    console.log(
      `  探针: 最新健康=${String(item.gates.probe.lastHealthy)} 假成功次数=${item.gates.probe.earlySuccesses.length}`,
    );
    if (item.reasons.length > 0) {
      for (const reason of item.reasons) {
        console.log(`  阻断: ${reason.code} @${reason.at ?? 'n/a'}`);
      }
    }
    if (item.safeReadyAt == null) {
      console.log('  安全接入点: 无（warmup 未完成或已回滚）');
    } else {
      console.log(`  安全接入点: ${item.safeReadyAt}（warmup 完成时间）`);
    }
    for (const admission of item.routing.unsafeAdmissions) {
      console.log(
        `  不安全接入: @${admission.at} phase=${admission.phase} 暴露=${admission.exposureMs ?? '未关闭'}ms`,
      );
    }
  }
}

function main() {
  const args = process.argv.slice(2);
  const asJson = args.includes('--json');
  const fileArg = args.find((arg) => !arg.startsWith('--'));
  if (!fileArg) {
    console.error('用法: node src/cli.js [--json] <events.json|events.jsonl>');
    process.exit(2);
  }

  const result = analyzeWarmup(loadEvents(fileArg));
  if (asJson) {
    console.log(JSON.stringify(result, null, 2));
  } else {
    printText(result);
  }
}

main();
