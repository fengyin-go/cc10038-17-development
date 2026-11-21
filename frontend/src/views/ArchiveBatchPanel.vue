<template>
  <section class="archive-panel" data-testid="archive-panel">
    <header class="panel-head">
      <div>
        <h3>可恢复归档批次</h3>
        <p class="page-desc">
          按归档条件保存完整正文，释放、归档、恢复都挂在原始行上；重放按原批次号读取正文。
        </p>
      </div>
      <button class="btn" type="button" data-testid="archive-refresh" @click="refresh">
        刷新归档进度
      </button>
    </header>

    <div class="archive-form">
      <label>
        <span>源批次号</span>
        <input v-model="batchNo" data-testid="archive-batch-no" placeholder="例如 INGE-0004" />
      </label>
      <label>
        <span>归档策略ID（可选）</span>
        <input v-model="policyId" data-testid="archive-policy-id" placeholder="例如 1" />
      </label>
      <button class="btn primary" type="button" data-testid="archive-run" :disabled="busy || !batchNo" @click="runArchive">
        执行归档
      </button>
      <button class="btn" type="button" data-testid="archive-verify" :disabled="busy || !archiveNo" @click="runVerify">
        正文核验
      </button>
      <button class="btn" type="button" data-testid="archive-release" :disabled="busy || !archiveNo" @click="runRelease">
        释放热存
      </button>
      <button class="btn" type="button" data-testid="archive-restore" :disabled="busy || !archiveNo" @click="runRestore">
        恢复正文
      </button>
    </div>

    <div class="archive-form">
      <label>
        <span>重放规则版本</span>
        <input v-model="parserRule" data-testid="replay-rule" placeholder="例如 rule-v2" />
      </label>
      <button class="btn" type="button" data-testid="replay-start" :disabled="busy || !batchNo || !parserRule" @click="runStartReplay">
        发起重放（占用正文）
      </button>
      <button class="btn" type="button" data-testid="replay-advance" :disabled="busy || !replayId" @click="runAdvanceReplay">
        执行重放
      </button>
      <button class="btn ghost" type="button" data-testid="replay-cancel" :disabled="busy || !replayId" @click="runCancelReplay">
        取消重放
      </button>
    </div>

    <p v-if="replayMessage" class="archive-message" data-testid="replay-message">{{ replayMessage }}</p>

    <p v-if="message" class="archive-message" data-testid="archive-message">{{ message }}</p>
    <p v-if="errorMessage" class="error-text" data-testid="archive-error">{{ errorMessage }}</p>

    <div v-if="progress" class="archive-stat-row">
      <article class="stat-card">
        <span class="stat-label">归档状态</span>
        <strong class="stat-value" data-testid="archive-status">{{ progress.归档状态 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">归档进度</span>
        <strong class="stat-value" data-testid="archive-percent">{{ progress.归档进度 }}%</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">核验状态</span>
        <strong class="stat-value" data-testid="archive-verify-state">{{ progress.核验状态 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">正文来源（热存/归档/不可用）</span>
        <strong class="stat-value" data-testid="archive-source">
          {{ progress.正文来源['热存'] ?? 0 }} / {{ progress.正文来源['归档存储'] ?? 0 }} /
          {{ progress.正文来源['不可用'] ?? 0 }}
        </strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">重放占用</span>
        <strong class="stat-value" data-testid="archive-lease">{{ progress.重放占用中 ? '占用中' : '空闲' }}</strong>
      </article>
    </div>

    <p v-if="progress?.归档中断原因" class="archive-message" data-testid="archive-interrupt">
      {{ progress.归档中断原因 }}
    </p>

    <table v-if="bodyLines.length" class="data-table" data-testid="archive-body-table">
      <thead>
        <tr>
          <th>原始行号</th>
          <th>正文来源</th>
          <th>归档状态</th>
          <th>归档位置</th>
          <th>正文摘要</th>
          <th>正文预览</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="line in bodyLines" :key="line.原始记录ID">
          <td>{{ line.原始行号 }}</td>
          <td>{{ line.正文来源 }}</td>
          <td>{{ line.归档状态 }}</td>
          <td>{{ line.归档位置 || '—' }}</td>
          <td>{{ line.正文摘要.slice(0, 12) || '—' }}</td>
          <td>{{ line.正文.slice(0, 48) || '—' }}</td>
        </tr>
      </tbody>
    </table>

    <details v-if="manifestText" class="archive-manifest" data-testid="archive-manifest">
      <summary>归档可读清单</summary>
      <pre>{{ manifestText }}</pre>
    </details>
  </section>
</template>

<script setup lang="ts">
import { ref } from 'vue'

import {
  advanceReplay,
  archiveBatch,
  cancelReplay,
  loadBodies,
  loadManifest,
  loadProgress,
  releaseArchive,
  restoreArchive,
  startReplay,
  verifyArchive,
} from '@/domain/archive'
import type { ArchiveProgress, BodyLine, ReplayResult } from '@/domain/archive'

const batchNo = ref('')
const policyId = ref('')
const archiveNo = ref('')
const parserRule = ref('')
const replayId = ref(0)
const replayMessage = ref('')
const progress = ref<ArchiveProgress | null>(null)
const bodyLines = ref<BodyLine[]>([])
const manifestText = ref('')
const message = ref('')
const errorMessage = ref('')
const busy = ref(false)

function fail(error: unknown) {
  errorMessage.value = error instanceof Error ? error.message : '归档操作失败'
}

async function applyResult(result: { ok: boolean; message: string; batch: { 归档批次号: string } | null }) {
  message.value = result.message
  if (result.batch) archiveNo.value = result.batch.归档批次号
  if (!result.ok) errorMessage.value = result.message
  await refresh()
}

async function refresh() {
  if (!batchNo.value) return
  errorMessage.value = ''
  try {
    const current = await loadProgress(batchNo.value)
    progress.value = current
    if (current.归档批次号) archiveNo.value = current.归档批次号
    const bodies = await loadBodies(batchNo.value)
    bodyLines.value = bodies.行明细
    if (archiveNo.value) manifestText.value = await loadManifest(archiveNo.value)
  } catch (error) {
    fail(error)
  }
}

async function runArchive() {
  busy.value = true
  errorMessage.value = ''
  try {
    const policy = policyId.value.trim()
    const result = await archiveBatch(batchNo.value.trim(), {
      policyId: policy ? Number(policy) : null,
    })
    await applyResult(result)
  } catch (error) {
    fail(error)
  } finally {
    busy.value = false
  }
}

async function runVerify() {
  busy.value = true
  try {
    await applyResult(await verifyArchive(archiveNo.value))
  } catch (error) {
    fail(error)
  } finally {
    busy.value = false
  }
}

async function runRelease() {
  busy.value = true
  try {
    await applyResult(await releaseArchive(archiveNo.value))
  } catch (error) {
    fail(error)
  } finally {
    busy.value = false
  }
}

async function runRestore() {
  busy.value = true
  try {
    await applyResult(await restoreArchive(archiveNo.value))
  } catch (error) {
    fail(error)
  } finally {
    busy.value = false
  }
}

async function applyReplay(result: ReplayResult) {
  replayMessage.value = result.message
  const replay = result.replay as { id?: number; 重放状态?: string } | null
  if (replay?.id) replayId.value = replay.id
  await refresh()
}

async function runStartReplay() {
  busy.value = true
  errorMessage.value = ''
  try {
    await applyReplay(await startReplay(batchNo.value.trim(), parserRule.value.trim()))
  } catch (error) {
    fail(error)
  } finally {
    busy.value = false
  }
}

async function runAdvanceReplay() {
  busy.value = true
  try {
    await applyReplay(await advanceReplay(replayId.value))
  } catch (error) {
    fail(error)
  } finally {
    busy.value = false
  }
}

async function runCancelReplay() {
  busy.value = true
  try {
    await applyReplay(await cancelReplay(replayId.value))
  } catch (error) {
    fail(error)
  } finally {
    busy.value = false
  }
}
</script>
