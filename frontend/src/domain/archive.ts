/** 可恢复归档批次的前端调用层：归档、核验、释放、恢复和重放取正文。 */
import { request } from '@/api/client'

export type ArchiveManifestRow = {
  原始行号: string
  原始记录ID: number
  正文摘要: string
  字节数: number
  归档位置: string
  行状态: string
  正文来源: string
  迁移确认: boolean
}

export type ArchiveBatch = {
  id: number
  归档批次号: string
  源批次号: string
  策略ID: number | null
  策略名称: string
  归档格式: string
  目标存储: string
  归档条件: string
  status: string
  归档状态: string
  核验状态: string
  归档进度: number
  原始行数: number
  已归档行数: number
  已核验行数: number
  已释放行数: number
  已恢复行数: number
  待释放行数: number
  重试次数: number
  本次迁移行数: number
  跳过已确认行数: number
  迁移失败行数: number
  归档中断原因?: string
  归档位置?: string
  manifest: ArchiveManifestRow[]
  manifest_text: string
  manifest_rows: number
  updated_at: string
}

export type ArchiveProgress = {
  批次编号: string
  归档批次号: string
  归档状态: string
  归档进度: number
  核验状态: string
  已核验行数: number
  已释放行数: number
  已恢复行数: number
  待释放行数: number
  热存行数: number
  归档存储行数: number
  正文来源: Record<string, number>
  重放占用中: boolean
  归档中断原因?: string
  可重放: boolean
}

export type BodyLine = {
  原始记录ID: number
  原始行号: string
  正文: string
  正文来源: string
  归档批次号: string
  归档位置: string
  归档状态: string
  正文摘要: string
}

export type ArchiveResult = {
  ok: boolean
  message: string
  batch: ArchiveBatch | null
  release?: { released: number; deferred: string[]; blocked_by_replay: boolean }
  skipped?: boolean
  reused?: boolean
}

export type ReplayResult = {
  ok: boolean
  message: string
  replay: Record<string, unknown>
  replayed_count?: number
  missing_count?: number
  正文来源统计?: Record<string, number>
  归档进度?: Record<string, ArchiveProgress>
}

async function postJson<T>(path: string, body?: unknown): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  if (!response.ok) {
    const detail = await response.text()
    throw new Error(`接口返回 ${response.status}：${detail.slice(0, 120)}`)
  }
  return (await response.json()) as T
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) throw new Error(`接口返回 ${response.status}，数据未读取`)
  return (await response.json()) as T
}

export function archiveBatch(
  batchNo: string,
  options: { policyId?: number | null; conditions?: Record<string, unknown>; autoRelease?: boolean } = {},
): Promise<ArchiveResult> {
  return postJson<ArchiveResult>('/api/pipeline/archive/batch', {
    batch_no: batchNo,
    policy_id: options.policyId ?? null,
    conditions: options.conditions ?? {},
    auto_release: options.autoRelease ?? true,
  })
}

export function verifyArchive(archiveNo: string): Promise<ArchiveResult> {
  return postJson<ArchiveResult>(`/api/pipeline/archive/batch/${archiveNo}/verify`)
}

export function releaseArchive(archiveNo: string): Promise<ArchiveResult> {
  return postJson<ArchiveResult>(`/api/pipeline/archive/batch/${archiveNo}/release`)
}

export function restoreArchive(archiveNo: string): Promise<ArchiveResult> {
  return postJson<ArchiveResult>(`/api/pipeline/archive/batch/${archiveNo}/restore`)
}

export function loadManifest(archiveNo: string, fmt = 'md'): Promise<string> {
  return request(`/api/pipeline/archive/batch/${archiveNo}/manifest?fmt=${fmt}`).then(async (response) => {
    if (!response.ok) throw new Error('归档清单读取失败')
    return await response.text()
  })
}

export function loadProgress(batchNo: string): Promise<ArchiveProgress> {
  return getJson<ArchiveProgress>(`/api/pipeline/archive/progress/${encodeURIComponent(batchNo)}`)
}

export function loadBodies(batchNo: string): Promise<{ ok: boolean; 批次编号: string; 行明细: BodyLine[]; 归档进度: ArchiveProgress }> {
  return getJson(`/api/pipeline/body/${encodeURIComponent(batchNo)}`)
}

export function startReplay(batchNo: string, parserRule: string): Promise<ReplayResult> {
  return postJson<ReplayResult>('/api/pipeline/replay/start', {
    history_batch_range: batchNo,
    new_parser_rule: parserRule,
  })
}

export function advanceReplay(replayId: number): Promise<ReplayResult> {
  return postJson<ReplayResult>(`/api/pipeline/replay/${replayId}/advance`)
}

export function cancelReplay(replayId: number): Promise<ReplayResult> {
  return postJson<ReplayResult>(`/api/pipeline/replay/${replayId}/cancel`)
}

export function replayStatus(
  replayId: number,
): Promise<{ ok: boolean; replay: Record<string, unknown>; 归档进度: Record<string, ArchiveProgress>; 重放占用: Record<string, boolean> }> {
  return getJson(`/api/pipeline/replay/${replayId}/status`)
}

export function listBatches(batchNo = '', status = ''): Promise<{ ok: boolean; items: ArchiveBatch[]; total: number }> {
  const query = new URLSearchParams()
  if (batchNo) query.set('batch_no', batchNo)
  if (status) query.set('status', status)
  return getJson(`/api/pipeline/archive/batches?${query.toString()}`)
}
