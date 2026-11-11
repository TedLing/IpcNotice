<script setup lang="ts">
import { ref, onMounted, onUnmounted } from "vue";
import { ElMessage } from "element-plus";
import { ipcApi, type EventLog } from "@/api/ipc";

defineOptions({ name: "MonitorEvents" });

const loading = ref(false);
const autoRefresh = ref(true);
const limit = ref(50);
const page = ref(1);
const total = ref(0);
const rows = ref<EventLog[]>([]);
const detailVisible = ref(false);
const detail = ref<EventLog | null>(null);
let timer: any = null;

const errMsg = (e: any) => e?.message ?? String(e);

async function load(silent = false) {
  if (!silent) loading.value = true;
  try {
    const res = await ipcApi.events(limit.value, (page.value - 1) * limit.value);
    rows.value = res.list ?? [];
    total.value = res.total ?? 0;
  } catch (e) {
    if (!silent) ElMessage.error("加载事件日志失败：" + errMsg(e));
  } finally {
    loading.value = false;
  }
}

function toggleAuto(v: boolean) {
  if (timer) clearInterval(timer);
  timer = null;
  if (v) timer = setInterval(() => load(true), 5000);
}

function showDetail(row: EventLog) {
  detail.value = row;
  detailVisible.value = true;
}

function fmtTime(v: string) {
  return v ? v.replace("T", " ").slice(0, 23) : "-";
}

onMounted(() => {
  load();
  toggleAuto(autoRefresh.value);
});
onUnmounted(() => toggleAuto(false));
</script>

<template>
  <div class="p-4">
    <el-card shadow="never">
      <template #header>
        <div class="flex items-center justify-between">
          <div>
            <span class="font-bold">事件日志</span>
            <span class="ml-3 text-xs text-gray-500">
              所有收到的事件都会落库；matched 表示命中订阅，dispatched 表示至少一条通道推送成功
            </span>
          </div>
          <div class="flex items-center gap-3">
            <el-switch v-model="autoRefresh" active-text="5 秒自动刷新" @change="toggleAuto" />
            <el-button @click="load()">刷新</el-button>
          </div>
        </div>
      </template>

      <el-table v-loading="loading" :data="rows" border stripe max-height="620" style="width: 100%">
        <el-table-column prop="id" label="#" width="80" />
        <el-table-column label="时间" width="200">
          <template #default="{ row }">{{ fmtTime(row.created_at) }}</template>
        </el-table-column>
        <el-table-column prop="device" label="设备" width="150" />
        <el-table-column label="事件名称" min-width="140" show-overflow-tooltip>
          <template #default="{ row }">{{ row.payload?.title || "-" }}</template>
        </el-table-column>
        <el-table-column prop="transport" label="通道" width="90" align="center" />
        <el-table-column label="事件主题" min-width="280" show-overflow-tooltip>
          <template #default="{ row }"><code class="text-xs">{{ row.topic }}</code></template>
        </el-table-column>
        <el-table-column label="命中订阅" width="100" align="center">
          <template #default="{ row }">
            <el-tag :type="row.matched ? 'success' : 'info'" size="small">
              {{ row.matched ? "是" : "否" }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="推送" width="90" align="center">
          <template #default="{ row }">
            <el-tag :type="row.dispatched ? 'success' : 'warning'" size="small">
              {{ row.dispatched ? "成功" : "未送出" }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="通知渠道" min-width="160" show-overflow-tooltip>
          <template #default="{ row }">{{ row.channels || "-" }}</template>
        </el-table-column>
        <el-table-column label="操作" width="90" fixed="right">
          <template #default="{ row }">
            <el-button link type="primary" @click="showDetail(row)">详情</el-button>
          </template>
        </el-table-column>
      </el-table>

      <div class="mt-3 flex items-center justify-between">
        <span class="text-xs text-gray-500">共 {{ total }} 条</span>
        <el-pagination
          v-model:current-page="page"
          :page-size="limit"
          :total="total"
          layout="prev, pager, next"
          background
          @current-change="load()"
        />
      </div>
    </el-card>

    <el-dialog v-model="detailVisible" title="事件载荷" width="720px">
      <pre class="json-view">{{ detail ? JSON.stringify(detail.payload, null, 2) : '' }}</pre>
      <template #footer><el-button @click="detailVisible = false">关闭</el-button></template>
    </el-dialog>
  </div>
</template>

<style scoped>
.json-view {
  max-height: 520px;
  overflow: auto;
  padding: 12px;
  border-radius: 6px;
  background: #f6f8fa;
  color: #24292f;
  font-size: 12px;
  line-height: 1.6;
}
</style>
