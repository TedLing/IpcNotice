<script setup lang="ts">
import { ref, onMounted, onUnmounted } from "vue";
import { ElMessage } from "element-plus";
import { ipcApi, type RuntimeInfo } from "@/api/ipc";
import { formatLocalTime } from "@/utils/time";

defineOptions({ name: "MonitorRuntime" });

const loading = ref(false);
const info = ref<RuntimeInfo | null>(null);
const reloadLoading = ref(false);
let timer: any = null;

const errMsg = (e: any) => e?.message ?? String(e);

async function load(silent = false) {
  if (!silent) loading.value = true;
  try {
    info.value = await ipcApi.runtime();
  } catch (e) {
    if (!silent) ElMessage.error("加载运行态失败：" + errMsg(e));
  } finally {
    loading.value = false;
  }
}

async function doReload() {
  reloadLoading.value = true;
  try {
    info.value = await ipcApi.reload();
    ElMessage.success("已按数据库配置重建订阅");
  } catch (e) {
    ElMessage.error("重载失败：" + errMsg(e));
  } finally {
    reloadLoading.value = false;
  }
}

function fmtTime(v: string) {
  return formatLocalTime(v);
}

onMounted(() => {
  load();
  timer = setInterval(() => load(true), 5000);
});
onUnmounted(() => timer && clearInterval(timer));
</script>

<template>
  <div class="p-4" v-loading="loading">
    <el-card shadow="never" class="mb-4">
      <template #header>
        <div class="flex items-center justify-between">
          <span class="font-bold">运行态</span>
          <div class="flex gap-2">
            <el-button :loading="reloadLoading" type="primary" @click="doReload">重建订阅</el-button>
            <el-button @click="load()">刷新</el-button>
          </div>
        </div>
      </template>

      <template v-if="info">
        <el-descriptions :column="2" border size="small">
          <el-descriptions-item label="启动时间">{{ fmtTime(info.started_at) }}</el-descriptions-item>
          <el-descriptions-item label="最近重载">{{ fmtTime(info.last_reload) }}</el-descriptions-item>
          <el-descriptions-item label="通告地址">{{ info.advertise_host }}</el-descriptions-item>
          <el-descriptions-item label="Notify 监听">{{ info.notify_listen }}</el-descriptions-item>
          <el-descriptions-item label="Consumer 基址">{{ info.consumer_base }}</el-descriptions-item>
          <el-descriptions-item label="订阅模式">{{ info.event_mode }}</el-descriptions-item>
          <el-descriptions-item label="Notify 接收端">
            <el-tag :type="info.notify_running ? 'success' : 'danger'" size="small">
              {{ info.notify_running ? "运行中" : "未启动" }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="启用的订阅 / 路由">
            {{ info.subscriptions }} / {{ info.active_routes }}
          </el-descriptions-item>
          <el-descriptions-item label="重载错误" :span="2">
            <span :class="info.last_reload_err ? 'text-red-500' : ''">
              {{ info.last_reload_err || "无" }}
            </span>
          </el-descriptions-item>
        </el-descriptions>
      </template>
    </el-card>

    <el-row :gutter="16">
      <el-col :span="24">
        <el-card shadow="never">
          <template #header>
            <div class="flex items-center justify-between">
              <span class="font-bold">设备订阅与诊断</span>
              <span class="text-xs text-gray-500">
                诊断统计为本进程内数据，重启后清零；订阅管理地址持久化于数据库
              </span>
            </div>
          </template>
          <el-table :data="info?.workers || []" border stripe size="small">
            <el-table-column type="expand">
              <template #default="{ row }">
                <el-descriptions :column="2" border size="small" class="m-2">
                  <el-descriptions-item label="回调 token 路径">
                    c{{ row.camera_id }}（稳定 token，改名/重启不变；同一摄像头的多个订阅都推到该地址）
                  </el-descriptions-item>
                  <el-descriptions-item label="订阅管理地址">
                    <span class="break-all">{{ row.sub_manager || "未知（尚未建立订阅）" }}</span>
                  </el-descriptions-item>
                  <el-descriptions-item label="最近订阅创建">
                    {{ fmtTime(row.last_create_at) }}
                    <el-tag :type="row.last_create_ok ? 'success' : 'danger'" size="small" class="ml-1">
                      {{ row.last_create_ok ? "成功" : "失败" }}
                    </el-tag>
                  </el-descriptions-item>
                  <el-descriptions-item label="最近续约">
                    {{ fmtTime(row.last_renew_at) }}
                    <el-tag v-if="row.last_renew_at" :type="row.last_renew_ok ? 'success' : 'danger'" size="small" class="ml-1">
                      {{ row.last_renew_ok ? "成功" : "失败" }}
                    </el-tag>
                    <span v-if="row.last_renew_err" class="text-red-500 ml-1">{{ row.last_renew_err }}</span>
                  </el-descriptions-item>
                  <el-descriptions-item label="最近退订">
                    {{ fmtTime(row.last_unsubscribe_at) }}
                    <el-tag v-if="row.last_unsubscribe_at" :type="row.last_unsubscribe_ok ? 'success' : 'warning'" size="small" class="ml-1">
                      {{ row.last_unsubscribe_ok ? "成功" : "失败（保留地址重试）" }}
                    </el-tag>
                  </el-descriptions-item>
                  <el-descriptions-item label="最近回调时间">
                    {{ fmtTime(row.last_notify_at) }}
                  </el-descriptions-item>
                  <el-descriptions-item label="健康探测">
                    {{ fmtTime(row.last_probe_at) || "-" }}
                    <el-tag v-if="row.probe_fails > 0" type="warning" size="small" class="ml-1">
                      连续失败 {{ row.probe_fails }} 次
                    </el-tag>
                    <span v-if="row.last_probe_err" class="text-red-500 ml-1">{{ row.last_probe_err }}</span>
                  </el-descriptions-item>
                  <el-descriptions-item label="启动时间">{{ fmtTime(row.started_at) }}</el-descriptions-item>
                  <el-descriptions-item label="更新时间">{{ fmtTime(row.updated_at) }}</el-descriptions-item>
                </el-descriptions>
              </template>
            </el-table-column>
            <el-table-column prop="device" label="设备" width="140" show-overflow-tooltip />
            <el-table-column label="设备" width="90">
              <template #default="{ row }">
                <el-tooltip :content="`最近探测 ${row.last_probe_at ? fmtTime(row.last_probe_at) : '-'}${row.last_probe_err ? '：' + row.last_probe_err : ''}`" placement="top">
                  <el-tag :type="row.device_online ? 'success' : 'danger'" size="small">
                    {{ row.device_online ? "在线" : "离线" }}
                  </el-tag>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column label="订阅" width="90">
              <template #default="{ row }">
                <el-tag :type="row.connected ? 'success' : 'warning'" size="small">
                  {{ row.connected ? "已连接" : "未连接" }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="传输" width="80">
              <template #default="{ row }">{{ row.transport || "-" }}</template>
            </el-table-column>
            <el-table-column label="创建" width="70">
              <template #default="{ row }">{{ row.create_count ?? 0 }} 次</template>
            </el-table-column>
            <el-table-column label="最近续约" width="170">
              <template #default="{ row }">
                <span v-if="row.last_renew_at">
                  <el-tag :type="row.last_renew_ok ? 'success' : 'danger'" size="small">
                    {{ row.last_renew_ok ? "OK" : "失败" }}
                  </el-tag>
                  {{ fmtTime(row.last_renew_at).slice(11) }}
                </span>
                <span v-else>-</span>
              </template>
            </el-table-column>
            <el-table-column label="最近回调" width="170">
              <template #default="{ row }">{{ fmtTime(row.last_notify_at) }}</template>
            </el-table-column>
            <el-table-column label="重试" width="60" prop="retry_count" />
            <el-table-column label="最近错误" min-width="200" show-overflow-tooltip>
              <template #default="{ row }">{{ row.last_error || "-" }}</template>
            </el-table-column>
          </el-table>
          <el-empty v-if="!info?.workers?.length" description="尚无设备订阅，请先在「事件订阅」页保存订阅" />
          <div class="mt-3 text-xs text-gray-500">
            说明：创建次数是本进程内建立订阅的总次数（含重连）；若「最近回调」长期不更新但订阅显示已连接，
            结合「订阅管理地址」检查摄像头订阅槽是否被残留订阅占满（此时给摄像头断电重启可彻底清理）。
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-row class="mt-4">
      <el-col :span="24">
        <el-card shadow="never">
          <template #header><span class="font-bold">回调来源计数（Notify POST，按 token/名称）</span></template>
          <el-table :data="Object.entries(info?.notify_posts || {}).map(([k, v]) => ({ name: k, count: v }))"
            border stripe size="small">
            <el-table-column prop="name" label="来源" min-width="200" show-overflow-tooltip />
            <el-table-column prop="count" label="POST 次数" width="140" />
          </el-table>
          <div class="mt-3 text-xs text-gray-500">
            计数大于 0 表示摄像头已能反向连到本机；若长期为 0，请检查防火墙与通告地址。
            来源名不在设备列表中（如旧名称路径或未知 token）说明存在指向旧地址的残留订阅推送，
            服务已回 200 防止重发；若新订阅收不到事件，给摄像头断电重启可清空其订阅表。
          </div>
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>
