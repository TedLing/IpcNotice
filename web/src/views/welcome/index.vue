<script setup lang="ts">
import { ref, onMounted } from "vue";
import { useRouter } from "vue-router";
import { ElMessage } from "element-plus";
import { ipcApi, type RuntimeInfo } from "@/api/ipc";

defineOptions({ name: "Welcome" });

const router = useRouter();
const loading = ref(false);
const counts = ref<Record<string, number>>({});
const runtime = ref<RuntimeInfo | null>(null);

const cards = [
  { key: "cameras", label: "摄像头总数", hint: "已配置设备", route: "/device/camera" },
  { key: "cameras_enabled", label: "已启用设备", hint: "参与监听", route: "/device/camera" },
  { key: "channels", label: "通知通道", hint: "启用中", route: "/config/channel" },
  { key: "subscriptions", label: "事件订阅", hint: "启用中", route: "/device/subscribe" },
  { key: "topics", label: "已发现事件", hint: "声明 + 实抓", route: "/device/subscribe" },
  { key: "events", label: "事件记录", hint: "累计落库", route: "/monitor/events" },
  { key: "events_today", label: "今日事件", hint: "UTC 自然日", route: "/monitor/events" }
];

async function load() {
  loading.value = true;
  try {
    const res = await ipcApi.overview();
    counts.value = res.counts ?? {};
    runtime.value = res.runtime ?? null;
  } catch (e: any) {
    ElMessage.error("加载概览失败：" + (e?.message ?? e));
  } finally {
    loading.value = false;
  }
}

onMounted(load);
</script>

<template>
  <div class="p-4" v-loading="loading">
    <el-card shadow="never" class="mb-4">
      <template #header>
        <div class="flex items-center justify-between">
          <span class="font-bold">IpcNotice</span>
          <el-button @click="load">刷新</el-button>
        </div>
      </template>
      <p class="text-sm text-gray-600 leading-6">
        ONVIF Event 监听 → 事件订阅匹配 → Webhook 通道推送。设备、通道、订阅与运行参数全部在界面维护并存入
        SQLite；config.json 只保留登录用户名、密码与管理端口。
      </p>
      <div class="mt-3 flex flex-wrap gap-2">
        <el-button type="primary" @click="router.push('/device/camera')">配置摄像头</el-button>
        <el-button @click="router.push('/device/subscribe')">抓取并订阅事件</el-button>
        <el-button @click="router.push('/config/channel')">配置通知通道</el-button>
        <el-button @click="router.push('/monitor/runtime')">查看运行态</el-button>
      </div>
    </el-card>

    <el-row :gutter="16" class="mb-4">
      <el-col v-for="c in cards" :key="c.key" :xs="12" :sm="8" :md="6" :lg="4" class="mb-3">
        <el-card shadow="hover" class="cursor-pointer stat-card" @click="router.push(c.route)">
          <div class="text-xs text-gray-500">{{ c.label }}</div>
          <div class="text-3xl font-bold mt-2 text-blue-600">{{ counts[c.key] ?? 0 }}</div>
          <div class="text-xs text-gray-400 mt-1">{{ c.hint }}</div>
        </el-card>
      </el-col>
    </el-row>

    <el-card shadow="never">
      <template #header><span class="font-bold">链路状态</span></template>
      <template v-if="runtime">
        <el-descriptions :column="2" border size="small">
          <el-descriptions-item label="通告地址">{{ runtime.advertise_host }}</el-descriptions-item>
          <el-descriptions-item label="Notify 监听">{{ runtime.notify_listen }}</el-descriptions-item>
          <el-descriptions-item label="订阅模式">{{ runtime.event_mode }}</el-descriptions-item>
          <el-descriptions-item label="Notify 接收端">
            <el-tag :type="runtime.notify_running ? 'success' : 'danger'" size="small">
              {{ runtime.notify_running ? "运行中" : "未启动" }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="活动连接">
            {{ (runtime.workers || []).filter(w => w.connected).length }} /
            {{ (runtime.workers || []).length }}
          </el-descriptions-item>
          <el-descriptions-item label="启用订阅 / 路由">
            {{ runtime.subscriptions }} / {{ runtime.active_routes }}
          </el-descriptions-item>
        </el-descriptions>
      </template>
      <el-empty v-else description="暂无运行态数据" />
    </el-card>
  </div>
</template>

<style scoped>
.stat-card {
  min-height: 108px;
}
</style>
