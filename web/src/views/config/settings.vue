<script setup lang="ts">
import { ref, reactive, onMounted } from "vue";
import { ElMessage } from "element-plus";
import { ipcApi, type Settings } from "@/api/ipc";

defineOptions({ name: "ConfigSettings" });

const loading = ref(false);
const saving = ref(false);
const form = reactive<Settings>({} as Settings);

const errMsg = (e: any) => e?.message ?? String(e);

async function load() {
  loading.value = true;
  try {
    Object.assign(form, await ipcApi.settings());
  } catch (e) {
    ElMessage.error("加载设置失败：" + errMsg(e));
  } finally {
    loading.value = false;
  }
}

async function save() {
  saving.value = true;
  try {
    Object.assign(form, await ipcApi.saveSettings({ ...form }));
    ElMessage.success("设置已保存，运行态已热更新");
  } catch (e) {
    ElMessage.error("保存失败：" + errMsg(e));
  } finally {
    saving.value = false;
  }
}

async function reset() {
  try {
    await ipcApi.reload();
    ElMessage.success("已按当前数据库配置重建订阅");
  } catch (e) {
    ElMessage.error("重载失败：" + errMsg(e));
  }
}

onMounted(load);
</script>

<template>
  <div class="p-4" v-loading="loading">
    <el-card shadow="never" class="mb-4">
      <template #header><span class="font-bold">系统设置</span></template>
      <el-alert
        type="info"
        :closable="false"
        show-icon
        class="mb-4"
        title="config.json 仅保留登录用户名、密码、管理端口；以下全部参数存于 SQLite，修改后无需重启即可对订阅生效。"
      />

      <el-form label-width="190px" style="max-width: 820px">
        <el-divider content-position="left">回调接收端（WS-BaseNotification Push）</el-divider>
        <el-form-item label="Notify 监听地址">
          <el-input v-model="form.notify_listen_addr" placeholder="0.0.0.0:9100" />
          <div class="text-xs text-gray-400">修改监听地址需重启程序；设备必须能访问该端口。</div>
        </el-form-item>
        <el-form-item label="通告地址">
          <el-input v-model="form.advertise_host" placeholder="设备可路由到的本机 IP，或 auto" />
          <div class="text-xs text-gray-400">
            写进 ConsumerReference 的地址，必须是摄像头能路由到的本机 IP；auto 会按到设备的路由自动探测。修改后需重启。
          </div>
        </el-form-item>
        <el-form-item label="Notify 路径">
          <el-input v-model="form.notify_path" placeholder="/onvif/notify" />
        </el-form-item>

        <el-divider content-position="left">事件订阅</el-divider>
        <el-form-item label="订阅模式">
          <el-select v-model="form.event_mode" style="width: 200px">
            <el-option label="auto（Push 优先，失败回退 PullPoint）" value="auto" />
            <el-option label="push（仅 WS-BaseNotification）" value="push" />
            <el-option label="pull（仅 PullPoint 长轮询）" value="pull" />
          </el-select>
        </el-form-item>
        <el-form-item label="订阅租约（秒）">
          <el-input-number v-model="form.subscription_seconds" :min="10" :max="3600" />
        </el-form-item>
        <el-form-item label="长轮询超时（秒）">
          <el-input-number v-model="form.pull_timeout_seconds" :min="1" :max="300" />
          <div class="text-xs text-gray-400">必须小于订阅租约。</div>
        </el-form-item>
        <el-form-item label="单次拉取消息上限">
          <el-input-number v-model="form.message_limit" :min="1" :max="1000" />
        </el-form-item>
        <el-form-item label="状态去重窗口（秒）">
          <el-input-number v-model="form.dedup_seconds" :min="0" :max="86400" />
          <div class="text-xs text-gray-400">
            TP-Link 会反复推送同一状态；0 表示关闭去重。仅状态跃迁会外发 webhook。
          </div>
        </el-form-item>

        <el-divider content-position="left">事件外发与存储</el-divider>
        <el-form-item label="Webhook 超时（秒）">
          <el-input-number v-model="form.webhook_timeout_seconds" :min="1" :max="120" />
          <div class="text-xs text-gray-400">保存后立即生效（热更新）。</div>
        </el-form-item>
        <el-form-item label="分发 worker 数">
          <el-input-number v-model="form.webhook_workers" :min="1" :max="64" />
          <div class="text-xs text-amber-500">需重启程序后生效。</div>
        </el-form-item>
        <el-form-item label="事件队列容量">
          <el-input-number v-model="form.queue_size" :min="16" :max="100000" />
          <div class="text-xs text-amber-500">需重启程序后生效。</div>
        </el-form-item>
        <el-form-item label="读取超时（秒）">
          <el-input-number v-model="form.read_timeout_seconds" :min="1" :max="120" />
          <div class="text-xs text-amber-500">需重启程序后生效。</div>
        </el-form-item>
        <el-form-item label="事件 JSONL 文件">
          <el-input v-model="form.event_log_file" placeholder="logs/events.jsonl" />
        </el-form-item>
        <el-form-item label="数据库事件日志上限">
          <el-input-number v-model="form.max_event_logs" :min="100" :max="1000000" :step="100" />
        </el-form-item>

        <el-form-item>
          <el-button type="primary" :loading="saving" @click="save">保存设置</el-button>
          <el-button @click="load">重新载入</el-button>
          <el-button @click="reset">立即重建订阅</el-button>
        </el-form-item>
      </el-form>
    </el-card>
  </div>
</template>
