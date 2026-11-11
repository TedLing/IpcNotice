<script setup lang="ts">
import { ref, reactive, onMounted } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import { ipcApi, type WebhookChannel } from "@/api/ipc";

defineOptions({ name: "ConfigChannel" });

interface HeaderRow {
  key: string;
  value: string;
}

const TEMPLATE_EXAMPLE = `{"title":"{title}","content":"{content}"}`;

const loading = ref(false);
const testing = ref(0);
const saving = ref(false);
const savingTest = ref(false);
const rows = ref<WebhookChannel[]>([]);
const dialogVisible = ref(false);
const editing = ref(false);
const headerRows = ref<HeaderRow[]>([]);
const form = reactive<Partial<WebhookChannel>>({});

const errMsg = (e: any) => e?.message ?? String(e);
const emptyForm = (): Partial<WebhookChannel> => ({
  name: "",
  url: "http://127.0.0.1:8080/webhook",
  headers: {},
  body_template: "",
  enabled: true,
  timeout_seconds: 5
});

/** 模板（占位符替换为普通文本后）必须是合法 JSON */
function templateValid(tpl: string): boolean {
  const t = (tpl ?? "").trim();
  if (!t) return true;
  try {
    JSON.parse(t.replace(/\{title\}/g, "t").replace(/\{content\}/g, "c"));
    return true;
  } catch {
    return false;
  }
}

async function load() {
  loading.value = true;
  try {
    rows.value = await ipcApi.channels();
  } catch (e) {
    ElMessage.error("加载通道失败：" + errMsg(e));
  } finally {
    loading.value = false;
  }
}

function toHeaderRows(headers: Record<string, string>): HeaderRow[] {
  return Object.entries(headers ?? {}).map(([key, value]) => ({ key, value }));
}
function toHeaders(list: HeaderRow[]): Record<string, string> {
  const out: Record<string, string> = {};
  list.forEach(h => {
    if (h.key.trim()) out[h.key.trim()] = h.value;
  });
  return out;
}

function openCreate() {
  Object.assign(form, emptyForm());
  headerRows.value = [{ key: "X-Source", value: "IpcNotice" }];
  editing.value = false;
  dialogVisible.value = true;
}

function openEdit(row: WebhookChannel) {
  Object.assign(form, {
    id: row.id,
    name: row.name,
    url: row.url,
    enabled: row.enabled,
    timeout_seconds: row.timeout_seconds,
    body_template: row.body_template ?? ""
  });
  headerRows.value = toHeaderRows(row.headers);
  if (!headerRows.value.length) headerRows.value = [{ key: "", value: "" }];
  editing.value = true;
  dialogVisible.value = true;
}

async function save(): Promise<number | null> {
  if (!form.name?.trim()) {
    ElMessage.warning("请填写通道名称");
    return null;
  }
  if (!form.url?.trim()) {
    ElMessage.warning("请填写通道 URL");
    return null;
  }
  const tpl = (form.body_template ?? "").trim();
  if (tpl && !templateValid(tpl)) {
    ElMessage.error("请求体模板不是合法 JSON（占位符 {title} / {content} 需写在字符串值内）");
    return null;
  }
  const payload: Partial<WebhookChannel> = {
    ...form,
    body_template: tpl,
    headers: toHeaders(headerRows.value)
  };
  saving.value = true;
  try {
    let id = form.id ?? 0;
    if (editing.value && id) await ipcApi.updateChannel(id, payload);
    else {
      const created = await ipcApi.createChannel(payload);
      id = created.id;
    }
    ElMessage.success(editing.value ? "已更新" : "已新增");
    dialogVisible.value = false;
    await load();
    return id;
  } catch (e) {
    ElMessage.error("保存失败：" + errMsg(e));
    return null;
  } finally {
    saving.value = false;
  }
}

/** 保存后立即用示例事件做一次真实推送，并展示实际发出的请求体 */
async function saveAndTest() {
  const id = await save();
  if (!id) return;
  savingTest.value = true;
  try {
    const res = await ipcApi.testChannel(id);
    showTestResult(res.ok, res.message, res.body);
    await load();
  } catch (e) {
    ElMessage.error("测试失败：" + errMsg(e));
  } finally {
    savingTest.value = false;
  }
}

async function test(row: WebhookChannel) {
  testing.value = row.id;
  try {
    const res = await ipcApi.testChannel(row.id);
    showTestResult(res.ok, res.message, res.body);
    await load();
  } catch (e) {
    ElMessage.error("测试失败：" + errMsg(e));
  } finally {
    testing.value = 0;
  }
}

function showTestResult(ok: boolean, message: string, body?: string) {
  if (ok) {
    ElMessageBox.alert(
      body ? `<pre style="margin:0;white-space:pre-wrap;word-break:break-all;font-size:12px">${escapeHtml(body)}</pre>` : message,
      `通道可用（${message}）实际发送的请求体：`,
      { dangerouslyUseHTMLString: true, confirmButtonText: "知道了" }
    ).catch(() => {});
  } else {
    ElMessage.error(`通道测试失败：${message}`);
  }
}

function escapeHtml(s: string) {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

async function remove(row: WebhookChannel) {
  try {
    await ElMessageBox.confirm(
      `删除「${row.name}」会同时解除所有订阅与该渠道的绑定，确认继续？`,
      "删除确认",
      { type: "warning" }
    );
  } catch {
    return;
  }
  try {
    await ipcApi.deleteChannel(row.id);
    ElMessage.success("已删除");
    await load();
  } catch (e) {
    ElMessage.error("删除失败：" + errMsg(e));
  }
}

function fillExample() {
  form.body_template = TEMPLATE_EXAMPLE;
}

function fmtTime(v: string) {
  return v ? v.replace("T", " ").slice(0, 19) : "-";
}

onMounted(load);
</script>

<template>
  <div class="p-4">
    <el-card shadow="never">
      <template #header>
        <div class="flex items-center justify-between">
          <div>
            <span class="font-bold">Webhook 通道</span>
            <span class="ml-3 text-xs text-gray-500">
              事件命中订阅后以 POST 推送到所选通道；支持自定义请求头与请求体模板
            </span>
          </div>
          <el-button type="primary" @click="openCreate">新增通道</el-button>
        </div>
      </template>

      <el-table v-loading="loading" :data="rows" border stripe style="width: 100%">
        <el-table-column prop="name" label="通道名称" width="160" />
        <el-table-column label="URL" min-width="260" show-overflow-tooltip>
          <template #default="{ row }"><code class="text-xs">{{ row.url }}</code></template>
        </el-table-column>
        <el-table-column label="请求体" width="120" align="center">
          <template #default="{ row }">
            <el-tag :type="row.body_template ? 'success' : 'info'" size="small">
              {{ row.body_template ? "自定义模板" : "完整事件 JSON" }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="请求头" min-width="160">
          <template #default="{ row }">
            <span class="text-xs text-gray-500">
              {{ Object.keys(row.headers || {}).length }} 项
              <span v-if="Object.keys(row.headers || {}).length">
                （{{ Object.keys(row.headers).slice(0, 2).join(", ") }}）
              </span>
            </span>
          </template>
        </el-table-column>
        <el-table-column label="启用" width="80" align="center">
          <template #default="{ row }">
            <el-tag :type="row.enabled ? 'success' : 'info'" size="small">
              {{ row.enabled ? "是" : "否" }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="最近测试" width="180">
          <template #default="{ row }">
            <el-tag v-if="row.last_test_at" :type="row.last_test_ok ? 'success' : 'danger'" size="small">
              {{ row.last_test_ok ? "通过" : "失败" }}
            </el-tag>
            <span v-else class="text-gray-400">未测试</span>
            <div class="text-xs text-gray-400">{{ row.last_test_msg || fmtTime(row.last_test_at) }}</div>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="220" fixed="right">
          <template #default="{ row }">
            <el-button link type="primary" @click="openEdit(row)">编辑</el-button>
            <el-button link type="primary" :loading="testing === row.id" @click="test(row)">
              测试推送
            </el-button>
            <el-button link type="danger" @click="remove(row)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-dialog v-model="dialogVisible" :title="editing ? '编辑通道' : '新增通道'" width="680px" destroy-on-close>
      <el-form label-width="110px">
        <el-form-item label="通道名称" required>
          <el-input v-model="form.name" placeholder="例如 企业微信机器人 / 家庭服务器" />
        </el-form-item>
        <el-form-item label="URL" required>
          <el-input v-model="form.url" placeholder="http(s)://host/path" />
        </el-form-item>
        <el-form-item label="请求方法">
          <el-tag>POST（固定）</el-tag>
        </el-form-item>
        <el-form-item label="超时（秒）">
          <el-input-number v-model="form.timeout_seconds" :min="1" :max="60" />
        </el-form-item>
        <el-form-item label="启用">
          <el-switch v-model="form.enabled" />
        </el-form-item>
        <el-form-item label="自定义请求头">
          <div class="w-full">
            <div v-for="(h, i) in headerRows" :key="i" class="flex items-center gap-2 mb-2">
              <el-input v-model="h.key" placeholder="Header 名称" style="width: 220px" />
              <el-input v-model="h.value" placeholder="值" />
              <el-button link type="danger" @click="headerRows.splice(i, 1)">移除</el-button>
            </div>
            <el-button size="small" @click="headerRows.push({ key: '', value: '' })">
              添加请求头
            </el-button>
          </div>
        </el-form-item>
        <el-form-item label="请求体模板">
          <div class="w-full">
            <el-input
              v-model="form.body_template"
              type="textarea"
              :rows="5"
              :placeholder="`留空则推送完整事件 JSON。示例：\n${TEMPLATE_EXAMPLE}`"
              style="font-family: monospace"
            />
            <div class="mt-1 flex items-center gap-2">
              <el-button size="small" @click="fillExample">填入示例</el-button>
              <span class="text-xs text-gray-400">
                占位符仅支持 {title}（摄像头xxx 发生-xxxx事件名称）与
                {content}（摄像头名称 + 事件 + 触发时间）
              </span>
            </div>
          </div>
        </el-form-item>
        <el-alert type="info" :closable="false" show-icon
          title="推送固定使用 POST；自动携带 X-IpcNotice-Event-ID / Device / Topic 请求头。" />
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="saving" @click="save">保存</el-button>
        <el-button type="success" :loading="savingTest" @click="saveAndTest">
          保存并测试推送
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>
