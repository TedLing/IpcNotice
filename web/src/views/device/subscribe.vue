<script setup lang="ts">
import { ref, computed, onMounted, watch } from "vue";
import { useRoute } from "vue-router";
import { ElMessage } from "element-plus";
import {
  ipcApi,
  type Camera,
  type CameraTopic,
  type WebhookChannel,
  type Subscription,
  type DeviceTestResult
} from "@/api/ipc";

defineOptions({ name: "DeviceSubscribe" });

interface TopicRow {
  topic: string;
  source: string;
  /** 事件名称/备注，推送时作为 title 一起发出 */
  remark: string;
  selected: boolean;
  enabled: boolean;
  channelIds: number[];
  /** 事件生命周期模式：instant（默认）| sustained（持续事件状态管理） */
  eventMode: "instant" | "sustained";
  /** sustained 静默超时秒数（0 = 默认 30） */
  inactivityTimeout: number;
  notifyStart: boolean;
  notifyEnd: boolean;
}

/** 表格行：叶子节点挂真实数据 data；分组节点（主题中间层级）仅展示结构 */
interface TableRow {
  key: string;
  topic: string;
  label: string;
  isGroup: boolean;
  leafCount: number;
  groupRemark?: string;
  data?: TopicRow;
  children?: TableRow[];
}

const route = useRoute();
const cameras = ref<Camera[]>([]);
const channels = ref<WebhookChannel[]>([]);
const rows = ref<TopicRow[]>([]);
const cameraId = ref<number | null>(null);
const captureSeconds = ref(6);
const loading = ref(false);
const discovering = ref(false);
const saving = ref(false);
const savingRemarks = ref(false);
const bulkChannel = ref<number[]>([]);
const customTopic = ref("");
const filterText = ref("");
const viewMode = ref<"tree" | "flat">("tree");
const lastResult = ref<{ test: DeviceTestResult; topics: CameraTopic[] } | null>(
  null
);

const errMsg = (e: any) => e?.message ?? String(e);

const filteredRows = computed(() => {
  const kw = filterText.value.trim().toLowerCase();
  if (!kw) return rows.value;
  return rows.value.filter(
    r =>
      r.topic.toLowerCase().includes(kw) ||
      (r.remark ?? "").toLowerCase().includes(kw)
  );
});
const selectedCount = computed(() => rows.value.filter(r => r.selected).length);
const enabledChannels = computed(() => channels.value.filter(c => c.enabled));

/** 表格数据：树状视图按主题路径层级分组（勾选只落在叶子）；平铺视图只显示最后一级 */
const tableData = computed<TableRow[]>(() => {
  if (viewMode.value === "flat") {
    return filteredRows.value.filter(r => !isIntermediate(r)).map(r => leafRow(r));
  }
  return buildTree(filteredRows.value);
});

/** 中间层级：存在以「该主题/」开头的其他主题，即视为父类节点 */
const intermediateSet = computed(() => {
  const set = new Set<string>();
  const list = filteredRows.value.map(r => r.topic);
  for (const t of list) {
    for (const other of list) {
      if (other !== t && other.startsWith(t + "/")) {
        set.add(t);
        break;
      }
    }
  }
  return set;
});

/** 平铺视图过滤：中间层级不显示（已有订阅的除外，便于取消） */
function isIntermediate(r: TopicRow) {
  return intermediateSet.value.has(r.topic) && !r.selected;
}

function leafRow(r: TopicRow): TableRow {
  const parts = r.topic.split("/").filter(Boolean);
  return {
    key: r.topic,
    topic: r.topic,
    label: parts[parts.length - 1] ?? r.topic,
    isGroup: false,
    leafCount: 1,
    data: r
  };
}

/** 按主题路径分段构建树：RuleEngine → RuleEngine/CellMotionDetector → …/Motion。
 *  有子节点的层级一律视为分组（不可勾选）；仅当该层级本身已存在旧订阅时保留为可勾选叶子，便于取消。 */
function buildTree(list: TopicRow[]): TableRow[] {
  type TmpNode = { children: Map<string, TmpNode>; row?: TopicRow };
  const root: TmpNode = { children: new Map() };
  for (const r of list) {
    const parts = r.topic.split("/").filter(Boolean);
    let cur = root;
    for (const seg of parts) {
      if (!cur.children.has(seg)) {
        cur.children.set(seg, { children: new Map() });
      }
      cur = cur.children.get(seg)!;
    }
    cur.row = r;
  }
  function toRows(node: TmpNode, parentKey: string): TableRow[] {
    const out: TableRow[] = [];
    for (const [seg, child] of [...node.children.entries()].sort((a, b) =>
      a[0].localeCompare(b[0])
    )) {
      const key = parentKey ? `${parentKey}/${seg}` : seg;
      const children = toRows(child, key);
      const childLeafCount =
        1 + children.reduce((sum, c) => sum + c.leafCount, 0);
      // 该层级自身是主题：默认分组展示（不可勾选）；已存在订阅时保留叶子形态
      if (child.row && (!children.length || child.row.selected)) {
        const row = leafRow(child.row);
        row.children = children.length ? children : undefined;
        row.leafCount = childLeafCount;
        out.push(row);
      } else {
        out.push({
          key,
          topic: key,
          label: seg,
          isGroup: true,
          leafCount: children.reduce((sum, c) => sum + c.leafCount, 0),
          groupRemark: child.row?.remark ?? "",
          children
        });
      }
    }
    return out;
  }
  return toRows(root, "");
}

async function loadBase() {
  loading.value = true;
  try {
    const [cams, chs] = await Promise.all([ipcApi.cameras(), ipcApi.channels()]);
    cameras.value = cams;
    channels.value = chs;
    const q = Number(route.query.camera);
    if (!cameraId.value && q && cams.some(c => c.id === q)) cameraId.value = q;
    else if (!cameraId.value && cams.length) cameraId.value = cams[0].id;
    if (cameraId.value) await loadTopics();
  } catch (e) {
    ElMessage.error("加载数据失败：" + errMsg(e));
  } finally {
    loading.value = false;
  }
}

async function loadTopics() {
  if (!cameraId.value) return;
  loading.value = true;
  try {
    const [topics, subs] = await Promise.all([
      ipcApi.topics(cameraId.value),
      ipcApi.cameraSubscriptions(cameraId.value)
    ]);
    rows.value = merge(topics, subs);
  } catch (e) {
    ElMessage.error("加载事件列表失败：" + errMsg(e));
  } finally {
    loading.value = false;
  }
}

function merge(topics: CameraTopic[], subs: Subscription[]): TopicRow[] {
  const map = new Map<string, TopicRow>();
  for (const t of topics) {
    map.set(t.topic, {
      topic: t.topic,
      source: t.source,
      remark: t.remark ?? "",
      selected: false,
      enabled: true,
      channelIds: [],
      eventMode: "instant",
      inactivityTimeout: 30,
      notifyStart: true,
      notifyEnd: true
    });
  }
  for (const s of subs) {
    const mode = s.event_mode === "sustained" ? "sustained" : "instant";
    const patch = {
      selected: true,
      enabled: s.enabled,
      channelIds: [...(s.channel_ids ?? [])],
      eventMode: mode as "instant" | "sustained",
      inactivityTimeout: s.inactivity_timeout > 0 ? s.inactivity_timeout : 30,
      notifyStart: s.notify_start !== false,
      notifyEnd: s.notify_end !== false
    };
    const existing = map.get(s.topic);
    if (existing) {
      Object.assign(existing, patch);
    } else {
      map.set(s.topic, {
        topic: s.topic,
        source: "custom",
        remark: "",
        ...patch
      });
    }
  }
  return Array.from(map.values()).sort((a, b) => a.topic.localeCompare(b.topic));
}

async function discover() {
  if (!cameraId.value) return ElMessage.warning("请先选择摄像头");
  discovering.value = true;
  try {
    const res = await ipcApi.discoverCamera(cameraId.value, captureSeconds.value);
    lastResult.value = res;
    const [topics, subs] = await Promise.all([
      ipcApi.topics(cameraId.value),
      ipcApi.cameraSubscriptions(cameraId.value)
    ]);
    rows.value = merge(topics, subs);
    const declared = res.test?.topics?.length ?? 0;
    const observed = res.test?.observed_topics?.length ?? 0;
    ElMessage.success(
      `抓取完成：声明主题 ${declared} 个，实抓主题 ${observed} 个，合计 ${topics.length} 个`
    );
    if (res.test?.warnings?.length) {
      ElMessage.warning(`有 ${res.test.warnings.length} 条告警，详见控制台日志`);
    }
    await loadBaseCamerasOnly();
  } catch (e) {
    ElMessage.error("测试/抓取失败：" + errMsg(e));
  } finally {
    discovering.value = false;
  }
}

async function loadBaseCamerasOnly() {
  cameras.value = await ipcApi.cameras();
}

// 备注覆盖表格所有行（含未订阅的事件），保存订阅时一并提交。
function remarkItems() {
  return rows.value
    .map(r => ({ topic: r.topic.trim(), remark: (r.remark ?? "").trim() }))
    .filter(i => i.topic);
}

async function save() {
  if (!cameraId.value) return ElMessage.warning("请先选择摄像头");
  const items = rows.value
    .filter(r => r.selected)
    .map(r => ({
      topic: r.topic.trim(),
      enabled: r.enabled,
      channel_ids: r.channelIds,
      event_mode: r.eventMode,
      inactivity_timeout: r.eventMode === "sustained" ? r.inactivityTimeout : 30,
      notify_start: r.notifyStart,
      notify_end: r.notifyEnd
    }))
    .filter(i => i.topic);
  if (items.some(i => !i.channel_ids.length)) {
    // 允许保存「只记录、不推送」的订阅，但给出明确提示
    ElMessage.info("存在未选择通知渠道的订阅：事件仍会落库，但不会外发 webhook");
  }
  saving.value = true;
  try {
    await ipcApi.saveSubscriptions(cameraId.value, items, remarkItems());
    ElMessage.success(`已保存 ${items.length} 条订阅与事件名称，运行态已热更新`);
    await loadTopics();
  } catch (e) {
    ElMessage.error("保存失败：" + errMsg(e));
  } finally {
    saving.value = false;
  }
}

async function saveRemarks() {
  if (!cameraId.value) return ElMessage.warning("请先选择摄像头");
  savingRemarks.value = true;
  try {
    await ipcApi.saveTopicRemarks(cameraId.value, remarkItems());
    ElMessage.success("事件名称已保存，后续推送会使用新的 title");
    await loadTopics();
  } catch (e) {
    ElMessage.error("保存事件名称失败：" + errMsg(e));
  } finally {
    savingRemarks.value = false;
  }
}

function topicPlaceholder(topic: string) {
  const parts = topic.replace(/^.*:/, "").split("/").filter(Boolean);
  const last = parts[parts.length - 1];
  return last && last !== "*" ? last : "填写事件名称";
}

function addCustom() {
  const t = customTopic.value.trim();
  if (!t) return;
  if (rows.value.some(r => r.topic === t)) return ElMessage.warning("主题已存在");
  rows.value.unshift({
    topic: t,
    source: "custom",
    remark: "",
    selected: true,
    enabled: true,
    channelIds: []
  });
  customTopic.value = "";
  ElMessage.success("已添加，保存后生效（支持以 /* 结尾通配子树）");
}

/** 只勾选叶子（最后一级事件）；分组/父类节点不参与 */
function toggleAll(v: boolean) {
  filteredRows.value.forEach(r => {
    if (!intermediateSet.value.has(r.topic) || r.selected) r.selected = v;
  });
}

function applyBulkChannels() {
  const n = filteredRows.value.filter(r => r.selected);
  n.forEach(r => (r.channelIds = [...bulkChannel.value]));
  ElMessage.success(`已为 ${n.length} 条勾选订阅设置通知渠道`);
}

function sourceLabel(s: string) {
  return s === "declared" ? "TopicSet 声明" : s === "observed" ? "实抓/运行观察" : "自定义";
}
function sourceType(s: string) {
  return s === "declared" ? "primary" : s === "observed" ? "success" : "warning";
}

watch(cameraId, () => {
  loadTopics();
});

onMounted(loadBase);
</script>

<template>
  <div class="p-4" v-loading="loading">
    <el-card shadow="never" class="mb-4">
      <template #header>
        <div class="flex items-center justify-between">
          <div>
            <span class="font-bold">事件订阅</span>
            <span class="ml-3 text-xs text-gray-500">
              选择摄像头 → 测试并抓取全部事件 → 树状勾选要订阅的事件 → 绑定通知渠道
            </span>
          </div>
          <el-button @click="loadTopics">刷新</el-button>
        </div>
      </template>

      <div class="flex flex-wrap items-center gap-3">
        <span class="text-sm">摄像头：</span>
        <el-select v-model="cameraId" placeholder="选择摄像头" style="width: 240px">
          <el-option
            v-for="c in cameras"
            :key="c.id"
            :label="`${c.name}（${c.model || c.url}）`"
            :value="c.id"
          />
        </el-select>

        <span class="text-sm ml-2">实抓时长：</span>
        <el-input-number v-model="captureSeconds" :min="0" :max="30" :step="1" controls-position="right" />
        <span class="text-xs text-gray-400">秒（0 = 仅枚举 TopicSet，不实抓）</span>

        <el-button type="primary" :loading="discovering" :disabled="!cameraId" @click="discover">
          测试连接并抓取事件
        </el-button>

        <div class="ml-auto flex items-center gap-2">
          <span class="text-sm text-gray-500">已勾选 {{ selectedCount }} / {{ rows.length }}</span>
          <el-button
            :loading="savingRemarks"
            :disabled="!cameraId"
            @click="saveRemarks"
          >
            仅保存名称
          </el-button>
          <el-button type="success" :loading="saving" :disabled="!cameraId" @click="save">
            保存订阅与名称
          </el-button>
        </div>
      </div>

      <el-alert
        v-if="lastResult"
        class="mt-3"
        type="success"
        :closable="false"
        show-icon
        :title="`最近一次抓取：${lastResult.test.model || '设备'}，声明主题 ${lastResult.test.topics?.length ?? 0} 个，实抓主题 ${lastResult.test.observed_topics?.length ?? 0} 个，鉴权 ${lastResult.test.auth_mode}`"
      />
      <div v-if="lastResult?.test?.warnings?.length" class="mt-2">
        <el-alert
          v-for="(w, i) in lastResult.test.warnings"
          :key="i"
          type="warning"
          :closable="false"
          :title="w"
          class="mb-1"
        />
      </div>
    </el-card>

    <el-card shadow="never">
      <template #header>
        <div class="flex flex-wrap items-center gap-2">
          <el-radio-group v-model="viewMode" size="small">
            <el-radio-button value="tree">树状</el-radio-button>
            <el-radio-button value="flat">平铺</el-radio-button>
          </el-radio-group>
          <el-input v-model="filterText" placeholder="按主题或事件名称过滤" clearable style="width: 240px" />
          <el-button @click="toggleAll(true)">全选当前视图</el-button>
          <el-button @click="toggleAll(false)">取消全选</el-button>
          <el-select
            v-model="bulkChannel"
            multiple
            collapse-tags
            collapse-tags-tooltip
            placeholder="批量设置通知渠道"
            style="width: 240px"
          >
            <el-option
              v-for="c in enabledChannels"
              :key="c.id"
              :label="c.name"
              :value="c.id"
            />
          </el-select>
          <el-button @click="applyBulkChannels">应用到勾选项</el-button>
          <div class="ml-auto flex items-center gap-2">
            <el-input
              v-model="customTopic"
              placeholder="自定义主题，如 RuleEngine/CellMotionDetector/Motion"
              style="width: 320px"
              @keyup.enter="addCustom"
            />
            <el-button @click="addCustom">添加</el-button>
          </div>
        </div>
      </template>

      <el-table
        :data="tableData"
        row-key="key"
        border
        stripe
        max-height="560"
        default-expand-all
        :tree-props="{ children: 'children' }"
        style="width: 100%"
      >
        <el-table-column label="订阅" width="70" align="center">
          <template #default="{ row }">
            <el-checkbox v-if="!row.isGroup" v-model="row.data.selected" />
            <span v-else class="text-xs text-gray-300">—</span>
          </template>
        </el-table-column>
        <el-table-column label="事件主题" min-width="340">
          <template #default="{ row }">
            <code
              v-if="row.isGroup"
              class="text-xs text-gray-400"
            >{{ row.label }}<span v-if="row.groupRemark"> · {{ row.groupRemark }}</span><span class="ml-1">（{{ row.leafCount }} 个事件）</span></code>
            <code v-else-if="viewMode === 'tree'" class="text-xs">{{ row.label }}</code>
            <code v-else class="text-xs">{{ row.topic }}</code>
          </template>
        </el-table-column>
        <el-table-column label="事件名称 / 备注（推送 title）" min-width="200">
          <template #default="{ row }">
            <el-input
              v-if="!row.isGroup"
              v-model="row.data.remark"
              size="small"
              maxlength="200"
              clearable
              :placeholder="topicPlaceholder(row.topic)"
            />
          </template>
        </el-table-column>
        <el-table-column label="来源" width="130">
          <template #default="{ row }">
            <el-tag v-if="!row.isGroup" :type="sourceType(row.data.source)" size="small">
              {{ sourceLabel(row.data.source) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="通知渠道" min-width="260">
          <template #default="{ row }">
            <el-select
              v-if="!row.isGroup"
              v-model="row.data.channelIds"
              multiple
              collapse-tags
              collapse-tags-tooltip
              :disabled="!row.data.selected"
              placeholder="选择渠道（可多选）"
              style="width: 100%"
            >
              <el-option
                v-for="c in channels"
                :key="c.id"
                :label="`${c.name}${c.enabled ? '' : '（已停用）'}`"
                :value="c.id"
                :disabled="!c.enabled"
              />
            </el-select>
          </template>
        </el-table-column>
        <el-table-column label="事件模式" width="190">
          <template #default="{ row }">
            <div v-if="!row.isGroup" class="flex items-center gap-1">
              <el-select
                v-model="row.data.eventMode"
                :disabled="!row.data.selected"
                size="small"
                style="width: 110px"
              >
                <el-option label="即时（默认）" value="instant" />
                <el-option label="持续（开始/结束）" value="sustained" />
              </el-select>
              <el-popover
                v-if="row.data.eventMode === 'sustained'"
                trigger="click"
                width="260"
              >
                <template #reference>
                  <el-button size="small" link type="primary">设置</el-button>
                </template>
                <div class="flex flex-col gap-2 text-xs">
                  <div class="flex items-center justify-between gap-2">
                    <span>静默超时（秒）</span>
                    <el-input-number
                      v-model="row.data.inactivityTimeout"
                      :min="0"
                      :max="3600"
                      size="small"
                      style="width: 110px"
                    />
                  </div>
                  <div class="text-gray-400">0 表示默认 30 秒；持续期间不再有事件即判定结束</div>
                  <div class="flex items-center justify-between">
                    <span>开始通知</span>
                    <el-switch v-model="row.data.notifyStart" size="small" />
                  </div>
                  <div class="flex items-center justify-between">
                    <span>结束通知</span>
                    <el-switch v-model="row.data.notifyEnd" size="small" />
                  </div>
                </div>
              </el-popover>
            </div>
          </template>
        </el-table-column>
        <el-table-column label="启用" width="80" align="center">
          <template #default="{ row }">
            <el-switch
              v-if="!row.isGroup"
              v-model="row.data.enabled"
              :disabled="!row.data.selected"
              size="small"
            />
          </template>
        </el-table-column>
      </el-table>

      <el-empty v-if="!rows.length" description="尚无事件，请先点击「测试连接并抓取事件」" />
      <div class="mt-3 text-xs text-gray-500">
        说明：事件主题按层级展示（RuleEngine → CellMotionDetector → Motion …），父类层级仅做分组，
        勾选、编辑名称与绑定渠道只作用于最后一级事件；「平铺」视图也只显示最后一级。<br />
        事件到达后按勾选主题做匹配（支持以 /* 结尾通配子树）再推送到所选渠道；未选渠道的事件仍会写入事件日志数据库。
        抓取到的新事件会自动预填中文名称，该名称作为 <code>title</code> 字段随 webhook 一起推送，未填写时回退为主题最后一段。
      </div>
    </el-card>
  </div>
</template>
