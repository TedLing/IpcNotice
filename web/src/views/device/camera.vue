<script setup lang="ts">
import { ref, reactive, onMounted, onBeforeUnmount, nextTick } from "vue";
import { useRouter } from "vue-router";
import { ElMessage, ElMessageBox } from "element-plus";
import { ipcApi, type Camera, type DeviceTestResult, type DeviceInfoResult, type DeviceInfoItem, type DeviceInfoSection, type PreviewAnswer } from "@/api/ipc";

defineOptions({ name: "DeviceCamera" });

const router = useRouter();
const loading = ref(false);
const testing = ref(0);
const rows = ref<Camera[]>([]);
const dialogVisible = ref(false);
const editing = ref(false);
const saving = ref(false);
const testVisible = ref(false);
const testResult = ref<DeviceTestResult | null>(null);

const infoVisible = ref(false);
const infoLoading = ref(false);
const infoDevice = ref<Camera | null>(null);
const deviceInfo = ref<DeviceInfoResult | null>(null);
const infoError = ref("");
const activeSection = ref("basic");

// ---- WebRTC 预览 ----
const previewVisible = ref(false);
const previewDevice = ref<Camera | null>(null);
const previewStatus = ref("");
const previewError = ref("");
const previewMeta = ref<PreviewAnswer | null>(null);
const videoRef = ref<HTMLVideoElement | null>(null);
let peerConn: RTCPeerConnection | null = null;
let previewSession = "";
let previewCameraId = 0;

const emptyForm = (): Partial<Camera> => ({
  name: "",
  url: "onvif://user:password@192.0.2.10:2020",
  username: "",
  password: "",
  enabled: true
});
const form = reactive<Partial<Camera>>(emptyForm());

const errMsg = (e: any) => e?.message ?? String(e);

async function load() {
  loading.value = true;
  try {
    rows.value = await ipcApi.cameras();
  } catch (e) {
    ElMessage.error("加载摄像头失败：" + errMsg(e));
  } finally {
    loading.value = false;
  }
}

function openCreate() {
  Object.assign(form, emptyForm());
  editing.value = false;
  dialogVisible.value = true;
}

function openEdit(row: Camera) {
  Object.assign(form, {
    id: row.id,
    name: row.name,
    url: row.url,
    username: row.username,
    password: row.password,
    enabled: row.enabled
  });
  editing.value = true;
  dialogVisible.value = true;
}

async function save() {
  if (!form.name?.trim()) return ElMessage.warning("请填写设备名称");
  if (!form.url?.trim()) return ElMessage.warning("请填写连接地址");
  saving.value = true;
  try {
    if (editing.value && form.id) {
      await ipcApi.updateCamera(form.id, form);
    } else {
      await ipcApi.createCamera(form);
    }
    ElMessage.success(editing.value ? "已更新" : "已新增");
    dialogVisible.value = false;
    await load();
  } catch (e) {
    ElMessage.error("保存失败：" + errMsg(e));
  } finally {
    saving.value = false;
  }
}

async function test(row: Camera) {
  testing.value = row.id;
  try {
    const res = await ipcApi.testCamera(row.id);
    testResult.value = res;
    testVisible.value = true;
    if (res.ok) ElMessage.success(`${row.name} 连接成功（${res.model || "未知型号"}）`);
    else ElMessage.error(res.message);
    await load();
  } catch (e) {
    ElMessage.error("测试失败：" + errMsg(e));
  } finally {
    testing.value = 0;
  }
}

async function remove(row: Camera) {
  try {
    await ElMessageBox.confirm(
      `删除「${row.name}」会同时删除它的事件抓取记录与订阅，确认继续？`,
      "删除确认",
      { type: "warning" }
    );
  } catch {
    return;
  }
  try {
    await ipcApi.deleteCamera(row.id);
    ElMessage.success("已删除");
    await load();
  } catch (e) {
    ElMessage.error("删除失败：" + errMsg(e));
  }
}

function gotoSubscribe(row: Camera) {
  router.push({ path: "/device/subscribe", query: { camera: String(row.id) } });
}

/** 打开「设备信息」弹窗并触发一次 ONVIF 采集 */
async function openDeviceInfo(row: Camera) {
  infoDevice.value = row;
  deviceInfo.value = null;
  infoError.value = "";
  activeSection.value = "basic";
  infoVisible.value = true;
  await fetchDeviceInfo(row.id);
}

async function fetchDeviceInfo(id: number) {
  infoLoading.value = true;
  try {
    deviceInfo.value = await ipcApi.deviceInfo(id);
  } catch (e) {
    infoError.value = errMsg(e);
    ElMessage.error("获取设备信息失败：" + errMsg(e));
  } finally {
    infoLoading.value = false;
  }
}

/** 把后端 [][]string 表格数据转成 el-table 需要的对象数组 */
function recordsAsObjects(item: DeviceInfoItem) {
  const cols = item.columns || [];
  return (item.records || []).map(record =>
    Object.fromEntries(cols.map((col, idx) => [col, record[idx] ?? ""]))
  );
}

function infoStats(res: DeviceInfoResult) {
  let ok = 0;
  let total = 0;
  res.sections.forEach(sec => {
    sec.items.forEach(item => {
      total++;
      if (item.ok) ok++;
    });
  });
  return { ok, total };
}

/** 每个分区的成功数，用于 tab 标签快速定位失败项 */
function sectionStats(sec: DeviceInfoSection) {
  const total = sec.items?.length ?? 0;
  const ok = (sec.items || []).filter(item => item.ok).length;
  return { ok, total };
}

function fmtTime(v: string) {
  return v ? v.replace("T", " ").slice(0, 19) : "-";
}

/** 打开预览弹窗：创建浏览器 PeerConnection，与后端交换 SDP 后播放视频 */
async function openPreview(row: Camera) {
  cleanupPreview();
  previewDevice.value = row;
  previewMeta.value = null;
  previewError.value = "";
  previewStatus.value = "正在连接设备...";
  previewVisible.value = true;
  await nextTick();
  await startPreview(row);
}

async function retryPreview() {
  if (previewDevice.value) {
    cleanupPreview();
    previewMeta.value = null;
    previewError.value = "";
    previewStatus.value = "正在连接设备...";
    await nextTick();
    await startPreview(previewDevice.value);
  }
}

async function startPreview(row: Camera) {
  try {
    // 服务与摄像头都在局域网内，host candidate 足够，无需 STUN/TURN
    const peer = new RTCPeerConnection({ iceServers: [] });
    peerConn = peer;
    peer.addTransceiver("video", { direction: "recvonly" });

    peer.ontrack = event => {
      const video = videoRef.value;
      if (video && event.streams[0]) {
        video.srcObject = event.streams[0];
        video.play().catch(() => {});
      }
      previewStatus.value = "播放中";
    };
    peer.oniceconnectionstatechange = () => {
      switch (peer.iceConnectionState) {
        case "connected":
          previewStatus.value = "播放中";
          break;
        case "disconnected":
          previewStatus.value = "连接中断，尝试恢复中...";
          break;
        case "failed":
          previewStatus.value = "连接失败";
          previewError.value =
            "WebRTC 连接失败：请确认本机防火墙放行 UDP，且浏览器与服务在同一可达网络";
          break;
      }
    };

    const offer = await peer.createOffer();
    await peer.setLocalDescription(offer);
    previewStatus.value = "正在选择 H.264 码流...";
    const answer = await ipcApi.previewOffer(row.id, offer.sdp || "");
    previewSession = answer.session;
    previewCameraId = row.id;
    previewMeta.value = answer;
    await peer.setRemoteDescription({ type: "answer", sdp: answer.sdp });
    previewStatus.value = "正在建立连接...";
  } catch (e) {
    previewStatus.value = "预览失败";
    previewError.value = errMsg(e);
    ElMessage.error("预览失败：" + errMsg(e));
    cleanupPreview();
  }
}

/** 释放会话：通知后端断开 RTSP，并销毁浏览器侧 PeerConnection */
function cleanupPreview() {
  if (previewSession && previewCameraId) {
    ipcApi.previewClose(previewCameraId, previewSession).catch(() => {});
  }
  previewSession = "";
  previewCameraId = 0;
  if (peerConn) {
    peerConn.ontrack = null;
    peerConn.oniceconnectionstatechange = null;
    peerConn.close();
    peerConn = null;
  }
  if (videoRef.value) videoRef.value.srcObject = null;
}

onMounted(load);
onBeforeUnmount(cleanupPreview);
</script>

<template>
  <div class="p-4">
    <el-card shadow="never">
      <template #header>
        <div class="flex items-center justify-between">
          <div>
            <span class="font-bold">摄像头管理</span>
            <span class="ml-3 text-xs text-gray-500">
              连接地址格式 onvif://用户名:密码@IP:端口，凭据也可在下方单独填写
            </span>
          </div>
          <el-button type="primary" @click="openCreate">新增摄像头</el-button>
        </div>
      </template>

      <el-table v-loading="loading" :data="rows" border stripe style="width: 100%">
        <el-table-column prop="name" label="设备名称" width="150" />
        <el-table-column label="连接地址" min-width="260" show-overflow-tooltip>
          <template #default="{ row }">
            <code class="text-xs">{{ row.url }}</code>
          </template>
        </el-table-column>
        <el-table-column label="启用" width="80" align="center">
          <template #default="{ row }">
            <el-tag :type="row.enabled ? 'success' : 'info'" size="small">
              {{ row.enabled ? "是" : "否" }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="型号 / 固件" min-width="180">
          <template #default="{ row }">
            <span>{{ row.model || "-" }}</span>
            <span v-if="row.firmware" class="ml-1 text-xs text-gray-400">
              {{ row.firmware }}
            </span>
          </template>
        </el-table-column>
        <el-table-column label="最近测试" width="180">
          <template #default="{ row }">
            <el-tag v-if="row.last_test_at" :type="row.last_test_ok ? 'success' : 'danger'" size="small">
              {{ row.last_test_ok ? "成功" : "失败" }}
            </el-tag>
            <span v-else class="text-gray-400">未测试</span>
            <div class="text-xs text-gray-400">{{ fmtTime(row.last_test_at) }}</div>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="460" fixed="right">
          <template #default="{ row }">
            <el-button link type="primary" @click="openPreview(row)">预览</el-button>
            <el-button link type="primary" @click="openEdit(row)">编辑</el-button>
            <el-button link type="primary" :loading="testing === row.id" @click="test(row)">
              测试连接
            </el-button>
            <el-button link type="primary" @click="openDeviceInfo(row)">设备信息</el-button>
            <el-button link type="primary" @click="gotoSubscribe(row)">事件订阅</el-button>
            <el-button link type="danger" @click="remove(row)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-dialog
      v-model="dialogVisible"
      :title="editing ? '编辑摄像头' : '新增摄像头'"
      width="620px"
      destroy-on-close
    >
      <el-form label-width="110px">
        <el-form-item label="设备名称" required>
          <el-input v-model="form.name" placeholder="例如 camera-01（唯一，用于回调路由）" />
        </el-form-item>
        <el-form-item label="连接地址" required>
          <el-input v-model="form.url" placeholder="onvif://user:password@192.0.2.10:2020" />
        </el-form-item>
        <el-form-item label="用户名">
          <el-input v-model="form.username" placeholder="可选：覆盖 URL 中的用户名" />
        </el-form-item>
        <el-form-item label="密码">
          <el-input v-model="form.password" type="password" show-password placeholder="可选：覆盖 URL 中的密码" />
        </el-form-item>
        <el-form-item label="启用">
          <el-switch v-model="form.enabled" />
        </el-form-item>
        <el-alert
          type="info"
          :closable="false"
          show-icon
          title="设备必须能被本机访问；Push 模式还要求设备能反向访问「系统设置」中的通告地址。"
        />
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="saving" @click="save">保存</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="testVisible" title="连接测试结果" width="680px">
      <template v-if="testResult">
        <el-descriptions :column="2" border size="small">
          <el-descriptions-item label="结果">
            <el-tag :type="testResult.ok ? 'success' : 'danger'" size="small">
              {{ testResult.ok ? "成功" : "失败" }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="说明">{{ testResult.message }}</el-descriptions-item>
          <el-descriptions-item label="厂商">{{ testResult.manufacturer || "-" }}</el-descriptions-item>
          <el-descriptions-item label="型号">{{ testResult.model || "-" }}</el-descriptions-item>
          <el-descriptions-item label="固件">{{ testResult.firmware || "-" }}</el-descriptions-item>
          <el-descriptions-item label="序列号">{{ testResult.serial || "-" }}</el-descriptions-item>
          <el-descriptions-item label="鉴权方式">{{ testResult.auth_mode || "-" }}</el-descriptions-item>
          <el-descriptions-item label="事件服务地址">{{ testResult.events_xaddr || "-" }}</el-descriptions-item>
          <el-descriptions-item label="PullPoint">
            {{ testResult.pullpoint_support ? "支持" : "未声明" }}
          </el-descriptions-item>
          <el-descriptions-item label="Push">
            {{ testResult.push_support ? "支持" : "未声明" }}
          </el-descriptions-item>
          <el-descriptions-item label="声明主题数">{{ testResult.topics?.length ?? 0 }}</el-descriptions-item>
          <el-descriptions-item label="最大 PullPoint">{{ testResult.max_pull_points }}</el-descriptions-item>
        </el-descriptions>
        <div v-if="testResult.warnings?.length" class="mt-3">
          <el-alert
            v-for="(w, i) in testResult.warnings"
            :key="i"
            type="warning"
            :closable="false"
            :title="w"
            class="mb-1"
          />
        </div>
      </template>
      <template #footer>
        <el-button @click="testVisible = false">关闭</el-button>
      </template>
    </el-dialog>

    <el-dialog
      v-model="infoVisible"
      :title="`设备信息 - ${infoDevice?.name ?? ''}`"
      width="960px"
      top="5vh"
      destroy-on-close
    >
      <div v-loading="infoLoading" style="min-height: 120px">
        <el-alert v-if="infoError" type="error" :closable="false" :title="infoError" class="mb-3" />

        <template v-if="deviceInfo">
          <el-descriptions :column="4" border size="small" class="mb-3">
            <el-descriptions-item label="连接地址">
              <span class="break-all">{{ deviceInfo.endpoint }}</span>
            </el-descriptions-item>
            <el-descriptions-item label="鉴权方式">{{ deviceInfo.auth_mode || "-" }}</el-descriptions-item>
            <el-descriptions-item label="采集时间">{{ fmtTime(deviceInfo.fetched_at) }}</el-descriptions-item>
            <el-descriptions-item label="耗时">{{ deviceInfo.elapsed_ms }} ms</el-descriptions-item>
            <el-descriptions-item label="成功 / 总数">
              {{ infoStats(deviceInfo).ok }} / {{ infoStats(deviceInfo).total }}
            </el-descriptions-item>
            <el-descriptions-item label="Push 订阅槽上限">
              <el-tag :type="deviceInfo.max_notification_producers > 0 ? 'warning' : 'info'" size="small">
                {{ deviceInfo.max_notification_producers > 0 ? deviceInfo.max_notification_producers : "未知" }}
              </el-tag>
            </el-descriptions-item>
            <el-descriptions-item label="PullPoint 上限">
              {{ deviceInfo.max_pull_points > 0 ? deviceInfo.max_pull_points : "未知" }}
            </el-descriptions-item>
            <el-descriptions-item label="说明">
              单项失败通常是设备不支持该 ONVIF 操作，不影响其他项
            </el-descriptions-item>
          </el-descriptions>
          <el-alert
            type="info"
            :closable="false"
            class="mb-3"
            title="订阅上限来自 Event 服务的 GetServiceCapabilities（Device 服务的 GetCapabilities 不含此项）"
          >
            <div class="text-xs leading-5">
              MaxNotificationProducers 是设备端 Push 订阅槽位上限。TP-Link 不遵守订阅 TTL，
              进程异常退出会残留僵尸订阅占用槽位，占满后新订阅虽建立成功却收不到任何回调；
              可在「运行态」页查看创建/续约/退订与最近回调诊断，彻底清理需给摄像头断电重启。
            </div>
          </el-alert>
          <el-alert
            v-for="(w, i) in deviceInfo.warnings"
            :key="i"
            type="warning"
            :closable="false"
            :title="w"
            class="mb-2"
          />

          <el-tabs v-model="activeSection">
            <el-tab-pane
              v-for="sec in deviceInfo.sections"
              :key="sec.key"
              :name="sec.key"
            >
              <template #label>
                <span>{{ sec.title }}</span>
                <el-tag
                  :type="sectionStats(sec).ok === sectionStats(sec).total ? 'success' : 'warning'"
                  size="small"
                  class="ml-1"
                >
                  {{ sectionStats(sec).ok }}/{{ sectionStats(sec).total }}
                </el-tag>
              </template>
              <div v-for="item in sec.items" :key="item.operation" class="mb-4">
                <div class="flex items-center gap-2 mb-2">
                  <code class="text-sm font-bold">{{ item.operation }}</code>
                  <span class="text-xs text-gray-500">{{ item.label }}</span>
                  <el-tag :type="item.ok ? 'success' : 'warning'" size="small">
                    {{ item.ok ? "获取成功" : "不支持 / 失败" }}
                  </el-tag>
                </div>

                <el-alert
                  v-if="!item.ok"
                  type="warning"
                  :closable="false"
                  :title="item.error || '设备未返回数据'"
                  class="mb-2"
                />
                <template v-else>
                  <el-table
                    v-if="item.columns?.length"
                    :data="recordsAsObjects(item)"
                    border
                    stripe
                    size="small"
                    empty-text="设备未返回条目"
                  >
                    <el-table-column
                      v-for="col in item.columns"
                      :key="col"
                      :prop="col"
                      :label="col"
                      min-width="120"
                      show-overflow-tooltip
                    />
                  </el-table>
                  <el-descriptions v-else-if="item.rows?.length" :column="2" border size="small">
                    <el-descriptions-item
                      v-for="(rowItem, idx) in item.rows"
                      :key="idx"
                      :label="rowItem.label"
                    >
                      <span class="break-all">{{ rowItem.value || "-" }}</span>
                    </el-descriptions-item>
                  </el-descriptions>
                  <div v-else-if="item.text">
                    <pre class="info-text">{{ item.text }}</pre>
                    <div v-if="item.truncated" class="text-xs text-gray-500 mt-1">
                      内容过长已截断，仅展示前 16 KiB
                    </div>
                  </div>
                  <div v-else class="text-xs text-gray-400">设备未返回内容</div>
                </template>
              </div>
            </el-tab-pane>
          </el-tabs>
        </template>
      </div>

      <template #footer>
        <el-button
          v-if="infoDevice"
          :loading="infoLoading"
          @click="fetchDeviceInfo(infoDevice.id)"
        >
          重新获取
        </el-button>
        <el-button type="primary" @click="infoVisible = false">关闭</el-button>
      </template>
    </el-dialog>

    <el-dialog
      v-model="previewVisible"
      :title="`视频预览 - ${previewDevice?.name ?? ''}`"
      width="880px"
      top="6vh"
      destroy-on-close
      @close="cleanupPreview"
    >
      <div class="preview-stage">
        <video ref="videoRef" autoplay muted playsinline controls />
        <div v-if="previewError" class="preview-overlay">
          <el-alert type="error" :closable="false" show-icon title="预览失败" :description="previewError" />
        </div>
        <div v-else-if="previewStatus !== '播放中'" class="preview-overlay preview-pending">
          <span class="text-sm">{{ previewStatus || "正在连接..." }}</span>
        </div>
      </div>

      <el-descriptions :column="3" border size="small" class="mt-3">
        <el-descriptions-item label="状态">
          <el-tag :type="previewStatus === '播放中' ? 'success' : previewError ? 'danger' : 'info'" size="small">
            {{ previewStatus || "-" }}
          </el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="编码">
          {{ previewMeta?.codec || "-" }}
        </el-descriptions-item>
        <el-descriptions-item label="码流">
          {{ previewMeta?.profile || "-" }}
        </el-descriptions-item>
        <el-descriptions-item label="取流地址" :span="3">
          <span class="break-all text-xs">{{ previewMeta?.stream || "-" }}</span>
        </el-descriptions-item>
      </el-descriptions>

      <el-alert type="info" :closable="false" class="mt-3" title="关于预览">
        <div class="text-xs leading-5">
          通过 WebRTC 播放，后端用 ONVIF 取流地址直连摄像头 RTSP 并转发 H.264，不转码、不录像。
          浏览器普遍不支持 H.265，因此会自动选择设备的 H.264 码流（通常是子码流）；
          若设备只有 H.265 码流则无法预览。预览会话独立占用一路设备连接，关闭窗口即释放。
        </div>
      </el-alert>

      <template #footer>
        <el-button @click="retryPreview" :disabled="!previewDevice">重新连接</el-button>
        <el-button type="primary" @click="previewVisible = false">关闭</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.info-text {
  max-height: 260px;
  overflow: auto;
  margin: 0;
  padding: 10px 12px;
  background: var(--el-fill-color-light);
  color: var(--el-text-color-primary);
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 6px;
  font-size: 12px;
  line-height: 1.6;
  white-space: pre-wrap;
  word-break: break-all;
}

.preview-stage {
  position: relative;
  width: 100%;
  aspect-ratio: 16 / 9;
  background: #101114;
  border-radius: 6px;
  overflow: hidden;
}

.preview-stage video {
  display: block;
  width: 100%;
  height: 100%;
  object-fit: contain;
  background: #101114;
}

.preview-overlay {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 24px;
  pointer-events: none;
}

.preview-overlay :deep(.el-alert) {
  pointer-events: auto;
  max-width: 560px;
}

.preview-pending {
  color: #d6d9e0;
  background: rgba(0, 0, 0, 0.35);
}
</style>
