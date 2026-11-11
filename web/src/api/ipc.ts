import { http } from "@/utils/http";

export interface Camera {
  id: number;
  name: string;
  url: string;
  username: string;
  password: string;
  enabled: boolean;
  manufacturer: string;
  model: string;
  firmware: string;
  serial: string;
  last_test_at: string;
  last_test_ok: boolean;
  last_test_msg: string;
  created_at: string;
  updated_at: string;
}

export interface DeviceTestResult {
  ok: boolean;
  message: string;
  manufacturer: string;
  model: string;
  firmware: string;
  serial: string;
  auth_mode: string;
  events_xaddr: string;
  pullpoint_support: boolean;
  push_support: boolean;
  max_pull_points: number;
  topics: string[];
  observed_topics: string[];
  warnings: string[];
}

/** ONVIF 设备信息采集：一次操作的结果，展示形态三选一 */
export interface DeviceInfoRow {
  label: string;
  value: string;
}
export interface DeviceInfoItem {
  operation: string;
  label: string;
  ok: boolean;
  error?: string;
  rows?: DeviceInfoRow[];
  columns?: string[];
  records?: string[][];
  text?: string;
  truncated?: boolean;
}
export interface DeviceInfoSection {
  key: string;
  title: string;
  items: DeviceInfoItem[];
}
export interface DeviceInfoResult {
  device: string;
  endpoint: string;
  auth_mode: string;
  fetched_at: string;
  elapsed_ms: number;
  sections: DeviceInfoSection[];
  warnings: string[];
  /** Event 服务声明的 Push 订阅槽上限；0 表示未取到 */
  max_notification_producers: number;
  /** Event 服务声明的 PullPoint 上限；0 表示未取到 */
  max_pull_points: number;
}

export interface CameraTopic {
  id: number;
  camera_id: number;
  topic: string;
  source: "declared" | "observed" | "custom";
  /** 用户填写的事件名称/备注，推送时作为 title 输出 */
  remark: string;
  discovered_at: string;
}

export interface WebhookChannel {
  id: number;
  name: string;
  url: string;
  /** 固定 POST */
  method: string;
  headers: Record<string, string>;
  /** 请求体模板（JSON 文本，占位符 {title} / {content}）；{content} 为带标题头的多行文本（标题/摄像头/事件/触发时间），渲染时自动 JSON 转义；留空推送完整事件 JSON */
  body_template: string;
  enabled: boolean;
  timeout_seconds: number;
  last_test_at: string;
  last_test_ok: boolean;
  last_test_msg: string;
  created_at: string;
  updated_at: string;
}

export interface Subscription {
  id: number;
  camera_id: number;
  camera_name?: string;
  topic: string;
  enabled: boolean;
  channel_ids: number[];
  created_at: string;
  updated_at: string;
}

export interface Settings {
  notify_listen_addr: string;
  advertise_host: string;
  notify_path: string;
  event_mode: string;
  subscription_seconds: number;
  pull_timeout_seconds: number;
  message_limit: number;
  dedup_seconds: number;
  webhook_timeout_seconds: number;
  webhook_workers: number;
  queue_size: number;
  read_timeout_seconds: number;
  max_event_logs: number;
  event_log_file: string;
  updated_at?: string;
}

/** WebRTC 预览信令：后端返回 answer SDP 与会话 ID */
export interface PreviewAnswer {
  session: string;
  sdp: string;
  codec: string;
  profile: string;
  stream: string;
}

export interface WorkerStatus {
  camera_id: number;
  device: string;
  transport: string;
  connected: boolean;
  last_error: string;
  started_at: string;
  updated_at: string;
  retry_count: number;
  /** 订阅诊断：本次运行内订阅创建次数 */
  create_count: number;
  last_create_at: string;
  last_create_ok: boolean;
  last_renew_at: string;
  last_renew_ok: boolean;
  last_renew_err: string;
  last_unsubscribe_at: string;
  last_unsubscribe_ok: boolean;
  /** 当前已知的设备端订阅管理地址（持久化于 cameras.sub_manager） */
  sub_manager: string;
  /** 最近收到该设备回调的时间（NotifyServer 填充） */
  last_notify_at: string;
}

export interface RuntimeInfo {
  started_at: string;
  advertise_host: string;
  notify_listen: string;
  notify_path: string;
  consumer_base: string;
  event_mode: string;
  notify_running: boolean;
  notify_posts: Record<string, string>;
  workers: WorkerStatus[];
  active_routes: number;
  subscriptions: number;
  last_reload: string;
  last_reload_err: string;
}

export interface EventLog {
  id: number;
  event_id: string;
  camera_id: number;
  device: string;
  topic: string;
  transport: string;
  payload: any;
  matched: boolean;
  dispatched: boolean;
  channels: string;
  created_at: string;
}

const get = <T>(url: string, params?: object) =>
  http.request<T>("get", url, params ? { params } : {});
const post = <T>(url: string, data?: object) =>
  http.request<T>("post", url, data ? { data } : {});
const put = <T>(url: string, data?: object) =>
  http.request<T>("put", url, data ? { data } : {});
const del = <T>(url: string) => http.request<T>("delete", url);

export const ipcApi = {
  // 摄像头
  cameras: () => get<Camera[]>("/api/cameras"),
  createCamera: (data: Partial<Camera>) => post<Camera>("/api/cameras", data),
  updateCamera: (id: number, data: Partial<Camera>) =>
    put<Camera>(`/api/cameras/${id}`, data),
  deleteCamera: (id: number) => del<any>(`/api/cameras/${id}`),
  testCamera: (id: number) =>
    post<DeviceTestResult>(`/api/cameras/${id}/test`, {}),
  /** 通过 ONVIF 采集设备信息（基本信息 / 系统信息 / 网络信息 / 分析配置） */
  deviceInfo: (id: number) =>
    post<DeviceInfoResult>(`/api/cameras/${id}/device-info`, {}),
  /** WebRTC 预览：交换 SDP offer/answer，后端把 RTSP H.264 转发为 WebRTC */
  previewOffer: (id: number, sdp: string) =>
    post<PreviewAnswer>(`/api/cameras/${id}/preview/offer`, { sdp }),
  /** 关闭预览会话，立即释放设备的 RTSP 连接 */
  previewClose: (id: number, session: string) =>
    del<{ ok: boolean }>(`/api/cameras/${id}/preview/${encodeURIComponent(session)}`),
  discoverCamera: (id: number, captureSeconds = 6) =>
    post<{ ok: boolean; topics: CameraTopic[]; test: DeviceTestResult }>(
      `/api/cameras/${id}/discover`,
      { capture_seconds: captureSeconds }
    ),
  topics: (id: number) => get<CameraTopic[]>(`/api/cameras/${id}/topics`),
  saveTopicRemarks: (id: number, items: { topic: string; remark: string }[]) =>
    put<CameraTopic[]>(`/api/cameras/${id}/topics`, { items }),

  // 订阅
  subscriptions: (cameraId?: number) =>
    get<Subscription[]>("/api/subscriptions", cameraId ? { camera_id: cameraId } : undefined),
  cameraSubscriptions: (id: number) =>
    get<Subscription[]>(`/api/cameras/${id}/subscriptions`),
  saveSubscriptions: (
    id: number,
    items: any[],
    remarks: { topic: string; remark: string }[] = []
  ) => put<any>(`/api/cameras/${id}/subscriptions`, { items, remarks }),

  // 通知通道
  channels: () => get<WebhookChannel[]>("/api/channels"),
  createChannel: (data: Partial<WebhookChannel>) =>
    post<WebhookChannel>("/api/channels", data),
  updateChannel: (id: number, data: Partial<WebhookChannel>) =>
    put<WebhookChannel>(`/api/channels/${id}`, data),
  deleteChannel: (id: number) => del<any>(`/api/channels/${id}`),
  testChannel: (id: number) =>
    post<{ ok: boolean; message: string; status: number; body: string }>(
      `/api/channels/${id}/test`,
      {}
    ),

  // 设置 / 监控
  settings: () => get<Settings>("/api/settings"),
  saveSettings: (data: Settings) => put<Settings>("/api/settings", data),
  overview: () =>
    get<{ counts: Record<string, number>; runtime: RuntimeInfo }>("/api/overview"),
  runtime: () => get<RuntimeInfo>("/api/runtime"),
  reload: () => post<RuntimeInfo>("/api/runtime/reload", {}),
  events: (limit = 50, offset = 0) =>
    get<{ list: EventLog[]; total: number }>("/api/events", { limit, offset }),
  logout: () => post<any>("/api/logout", {})
};
