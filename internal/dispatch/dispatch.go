// Package dispatch 负责事件信封构造、去重、落库与按订阅路由异步推送到 webhook 通道。
package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	_ "time/tzdata" // 内嵌时区数据库，保证交叉编译/精简容器下 LoadLocation("Asia/Shanghai") 可用

	"IpcNotice/internal/config"
	"IpcNotice/internal/store"
	"IpcNotice/internal/util"
	"github.com/mickeyzzc/onvif-go/v2/events"
	"github.com/mickeyzzc/onvif-go/v2/onvif"
	"github.com/mickeyzzc/onvif-go/v2/types"
)

// Event 是推送给 webhook 的统一事件信封（同时写入 events.jsonl 与 SQLite）。
type Event struct {
	ID         string `json:"event_id"`
	Device     string `json:"device"`
	DeviceHost string `json:"device_host"`
	Transport  string `json:"transport"` // push | pull
	Topic      string `json:"topic"`
	// Title 是事件的中文名称：优先取用户在「事件订阅」里填写的备注，
	// 没填时回退为主题最后一段。接收方直接用 title 展示即可。
	Title      string            `json:"title"`
	Remark     string            `json:"remark,omitempty"`
	Operation  string            `json:"property_operation,omitempty"`
	DeviceTime string            `json:"device_time"`
	ReceivedAt string            `json:"received_at"`
	Producer   string            `json:"producer_address,omitempty"`
	Motion     *MotionPayload    `json:"motion,omitempty"`
	Source     map[string]string `json:"source,omitempty"`
	Key        map[string]string `json:"key,omitempty"`
	Data       map[string]string `json:"data,omitempty"`

	// ---- sustained 模式的生命周期字段（instant 模式事件这些字段为空）----
	// EventPhase: started | ended。state_key 标识事件实例；
	// started_at/last_seen_at/ended_at 为 RFC3339；duration_seconds 是活动持续秒数；
	// event_count 是本轮活动期间收到的事件总数；end_reason: EXPLICIT_STOP | INACTIVITY_TIMEOUT。
	EventPhase      string `json:"event_phase,omitempty"`
	StateKey        string `json:"state_key,omitempty"`
	StartedAt       string `json:"started_at,omitempty"`
	LastSeenAt      string `json:"last_seen_at,omitempty"`
	EndedAt         string `json:"ended_at,omitempty"`
	DurationSeconds int64  `json:"duration_seconds,omitempty"`
	EventCount      int    `json:"event_count,omitempty"`
	EndReason       string `json:"end_reason,omitempty"`
}

// MotionPayload 是移动侦测结构化数据（如带置信度）。
type MotionPayload struct {
	Active bool    `json:"active"`
	Score  float64 `json:"score,omitempty"`
	Source string  `json:"source,omitempty"`
}

var eventSeq atomic.Uint64

func buildEvent(dev *config.DeviceConfig, transport string, msg onvif.NotificationMessage) Event {
	t := msg.Message.UtcTime
	if t.IsZero() {
		t = time.Now().UTC()
	}
	evt := Event{
		Device:     dev.Name,
		DeviceHost: dev.Host,
		Transport:  transport,
		Topic:      msg.Topic,
		Operation:  msg.Message.PropertyOperation,
		DeviceTime: t.UTC().Format(time.RFC3339Nano),
		ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Producer:   msg.ProducerAddress,
		Source:     itemsToMap(msg.Message.Source),
		Key:        itemsToMap(msg.Message.Key),
		Data:       itemsToMap(msg.Message.Data),
	}
	if alarm, ok := events.ParseMotionAlarm(msg); ok {
		evt.Motion = &MotionPayload{Active: alarm.Active, Score: alarm.Score, Source: alarm.Source}
	}
	return evt
}

func itemsToMap(items []types.SimpleItem) map[string]string {
	if len(items) == 0 {
		return nil
	}
	m := make(map[string]string, len(items))
	for _, it := range items {
		m[it.Name] = it.Value
	}
	return m
}

// Route 是「某主题 → 若干通知渠道」的一条路由。
// Mode: instant（默认，冷却去重）/ sustained（持续事件状态管理）；
// Timeout/NotifyStart/NotifyEnd 仅在 sustained 模式生效。
type Route struct {
	Topic             string
	Channels          []store.WebhookChannel
	Mode              string
	InactivityTimeout int
	NotifyStart       bool
	NotifyEnd         bool
}

// RouteTable 是运行期由数据库订阅配置生成的一份只读快照。
// Remarks: 设备名 -> 归一化主题 -> 用户填写的事件名称/备注。
type RouteTable struct {
	ByCamera  map[string][]Route
	CameraIDs map[string]int64
	Remarks   map[string]map[string]string
}

// EmptyRouteTable 返回一张空的路由表。
func EmptyRouteTable() *RouteTable {
	return &RouteTable{
		ByCamera:  map[string][]Route{},
		CameraIDs: map[string]int64{},
		Remarks:   map[string]map[string]string{},
	}
}

// LookupTitle 返回事件标题与原始备注。匹配顺序：精确主题 → 最长通配/前缀主题 →
// 主题最后一段。这样即使订阅用了 /* 通配，也能带上该行填写的中文名称。
func (rt *RouteTable) LookupTitle(device, topic string) (string, string) {
	norm := util.NormalizeTopic(topic)
	remarks := map[string]string{}
	if rt != nil {
		remarks = rt.Remarks[device]
	}
	if r := remarks[norm]; r != "" {
		return r, r
	}
	best, bestLen := "", -1
	for pattern, r := range remarks {
		if r == "" || !util.MatchTopic(pattern, norm) {
			continue
		}
		if len(pattern) > bestLen {
			best, bestLen = r, len(pattern)
		}
	}
	if best != "" {
		return best, best
	}
	parts := strings.Split(strings.Trim(norm, "/"), "/")
	fallback := parts[len(parts)-1]
	if fallback == "" {
		fallback = norm
	}
	return fallback, ""
}

func (rt *RouteTable) match(device, topic string) (bool, []store.WebhookChannel, string) {
	if rt == nil {
		return false, nil, ""
	}
	var picked []store.WebhookChannel
	seen := map[int64]bool{}
	names := []string{}
	topicMatched := false
	for _, r := range rt.ByCamera[device] {
		if !util.MatchTopic(r.Topic, topic) {
			continue
		}
		topicMatched = true
		for _, ch := range r.Channels {
			if !ch.Enabled || seen[ch.ID] {
				continue
			}
			seen[ch.ID] = true
			picked = append(picked, ch)
			names = append(names, ch.Name)
		}
	}
	return topicMatched, picked, strings.Join(names, ", ")
}

// Dispatcher 把事件记录到日志 / JSONL / SQLite，并按订阅路由异步推送到 webhook 通道。
// ONVIF 的 handler 只做入队，绝不阻塞订阅轮询循环。
type Dispatcher struct {
	store *store.Store
	// webhookClient 可原子整体替换：configure() 热更新 Webhook 超时后换新客户端，
	// 旧客户端被进行中的请求持有，不影响在途推送。
	webhookClient atomic.Pointer[http.Client]
	queue         chan dispatchItem
	wg            sync.WaitGroup
	log           *slog.Logger

	mu           sync.RWMutex
	closed       bool
	logFile      *os.File
	logFilePath  string
	dedupTTL     time.Duration
	maxEventLogs int
	routes       atomic.Pointer[RouteTable]

	seenMu sync.Mutex
	seen   map[string]dedupState

	// ---- sustained 持续事件状态管理（见 state.go）----
	smStates     map[string]*eventState
	smMu         sync.Mutex
	smStop       chan struct{}
	smNow        func() time.Time         // 可注入时钟（测试用）
	deviceHealth func(device string) bool // nil 时视为全部健康
}

// dispatchItem 是分发队列的一条记录。route 非 nil 时使用显式路由
// （sustained 原始事件仅落库、生命周期通知走指定通道），否则按路由表匹配（instant 原有路径）。
type dispatchItem struct {
	evt   Event
	route *routeDirect
}

type routeDirect struct {
	channels []store.WebhookChannel
	names    string
	matched  bool
	push     bool // false = 仅落库/日志，不推 webhook
}

// dedupState 记录某条「设备+主题+操作+来源」的冷却截止时间。
// 去重是「冷却窗口」：键首次出现放行，窗口内的所有后续事件——同状态心跳、
// 多订阅实例重发、以及 TP-Link 侦测抖动造成的 true/false 快速翻转——一律抑制；
// 窗口过期后下一个事件重新放行并开启新冷却。抑制时不刷新窗口，
// 因此持续抖动最多每个 TTL 窗口收到一次推送。
type dedupState struct {
	expiresAt time.Time
}

func NewDispatcher(store *store.Store, st config.Settings, logger *slog.Logger) (*Dispatcher, error) {
	d := &Dispatcher{
		store:        store,
		queue:        make(chan dispatchItem, st.QueueSize),
		log:          logger.With("component", "dispatch"),
		dedupTTL:     time.Duration(st.DedupSeconds) * time.Second,
		maxEventLogs: st.MaxEventLogs,
		seen:         make(map[string]dedupState),
		smStates:     make(map[string]*eventState),
		smStop:       make(chan struct{}),
		smNow:        time.Now,
	}
	d.webhookClient.Store(newWebhookClient(st.WebhookTimeoutSeconds))
	d.routes.Store(EmptyRouteTable())
	if err := d.configure(st, false); err != nil {
		return nil, err
	}
	for i := 0; i < st.WebhookWorkers; i++ {
		d.wg.Add(1)
		go d.worker(i)
	}
	// 统一的静默超时扫描：每秒一次检查 ACTIVE 状态，不为单个事件建定时器。
	d.wg.Add(1)
	go d.scanLoop()
	return d, nil
}

// Configure 在「系统设置」变更后热更新去重窗口、事件文件与日志上限。
func (d *Dispatcher) Configure(st config.Settings) error {
	return d.configure(st, true)
}

// newWebhookClient 构造带推送超时的 HTTP 客户端。
func newWebhookClient(timeoutSeconds int) *http.Client {
	return &http.Client{Timeout: time.Duration(timeoutSeconds) * time.Second}
}

func (d *Dispatcher) configure(st config.Settings, reopen bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dedupTTL = time.Duration(st.DedupSeconds) * time.Second
	d.maxEventLogs = st.MaxEventLogs
	// 热更新推送超时：整体替换 HTTP 客户端（WebhookTimeoutSeconds 立即生效）。
	t := time.Duration(st.WebhookTimeoutSeconds) * time.Second
	if old := d.webhookClient.Load(); old == nil || old.Timeout != t {
		d.webhookClient.Store(newWebhookClient(st.WebhookTimeoutSeconds))
	}
	if d.logFilePath == st.EventLogFile && d.logFile != nil {
		return nil
	}
	if st.EventLogFile == "" {
		// 热更新清空日志路径：关闭旧文件并清空引用，避免继续向已废弃路径写入。
		if d.logFile != nil {
			_ = d.logFile.Close()
			d.logFile = nil
			if reopen {
				d.log.Info("事件 JSONL 已关闭（日志文件路径已清空）")
			}
		}
		d.logFilePath = ""
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(st.EventLogFile), 0o755); err != nil {
		return fmt.Errorf("创建事件日志目录失败: %w", err)
	}
	f, err := os.OpenFile(st.EventLogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("打开事件日志文件 %s 失败: %w", st.EventLogFile, err)
	}
	if d.logFile != nil {
		_ = d.logFile.Close()
	}
	d.logFile = f
	d.logFilePath = st.EventLogFile
	if reopen {
		d.log.Info("事件 JSONL 已切换", "file", st.EventLogFile)
	}
	return nil
}

// SetRoutes 原子替换路由表，由 Manager.Reload 调用。
func (d *Dispatcher) SetRoutes(rt *RouteTable) {
	if rt == nil {
		rt = EmptyRouteTable()
	}
	d.routes.Store(rt)
}

// RouteCount 返回当前生效的订阅路由条数（供运行态展示）。
func (d *Dispatcher) RouteCount() int {
	rt := d.routes.Load()
	if rt == nil {
		return 0
	}
	n := 0
	for _, rs := range rt.ByCamera {
		n += len(rs)
	}
	return n
}

// SetDeviceHealth 注册设备接收链路健康检查回调（Manager 提供）。
// 返回 false 表示该设备的订阅/接收链路异常，静默超时扫描不得将其判定为结束。
func (d *Dispatcher) SetDeviceHealth(fn func(device string) bool) {
	d.deviceHealth = fn
}

// Submit 入队一批事件；队列满时丢弃并记录，避免拖垮订阅循环。
func (d *Dispatcher) Submit(dev *config.DeviceConfig, transport string, msgs []onvif.NotificationMessage) {
	for _, msg := range msgs {
		evt := buildEvent(dev, transport, msg)
		// sustained 订阅：绕过冷却去重；原始事件照常落库（不推 webhook）；
		// 由状态机决定 started/ended 通知。
		if s, ok := d.routes.Load().sustainedFor(dev.Name, evt.Topic); ok {
			evt.ID = fmt.Sprintf("%d-%06d", time.Now().UnixNano(), eventSeq.Add(1))
			d.processSustained(dev, evt, s)
			continue
		}
		if d.isDuplicate(evt) {
			d.log.Debug("去重：跳过重复状态", "device", dev.Name, "topic", evt.Topic,
				"device_time", evt.DeviceTime, "data", util.JSONDump(evt.Data))
			continue
		}
		evt.ID = fmt.Sprintf("%d-%06d", time.Now().UnixNano(), eventSeq.Add(1))
		d.enqueueItem(dispatchItem{evt: evt}, dev.Name, evt.Topic)
	}
}

// enqueueItem 非阻塞入队；队列满时丢弃并记录。
func (d *Dispatcher) enqueueItem(it dispatchItem, device, topic string) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return
	}
	select {
	case d.queue <- it:
	default:
		d.log.Warn("事件队列已满，丢弃事件", "device", device, "topic", topic)
	}
}

// isDuplicate 按「设备 + 主题 + 操作 + 来源」维护冷却窗口：键首次出现放行，
// 窗口内的所有后续事件（同状态心跳 + 状态快速抖动）一律抑制，过期后重新放行。
func (d *Dispatcher) isDuplicate(evt Event) bool {
	d.mu.RLock()
	ttl := d.dedupTTL
	d.mu.RUnlock()
	if ttl <= 0 {
		return false
	}
	key := strings.Join([]string{
		evt.Device, util.NormalizeTopic(evt.Topic), evt.Operation, util.JSONDump(evt.Source),
	}, "\x00")

	d.seenMu.Lock()
	defer d.seenMu.Unlock()
	now := time.Now()
	if len(d.seen) > 4096 {
		for k, s := range d.seen {
			if now.After(s.expiresAt) {
				delete(d.seen, k)
			}
		}
	}
	if s, ok := d.seen[key]; ok && now.Before(s.expiresAt) {
		return true
	}
	d.seen[key] = dedupState{expiresAt: now.Add(ttl)}
	return false
}

func (d *Dispatcher) worker(_ int) {
	defer d.wg.Done()
	for it := range d.queue {
		d.handle(it)
	}
}

func (d *Dispatcher) handle(it dispatchItem) {
	evt := it.evt
	rt := d.routes.Load()
	matched, channels, channelNames := rt.match(evt.Device, evt.Topic)
	push := true
	if it.route != nil {
		// sustained 显式路由：原始事件仅落库；生命周期通知走指定通道。
		matched = it.route.matched
		channels = it.route.channels
		channelNames = it.route.names
		push = it.route.push
	}
	// 在写 JSONL / SQLite / webhook 之前补上标题，保证三处内容与 HTTP body 完全一致。
	evt.Title, evt.Remark = rt.LookupTitle(evt.Device, evt.Topic)
	cameraID := int64(0)
	if rt != nil {
		cameraID = rt.CameraIDs[evt.Device]
	}

	// 记录运行期真实出现的主题（TP-Link 的实际主题常不在 TopicSet 声明树里）。
	if cameraID > 0 && util.NormalizeTopic(evt.Topic) != "" && d.store != nil {
		if _, err := d.store.UpsertObservedTopic(cameraID, util.NormalizeTopic(evt.Topic)); err != nil {
			d.log.Warn("记录观察到的事件主题失败", "err", err)
		}
	}

	dispatched := false
	if push {
		now := time.Now()
		for _, ch := range channels {
			if !ChannelWindowAllows(ch, now) {
				d.log.Info("webhook 处于勿扰时段，跳过推送（事件仍会落库）",
					"channel", ch.Name, "device", evt.Device, "topic", evt.Topic,
					"at", now.In(cst).Format("2006-01-02 15:04:05"),
					"weekdays", ch.Weekdays, "start_time", ch.StartTime, "end_time", ch.EndTime)
				continue
			}
			if d.postWebhook(evt, ch) {
				dispatched = true
			}
		}
	}
	if evt.EventPhase != "" {
		d.log.Info("LIFECYCLE", "phase", evt.EventPhase, "state_key", evt.StateKey,
			"device", evt.Device, "topic", evt.Topic, "title", evt.Title,
			"event_count", evt.EventCount, "duration_seconds", evt.DurationSeconds,
			"end_reason", evt.EndReason, "push", push)
	}

	d.log.Info("EVENT",
		"device", evt.Device, "transport", evt.Transport, "topic", evt.Topic,
		"title", evt.Title,
		"operation", evt.Operation, "device_time", evt.DeviceTime,
		"data", util.JSONDump(evt.Data), "motion", util.JSONDump(evt.Motion),
		"matched", matched, "channels", channelNames)

	payload, err := json.Marshal(evt)
	if err != nil {
		return
	}
	d.mu.RLock()
	f := d.logFile
	maxLogs := d.maxEventLogs
	d.mu.RUnlock()
	if f != nil {
		line := append(append([]byte(nil), payload...), '\n')
		d.mu.Lock()
		_, _ = f.Write(line)
		d.mu.Unlock()
	}
	if d.store != nil {
		if err := d.store.InsertEventLog(store.EventLog{
			EventID: evt.ID, CameraID: cameraID, Device: evt.Device,
			Topic: util.NormalizeTopic(evt.Topic), Transport: evt.Transport,
			Payload: payload, Matched: matched, Dispatched: dispatched,
			Channels: channelNames, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		}); err != nil {
			d.log.Warn("事件落库失败", "err", err)
		} else if maxLogs > 0 && eventSeq.Load()%50 == 0 {
			_ = d.store.TrimEventLogs(maxLogs)
		}
	}
}

func (d *Dispatcher) postWebhook(evt Event, ch store.WebhookChannel) bool {
	var payload []byte
	if strings.TrimSpace(ch.BodyTemplate) != "" {
		payload = []byte(RenderBodyTemplate(ch.BodyTemplate, evt))
	} else {
		var err error
		payload, err = json.Marshal(evt)
		if err != nil {
			d.log.Error("webhook 序列化失败", "err", err)
			return false
		}
	}
	timeout := ch.TimeoutSeconds
	if timeout <= 0 {
		timeout = 5
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	// 通道仅支持 POST
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ch.URL, bytes.NewReader(payload))
	if err != nil {
		d.log.Error("webhook 请求构造失败", "channel", ch.Name, "err", err)
		return false
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("User-Agent", "IpcNotice/0.2")
	req.Header.Set("X-IpcNotice-Event-ID", evt.ID)
	req.Header.Set("X-IpcNotice-Device", evt.Device)
	req.Header.Set("X-IpcNotice-Topic", evt.Topic)
	for k, v := range ch.Headers {
		req.Header.Set(k, v)
	}

	resp, err := d.webhookClient.Load().Do(req)
	if err != nil {
		d.log.Error("webhook 推送失败", "channel", ch.Name, "device", evt.Device,
			"topic", evt.Topic, "err", err)
		return false
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		d.log.Error("webhook 返回非 2xx", "channel", ch.Name, "status", resp.StatusCode)
		return false
	}
	d.log.Info("webhook 推送成功", "channel", ch.Name, "device", evt.Device,
		"topic", evt.Topic, "status", resp.StatusCode, "event_id", evt.ID)
	return true
}

// PushTitle 组装占位符 {title}：摄像头xxx 发生-xxxx事件名称。
func PushTitle(evt Event) string {
	return fmt.Sprintf("摄像头%s 发生-%s", evt.Device, evt.Title)
}

// PushContent 组装占位符 {content}：首行 Emoji 标题头 + 多行文本
// （摄像头名称 / 事件描述 / 触发时间）。换行符是真实换行；
// 渲染进 JSON 模板时由 RenderBodyTemplate 做 JSON 转义（\n）。
func PushContent(evt Event) string {
	return fmt.Sprintf("📹IpcNotice订阅消息通知\n摄像头：%s\n事件：发生了%s事件\n触发时间：%s",
		evt.Device, evt.Title, FormatEventTime(evt))
}

// cst 是中国标准时间（UTC+8）。通知时间统一按此时区展示，避免依赖运行机器
// 的本地时区（CI/容器常为 UTC），保证行为确定且符合国内使用场景。
var cst = func() *time.Location {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return time.UTC
	}
	return loc
}()

// FormatEventTime 优先本机接收时间，其次设备时间，都解析失败时原样返回。
// TP-Link 固件推送消息里的 utcTime 在订阅期内不更新（冻结在订阅建立时刻），
// 直接展示会造成触发时间严重偏差；推送链路里 ReceivedAt 与真实触发时刻相差毫秒级。
// 解析成功后固定按中国时区（UTC+8）展示，不随运行机器时区变化。
func FormatEventTime(evt Event) string {
	for _, s := range []string{evt.ReceivedAt, evt.DeviceTime} {
		if s == "" {
			continue
		}
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t.In(cst).Format("2006-01-02 15:04:05")
		}
		return s
	}
	return time.Now().In(cst).Format("2006-01-02 15:04:05")
}

// jsonEscape 把文本安全嵌入 JSON 模板的字符串值：换行转义为 \n、引号转义为 \"、
// 反斜杠与控制字符同样处理。模板是 JSON 文本，裸换行会直接破坏 JSON 合法性。
func jsonEscape(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		return s
	}
	return string(b[1 : len(b)-1])
}

// 请求体模板支持的占位符。{eventtime} 是标准拼写，{evettime} 为兼容
// 早期错拼模板保留的别名（旧模板不会因为改名而失效）。
// 用 strings.NewReplacer 一次性替换，避免链式 ReplaceAll 造成「替换值里
// 恰好含另一个占位符文本」时被二次替换。
var bodyPlaceholderStubs = strings.NewReplacer(
	"{title}", "t",
	"{content}", "c",
	"{devname}", "d",
	"{eventname}", "e",
	"{eventstatus}", "s",
	"{eventtime}", "s",
	"{evettime}", "s",
)

// EventStatusText 返回 {eventstatus}：持续事件（sustained）的生命周期状态。
// started → 开始事件，ended → 结束事件；即时事件 / 测试推送没有生命周期，
// 返回「事件」，使 `{eventname}的{eventstatus}` 这类模板在任何模式下都通顺。
func EventStatusText(evt Event) string {
	switch evt.EventPhase {
	case PhaseStarted:
		return "开始事件"
	case PhaseEnded:
		return "结束事件"
	default:
		return "事件"
	}
}

// bodyReplacer 把各占位符映射为经 JSON 转义后的实际内容：
//   - {title}       摄像头xxx 发生-xxx事件名称（保留旧行为）
//   - {content}     带 Emoji 标题头的多行文本（保留旧行为）
//   - {devname}     设备名称
//   - {eventname}   事件名称（订阅里填写的中文备注，未填时为事件主题末段）
//   - {eventstatus} 开始事件 / 结束事件 / 事件（无生命周期时）
//   - {eventtime}   触发时间（优先本机接收时间，按北京时间展示）
//   - {evettime}    同 {eventtime}，兼容早期错拼模板
func bodyReplacer(evt Event) *strings.Replacer {
	return strings.NewReplacer(
		"{title}", jsonEscape(PushTitle(evt)),
		"{content}", jsonEscape(PushContent(evt)),
		"{devname}", jsonEscape(evt.Device),
		"{eventname}", jsonEscape(evt.Title),
		"{eventstatus}", jsonEscape(EventStatusText(evt)),
		"{eventtime}", jsonEscape(FormatEventTime(evt)),
		"{evettime}", jsonEscape(FormatEventTime(evt)),
	)
}

// RenderBodyTemplate 把请求体模板里的占位符替换为实际内容。
// 模板本身是 JSON 文本，占位符写在字符串值里；替换值先做 JSON 转义，
// 保证多行内容（\n）、引号等不会破坏模板结构。未出现占位符则原样输出。
func RenderBodyTemplate(tpl string, evt Event) string {
	return bodyReplacer(evt).Replace(tpl)
}

// ValidBodyTemplate 校验模板（把占位符替换成普通文本后）是否为合法 JSON。
func ValidBodyTemplate(tpl string) bool {
	if strings.TrimSpace(tpl) == "" {
		return true
	}
	return json.Valid([]byte(bodyPlaceholderStubs.Replace(tpl)))
}

// Close 停止接收新事件，等 worker 与静默扫描协程退出后关闭文件。
func (d *Dispatcher) Close() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.closed = true
	close(d.queue)
	close(d.smStop)
	d.mu.Unlock()

	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		d.log.Warn("等待事件分发器排空超时")
	}
	d.mu.Lock()
	if d.logFile != nil {
		_ = d.logFile.Close()
		d.logFile = nil
	}
	d.mu.Unlock()
}
