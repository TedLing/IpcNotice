package main

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
}

type MotionPayload struct {
	Active bool    `json:"active"`
	Score  float64 `json:"score,omitempty"`
	Source string  `json:"source,omitempty"`
}

var eventSeq atomic.Uint64

func buildEvent(dev *DeviceConfig, transport string, msg onvif.NotificationMessage) Event {
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
type Route struct {
	Topic    string
	Channels []WebhookChannel
}

// RouteTable 是运行期由数据库订阅配置生成的一份只读快照。
// Remarks: 设备名 -> 归一化主题 -> 用户填写的事件名称/备注。
type RouteTable struct {
	ByCamera  map[string][]Route
	CameraIDs map[string]int64
	Remarks   map[string]map[string]string
}

func emptyRouteTable() *RouteTable {
	return &RouteTable{
		ByCamera:  map[string][]Route{},
		CameraIDs: map[string]int64{},
		Remarks:   map[string]map[string]string{},
	}
}

// lookupTitle 返回事件标题与原始备注。匹配顺序：精确主题 → 最长通配/前缀主题 →
// 主题最后一段。这样即使订阅用了 /* 通配，也能带上该行填写的中文名称。
func (rt *RouteTable) lookupTitle(device, topic string) (string, string) {
	norm := normalizeTopic(topic)
	remarks := map[string]string{}
	if rt != nil {
		remarks = rt.Remarks[device]
	}
	if r := remarks[norm]; r != "" {
		return r, r
	}
	best, bestLen := "", -1
	for pattern, r := range remarks {
		if r == "" || !matchTopic(pattern, norm) {
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

func (rt *RouteTable) match(device, topic string) (bool, []WebhookChannel, string) {
	if rt == nil {
		return false, nil, ""
	}
	var picked []WebhookChannel
	seen := map[int64]bool{}
	names := []string{}
	topicMatched := false
	for _, r := range rt.ByCamera[device] {
		if !matchTopic(r.Topic, topic) {
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
	store  *Store
	client *http.Client
	queue  chan Event
	wg     sync.WaitGroup
	log    *slog.Logger

	mu           sync.RWMutex
	closed       bool
	logFile      *os.File
	logFilePath  string
	dedupTTL     time.Duration
	maxEventLogs int
	routes       atomic.Pointer[RouteTable]

	seenMu sync.Mutex
	seen   map[string]dedupState
}

// dedupState 记录某条「设备+主题+操作+来源」上次出现的事件状态值。
// 去重是「状态抑制」而不是「按 Topic 盲杀」：只有同一状态在 TTL 内重复出现
// （如 TP-Link 每秒重发的 Motion=true 心跳、多个订阅实例推送同一状态）才被抑制；
// 状态值变化（Motion=true→false 的结束、false→true 的再次触发）永远放行。
type dedupState struct {
	data      string
	source    string
	expiresAt time.Time
}

func NewDispatcher(store *Store, st Settings, logger *slog.Logger) (*Dispatcher, error) {
	d := &Dispatcher{
		store:        store,
		client:       &http.Client{Timeout: time.Duration(st.WebhookTimeoutSeconds) * time.Second},
		queue:        make(chan Event, st.QueueSize),
		log:          logger.With("component", "dispatch"),
		dedupTTL:     time.Duration(st.DedupSeconds) * time.Second,
		maxEventLogs: st.MaxEventLogs,
		seen:         make(map[string]dedupState),
	}
	d.routes.Store(emptyRouteTable())
	if err := d.configure(st, false); err != nil {
		return nil, err
	}
	for i := 0; i < st.WebhookWorkers; i++ {
		d.wg.Add(1)
		go d.worker(i)
	}
	return d, nil
}

// Configure 在「系统设置」变更后热更新去重窗口、事件文件与日志上限。
func (d *Dispatcher) Configure(st Settings) error {
	return d.configure(st, true)
}

func (d *Dispatcher) configure(st Settings, reopen bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dedupTTL = time.Duration(st.DedupSeconds) * time.Second
	d.maxEventLogs = st.MaxEventLogs
	if d.logFilePath == st.EventLogFile && d.logFile != nil {
		return nil
	}
	if st.EventLogFile == "" {
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
		rt = emptyRouteTable()
	}
	d.routes.Store(rt)
}

// Submit 入队一批事件；队列满时丢弃并记录，避免拖垮订阅循环。
func (d *Dispatcher) Submit(dev *DeviceConfig, transport string, msgs []onvif.NotificationMessage) {
	for _, msg := range msgs {
		evt := buildEvent(dev, transport, msg)
		if d.isDuplicate(evt) {
			d.log.Debug("去重：跳过重复状态", "device", dev.Name, "topic", evt.Topic,
				"device_time", evt.DeviceTime, "data", jsonDump(evt.Data))
			continue
		}
		evt.ID = fmt.Sprintf("%d-%06d", time.Now().UnixNano(), eventSeq.Add(1))
		d.mu.RLock()
		if d.closed {
			d.mu.RUnlock()
			return
		}
		select {
		case d.queue <- evt:
		default:
			d.log.Warn("事件队列已满，丢弃事件", "device", dev.Name, "topic", evt.Topic)
		}
		d.mu.RUnlock()
	}
}

// isDuplicate 按「设备 + 主题 + 操作 + 来源」记住最近一次的状态值，在 TTL 内抑制
// 同一状态的重复推送；状态值一旦变化立即放行并刷新记录。
//
// 设计要点：
//   - 故意不含 device_time：TP-Link 的 PullPoint 每秒重发一次 Motion=true 且带新
//     时间戳的心跳，按时间戳去重等于没去重。
//   - 键不含 data、data 参与值比较：保证 事件开始(true)→结束(false)→再次触发(true)
//     的完整序列不被误杀，真实连续触发可以到达下游。
//   - 多个旧订阅实例向同一 token 推送同一状态时，只有第一份会通过，其余在 TTL
//     内被抑制——token 稳定性带来的重复订阅不会产生重复推送。
func (d *Dispatcher) isDuplicate(evt Event) bool {
	d.mu.RLock()
	ttl := d.dedupTTL
	d.mu.RUnlock()
	if ttl <= 0 {
		return false
	}
	key := strings.Join([]string{
		evt.Device, normalizeTopic(evt.Topic), evt.Operation, jsonDump(evt.Source),
	}, "\x00")
	data := jsonDump(evt.Data)
	source := jsonDump(evt.Source)

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
	if s, ok := d.seen[key]; ok && now.Before(s.expiresAt) && s.data == data && s.source == source {
		return true
	}
	d.seen[key] = dedupState{data: data, source: source, expiresAt: now.Add(ttl)}
	return false
}

func (d *Dispatcher) worker(_ int) {
	defer d.wg.Done()
	for evt := range d.queue {
		d.handle(evt)
	}
}

func (d *Dispatcher) handle(evt Event) {
	rt := d.routes.Load()
	matched, channels, channelNames := rt.match(evt.Device, evt.Topic)
	// 在写 JSONL / SQLite / webhook 之前补上标题，保证三处内容与 HTTP body 完全一致。
	evt.Title, evt.Remark = rt.lookupTitle(evt.Device, evt.Topic)
	cameraID := int64(0)
	if rt != nil {
		cameraID = rt.CameraIDs[evt.Device]
	}

	// 记录运行期真实出现的主题（TP-Link 的实际主题常不在 TopicSet 声明树里）。
	if cameraID > 0 && normalizeTopic(evt.Topic) != "" {
		if _, err := d.store.UpsertObservedTopic(cameraID, normalizeTopic(evt.Topic)); err != nil {
			d.log.Warn("记录观察到的事件主题失败", "err", err)
		}
	}

	dispatched := false
	for _, ch := range channels {
		if d.postWebhook(evt, ch) {
			dispatched = true
		}
	}

	d.log.Info("EVENT",
		"device", evt.Device, "transport", evt.Transport, "topic", evt.Topic,
		"title", evt.Title,
		"operation", evt.Operation, "device_time", evt.DeviceTime,
		"data", jsonDump(evt.Data), "motion", jsonDump(evt.Motion),
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
	if err := d.store.InsertEventLog(EventLog{
		EventID: evt.ID, CameraID: cameraID, Device: evt.Device,
		Topic: normalizeTopic(evt.Topic), Transport: evt.Transport,
		Payload: payload, Matched: matched, Dispatched: dispatched,
		Channels: channelNames, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		d.log.Warn("事件落库失败", "err", err)
	} else if maxLogs > 0 && eventSeq.Load()%50 == 0 {
		_ = d.store.TrimEventLogs(maxLogs)
	}
}

func (d *Dispatcher) postWebhook(evt Event, ch WebhookChannel) bool {
	var payload []byte
	if strings.TrimSpace(ch.BodyTemplate) != "" {
		payload = []byte(renderBodyTemplate(ch.BodyTemplate, evt))
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

	resp, err := d.client.Do(req)
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

// pushTitle 组装占位符 {title}：摄像头xxx 发生-xxxx事件名称。
func pushTitle(evt Event) string {
	return fmt.Sprintf("摄像头%s 发生-%s", evt.Device, evt.Title)
}

// pushContent 组装占位符 {content}：摄像头名称 + 发生了什么事件 + 触发时间。
func pushContent(evt Event) string {
	return fmt.Sprintf("摄像头%s 发生了%s事件，触发时间 %s", evt.Device, evt.Title, formatEventTime(evt))
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

// formatEventTime 优先设备时间，其次本机接收时间，都解析失败时原样返回。
// 解析成功后固定按中国时区（UTC+8）展示，不随运行机器时区变化。
func formatEventTime(evt Event) string {
	for _, s := range []string{evt.DeviceTime, evt.ReceivedAt} {
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

// renderBodyTemplate 把请求体模板里的 {title} / {content} 占位符替换为实际内容。
// 模板本身是 JSON 文本，占位符写在字符串值里；未出现占位符则原样输出。
func renderBodyTemplate(tpl string, evt Event) string {
	out := strings.ReplaceAll(tpl, "{title}", pushTitle(evt))
	out = strings.ReplaceAll(out, "{content}", pushContent(evt))
	return out
}

// validBodyTemplate 校验模板（把占位符替换成普通文本后）是否为合法 JSON。
func validBodyTemplate(tpl string) bool {
	if strings.TrimSpace(tpl) == "" {
		return true
	}
	s := strings.ReplaceAll(tpl, "{title}", "t")
	s = strings.ReplaceAll(s, "{content}", "c")
	return json.Valid([]byte(s))
}

// Close 停止接收新事件，等 worker 排空队列后关闭文件。
func (d *Dispatcher) Close() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.closed = true
	close(d.queue)
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
