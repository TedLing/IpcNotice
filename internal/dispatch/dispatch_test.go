package dispatch

import (
	"encoding/json"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"IpcNotice/internal/config"
)

func TestLookupTitle(t *testing.T) {
	rt := EmptyRouteTable()
	rt.Remarks["cam1"] = map[string]string{
		"RuleEngine/CellMotionDetector/Motion": "移动侦测",
		"RuleEngine/CellMotionDetector/*":      "区域侦测事件",
		"RuleEngine/TamperDetector/Tamper":     "遮挡告警",
	}
	cases := []struct {
		name, device, topic, title, remark string
	}{
		{"精确匹配优先于通配", "cam1", "tns1:RuleEngine/CellMotionDetector/Motion", "移动侦测", "移动侦测"},
		{"通配订阅的备注", "cam1", "tns1:RuleEngine/CellMotionDetector/Intrusion", "区域侦测事件", "区域侦测事件"},
		{"其它精确备注", "cam1", "RuleEngine/TamperDetector/Tamper", "遮挡告警", "遮挡告警"},
		{"无备注回退主题末段", "cam2", "tns1:RuleEngine/CellMotionDetector/People", "People", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			title, remark := rt.LookupTitle(tc.device, tc.topic)
			if title != tc.title || remark != tc.remark {
				t.Errorf("LookupTitle(%q,%q) = (%q,%q), want (%q,%q)",
					tc.device, tc.topic, title, remark, tc.title, tc.remark)
			}
		})
	}
	if title, remark := (*RouteTable)(nil).LookupTitle("cam1", "RuleEngine/Motion"); title != "Motion" || remark != "" {
		t.Errorf("nil 路由表未正确回退: %q %q", title, remark)
	}
}

func TestRenderBodyTemplate(t *testing.T) {
	evt := Event{
		Device:     "camera-01",
		Topic:      "RuleEngine/CellMotionDetector/Motion",
		Title:      "移动侦测",
		DeviceTime: "2026-11-10T08:00:00Z",
		ReceivedAt: "2026-11-10T08:00:01Z",
	}
	if got := PushTitle(evt); got != "摄像头camera-01 发生-移动侦测" {
		t.Errorf("PushTitle = %q", got)
	}
	// 触发时间优先取 ReceivedAt（TP-Link 的设备 utcTime 在订阅期内冻结不更新）
	want := "📹IpcNotice订阅消息通知\n摄像头：camera-01\n事件：发生了移动侦测事件\n触发时间：2026-11-10 16:00:01"
	if got := PushContent(evt); got != want {
		t.Errorf("PushContent = %q, want %q", got, want)
	}
	tpl := `{"title":"{title}","content":"{content}"}`
	out := RenderBodyTemplate(tpl, evt)
	if !strings.Contains(out, `"摄像头camera-01 发生-移动侦测"`) || !strings.Contains(out, `发生了移动侦测事件`) {
		t.Errorf("RenderBodyTemplate = %q", out)
	}
	// 多行内容嵌入 JSON 模板时必须转义为 \n，否则 JSON 失效
	if !strings.Contains(out, `\n事件：发生了移动侦测事件\n`) {
		t.Errorf("换行未转义为 \\n: %q", out)
	}
	if !json.Valid([]byte(out)) {
		t.Errorf("渲染结果不是合法 JSON: %q", out)
	}
	// 特殊字符（引号/反斜杠）也要转义后才能保持 JSON 合法
	evt.Title = `带"引号"的\事件`
	if out := RenderBodyTemplate(tpl, evt); !json.Valid([]byte(out)) {
		t.Errorf("含特殊字符时渲染结果不是合法 JSON: %q", out)
	}
	if RenderBodyTemplate("{}", evt) != "{}" {
		t.Errorf("无占位符模板应原样输出")
	}
}

func TestValidBodyTemplate(t *testing.T) {
	for _, ok := range []string{
		"",
		`{}`,
		`{"title":"{title}","content":"{content}"}`,
		`{"msgtype":"text","text":{"content":"{content}"}}`,
	} {
		if !ValidBodyTemplate(ok) {
			t.Errorf("应判定为合法模板: %q", ok)
		}
	}
	for _, bad := range []string{
		`{"title":"{title}"`,
		`not json`,
	} {
		if ValidBodyTemplate(bad) {
			t.Errorf("应判定为非法模板: %q", bad)
		}
	}
}

// newTestDispatcher 构造只用于去重测试的分发器（不开 worker、不写文件）。
func newTestDispatcher(dedupSeconds int) *Dispatcher {
	st := config.DefaultSettings()
	st.DedupSeconds = dedupSeconds
	st.EventLogFile = ""
	st.QueueSize = 8
	st.WebhookWorkers = 1
	d := &Dispatcher{
		queue:    make(chan dispatchItem, st.QueueSize),
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		dedupTTL: time.Duration(dedupSeconds) * time.Second,
		seen:     make(map[string]dedupState),
	}
	d.webhookClient.Store(newWebhookClient(1))
	d.routes.Store(EmptyRouteTable())
	return d
}

func dedupEvent(device, topic, op string, data map[string]string) Event {
	return Event{Device: device, Topic: topic, Operation: op,
		Data: data, Source: map[string]string{"Source": "CellMotionDetector"}}
}

func TestDedupCooldownSuppressesFlapping(t *testing.T) {
	d := newTestDispatcher(60)
	topic := "tns1:RuleEngine/CellMotionDetector/Motion"
	on := map[string]string{"Motion": "true"}
	off := map[string]string{"Motion": "false"}

	// 1. 首个事件通过
	if d.isDuplicate(dedupEvent("cam", topic, "Changed", on)) {
		t.Fatal("首次事件不应被判重")
	}
	// 2. 心跳重复（新 device_time，键不含时间）被抑制
	if !d.isDuplicate(dedupEvent("cam", topic, "Changed", on)) {
		t.Fatal("TTL 内重复同状态应被判重（心跳风暴）")
	}
	// 3. TTL 内的状态变化（true→false 侦测抖动）被冷却抑制——
	//    实测 TP-Link 会在 1 分钟内翻转 8 次，若放行会连发 8 条推送
	if !d.isDuplicate(dedupEvent("cam", topic, "Changed", off)) {
		t.Fatal("TTL 内状态变化应被冷却窗口抑制")
	}
	// 4. 抖回去（false→true）同样抑制
	if !d.isDuplicate(dedupEvent("cam", topic, "Changed", on)) {
		t.Fatal("TTL 内再次状态变化应被冷却窗口抑制")
	}
	// 5. 冷却过期后重新放行（抑制时不刷新窗口）
	d.seenMu.Lock()
	for k, s := range d.seen {
		s.expiresAt = time.Now().Add(-time.Second)
		d.seen[k] = s
	}
	d.seenMu.Unlock()
	if d.isDuplicate(dedupEvent("cam", topic, "Changed", off)) {
		t.Fatal("冷却过期后应重新放行")
	}
}

func TestDedupDistinguishesOperationAndSource(t *testing.T) {
	d := newTestDispatcher(60)
	topic := "tns1:RuleEngine/CellMotionDetector/Motion"
	on := map[string]string{"Motion": "true"}
	if d.isDuplicate(dedupEvent("cam", topic, "Changed", on)) {
		t.Fatal("首次 Changed 不应被判重")
	}
	// 不同 Operation（如 Initialized 同步点）是不同键
	if d.isDuplicate(dedupEvent("cam", topic, "Initialized", on)) {
		t.Fatal("不同 Operation 不应与 Changed 判重")
	}
	// 不同设备互不影响
	if d.isDuplicate(dedupEvent("cam2", topic, "Changed", on)) {
		t.Fatal("不同设备不应判重")
	}
}

func TestDedupTTLExpiry(t *testing.T) {
	d := newTestDispatcher(60)
	topic := "tns1:RuleEngine/CellMotionDetector/Motion"
	on := map[string]string{"Motion": "true"}
	if d.isDuplicate(dedupEvent("cam", topic, "Changed", on)) {
		t.Fatal("首次不应判重")
	}
	// 手动把过期时间拨回过去，模拟 TTL 到期
	d.seenMu.Lock()
	for k, s := range d.seen {
		s.expiresAt = time.Now().Add(-time.Second)
		d.seen[k] = s
	}
	d.seenMu.Unlock()
	if d.isDuplicate(dedupEvent("cam", topic, "Changed", on)) {
		t.Fatal("TTL 过期后同一状态应重新放行")
	}
}

// TestConfigureJSONLCloseOnEmptyPath 验证热更新清空日志路径时关闭旧文件，
// 以及设置 Normalize 不再把空的 event_log_file 回填为默认值（清空 = 关闭 JSONL）。
func TestConfigureJSONLCloseOnEmptyPath(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "events.jsonl")

	d := newTestDispatcher(60)
	st := config.DefaultSettings()
	st.EventLogFile = p
	if err := d.configure(st, false); err != nil {
		t.Fatalf("启用 JSONL 失败: %v", err)
	}
	if d.logFile == nil {
		t.Fatal("启用后 logFile 不应为 nil")
	}

	// 清空路径：旧文件必须关闭，引用清空
	if err := d.configure(config.Settings{DedupSeconds: 60}, false); err != nil {
		t.Fatalf("清空日志路径失败: %v", err)
	}
	if d.logFile != nil {
		t.Fatal("清空路径后 logFile 应为 nil（旧文件已关闭）")
	}
	if d.logFilePath != "" {
		t.Fatalf("清空路径后 logFilePath 应为空，实际 %q", d.logFilePath)
	}

	// Settings.Normalize 不回填空 EventLogFile
	ns := config.DefaultSettings()
	ns.EventLogFile = ""
	if err := ns.Normalize(); err != nil {
		t.Fatalf("Normalize 失败: %v", err)
	}
	if ns.EventLogFile != "" {
		t.Fatalf("Normalize 不应回填清空的日志路径，实际 %q", ns.EventLogFile)
	}
}
