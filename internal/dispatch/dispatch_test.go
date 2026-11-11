package dispatch

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
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
	if got := PushContent(evt); !strings.Contains(got, "摄像头camera-01 发生了移动侦测事件，触发时间 2026-11-10 16:00:00") {
		t.Errorf("PushContent = %q", got)
	}
	tpl := `{"title":"{title}","content":"{content}"}`
	out := RenderBodyTemplate(tpl, evt)
	if !strings.Contains(out, `"摄像头camera-01 发生-移动侦测"`) || !strings.Contains(out, "发生了移动侦测事件") {
		t.Errorf("RenderBodyTemplate = %q", out)
	}
	if !json.Valid([]byte(out)) {
		t.Errorf("渲染结果不是合法 JSON: %q", out)
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
		client:   &http.Client{Timeout: time.Second},
		queue:    make(chan Event, st.QueueSize),
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		dedupTTL: time.Duration(dedupSeconds) * time.Second,
		seen:     make(map[string]dedupState),
	}
	d.routes.Store(EmptyRouteTable())
	return d
}

func dedupEvent(device, topic, op string, data map[string]string) Event {
	return Event{Device: device, Topic: topic, Operation: op,
		Data: data, Source: map[string]string{"Source": "CellMotionDetector"}}
}

func TestDedupSuppressesHeartbeatButKeepsTransitions(t *testing.T) {
	d := newTestDispatcher(60)
	topic := "tns1:RuleEngine/CellMotionDetector/Motion"
	on := map[string]string{"Motion": "true"}
	off := map[string]string{"Motion": "false"}

	// 1. 开始事件通过
	if d.isDuplicate(dedupEvent("cam", topic, "Changed", on)) {
		t.Fatal("首次 Motion=true 不应被判重")
	}
	// 2. 心跳重复（新 device_time，键不含时间）被抑制
	if !d.isDuplicate(dedupEvent("cam", topic, "Changed", on)) {
		t.Fatal("TTL 内重复 Motion=true 应被判重（心跳风暴）")
	}
	// 3. 结束事件（true→false）必须放行
	if d.isDuplicate(dedupEvent("cam", topic, "Changed", off)) {
		t.Fatal("Motion=false（事件结束）不应被判重")
	}
	// 4. 真实连续触发（false→true，状态已变化）必须放行
	if d.isDuplicate(dedupEvent("cam", topic, "Changed", on)) {
		t.Fatal("false→true 的再次触发不应被判重")
	}
	// 5. on→off 仍是状态变化（再次结束），放行
	if d.isDuplicate(dedupEvent("cam", topic, "Changed", off)) {
		t.Fatal("on→false 的再次结束不应被判重")
	}
	// 6. false 状态的心跳重复被抑制
	if !d.isDuplicate(dedupEvent("cam", topic, "Changed", off)) {
		t.Fatal("TTL 内连续重复 Motion=false 应被判重")
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
