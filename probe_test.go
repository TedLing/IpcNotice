package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mickeyzzc/onvif-go/v2/analytics"
	"github.com/mickeyzzc/onvif-go/v2/device"
	"github.com/mickeyzzc/onvif-go/v2/events"
	"github.com/mickeyzzc/onvif-go/v2/types"
	"github.com/pion/rtp"
)

const sampleTopicSet = `
<tns1:VideoSource wstop:topic="true">
  <tns1:MotionAlarm wstop:topic="true">
    <tt:MessageDescription IsProperty="true">
      <tt:Source><tt:SimpleItemDescription Name="VideoSourceConfigurationToken" Type="tt:ReferenceToken"/></tt:Source>
      <tt:Data><tt:SimpleItemDescription Name="State" Type="xs:boolean"/></tt:Data>
    </tt:MessageDescription>
  </tns1:MotionAlarm>
</tns1:VideoSource>
<tns1:RuleEngine wstop:topic="true">
  <tns1:CellMotionDetector wstop:topic="true">
    <tns1:Motion wstop:topic="true">
      <tt:MessageDescription IsProperty="true">
        <tt:Data><tt:SimpleItemDescription Name="IsMotion" Type="xs:boolean"/></tt:Data>
      </tt:MessageDescription>
    </tns1:Motion>
  </tns1:CellMotionDetector>
</tns1:RuleEngine>`

func TestParseTopicSet(t *testing.T) {
	topics, err := parseTopicSet(sampleTopicSet)
	if err != nil {
		t.Fatalf("parseTopicSet: %v", err)
	}
	got := strings.Join(topics, ",")
	for _, want := range []string{
		"VideoSource",
		"VideoSource/MotionAlarm",
		"RuleEngine/CellMotionDetector/Motion",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("缺少主题 %q，实际: %s", want, got)
		}
	}
	for _, topic := range topics {
		for _, seg := range strings.Split(topic, "/") {
			if descriptionElements[seg] {
				t.Errorf("描述节点被误判为主题: %s (in %s)", seg, topic)
			}
		}
	}
	t.Logf("解析结果: %v", topics)
}

func TestResolveOnvifURL(t *testing.T) {
	cases := []struct {
		url, wantEndpoint, wantUser, wantPass string
	}{
		{"onvif://admin:examplePass1@192.0.2.10:2020", "http://192.0.2.10:2020/onvif/device_service", "admin", "examplePass1"},
		{"onvif://admin:examplePass2@192.0.2.20:2020", "http://192.0.2.20:2020/onvif/device_service", "admin", "examplePass2"},
		{"http://10.0.0.2/onvif/device_service", "http://10.0.0.2/onvif/device_service", "u", "p"},
		{"192.0.2.5:2020", "http://192.0.2.5:2020/onvif/device_service", "u", "p"},
	}
	for _, tc := range cases {
		d := &DeviceConfig{URL: tc.url, Username: "u", Password: "p"}
		if err := d.resolve(); err != nil {
			t.Fatalf("%s: %v", tc.url, err)
		}
		if tc.wantUser == "admin" {
			// url 内的凭据优先级低于显式字段，这里单独验证 userinfo 提取。
			d2 := &DeviceConfig{URL: tc.url}
			if err := d2.resolve(); err != nil {
				t.Fatalf("%s: %v", tc.url, err)
			}
			if d2.DeviceUser != tc.wantUser || d2.DevicePass != tc.wantPass {
				t.Errorf("userinfo: got %s/%s, want %s/%s", d2.DeviceUser, d2.DevicePass, tc.wantUser, tc.wantPass)
			}
		}
		if d.Endpoint != tc.wantEndpoint {
			t.Errorf("%s: endpoint = %s, want %s", tc.url, d.Endpoint, tc.wantEndpoint)
		}
	}
}

func TestMaskedURL(t *testing.T) {
	d := &DeviceConfig{URL: "onvif://admin:secret@192.0.2.10:2020"}
	if strings.Contains(d.MaskedURL(), "secret") {
		t.Fatalf("密码未被遮蔽: %s", d.MaskedURL())
	}
}

func TestDefaultTopicRemark(t *testing.T) {
	cases := map[string]string{
		"tns1:RuleEngine/CellMotionDetector/Motion":  "移动侦测",
		"RuleEngine/CellMotionDetector/Intrusion":    "区域入侵检测",
		"RuleEngine/CellMotionDetector/People":       "人行检测",
		"RuleEngine/CellMotionDetector/TpSmartEvent": "智能事件",
		"tns1:RuleEngine/TamperDetector/Tamper":      "遮挡告警",
		"RuleEngine/SomeVendor/UnknownThing":         "",
	}
	for topic, want := range cases {
		if got := defaultTopicRemark(topic); got != want {
			t.Errorf("defaultTopicRemark(%q) = %q, want %q", topic, got, want)
		}
	}
}

func TestLookupTitle(t *testing.T) {
	rt := emptyRouteTable()
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
			title, remark := rt.lookupTitle(tc.device, tc.topic)
			if title != tc.title || remark != tc.remark {
				t.Errorf("lookupTitle(%q,%q) = (%q,%q), want (%q,%q)",
					tc.device, tc.topic, title, remark, tc.title, tc.remark)
			}
		})
	}
	if title, remark := (*RouteTable)(nil).lookupTitle("cam1", "RuleEngine/Motion"); title != "Motion" || remark != "" {
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
	if got := pushTitle(evt); got != "摄像头camera-01 发生-移动侦测" {
		t.Errorf("pushTitle = %q", got)
	}
	if got := pushContent(evt); !strings.Contains(got, "摄像头camera-01 发生了移动侦测事件，触发时间 2026-11-10 16:00:00") {
		t.Errorf("pushContent = %q", got)
	}
	tpl := `{"title":"{title}","content":"{content}"}`
	out := renderBodyTemplate(tpl, evt)
	if !strings.Contains(out, `"摄像头camera-01 发生-移动侦测"`) || !strings.Contains(out, "发生了移动侦测事件") {
		t.Errorf("renderBodyTemplate = %q", out)
	}
	if !json.Valid([]byte(out)) {
		t.Errorf("渲染结果不是合法 JSON: %q", out)
	}
	if renderBodyTemplate("{}", evt) != "{}" {
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
		if !validBodyTemplate(ok) {
			t.Errorf("应判定为合法模板: %q", ok)
		}
	}
	for _, bad := range []string{
		`{"title":"{title}"`,
		`not json`,
	} {
		if validBodyTemplate(bad) {
			t.Errorf("应判定为非法模板: %q", bad)
		}
	}
}

// newTestDispatcher 构造只用于去重测试的分发器（不开 worker、不写文件）。
func newTestDispatcher(dedupSeconds int) *Dispatcher {
	st := DefaultSettings()
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
	d.routes.Store(emptyRouteTable())
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

func TestDeviceHashIgnoresDisplayName(t *testing.T) {
	st := DefaultSettings()
	a := &DeviceConfig{Name: "摄像头 A", Endpoint: "http://192.0.2.10:2020/onvif/device_service"}
	b := &DeviceConfig{Name: "ipc-a", Endpoint: "http://192.0.2.10:2020/onvif/device_service"}
	if deviceHash(a, st) != deviceHash(b, st) {
		t.Fatal("改显示名不应改变订阅哈希（不重建订阅）")
	}
	c := &DeviceConfig{Name: "摄像头 A", Endpoint: "http://192.0.2.20:2020/onvif/device_service"}
	if deviceHash(a, st) == deviceHash(c, st) {
		t.Fatal("设备地址变化必须改变订阅哈希（重建订阅）")
	}
}

func TestFlattenInfoJSON(t *testing.T) {
	rows := flattenInfoJSON(map[string]any{
		"Events": map[string]any{
			"XAddr":                "http://dev/onvif/events",
			"WSPullPointSupport":   true,
			"WSSubscriptionPolicy": false,
			"Unused":               nil,
			"Versions":             []any{"16", "20"},
		},
	})
	got := map[string]string{}
	for _, r := range rows {
		got[r.Label] = r.Value
	}
	if got["Events.XAddr"] != "http://dev/onvif/events" {
		t.Errorf("嵌套 XAddr 展平错误: %v", rows)
	}
	if got["Events.WSPullPointSupport"] != "是" || got["Events.WSSubscriptionPolicy"] != "否" {
		t.Errorf("布尔值未中文化: %v", rows)
	}
	if _, exists := got["Events.Unused"]; exists {
		t.Error("nil 字段不应出现在结果中")
	}
	for i := 1; i < len(rows); i++ {
		if rows[i-1].Label > rows[i].Label {
			t.Fatalf("结果未按字段名排序: %v", rows)
		}
	}
}

func TestClipInfoText(t *testing.T) {
	s, truncated := clipInfoText("系统日志内容")
	if truncated || s != "系统日志内容" {
		t.Fatalf("短文本不应截断: %q %v", s, truncated)
	}
	long := strings.Repeat("x", deviceInfoTextLimit+100)
	s, truncated = clipInfoText(long)
	if !truncated || len(s) != deviceInfoTextLimit {
		t.Fatalf("长文本未按上限截断: len=%d truncated=%v", len(s), truncated)
	}
}

func TestJoinIPAddressesAndHosts(t *testing.T) {
	ips := []types.IPAddress{
		{Type: "IPv4", IPv4Address: "192.0.2.1"},
		{Type: "IPv4", Address: "192.0.2.2"},
		{},
	}
	if got := joinIPAddresses(ips); got != "192.0.2.1, 192.0.2.2" {
		t.Errorf("joinIPAddresses = %q", got)
	}
	hosts := []device.NetworkHost{
		{Type: "IPv4", IPv4Address: "10.0.0.1"},
		{Type: "DNS", DNSname: "time.example.com"},
	}
	if got := joinNetworkHosts(hosts); got != "10.0.0.1, time.example.com" {
		t.Errorf("joinNetworkHosts = %q", got)
	}
}

func TestBriefInfoError(t *testing.T) {
	raw := errors.New(`GetSystemLog failed: HTTP request failed with status 500: <?xml version="1.0"?><Subcode><Value>ter:ActionNotSupported</Value></Subcode></xml>`)
	if got := briefInfoError(raw); got != "HTTP 500：设备不支持该 ONVIF 操作（ter:ActionNotSupported）" {
		t.Errorf("SOAP Fault 未精简: %q", got)
	}
	if got := briefInfoError(errors.New("connection refused")); got != "connection refused" {
		t.Errorf("普通错误不应改写: %q", got)
	}
}

func TestAnalyticsParamsAndConfigInfo(t *testing.T) {
	params := []analytics.SimpleItem{
		{Name: "Sensitivity", Value: "60"},
		{Name: "MinCount", Value: "5"},
	}
	if got := analyticsParams(params); got != "Sensitivity=60; MinCount=5" {
		t.Errorf("analyticsParams = %q", got)
	}
	cols, records := configInfo([]*analytics.Config{
		{Name: "MyCellMotionModule", Type: "tt:CellMotionEngine", Parameters: params[:1]},
		nil,
		{Name: "MyTamperDetecModule", Type: "hikxsd:TamperEngine"},
	})
	if len(cols) != 4 || len(records) != 2 {
		t.Fatalf("configInfo 列/行数错误: %v %v", cols, records)
	}
	if records[0][2] != "1" || records[1][3] != "" {
		t.Errorf("configInfo 参数列错误: %v", records)
	}
}

func TestSupportedInfoEmptyAndPopulated(t *testing.T) {
	rows, cols, records := supportedInfo(&analytics.Supported{
		ContentSchemaLocations: []string{"http://example.com/schema.xsd"},
	})
	if cols != nil || records != nil || len(rows) != 3 {
		t.Fatalf("空描述应返回说明行: rows=%v cols=%v records=%v", rows, cols, records)
	}
	if rows[0].Value != "0" || !strings.Contains(rows[1].Value, "未返回") {
		t.Errorf("空描述说明不清: %v", rows)
	}
	rows, cols, records = supportedInfo(&analytics.Supported{
		Descriptions: []analytics.ConfigDescription{
			{Name: "tt:CellMotionDetector", Parameters: []analytics.ItemDescription{
				{Name: "ActiveCells", Type: "tt:CellLayout"},
				{Name: "MinCount", Type: "xs:int"},
			}},
		},
	})
	if rows != nil || len(cols) != 3 || len(records) != 1 {
		t.Fatalf("有描述应返回表格: rows=%v cols=%v records=%v", rows, cols, records)
	}
	if records[0][1] != "2" || !strings.Contains(records[0][2], "ActiveCells:tt:CellLayout") {
		t.Errorf("描述参数格式化错误: %v", records)
	}
}

func TestEventCapabilityRows(t *testing.T) {
	rows := eventCapabilityRows(&events.EventServiceCapabilities{
		MaxNotificationProducers:    10,
		MaxPullPoints:               8,
		WSSubscriptionPolicySupport: true,
		EventBrokerProtocols:        []string{"mqtt", "amqp"},
		MetadataOverMQTT:            true,
	})
	got := map[string]string{}
	for _, r := range rows {
		got[r.Label] = r.Value
	}
	if got["MaxNotificationProducers（Push 订阅槽上限）"] != "10" {
		t.Errorf("订阅槽上限缺失: %v", rows)
	}
	if got["MaxPullPoints（PullPoint 上限）"] != "8" {
		t.Errorf("PullPoint 上限缺失: %v", rows)
	}
	if got["WS-SubscriptionPolicy 支持"] != "是" || got["EventBroker 协议"] != "mqtt, amqp" {
		t.Errorf("布尔/列表字段错误: %v", rows)
	}
	if eventCapabilityRows(nil) != nil {
		t.Error("nil 能力应返回 nil")
	}
}

func TestPreviewRTSPURL(t *testing.T) {
	dev := &DeviceConfig{DeviceUser: "admin", DevicePass: "p@ss:word"}
	u, masked, err := previewRTSPURL("rtsp://192.0.2.10:554/stream2", dev)
	if err != nil {
		t.Fatal(err)
	}
	if u.User == nil || u.User.Username() != "admin" {
		t.Fatalf("凭据未注入: %v", u.User)
	}
	if pass, _ := u.User.Password(); pass != "p@ss:word" {
		t.Fatalf("密码转义错误: %q", pass)
	}
	if masked != "rtsp://admin:***@192.0.2.10:554/stream2" {
		t.Fatalf("遮蔽后的地址错误: %q", masked)
	}
	if strings.Contains(masked, "p@ss") {
		t.Fatal("遮蔽后仍含密码")
	}
	// 设备已带凭据时不覆盖，但仍需遮蔽
	u2, masked2, err := previewRTSPURL("rtsp://u:secret@192.0.2.11/stream1", dev)
	if err != nil {
		t.Fatal(err)
	}
	if p, _ := u2.User.Password(); p != "secret" {
		t.Fatalf("覆盖了设备自带凭据: %q", p)
	}
	if strings.Contains(masked2, "secret") {
		t.Fatal("遮蔽后仍含密码")
	}
	if _, _, err := previewRTSPURL("http://192.0.2.10/live", dev); err == nil {
		t.Error("非 RTSP 协议应被拒绝")
	}
}

func TestPreviewStapA(t *testing.T) {
	sps := []byte{0x67, 0x42, 0x00, 0x1e}
	pps := []byte{0x68, 0xce, 0x3c}
	ref := &rtp.Packet{
		Header:  rtp.Header{Version: 2, PayloadType: 96, SequenceNumber: 100, Timestamp: 90000, Marker: true},
		Payload: []byte{0x65, 0x88},
	}
	stap := previewStapA(sps, pps, ref)
	if stap == nil {
		t.Fatal("应生成 STAP-A 包")
	}
	if stap.Header.SequenceNumber != 99 || stap.Header.Marker || stap.Header.Timestamp != 90000 {
		t.Fatalf("STAP-A 头部错误: %+v", stap.Header)
	}
	if stap.Payload[0] != 0x78 {
		t.Fatalf("NAL 类型不是 STAP-A: 0x%02x", stap.Payload[0])
	}
	if int(binary.BigEndian.Uint16(stap.Payload[1:3])) != len(sps) {
		t.Fatal("SPS 长度字段错误")
	}
	off := 3 + len(sps)
	if int(binary.BigEndian.Uint16(stap.Payload[off:off+2])) != len(pps) {
		t.Fatal("PPS 长度字段错误")
	}
	if len(stap.Payload) != 1+2+len(sps)+2+len(pps) {
		t.Fatalf("STAP-A 载荷长度错误: %d", len(stap.Payload))
	}
	// 原包不能被改写
	if !ref.Header.Marker || ref.Header.SequenceNumber != 100 {
		t.Fatal("构造 STAP-A 修改了原始 RTP 包")
	}
	if previewStapA(nil, pps, ref) != nil || previewStapA(sps, pps, nil) != nil {
		t.Error("缺少参数集或参考包时不应生成 STAP-A")
	}
}
