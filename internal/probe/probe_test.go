package probe

import (
	"errors"
	"strings"
	"testing"

	"github.com/mickeyzzc/onvif-go/v2/analytics"
	"github.com/mickeyzzc/onvif-go/v2/device"
	"github.com/mickeyzzc/onvif-go/v2/events"
	"github.com/mickeyzzc/onvif-go/v2/types"
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
	topics, err := ParseTopicSet(sampleTopicSet)
	if err != nil {
		t.Fatalf("ParseTopicSet: %v", err)
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

func TestFlattenInfoJSON(t *testing.T) {
	rows := FlattenInfoJSON(map[string]any{
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
	s, truncated := ClipInfoText("系统日志内容")
	if truncated || s != "系统日志内容" {
		t.Fatalf("短文本不应截断: %q %v", s, truncated)
	}
	long := strings.Repeat("x", deviceInfoTextLimit+100)
	s, truncated = ClipInfoText(long)
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
	if got := JoinIPAddressesAndHosts(ips, nil); got != "192.0.2.1, 192.0.2.2" {
		t.Errorf("JoinIPAddressesAndHosts(ips) = %q", got)
	}
	hosts := []device.NetworkHost{
		{Type: "IPv4", IPv4Address: "10.0.0.1"},
		{Type: "DNS", DNSname: "time.example.com"},
	}
	if got := JoinIPAddressesAndHosts(nil, hosts); got != "10.0.0.1, time.example.com" {
		t.Errorf("JoinIPAddressesAndHosts(hosts) = %q", got)
	}
}

func TestBriefInfoError(t *testing.T) {
	raw := errors.New(`GetSystemLog failed: HTTP request failed with status 500: <?xml version="1.0"?><Subcode><Value>ter:ActionNotSupported</Value></Subcode></xml>`)
	if got := BriefInfoError(raw); got != "HTTP 500：设备不支持该 ONVIF 操作（ter:ActionNotSupported）" {
		t.Errorf("SOAP Fault 未精简: %q", got)
	}
	if got := BriefInfoError(errors.New("connection refused")); got != "connection refused" {
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
	cols, records := ConfigInfo([]*analytics.Config{
		{Name: "MyCellMotionModule", Type: "tt:CellMotionEngine", Parameters: params[:1]},
		nil,
		{Name: "MyTamperDetecModule", Type: "hikxsd:TamperEngine"},
	})
	if len(cols) != 4 || len(records) != 2 {
		t.Fatalf("ConfigInfo 列/行数错误: %v %v", cols, records)
	}
	if records[0][2] != "1" || records[1][3] != "" {
		t.Errorf("ConfigInfo 参数列错误: %v", records)
	}
}

func TestSupportedInfoEmptyAndPopulated(t *testing.T) {
	rows, cols, records := SupportedInfo(&analytics.Supported{
		ContentSchemaLocations: []string{"http://example.com/schema.xsd"},
	})
	if cols != nil || records != nil || len(rows) != 3 {
		t.Fatalf("空描述应返回说明行: rows=%v cols=%v records=%v", rows, cols, records)
	}
	if rows[0].Value != "0" || !strings.Contains(rows[1].Value, "未返回") {
		t.Errorf("空描述说明不清: %v", rows)
	}
	rows, cols, records = SupportedInfo(&analytics.Supported{
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
	rows := EventCapabilityRows(&events.EventServiceCapabilities{
		MaxNotificationProducers:    10,
		MaxPullPoints:               8,
		WSSubscriptionPolicySupport: true,
		EventBrokerProtocols:        []string{"mqtt", "amqp"},
		MetadataOverMQTT:             true,
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
	if EventCapabilityRows(nil) != nil {
		t.Error("nil 能力应返回 nil")
	}
}
