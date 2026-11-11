package main

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/mickeyzzc/onvif-go/v2/analytics"
	"github.com/mickeyzzc/onvif-go/v2/device"
	"github.com/mickeyzzc/onvif-go/v2/events"
	"github.com/mickeyzzc/onvif-go/v2/onvif"
	"github.com/mickeyzzc/onvif-go/v2/types"
)

const eventsNamespace = "http://www.onvif.org/ver10/events/wsdl"

// DeviceTestResult 是「测试连接 / 抓取事件」接口返回的完整结果。
type DeviceTestResult struct {
	OK               bool     `json:"ok"`
	Message          string   `json:"message"`
	Manufacturer     string   `json:"manufacturer"`
	Model            string   `json:"model"`
	Firmware         string   `json:"firmware"`
	Serial           string   `json:"serial"`
	EventsXAddr      string   `json:"events_xaddr"`
	AuthMode         string   `json:"auth_mode"`
	PullPointSupport bool     `json:"pullpoint_support"`
	PushSupport      bool     `json:"push_support"`
	MaxPullPoints    int      `json:"max_pull_points"`
	Topics           []string `json:"topics"`
	ObservedTopics   []string `json:"observed_topics"`
	CaptureSeconds   int      `json:"capture_seconds"`
	Warnings         []string `json:"warnings"`
}

// newDeviceClient 构造带鉴权梯队与时钟偏差校正的 ONVIF 客户端。
func newDeviceClient(dev *DeviceConfig) (*onvif.Client, error) {
	timeout := 12 * time.Second
	return onvif.NewClient(
		dev.Endpoint,
		onvif.WithHTTPClient(newONVIFHTTPClient(timeout)),
		onvif.WithCredentials(dev.DeviceUser, dev.DevicePass),
		onvif.WithTimeout(timeout),
		onvif.WithAutoClockSkew(),
		onvif.WithAuthFallback(onvif.AuthPasswordText, onvif.AuthHTTPBasic),
	)
}

// testDevice 连接设备、读取设备信息与事件能力；captureSeconds>0 时额外做一次
// 短时 PullPoint 实抓，用于补齐 TP-Link 固件「实际推送主题与 TopicSet 不一致」的部分。
func testDevice(ctx context.Context, dev *DeviceConfig, logger *slog.Logger, captureSeconds int) (DeviceTestResult, error) {
	r := DeviceTestResult{Topics: []string{}, ObservedTopics: []string{}, Warnings: []string{}}
	cli, err := newDeviceClient(dev)
	if err != nil {
		return r, fmt.Errorf("创建 ONVIF 客户端失败: %w", err)
	}

	initCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	if err := cli.Initialize(initCtx); err != nil {
		return r, fmt.Errorf("连接设备失败（检查地址、账号、鉴权与设备时钟）: %w", err)
	}
	r.OK = true
	r.Message = "连接成功"
	r.AuthMode = string(cli.AuthLadderMode())
	r.EventsXAddr = cli.EndpointFor("events")

	callCtx, cancelCall := context.WithTimeout(ctx, 20*time.Second)
	defer cancelCall()

	if info, err := cli.Device().GetDeviceInformation(callCtx); err != nil {
		r.Warnings = append(r.Warnings, "GetDeviceInformation 失败: "+err.Error())
	} else {
		r.Manufacturer, r.Model = info.Manufacturer, info.Model
		r.Firmware, r.Serial = info.FirmwareVersion, info.SerialNumber
	}

	if caps, err := cli.Device().GetCapabilitiesCached(callCtx); err != nil {
		r.Warnings = append(r.Warnings, "GetCapabilities 失败: "+err.Error())
	} else if caps.Events != nil {
		r.PullPointSupport = caps.Events.WSPullPointSupport
		r.PushSupport = caps.Events.WSSubscriptionPolicySupport
	}

	if sc, err := cli.Events().GetEventServiceCapabilities(callCtx); err != nil {
		r.Warnings = append(r.Warnings, "GetEventServiceCapabilities 失败: "+err.Error())
	} else {
		r.MaxPullPoints = sc.MaxPullPoints
		if sc.MaxPullPoints > 0 {
			r.PullPointSupport = true
		}
	}

	if _, err := cli.Events().GetEventProperties(callCtx); err != nil {
		r.Warnings = append(r.Warnings, "GetEventProperties 失败: "+err.Error())
	}
	if topics, err := fetchTopicPaths(callCtx, cli); err != nil {
		r.Warnings = append(r.Warnings, "TopicSet 主题枚举失败: "+err.Error())
	} else {
		for _, t := range topics {
			r.Topics = append(r.Topics, normalizeTopic(t))
		}
		r.Topics = uniqueNonEmpty(r.Topics)
	}

	if captureSeconds > 0 {
		observed, err := captureLiveTopics(ctx, cli, captureSeconds)
		if err != nil {
			r.Warnings = append(r.Warnings, "实时事件抓取失败: "+err.Error())
		} else {
			r.ObservedTopics = observed
			r.CaptureSeconds = captureSeconds
		}
	}

	logger.Info("设备测试完成", "device", dev.Name, "ok", r.OK, "model", r.Model,
		"topics", len(r.Topics), "observed_topics", len(r.ObservedTopics), "warnings", len(r.Warnings))
	return r, nil
}

// captureLiveTopics 建立一个短租约 PullPoint，收集窗口内真实出现的事件主题。
func captureLiveTopics(ctx context.Context, cli *onvif.Client, seconds int) ([]string, error) {
	if seconds <= 0 {
		return []string{}, nil
	}
	if seconds > 30 {
		seconds = 30
	}
	ttl := time.Duration(seconds+20) * time.Second
	subCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	sub, err := cli.Events().CreatePullPointSubscription(subCtx, "", &ttl, "")
	cancel()
	if err != nil {
		return nil, err
	}
	defer func() {
		u, c := context.WithTimeout(context.WithoutCancel(ctx), 6*time.Second)
		defer c()
		_ = cli.Events().Unsubscribe(u, sub.SubscriptionReference)
	}()

	seen := map[string]bool{}
	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	for {
		remain := time.Until(deadline)
		if remain <= 0 {
			break
		}
		pullTimeout := 2 * time.Second
		if remain < pullTimeout {
			pullTimeout = remain
		}
		if pullTimeout < 200*time.Millisecond {
			break
		}
		msgs, err := cli.Events().PullMessages(ctx, sub.SubscriptionReference, pullTimeout, 100)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			// 长轮询被设备提前结束时继续下一轮，直到窗口耗尽。
			time.Sleep(150 * time.Millisecond)
			continue
		}
		for _, m := range msgs {
			if t := normalizeTopic(m.Topic); t != "" {
				seen[t] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	sort.Strings(out)
	return out, nil
}

// normalizeTopic 去掉各段命名空间前缀（tns1: / wstop: 等），
// 使 TopicSet 枚举出的层级路径与真实 Notify 主题可以互相匹配。
func normalizeTopic(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	parts := strings.Split(s, "/")
	for i, p := range parts {
		if idx := strings.LastIndex(p, ":"); idx >= 0 {
			p = p[idx+1:]
		}
		parts[i] = strings.TrimSpace(p)
	}
	return strings.Trim(strings.Join(parts, "/"), "/")
}

// matchTopic 支持以 /* 结尾的通配订阅，其余按规范化后的路径精确匹配。
func matchTopic(pattern, actual string) bool {
	p, a := normalizeTopic(pattern), normalizeTopic(actual)
	if p == "" || a == "" {
		return false
	}
	if p == a || p == "*" {
		return true
	}
	if strings.HasSuffix(p, "/*") {
		return strings.HasPrefix(a, strings.TrimSuffix(p, "*"))
	}
	return false
}

func uniqueNonEmpty(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// ---------------------------------------------------------------------------
// TopicSet 原始 XML 解析
// ---------------------------------------------------------------------------

type eventPropertiesRawResponse struct {
	XMLName  xml.Name `xml:"GetEventPropertiesResponse"`
	TopicSet struct {
		Inner string `xml:",innerxml"`
	} `xml:"TopicSet"`
}

type eventPropertiesRawRequest struct {
	XMLName xml.Name `xml:"tev:GetEventProperties"`
	Xmlns   string   `xml:"xmlns:tev,attr"`
}

func fetchTopicPaths(ctx context.Context, cli *onvif.Client) ([]string, error) {
	req := eventPropertiesRawRequest{Xmlns: eventsNamespace}
	var resp eventPropertiesRawResponse
	if err := cli.Call(ctx, cli.EndpointFor("events"), "", req, &resp); err != nil {
		return nil, err
	}
	return parseTopicSet(resp.TopicSet.Inner)
}

// descriptionElements 是 TopicSet 里描述消息结构的节点，不是事件主题。
var descriptionElements = map[string]bool{
	"MessageDescription":           true,
	"MessageDescriptionExtension":  true,
	"SimpleItemDescription":        true,
	"ElementItemDescription":       true,
	"Source":                       true,
	"Key":                          true,
	"Data":                         true,
	"Extension":                    true,
	"MessageDescriptionExtension2": true,
}

// parseTopicSet 用宽容的 token 解析收集 TopicSet 树中的全部主题路径。
// ONVIF TopicSet 用元素名表达主题层级，且常带未声明的前缀（tns1:/wstop:），
// 因此关闭 strict 命名空间校验。
func parseTopicSet(inner string) ([]string, error) {
	if strings.TrimSpace(inner) == "" {
		return nil, nil
	}
	dec := xml.NewDecoder(strings.NewReader(inner))
	dec.Strict = false

	var stack []string
	var topics []string
	seen := make(map[string]bool)
	skipDepth := 0

	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return topics, fmt.Errorf("解析 TopicSet XML: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			name := t.Name.Local
			if skipDepth > 0 || descriptionElements[name] {
				skipDepth++
				continue
			}
			stack = append(stack, name)
			path := strings.Join(stack, "/")
			if !seen[path] {
				seen[path] = true
				topics = append(topics, path)
			}
		case xml.EndElement:
			if skipDepth > 0 {
				skipDepth--
				continue
			}
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		}
	}
	return topics, nil
}

// ---------------- ONVIF 设备信息聚合（摄像头管理 → 设备信息） ----------------

const deviceInfoTextLimit = 16 << 10 // 系统日志/支持信息最长展示 16 KiB

// DeviceInfoRow 是一行「名称 → 值」。
type DeviceInfoRow struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

// DeviceInfoItem 对应一次 ONVIF 操作。单项失败（设备不支持或超时）不影响其他项。
// 展示形态三选一：Rows（键值对）、Columns+Records（多条目表格）、Text（长文本）。
type DeviceInfoItem struct {
	Operation string          `json:"operation"`
	Label     string          `json:"label"`
	OK        bool            `json:"ok"`
	Error     string          `json:"error,omitempty"`
	Rows      []DeviceInfoRow `json:"rows,omitempty"`
	Columns   []string        `json:"columns,omitempty"`
	Records   [][]string      `json:"records,omitempty"`
	Text      string          `json:"text,omitempty"`
	Truncated bool            `json:"truncated,omitempty"`
}

type DeviceInfoSection struct {
	Key   string           `json:"key"`
	Title string           `json:"title"`
	Items []DeviceInfoItem `json:"items"`
}

type DeviceInfoResult struct {
	Device    string              `json:"device"`
	Endpoint  string              `json:"endpoint"`
	AuthMode  string              `json:"auth_mode"`
	FetchedAt string              `json:"fetched_at"`
	ElapsedMs int64               `json:"elapsed_ms"`
	Sections  []DeviceInfoSection `json:"sections"`
	Warnings  []string            `json:"warnings"`

	// 来自 Event 服务 GetServiceCapabilities 的订阅上限汇总（0 表示未取到）。
	// MaxNotificationProducers 就是设备端 Push 订阅槽位上限：僵尸订阅耗尽它之后，
	// 新订阅会「注册成功但零回调」，详见 README §10。
	MaxNotificationProducers int `json:"max_notification_producers"`
	MaxPullPoints            int `json:"max_pull_points"`
}

// infoCall 为单次 ONVIF 操作提供独立的 8 秒超时与错误隔离。
func infoCall(ctx context.Context, op, label string, fn func(context.Context, *DeviceInfoItem)) DeviceInfoItem {
	item := DeviceInfoItem{Operation: op, Label: label}
	if ctx.Err() != nil {
		item.Error = "采集总超时，该项未执行"
		return item
	}
	callCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	fn(callCtx, &item)
	return item
}

func boolCN(v bool) string {
	if v {
		return "是"
	}
	return "否"
}

// briefInfoError 精简设备返回的错误：上游库会把整段 SOAP Fault XML 拼进 error，
// 界面只保留 HTTP 状态与 ONVIF 错误码，并对常见「不支持」给出可读说明。
func briefInfoError(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	code := ""
	if i := strings.Index(s, ">ter:"); i >= 0 {
		rest := s[i+len(">ter:"):]
		if j := strings.IndexByte(rest, '<'); j > 0 {
			candidate := rest[:j]
			valid := true
			for _, r := range candidate {
				if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
					valid = false
					break
				}
			}
			if valid {
				code = candidate
			}
		}
	}
	status := ""
	if i := strings.Index(s, "status "); i >= 0 {
		rest := s[i+len("status "):]
		j := 0
		for j < len(rest) && rest[j] >= '0' && rest[j] <= '9' {
			j++
		}
		if j > 0 {
			status = rest[:j]
		}
	}
	prefix := ""
	if status != "" {
		prefix = "HTTP " + status + "："
	}
	switch code {
	case "ActionNotSupported":
		return prefix + "设备不支持该 ONVIF 操作（ter:ActionNotSupported）"
	case "":
		if len(s) > 300 {
			return s[:300] + "…"
		}
		return s
	default:
		return prefix + "设备返回 SOAP 错误 ter:" + code
	}
}

func joinNonEmpty(parts []string) string {
	out := []string{}
	for _, p := range parts {
		if strings.TrimSpace(p) != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, ", ")
}

func clipInfoText(s string) (string, bool) {
	if len(s) <= deviceInfoTextLimit {
		return strings.ToValidUTF8(s, ""), false
	}
	return strings.ToValidUTF8(s[:deviceInfoTextLimit], ""), true
}

// flattenInfoJSON 把 GetCapabilities 的嵌套结构展平为可读的键值对。
func flattenInfoJSON(v any) []DeviceInfoRow {
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil
	}
	rows := []DeviceInfoRow{}
	var walk func(map[string]any, string)
	walk = func(cur map[string]any, prefix string) {
		for k, raw := range cur {
			key := k
			if prefix != "" {
				key = prefix + "." + k
			}
			switch x := raw.(type) {
			case map[string]any:
				walk(x, key)
			case []any:
				if len(x) > 0 {
					rows = append(rows, DeviceInfoRow{key, fmt.Sprint(x)})
				}
			case nil:
			case bool:
				rows = append(rows, DeviceInfoRow{key, boolCN(x)})
			default:
				rows = append(rows, DeviceInfoRow{key, fmt.Sprint(x)})
			}
		}
	}
	walk(m, "")
	sort.Slice(rows, func(i, j int) bool { return rows[i].Label < rows[j].Label })
	return rows
}

func ipAddressText(v types.IPAddress) string {
	return joinNonEmpty([]string{v.Address, v.IPv4Address, v.IPv6Address})
}

func networkHostText(v device.NetworkHost) string {
	return joinNonEmpty([]string{v.IPv4Address, v.IPv6Address, v.DNSname})
}

func joinIPAddresses(items []types.IPAddress) string {
	parts := make([]string, 0, len(items))
	for _, v := range items {
		if s := ipAddressText(v); s != "" {
			parts = append(parts, s)
		}
	}
	return strings.Join(parts, ", ")
}

func joinNetworkHosts(items []device.NetworkHost) string {
	parts := make([]string, 0, len(items))
	for _, v := range items {
		if s := networkHostText(v); s != "" {
			parts = append(parts, s)
		}
	}
	return strings.Join(parts, ", ")
}

func dateTimeText(dt *device.DateTime) string {
	if dt == nil {
		return ""
	}
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
		dt.Date.Year, dt.Date.Month, dt.Date.Day,
		dt.Time.Hour, dt.Time.Minute, dt.Time.Second)
}

// ---------------- ONVIF 事件能力 / 分析配置展示辅助 ----------------

// eventCapabilityRows 展示 Event 服务 GetServiceCapabilities 的全部字段。
// 注意：Device 服务的 GetCapabilities 不含 MaxNotificationProducers；
// 订阅槽上限只能从 Event 服务能力里取。
func eventCapabilityRows(v *events.EventServiceCapabilities) []DeviceInfoRow {
	if v == nil {
		return nil
	}
	return []DeviceInfoRow{
		{"MaxNotificationProducers（Push 订阅槽上限）", fmt.Sprint(v.MaxNotificationProducers)},
		{"MaxPullPoints（PullPoint 上限）", fmt.Sprint(v.MaxPullPoints)},
		{"WS-SubscriptionPolicy 支持", boolCN(v.WSSubscriptionPolicySupport)},
		{"可暂停订阅管理接口", boolCN(v.WSPausableSubscriptionManagerInterfaceSupport)},
		{"持久化通知存储", boolCN(v.PersistentNotificationStorage)},
		{"EventBroker 协议", strings.Join(v.EventBrokerProtocols, ", ")},
		{"MaxEventBrokers", fmt.Sprint(v.MaxEventBrokers)},
		{"MetadataOverMQTT", boolCN(v.MetadataOverMQTT)},
	}
}

// analyticsParams 把 tt:Config 的 SimpleItem 参数格式化为 name=value; ...。
func analyticsParams(items []analytics.SimpleItem) string {
	parts := make([]string, 0, len(items))
	for _, p := range items {
		parts = append(parts, p.Name+"="+p.Value)
	}
	return strings.Join(parts, "; ")
}

// supportedInfo 把 GetSupportedAnalyticsModules / GetSupportedRules 的结果转成
// 表格。设备可能只声明支持却返回空描述列表（TP-Link 固件即如此），此时给出
// 说明行，避免界面显示「获取成功」却一片空白。
func supportedInfo(v *analytics.Supported) ([]DeviceInfoRow, []string, [][]string) {
	if v == nil || len(v.Descriptions) == 0 {
		rows := []DeviceInfoRow{
			{"类型描述条目数", "0"},
			{"说明", "设备声明支持该操作，但未返回任何类型/参数描述（部分固件如此）"},
		}
		if v != nil && len(v.ContentSchemaLocations) > 0 {
			rows = append(rows, DeviceInfoRow{"内容 Schema 地址",
				strings.Join(v.ContentSchemaLocations, ", ")})
		}
		return rows, nil, nil
	}
	cols := []string{"类型名称", "参数个数", "参数（名称:类型）"}
	records := make([][]string, 0, len(v.Descriptions))
	for _, d := range v.Descriptions {
		params := make([]string, 0, len(d.Parameters))
		for _, p := range d.Parameters {
			params = append(params, p.Name+":"+p.Type)
		}
		records = append(records, []string{d.Name, fmt.Sprint(len(d.Parameters)),
			strings.Join(params, "; ")})
	}
	return nil, cols, records
}

// configInfo 把 GetAnalyticsModules / GetRules 的 Config 列表转成表格。
func configInfo(items []*analytics.Config) ([]string, [][]string) {
	cols := []string{"名称", "类型", "参数个数", "参数"}
	records := make([][]string, 0, len(items))
	for _, c := range items {
		if c == nil {
			continue
		}
		records = append(records, []string{c.Name, c.Type,
			fmt.Sprint(len(c.Parameters)), analyticsParams(c.Parameters)})
	}
	return cols, records
}

// collectDeviceInfo 逐项尽力调用 ONVIF 设备服务，按 基本信息/系统信息/网络信息/分析配置 分组返回。
// 单项失败只记录该项错误（通常是设备不支持），其余操作继续执行。
func collectDeviceInfo(parent context.Context, dev *DeviceConfig, logger *slog.Logger) DeviceInfoResult {
	start := time.Now()
	res := DeviceInfoResult{
		Device:    dev.Name,
		Endpoint:  dev.MaskedURL(),
		FetchedAt: start.UTC().Format(time.RFC3339),
		Sections:  []DeviceInfoSection{},
		Warnings:  []string{},
	}
	cli, err := newDeviceClient(dev)
	if err != nil {
		res.Warnings = append(res.Warnings, "创建 ONVIF 客户端失败: "+err.Error())
		res.ElapsedMs = time.Since(start).Milliseconds()
		return res
	}
	initCtx, cancelInit := context.WithTimeout(parent, 25*time.Second)
	err = cli.Initialize(initCtx)
	cancelInit()
	if err != nil {
		res.Warnings = append(res.Warnings,
			"连接设备失败（检查地址、账号、鉴权与设备时钟）: "+err.Error())
		res.ElapsedMs = time.Since(start).Milliseconds()
		return res
	}
	res.AuthMode = string(cli.AuthLadderMode())

	ctx, cancel := context.WithTimeout(parent, 90*time.Second)
	defer cancel()
	svc := cli.Device()
	// GetServices 里声明的服务命名空间；用于判断设备是否提供 Analytics 服务。
	serviceNamespaces := map[string]bool{}

	// ---- 基本信息 ----
	basic := DeviceInfoSection{Key: "basic", Title: "基本信息", Items: []DeviceInfoItem{}}
	basic.Items = append(basic.Items, infoCall(ctx, "GetDeviceInformation",
		"厂商、型号、固件版本、序列号、硬件 ID", func(c context.Context, it *DeviceInfoItem) {
			v, err := svc.GetDeviceInformation(c)
			if err != nil {
				it.Error = briefInfoError(err)
				return
			}
			it.OK = true
			it.Rows = []DeviceInfoRow{
				{"厂商", v.Manufacturer}, {"型号", v.Model},
				{"固件版本", v.FirmwareVersion}, {"序列号", v.SerialNumber},
				{"硬件 ID", v.HardwareID},
			}
		}))
	basic.Items = append(basic.Items, infoCall(ctx, "GetServices",
		"设备提供的 ONVIF 服务及 XAddr", func(c context.Context, it *DeviceInfoItem) {
			v, err := svc.GetServices(c, false)
			if err != nil {
				it.Error = briefInfoError(err)
				return
			}
			it.OK = true
			it.Columns = []string{"命名空间", "XAddr", "版本"}
			it.Records = [][]string{}
			for _, s := range v {
				if s == nil {
					continue
				}
				if s.Namespace != "" {
					serviceNamespaces[s.Namespace] = true
				}
				it.Records = append(it.Records, []string{s.Namespace, s.XAddr,
					fmt.Sprintf("%d.%d", s.Version.Major, s.Version.Minor)})
			}
		}))
	// Device 服务的 GetCapabilities 不含订阅槽上限，必须单独调 Event 服务的
	// GetServiceCapabilities 才能拿到 MaxNotificationProducers（僵尸订阅诊断关键值）。
	basic.Items = append(basic.Items, infoCall(ctx, "GetServiceCapabilities [Event]",
		"事件服务能力：MaxNotificationProducers（Push 订阅槽上限）、MaxPullPoints 等",
		func(c context.Context, it *DeviceInfoItem) {
			v, err := cli.Events().GetEventServiceCapabilities(c)
			if err != nil {
				it.Error = briefInfoError(err)
				return
			}
			it.OK = true
			it.Rows = eventCapabilityRows(v)
			res.MaxNotificationProducers = v.MaxNotificationProducers
			res.MaxPullPoints = v.MaxPullPoints
		}))
	basic.Items = append(basic.Items, infoCall(ctx, "GetCapabilities",
		"设备支持的功能能力", func(c context.Context, it *DeviceInfoItem) {
			v, err := svc.GetCapabilities(c)
			if err != nil {
				it.Error = briefInfoError(err)
				return
			}
			it.OK = true
			it.Rows = flattenInfoJSON(v)
		}))

	// ---- 系统信息 ----
	system := DeviceInfoSection{Key: "system", Title: "系统信息", Items: []DeviceInfoItem{}}
	system.Items = append(system.Items, infoCall(ctx, "GetSystemDateAndTime",
		"设备时间、时区、时间同步状态", func(c context.Context, it *DeviceInfoItem) {
			v, err := svc.FixedGetSystemDateAndTime(c)
			if err != nil {
				it.Error = briefInfoError(err)
				return
			}
			it.OK = true
			tz := ""
			if v.TimeZone != nil {
				tz = v.TimeZone.TZ
			}
			it.Rows = []DeviceInfoRow{
				{"时间同步方式", string(v.DateTimeType)},
				{"夏令时", boolCN(v.DaylightSavings)},
				{"时区 (POSIX TZ)", tz},
				{"UTC 时间", dateTimeText(v.UTCDateTime)},
				{"本地时间", dateTimeText(v.LocalDateTime)},
			}
		}))
	system.Items = append(system.Items, infoCall(ctx, "GetSystemLog",
		"系统日志（设备支持时）", func(c context.Context, it *DeviceInfoItem) {
			v, err := svc.GetSystemLog(c, device.SystemLogTypeSystem)
			if err != nil {
				it.Error = briefInfoError(err)
				return
			}
			it.OK = true
			if v.String != "" {
				it.Text, it.Truncated = clipInfoText(v.String)
			} else if v.Binary != nil {
				it.Text = "设备返回二进制日志（ContentType=" + v.Binary.ContentType + "），暂不展示内容"
			} else {
				it.Text = "设备未返回日志内容"
			}
		}))
	system.Items = append(system.Items, infoCall(ctx, "GetSystemSupportInformation",
		"系统支持信息（设备支持时）", func(c context.Context, it *DeviceInfoItem) {
			v, err := svc.GetSystemSupportInformation(c)
			if err != nil {
				it.Error = briefInfoError(err)
				return
			}
			it.OK = true
			if v.String != "" {
				it.Text, it.Truncated = clipInfoText(v.String)
			} else if v.Binary != nil {
				it.Text = "设备返回二进制支持信息（ContentType=" + v.Binary.ContentType + "），暂不展示内容"
			} else {
				it.Text = "设备未返回支持信息内容"
			}
		}))

	// ---- 网络信息 ----
	network := DeviceInfoSection{Key: "network", Title: "网络信息", Items: []DeviceInfoItem{}}
	network.Items = append(network.Items, infoCall(ctx, "GetNetworkInterfaces",
		"网卡、MAC、IP、子网掩码", func(c context.Context, it *DeviceInfoItem) {
			v, err := svc.GetNetworkInterfaces(c)
			if err != nil {
				it.Error = briefInfoError(err)
				return
			}
			it.OK = true
			it.Columns = []string{"Token", "名称", "启用", "MAC", "MTU", "IPv4 地址", "子网掩码", "DHCP", "IPv6 地址"}
			it.Records = [][]string{}
			for _, n := range v {
				if n == nil {
					continue
				}
				ipv4, mask, dhcp, ipv6 := "", "", "", ""
				if n.IPv4 != nil {
					dhcp = boolCN(n.IPv4.Config.DHCP)
					ips, masks := []string{}, []string{}
					for _, a := range n.IPv4.Config.Manual {
						ips = append(ips, a.Address)
						if a.Netmask != "" {
							masks = append(masks, a.Netmask)
						} else if a.PrefixLength > 0 {
							masks = append(masks, fmt.Sprintf("/%d", a.PrefixLength))
						}
					}
					ipv4, mask = strings.Join(ips, ", "), strings.Join(masks, ", ")
				}
				if n.IPv6 != nil {
					ips := []string{}
					for _, a := range n.IPv6.Config.Manual {
						if a.Address != "" {
							ips = append(ips, a.Address)
						}
					}
					ipv6 = strings.Join(ips, ", ")
				}
				it.Records = append(it.Records, []string{
					n.Token, n.Info.Name, boolCN(n.Enabled), n.Info.HwAddress,
					fmt.Sprint(n.Info.MTU), ipv4, mask, dhcp, ipv6,
				})
			}
		}))
	network.Items = append(network.Items, infoCall(ctx, "GetNetworkProtocols",
		"HTTP、HTTPS、RTSP 等协议及端口", func(c context.Context, it *DeviceInfoItem) {
			v, err := svc.GetNetworkProtocols(c)
			if err != nil {
				it.Error = briefInfoError(err)
				return
			}
			it.OK = true
			it.Columns = []string{"协议", "启用", "端口"}
			it.Records = [][]string{}
			for _, p := range v {
				if p == nil {
					continue
				}
				ports := make([]string, 0, len(p.Port))
				for _, n := range p.Port {
					ports = append(ports, fmt.Sprint(n))
				}
				it.Records = append(it.Records, []string{string(p.Name),
					boolCN(p.Enabled), strings.Join(ports, ", ")})
			}
		}))
	network.Items = append(network.Items, infoCall(ctx, "GetDNS",
		"DNS 配置", func(c context.Context, it *DeviceInfoItem) {
			v, err := svc.GetDNS(c)
			if err != nil {
				it.Error = briefInfoError(err)
				return
			}
			it.OK = true
			it.Rows = []DeviceInfoRow{
				{"FromDHCP", boolCN(v.FromDHCP)},
				{"SearchDomain", strings.Join(v.SearchDomain, ", ")},
				{"DNS（DHCP）", joinIPAddresses(v.DNSFromDHCP)},
				{"DNS（手动）", joinIPAddresses(v.DNSManual)},
			}
		}))
	network.Items = append(network.Items, infoCall(ctx, "GetNTP",
		"NTP 配置", func(c context.Context, it *DeviceInfoItem) {
			v, err := svc.GetNTP(c)
			if err != nil {
				it.Error = briefInfoError(err)
				return
			}
			it.OK = true
			it.Rows = []DeviceInfoRow{
				{"FromDHCP", boolCN(v.FromDHCP)},
				{"NTP（DHCP）", joinNetworkHosts(v.NTPFromDHCP)},
				{"NTP（手动）", joinNetworkHosts(v.NTPManual)},
			}
		}))
	network.Items = append(network.Items, infoCall(ctx, "GetHostname",
		"主机名", func(c context.Context, it *DeviceInfoItem) {
			v, err := svc.GetHostname(c)
			if err != nil {
				it.Error = briefInfoError(err)
				return
			}
			it.OK = true
			it.Rows = []DeviceInfoRow{
				{"FromDHCP", boolCN(v.FromDHCP)},
				{"主机名", v.Name},
			}
		}))

	// ---- 分析配置 ----
	// ONVIF Profile M：ConfigurationToken 传空串取默认分析配置（两台 TP-Link
	// 实测可用）。GetSupported* 返回空描述列表属于设备实现差异，不是调用失败。
	analytic := DeviceInfoSection{Key: "analytic", Title: "分析配置", Items: []DeviceInfoItem{}}
	if !serviceNamespaces[analytics.Namespace] {
		res.Warnings = append(res.Warnings,
			"GetServices 未声明 "+analytics.Namespace+"，该设备可能不支持分析配置类操作")
	}
	analytic.Items = append(analytic.Items, infoCall(ctx, "GetServiceCapabilities [Analytics]",
		"分析服务能力：规则与分析模块支持情况", func(c context.Context, it *DeviceInfoItem) {
			v, err := cli.Analytics().GetServiceCapabilities(c)
			if err != nil {
				it.Error = briefInfoError(err)
				return
			}
			it.OK = true
			it.Rows = []DeviceInfoRow{
				{"支持规则（RuleSupport）", boolCN(v.RuleSupport)},
				{"支持分析模块（AnalyticsModuleSupport）", boolCN(v.AnalyticsModuleSupport)},
			}
		}))
	analytic.Items = append(analytic.Items, infoCall(ctx, "GetSupportedAnalyticsModules",
		"支持的分析模块类型（默认配置）", func(c context.Context, it *DeviceInfoItem) {
			v, err := cli.Analytics().GetSupportedAnalyticsModules(c, "")
			if err != nil {
				it.Error = briefInfoError(err)
				return
			}
			it.OK = true
			it.Rows, it.Columns, it.Records = supportedInfo(v)
		}))
	analytic.Items = append(analytic.Items, infoCall(ctx, "GetAnalyticsModules",
		"当前配置的分析模块（默认配置）", func(c context.Context, it *DeviceInfoItem) {
			v, err := cli.Analytics().GetAnalyticsModules(c, "")
			if err != nil {
				it.Error = briefInfoError(err)
				return
			}
			it.OK = true
			it.Columns, it.Records = configInfo(v)
		}))
	analytic.Items = append(analytic.Items, infoCall(ctx, "GetSupportedRules",
		"支持的规则类型（默认配置）", func(c context.Context, it *DeviceInfoItem) {
			v, err := cli.Analytics().GetSupportedRules(c, "")
			if err != nil {
				it.Error = briefInfoError(err)
				return
			}
			it.OK = true
			it.Rows, it.Columns, it.Records = supportedInfo(v)
		}))
	analytic.Items = append(analytic.Items, infoCall(ctx, "GetRules",
		"当前配置的分析规则（默认配置）", func(c context.Context, it *DeviceInfoItem) {
			v, err := cli.Analytics().GetRules(c, "")
			if err != nil {
				it.Error = briefInfoError(err)
				return
			}
			it.OK = true
			it.Columns, it.Records = configInfo(v)
		}))

	res.Sections = append(res.Sections, basic, system, network, analytic)
	res.ElapsedMs = time.Since(start).Milliseconds()
	ok, failed := 0, 0
	for _, sec := range res.Sections {
		for _, it := range sec.Items {
			if it.OK {
				ok++
			} else {
				failed++
			}
		}
	}
	logger.Info("设备信息采集完成", "device", dev.Name, "ok", ok, "failed", failed,
		"elapsed_ms", res.ElapsedMs)
	return res
}
