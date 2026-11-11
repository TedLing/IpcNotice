package dispatch

import (
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"IpcNotice/internal/config"
	"IpcNotice/internal/store"
)

// ---- 测试工具 ----

func newSustainedDispatcher() *Dispatcher {
	d := &Dispatcher{
		queue:    make(chan dispatchItem, 4096),
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		seen:     make(map[string]dedupState),
		smStates: make(map[string]*eventState),
		smStop:   make(chan struct{}),
		smNow:    time.Now,
	}
	d.webhookClient.Store(newWebhookClient(1))
	d.routes.Store(EmptyRouteTable())
	return d
}

func sustainedTable(device string, timeoutSec int, notifyStart, notifyEnd bool) *RouteTable {
	rt := EmptyRouteTable()
	rt.ByCamera[device] = []Route{{
		Topic:             "RuleEngine/CellMotionDetector/Motion",
		Channels:          []store.WebhookChannel{{ID: 1, Name: "ch1", Enabled: true}},
		Mode:              "sustained",
		InactivityTimeout: timeoutSec,
		NotifyStart:       notifyStart,
		NotifyEnd:         notifyEnd,
	}}
	return rt
}

func sustainedSettingsFor(rt *RouteTable, device string) sustainedSettings {
	s, ok := rt.sustainedFor(device, "tns1:RuleEngine/CellMotionDetector/Motion")
	if !ok {
		panic("sustained route not found")
	}
	return s
}

func sustainedEvent(device string, data map[string]string) Event {
	return Event{Device: device, Topic: "tns1:RuleEngine/CellMotionDetector/Motion",
		Operation: "Changed", Data: data,
		Source: map[string]string{"Rule": "MyMotionDetectorRule"}, Transport: "push"}
}

func feed(d *Dispatcher, evt Event, s sustainedSettings) {
	dev := &config.DeviceConfig{Name: evt.Device}
	d.processSustained(dev, evt, s)
}

// drain 取出队列里的全部事件（raw 在前，lifecycle 在后）。
func drain(d *Dispatcher) []dispatchItem {
	out := []dispatchItem{}
	for {
		select {
		case it := <-d.queue:
			out = append(out, it)
		default:
			return out
		}
	}
}

func lifecycleEvents(items []dispatchItem) []Event {
	out := []Event{}
	for _, it := range items {
		if it.route != nil && it.route.push && it.evt.EventPhase != "" {
			out = append(out, it.evt)
		}
	}
	return out
}

func phaseOf(events []Event, phase string) []Event {
	out := []Event{}
	for _, e := range events {
		if e.EventPhase == phase {
			out = append(out, e)
		}
	}
	return out
}

// ---- 场景测试 ----

func TestSustainedFirstActivityProducesOneStarted(t *testing.T) {
	d := newSustainedDispatcher()
	rt := sustainedTable("cam", 30, true, true)
	feed(d, sustainedEvent("cam", map[string]string{"IsMotion": "true"}),
		sustainedSettingsFor(rt, "cam"))

	started := phaseOf(lifecycleEvents(drain(d)), PhaseStarted)
	if len(started) != 1 {
		t.Fatalf("首次活动应产生且仅产生 1 条 started，实际 %d", len(started))
	}
	if started[0].StateKey == "" || started[0].EventCount != 1 {
		t.Errorf("started 缺少 state_key/event_count: %+v", started[0])
	}
	if started[0].StartedAt == "" {
		t.Errorf("started 缺少 started_at")
	}
}

func TestSustainedRepeatDoesNotRestart(t *testing.T) {
	d := newSustainedDispatcher()
	rt := sustainedTable("cam", 30, true, true)
	s := sustainedSettingsFor(rt, "cam")
	on := sustainedEvent("cam", map[string]string{"IsMotion": "true"})
	for i := 0; i < 10; i++ {
		feed(d, on, s)
	}
	events := lifecycleEvents(drain(d))
	if got := len(phaseOf(events, PhaseStarted)); got != 1 {
		t.Fatalf("连续重复活动只应有 1 条 started，实际 %d", got)
	}
	if got := len(phaseOf(events, PhaseEnded)); got != 0 {
		t.Fatalf("活动期间不应有 ended，实际 %d", got)
	}
	d.smMu.Lock()
	count := d.smStates[stateKey("cam", on.Topic, on.Source)].eventCount
	d.smMu.Unlock()
	if count != 10 {
		t.Errorf("event_count 应为 10，实际 %d", count)
	}
}

func TestSustainedExplicitFalseEndsImmediately(t *testing.T) {
	d := newSustainedDispatcher()
	rt := sustainedTable("cam", 30, true, true)
	s := sustainedSettingsFor(rt, "cam")
	feed(d, sustainedEvent("cam", map[string]string{"IsMotion": "true"}), s)
	feed(d, sustainedEvent("cam", map[string]string{"IsMotion": "false"}), s)

	ended := phaseOf(lifecycleEvents(drain(d)), PhaseEnded)
	if len(ended) != 1 {
		t.Fatalf("明确 false 应立即产生 1 条 ended，实际 %d", len(ended))
	}
	if ended[0].EndReason != EndExplicit {
		t.Errorf("end_reason 应为 EXPLICIT_STOP，实际 %q", ended[0].EndReason)
	}
	if ended[0].EndedAt == "" || ended[0].StartedAt == "" {
		t.Errorf("ended 缺少时间字段: %+v", ended[0])
	}
}

func TestSustainedIdleFalseIgnored(t *testing.T) {
	d := newSustainedDispatcher()
	rt := sustainedTable("cam", 30, true, true)
	feed(d, sustainedEvent("cam", map[string]string{"IsMotion": "false"}),
		sustainedSettingsFor(rt, "cam"))
	events := lifecycleEvents(drain(d))
	if len(events) != 0 {
		t.Fatalf("IDLE 状态收到明确结束应忽略，实际产生了 %+v", events)
	}
}

func TestSustainedInactivityTimeoutEnds(t *testing.T) {
	d := newSustainedDispatcher()
	rt := sustainedTable("cam", 30, true, true)
	s := sustainedSettingsFor(rt, "cam")
	clock := time.Now()
	d.smNow = func() time.Time { return clock }

	feed(d, sustainedEvent("cam", map[string]string{"IsMotion": "true"}), s)
	drain(d)

	// 未到超时：不产生 ended
	clock = clock.Add(29 * time.Second)
	d.scanInactivity()
	if n := len(phaseOf(lifecycleEvents(drain(d)), PhaseEnded)); n != 0 {
		t.Fatalf("29 秒时不应产生 ended，实际 %d", n)
	}

	// 超过 30 秒静默 → ended(INACTIVITY_TIMEOUT)
	clock = clock.Add(2 * time.Second)
	d.scanInactivity()
	ended := phaseOf(lifecycleEvents(drain(d)), PhaseEnded)
	if len(ended) != 1 {
		t.Fatalf("静默超时应产生 1 条 ended，实际 %d", len(ended))
	}
	if ended[0].EndReason != EndInactivity {
		t.Errorf("end_reason 应为 INACTIVITY_TIMEOUT，实际 %q", ended[0].EndReason)
	}
	if ended[0].DurationSeconds < 30 {
		t.Errorf("duration_seconds 应 >= 30，实际 %d", ended[0].DurationSeconds)
	}
}

func TestSustainedUnhealthyDeviceNotEnded(t *testing.T) {
	d := newSustainedDispatcher()
	rt := sustainedTable("cam", 30, true, true)
	s := sustainedSettingsFor(rt, "cam")
	clock := time.Now()
	d.smNow = func() time.Time { return clock }
	d.deviceHealth = func(string) bool { return false } // 链路异常

	feed(d, sustainedEvent("cam", map[string]string{"IsMotion": "true"}), s)
	drain(d)
	clock = clock.Add(10 * time.Minute)
	d.scanInactivity()
	if n := len(phaseOf(lifecycleEvents(drain(d)), PhaseEnded)); n != 0 {
		t.Fatalf("链路异常时不应误报结束，实际 %d 条", n)
	}

	// 链路恢复后超时扫描才允许结束
	d.deviceHealth = func(string) bool { return true }
	d.scanInactivity()
	if n := len(phaseOf(lifecycleEvents(drain(d)), PhaseEnded)); n != 1 {
		t.Fatalf("链路恢复后应产生 ended，实际 %d 条", n)
	}
}

func TestSustainedNewRoundAfterEnd(t *testing.T) {
	d := newSustainedDispatcher()
	rt := sustainedTable("cam", 30, true, true)
	s := sustainedSettingsFor(rt, "cam")
	feed(d, sustainedEvent("cam", map[string]string{"IsMotion": "true"}), s)
	feed(d, sustainedEvent("cam", map[string]string{"IsMotion": "false"}), s)
	feed(d, sustainedEvent("cam", map[string]string{"IsMotion": "true"}), s)

	started := phaseOf(lifecycleEvents(drain(d)), PhaseStarted)
	if len(started) != 2 {
		t.Fatalf("结束后再次活动应产生第 2 条 started，实际 %d", len(started))
	}
}

func TestSustainedDeletedOperationIgnored(t *testing.T) {
	d := newSustainedDispatcher()
	rt := sustainedTable("cam", 30, true, true)
	s := sustainedSettingsFor(rt, "cam")
	feed(d, sustainedEvent("cam", map[string]string{"IsMotion": "true"}), s)
	del := sustainedEvent("cam", nil)
	del.Operation = "Deleted"
	feed(d, del, s) // Deleted 不视为结束
	feed(d, sustainedEvent("cam", map[string]string{"IsMotion": "false"}), s)

	events := lifecycleEvents(drain(d))
	if got := len(phaseOf(events, PhaseEnded)); got != 1 {
		t.Fatalf("Deleted 不应触发结束（结束后仅 1 条 ended），实际 %d", got)
	}
}

func TestSustainedConcurrencyNoDuplicateTransitions(t *testing.T) {
	d := newSustainedDispatcher()
	rt := sustainedTable("cam", 30, true, true)
	s := sustainedSettingsFor(rt, "cam")
	on := sustainedEvent("cam", map[string]string{"IsMotion": "true"})

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			feed(d, on, s)
		}()
	}
	wg.Wait()

	events := lifecycleEvents(drain(d))
	if got := len(phaseOf(events, PhaseStarted)); got != 1 {
		t.Fatalf("并发活动只应有 1 条 started，实际 %d", got)
	}
	if got := len(phaseOf(events, PhaseEnded)); got != 0 {
		t.Fatalf("并发活动不应产生 ended，实际 %d", got)
	}
}

func TestSustainedScannerAndEventRace(t *testing.T) {
	d := newSustainedDispatcher()
	rt := sustainedTable("cam", 30, true, true)
	s := sustainedSettingsFor(rt, "cam")
	clock := time.Now()
	d.smNow = func() time.Time { return clock }

	feed(d, sustainedEvent("cam", map[string]string{"IsMotion": "true"}), s)
	drain(d)
	// 事件恰好在超时临界点到达：扫描与新事件并发，不允许误判结束
	clock = clock.Add(29 * time.Second)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); d.scanInactivity() }()
	go func() {
		defer wg.Done()
		feed(d, sustainedEvent("cam", map[string]string{"IsMotion": "true"}), s)
	}()
	wg.Wait()
	events := lifecycleEvents(drain(d))
	if got := len(phaseOf(events, PhaseEnded)); got != 0 {
		t.Fatalf("临界并发不应误判结束，实际 %d 条 ended", got)
	}
	// 活动已刷新 last_seen，再扫描（即使时钟超过 30s）也只按新 last_seen 判断
	clock = clock.Add(2 * time.Second)
	d.scanInactivity()
	if got := len(phaseOf(lifecycleEvents(drain(d)), PhaseEnded)); got != 0 {
		t.Fatalf("刷新后未超时不应产生 ended，实际 %d 条", got)
	}
}

func TestSustainedStateIsolation(t *testing.T) {
	d := newSustainedDispatcher()
	rt := sustainedTable("cam1", 30, true, true)
	rt.ByCamera["cam2"] = rt.ByCamera["cam1"]
	s := sustainedSettingsFor(rt, "cam1")

	// 不同设备是不同状态
	feed(d, sustainedEvent("cam1", map[string]string{"IsMotion": "true"}), s)
	feed(d, sustainedEvent("cam2", map[string]string{"IsMotion": "true"}), s)
	// 不同 Source（Rule 实例）是不同状态
	alt := sustainedEvent("cam1", map[string]string{"IsMotion": "true"})
	alt.Source = map[string]string{"Rule": "OtherRule"}
	feed(d, alt, s)

	d.smMu.Lock()
	n := len(d.smStates)
	d.smMu.Unlock()
	if n != 3 {
		t.Fatalf("设备/来源不同应各自维护独立状态，实际 %d 个状态键", n)
	}
	// cam1/R1 的重复活动不因其他实例的存在而重复 started
	feed(d, sustainedEvent("cam1", map[string]string{"IsMotion": "true"}), s)
	if got := len(phaseOf(lifecycleEvents(drain(d)), PhaseStarted)); got != 3 {
		t.Fatalf("3 个独立实例应共 3 条 started，实际 %d", got)
	}
}

func TestSustainedOneHourActivityOneRound(t *testing.T) {
	d := newSustainedDispatcher()
	rt := sustainedTable("cam", 30, true, true)
	s := sustainedSettingsFor(rt, "cam")
	clock := time.Now()
	d.smNow = func() time.Time { return clock }

	on := sustainedEvent("cam", map[string]string{"IsMotion": "true"})
	for i := 0; i < 3600; i++ { // 模拟 1 小时：每秒一条活动事件
		clock = clock.Add(time.Second)
		feed(d, on, s)
	}
	clock = clock.Add(time.Second)
	feed(d, sustainedEvent("cam", map[string]string{"IsMotion": "false"}), s)

	events := lifecycleEvents(drain(d))
	started, ended := phaseOf(events, PhaseStarted), phaseOf(events, PhaseEnded)
	if len(started) != 1 || len(ended) != 1 {
		t.Fatalf("一小时持续活动应只产生 1 started + 1 ended，实际 %d + %d",
			len(started), len(ended))
	}
	if ended[0].EndReason != EndExplicit || ended[0].EventCount != 3600 {
		// event_count 统计活动事件数，结束信号（false）本身不计入
		t.Errorf("ended 元数据不符: reason=%q count=%d", ended[0].EndReason, ended[0].EventCount)
	}
	if ended[0].DurationSeconds < 3595 || ended[0].DurationSeconds > 3610 {
		t.Errorf("duration_seconds 应约 3600，实际 %d", ended[0].DurationSeconds)
	}
}

func TestSustainedRestartNoFakeEnd(t *testing.T) {
	d := newSustainedDispatcher()
	rt := sustainedTable("cam", 30, true, true)
	s := sustainedSettingsFor(rt, "cam")
	clock := time.Now()
	d.smNow = func() time.Time { return clock }
	feed(d, sustainedEvent("cam", map[string]string{"IsMotion": "true"}), s)
	drain(d)

	// 模拟服务重启：新进程内存状态为空，即使停机很久也不得伪造 ended。
	restarted := newSustainedDispatcher()
	restarted.smNow = func() time.Time { return clock.Add(time.Hour) }
	restarted.scanInactivity()
	if n := len(lifecycleEvents(drain(restarted))); n != 0 {
		t.Fatalf("重启后不得依据停机静默伪造结束通知，实际 %d 条", n)
	}
	// 新事件到达时重新确认（新一轮 started）
	feed(restarted, sustainedEvent("cam", map[string]string{"IsMotion": "true"}),
		sustainedSettingsFor(rt, "cam"))
	events := lifecycleEvents(drain(restarted))
	if got := len(phaseOf(events, PhaseStarted)); got != 1 {
		t.Fatalf("重启后新事件应重新确认（产生 1 条 started），实际 %d", got)
	}
}

func TestSustainedNotifyFlagsOff(t *testing.T) {
	d := newSustainedDispatcher()
	rt := sustainedTable("cam", 30, false, false)
	s := sustainedSettingsFor(rt, "cam")
	feed(d, sustainedEvent("cam", map[string]string{"IsMotion": "true"}), s)
	feed(d, sustainedEvent("cam", map[string]string{"IsMotion": "false"}), s)
	items := drain(d)
	if got := len(lifecycleEvents(items)); got != 0 {
		t.Fatalf("notify_start/notify_end 关闭时不应产生 webhook 通知，实际 %d 条", got)
	}
	// 状态机仍应正确转换（无推送但落库的 lifecycle 事件存在）
	found := 0
	for _, it := range items {
		if it.evt.EventPhase != "" {
			found++
		}
	}
	if found != 2 {
		t.Errorf("started/ended 生命周期记录仍应产生（仅不推 webhook），实际 %d 条", found)
	}
}

func TestSustainedRawEventsAlwaysLogged(t *testing.T) {
	d := newSustainedDispatcher()
	rt := sustainedTable("cam", 30, true, true)
	s := sustainedSettingsFor(rt, "cam")
	on := sustainedEvent("cam", map[string]string{"IsMotion": "true"})
	for i := 0; i < 5; i++ {
		feed(d, on, s)
	}
	items := drain(d)
	raw := 0
	for _, it := range items {
		if it.evt.EventPhase == "" && it.route != nil && !it.route.push {
			raw++ // 原始事件：仅落库不推送
		}
	}
	if raw != 5 {
		t.Fatalf("原始事件应全部保留落库（不因持续去重丢弃），实际 %d/%d", raw, len(items))
	}
}

func TestInstantModeKeepsCooldownBehavior(t *testing.T) {
	d := newSustainedDispatcher()
	d.dedupTTL = 60 * time.Second
	rt := EmptyRouteTable()
	rt.ByCamera["cam"] = []Route{{Topic: "RuleEngine/CellMotionDetector/Motion",
		Mode: "instant"}} // instant 路由：不触发 sustained
	d.routes.Store(rt)

	if _, ok := rt.sustainedFor("cam", "tns1:RuleEngine/CellMotionDetector/Motion"); ok {
		t.Fatal("instant 路由不应匹配 sustained")
	}
	evt := Event{Device: "cam", Topic: "tns1:RuleEngine/CellMotionDetector/Motion",
		Operation: "Changed", Data: map[string]string{"IsMotion": "true"},
		Source: map[string]string{"Rule": "R"}}
	if d.isDuplicate(evt) {
		t.Fatal("instant 首次不应判重")
	}
	// 状态抖动（false）在冷却窗口内同样被抑制（保持现有冷却语义）
	off := evt
	off.Data = map[string]string{"IsMotion": "false"}
	if !d.isDuplicate(off) {
		t.Fatal("instant 冷却窗口内的状态变化应被抑制（保持现有行为）")
	}
}
