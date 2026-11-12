// state.go 实现 sustained 模式的持续事件状态管理（Event State Manager）。
//
// 状态机（每个事件实例独立）：
//
//	IDLE + 活动事件              → ACTIVE，产生 started 通知
//	ACTIVE + 活动事件            → 刷新 last_seen / event_count，取消结束候选，不通知
//	ACTIVE + 明确停止信号        → 仅标记结束候选（不刷新 last_seen），不通知
//	ACTIVE(候选) + 活动事件      → 取消候选，继续本轮，不产生新的 started
//	ACTIVE + 静默超时            → IDLE，产生 ended（且链路健康时才判定）
//	IDLE + 明确停止 / Deleted    → 忽略
//
// 关键固件约束：TP-Link 每 1~2 秒上报一次 Is* 状态，一次持续移动会 true/false
// 反复翻转（实测活动期内 false 段最长 16.5 秒）。因此「明确停止信号」绝不能
// 直接结束本轮，否则一对 started/ended 会被放大成几十对推送。改为：停止信号
// 只作为结束候选，由 inactivity_timeout（默认 30 秒，从最后一次活动信号起算）
// 统一确认；窗口内再次活动即取消候选。end_reason=EXPLICIT_STOP 表示「收到过
// 明确停止信号且经静默确认」，INACTIVITY_TIMEOUT 表示「无停止信号、纯静默结束」。
//
// 所有状态转换在互斥锁内完成，保证原子性；结束通知绕过冷却去重，独立匹配通道。
package dispatch

import (
	"fmt"
	"strings"
	"time"

	"IpcNotice/internal/config"
	"IpcNotice/internal/store"
	"IpcNotice/internal/util"
)

// 生命周期阶段与结束原因（随 webhook JSON 的 event_phase / end_reason 字段输出）。
const (
	PhaseStarted = "started"
	PhaseEnded   = "ended"

	EndExplicit   = "EXPLICIT_STOP"
	EndInactivity = "INACTIVITY_TIMEOUT"
)

// sustainedSettings 是匹配到的一组 sustained 订阅规则的合并视图：
// 通道取并集，超时与通知开关取最长主题模式（最具体规则）的值。
type sustainedSettings struct {
	cameraID    int64
	channels    []store.WebhookChannel
	names       string
	timeout     time.Duration
	notifyStart bool
	notifyEnd   bool
}

// sustainedFor 返回匹配 device+topic 的 sustained 规则设置；无匹配时 ok=false。
func (rt *RouteTable) sustainedFor(device, topic string) (sustainedSettings, bool) {
	var s sustainedSettings
	if rt == nil {
		return s, false
	}
	best := -1
	seen := map[int64]bool{}
	names := []string{}
	matched := false
	for _, r := range rt.ByCamera[device] {
		if r.Mode != "sustained" || !util.MatchTopic(r.Topic, topic) {
			continue
		}
		matched = true
		for _, ch := range r.Channels {
			if !ch.Enabled || seen[ch.ID] {
				continue
			}
			seen[ch.ID] = true
			s.channels = append(s.channels, ch)
			names = append(names, ch.Name)
		}
		if len(r.Topic) > best {
			best = len(r.Topic)
			s.timeout = time.Duration(r.InactivityTimeout) * time.Second
			s.notifyStart = r.NotifyStart
			s.notifyEnd = r.NotifyEnd
		}
	}
	s.names = strings.Join(names, ", ")
	return s, matched
}

// eventState 是单个事件实例的内存状态（重启后为空，等待新事件重新确认）。
type eventState struct {
	key        string
	device     string
	topic      string
	sourceJSON string
	active     bool
	startedAt  time.Time
	// lastSeen 是最后一次「活动信号」时间，只有活动事件会刷新；明确停止信号
	// （Is*=false / Motion.Active=false）不刷新它，否则设备持续推 false 心跳
	// 会让静默超时永远无法触发。
	lastSeen time.Time
	// stopAt 是活动期内首次明确停止信号的时间（零值表示无结束候选）。
	// 仅用于判定 end_reason，不影响超时计时；窗口内再次活动会清零。
	stopAt     time.Time
	eventCount int
	lastEvent  Event // 最近一条原始事件，用于构造 ended 通知的基础字段
	settings   sustainedSettings
}

// stateKey 由 设备 + 归一化主题 + 来源（含 Rule 实例）组成。
// 刻意不含事件时间戳、随机值或动态 Data 值。
func stateKey(device, topic string, source map[string]string) string {
	return strings.Join([]string{device, util.NormalizeTopic(topic), util.JSONDump(source)}, "|")
}

// classifyActivity 判断事件是「活动信号 / 明确停止信号 / 忽略」。
// 优先级：PropertyOperation=Deleted 忽略（不视为结束）→ Motion 结构化字段 →
// Data 中 Is* 布尔键（IsMotion/IsPeople/IsVehicle...）。无布尔信号的事件
// 视为活动信号但不明确，只能靠结束确认窗口判定结束，不凭主题名猜测。
// 注意：明确停止信号只登记结束候选，不直接结束本轮（见 processSustained）。
func classifyActivity(evt Event) (activity, ignore bool) {
	if strings.EqualFold(evt.Operation, "Deleted") {
		return false, true
	}
	if evt.Motion != nil {
		return evt.Motion.Active, false
	}
	trues, falses := 0, 0
	for k, v := range evt.Data {
		if !strings.HasPrefix(k, "Is") {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "true":
			trues++
		case "false":
			falses++
		}
	}
	if trues > 0 {
		return true, false
	}
	if falses > 0 {
		return false, false
	}
	return true, false
}

// processSustained 把原始事件交给状态机：原始事件照常入队落库（不推 webhook），
// 本函数只在 IDLE→ACTIVE 时产生 started 通知；ended 由 scanInactivity 在结束
// 确认窗口到期后统一产生（见文件头状态机说明）。两者都绕过冷却去重，
// 只走匹配到的 sustained 订阅通道。
func (d *Dispatcher) processSustained(dev *config.DeviceConfig, raw Event, s sustainedSettings) {
	// 原始事件永远落库（保留原始事件日志，不因持续去重而丢弃）。
	d.enqueueItem(dispatchItem{evt: raw, route: &routeDirect{
		channels: s.channels, names: s.names, matched: true, push: false,
	}}, dev.Name, raw.Topic)

	activity, ignore := classifyActivity(raw)
	if ignore {
		return
	}
	now := d.smNow()

	d.smMu.Lock()
	key := stateKey(dev.Name, raw.Topic, raw.Source)
	st := d.smStates[key]
	if st == nil {
		st = &eventState{
			key: key, device: dev.Name, sourceJSON: util.JSONDump(raw.Source),
		}
		d.smStates[key] = st
	}
	st.topic = util.NormalizeTopic(raw.Topic)

	if !st.active {
		if !activity {
			// IDLE + 明确停止信号：不开启新一轮（TP-Link 停后仍会补发数条 false）。
			d.smMu.Unlock()
			return
		}
		// IDLE + 活动 → 新一轮 ACTIVE + started。
		st.active = true
		st.startedAt = now
		st.lastSeen = now
		st.stopAt = time.Time{}
		st.eventCount = 1
		st.settings = s
		st.lastEvent = raw
		lifecycle := d.buildLifecycleEvent(st, raw, PhaseStarted, "", now)
		channels, names, push := s.channels, s.names, s.notifyStart
		d.persistState(st, PhaseStarted, "", now)
		d.smMu.Unlock()
		// 锁外入队：只使用锁内复制的局部快照，不读 st。
		d.enqueueItem(dispatchItem{evt: lifecycle, route: &routeDirect{
			channels: channels, names: names,
			matched: true, push: push,
		}}, dev.Name, lifecycle.Topic)
		return
	}

	// ACTIVE：设置随 Reload 热更新（s 是值拷贝，锁内赋值安全），
	// 保证活动期内修改通道/通知开关/静默超时也能作用于本轮 ended。
	st.settings = s
	if activity {
		// 活动信号：刷新活动计时并取消结束候选，不产生任何通知。
		// 这是折叠 true/false 抖动的关键——抖动后再出现 true 不算新一轮。
		st.lastSeen = now
		st.stopAt = time.Time{}
		st.eventCount++
		st.lastEvent = raw
		d.smMu.Unlock()
		return
	}
	// 明确停止信号：只登记结束候选，不刷新 lastSeen、不结束、不通知。
	// 设备停止后往往还会连发数条 false 心跳；若在这里立即结束并推送，
	// 一次移动侦测就会被放大成几十对 started/ended。
	if st.stopAt.IsZero() {
		st.stopAt = now
	}
	st.lastEvent = raw
	d.smMu.Unlock()
}

// buildLifecycleEvent 以最近原始事件为基础构造生命周期通知信封。
// 必须在 smMu 持有期间调用（读取 st 的字段）。
func (d *Dispatcher) buildLifecycleEvent(st *eventState, raw Event, phase, reason string, at time.Time) Event {
	out := raw // 拷贝基础字段（device/topic/source/data/transport...）
	out.EventPhase = phase
	out.StateKey = st.key
	out.ID = fmt.Sprintf("%d-%06d", time.Now().UnixNano(), eventSeq.Add(1))
	out.StartedAt = st.startedAt.UTC().Format(time.RFC3339Nano)
	out.LastSeenAt = st.lastSeen.UTC().Format(time.RFC3339Nano)
	out.EventCount = st.eventCount
	if phase == PhaseEnded {
		out.EndedAt = at.UTC().Format(time.RFC3339Nano)
		out.EndReason = reason
		// 持续时间按「开始 → 最后一次活动信号」计算，不把结束确认窗口
		// （静默超时）算进事件本身；否则一次 5 秒的移动会显示成 35 秒。
		dur := int64(st.lastSeen.Sub(st.startedAt).Seconds())
		if dur < 0 {
			dur = 0
		}
		out.DurationSeconds = dur
	}
	return out
}

// persistState 把状态转换持久化到 event_states（重复活动事件不写库）。
func (d *Dispatcher) persistState(st *eventState, phase, endReason string, at time.Time) {
	if d.store == nil {
		return
	}
	es := store.EventState{
		StateKey: st.key, Device: st.device, Topic: st.topic,
		Source: st.sourceJSON, Phase: phase, EventCount: st.eventCount,
		StartedAt:  st.startedAt.UTC().Format(time.RFC3339Nano),
		LastSeenAt: st.lastSeen.UTC().Format(time.RFC3339Nano),
		UpdatedAt:  at.UTC().Format(time.RFC3339Nano),
	}
	if phase == PhaseEnded {
		es.EndedAt = at.UTC().Format(time.RFC3339Nano)
		es.EndReason = endReason
	}
	if err := d.store.UpsertEventState(es); err != nil {
		d.log.Warn("持续事件状态落库失败", "state_key", st.key, "err", err)
	}
}

// scanLoop 统一的静默超时扫描：每秒检查一次 ACTIVE 状态。
func (d *Dispatcher) scanLoop() {
	defer d.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-d.smStop:
			return
		case <-ticker.C:
			d.scanInactivity()
		}
	}
}

// scanInactivity 统一的结束确认：每秒检查一次 ACTIVE 状态，只有「最后一次
// 活动信号之后持续 inactivity_timeout 没有任何活动信号」才判定本轮结束。
// 明确停止信号（Is*=false）只登记候选、不参与计时，因此设备 stop 后连发的
// false 心跳不会让本轮无限延长，活动期内的 false 抖动也不会提前结束。
// end_reason：期间收到过明确停止信号 → EXPLICIT_STOP；纯静默 → INACTIVITY_TIMEOUT。
// 设备订阅/接收链路不健康（断网、订阅失效、Worker 异常）时不误判。
func (d *Dispatcher) scanInactivity() {
	now := d.smNow()

	type emission struct {
		evt      Event
		device   string
		channels []store.WebhookChannel
		names    string
		push     bool
	}
	var emissions []emission

	d.smMu.Lock()
	for _, st := range d.smStates {
		if !st.active {
			continue
		}
		timeout := st.settings.timeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		if now.Sub(st.lastSeen) < timeout {
			continue
		}
		if d.deviceHealth != nil && !d.deviceHealth(st.device) {
			continue // 链路异常，不能认定活动结束
		}
		st.active = false
		reason := EndInactivity
		if !st.stopAt.IsZero() {
			// 收到过明确停止信号且经过静默确认：保留原有 end_reason 取值，
			// 便于既有 webhook 消费方区分「设备报停」与「纯静默超时」。
			reason = EndExplicit
		}
		evt := d.buildLifecycleEvent(st, st.lastEvent, PhaseEnded, reason, now)
		d.persistState(st, PhaseEnded, reason, now)
		// 锁内复制需要的快照，解锁后只使用局部值（st 可能被其他协程并发更新）。
		emissions = append(emissions, emission{
			evt: evt, device: st.device,
			channels: st.settings.channels, names: st.settings.names,
			push: st.settings.notifyEnd,
		})
	}
	d.smMu.Unlock()

	for _, em := range emissions {
		d.enqueueItem(dispatchItem{evt: em.evt, route: &routeDirect{
			channels: em.channels, names: em.names,
			matched: true, push: em.push,
		}}, em.device, em.evt.Topic)
	}
}
