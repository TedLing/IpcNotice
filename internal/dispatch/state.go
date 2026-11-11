// state.go 实现 sustained 模式的持续事件状态管理（Event State Manager）。
//
// 状态机（每个事件实例独立）：
//
//	IDLE + 活动事件        → ACTIVE，产生 started 通知
//	ACTIVE + 活动事件      → 刷新 last_seen_at / event_count，不产生通知
//	ACTIVE + 明确结束事件  → IDLE，产生 ended（EXPLICIT_STOP）
//	IDLE + 明确结束事件    → 忽略
//	ACTIVE + 静默超时      → IDLE，产生 ended（INACTIVITY_TIMEOUT，且链路健康时才判定）
//	活动结束后再次活动     → 新一轮 started
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
	lastSeen   time.Time
	eventCount int
	lastEvent  Event // 最近一条原始事件，用于构造 ended 通知的基础字段
	settings   sustainedSettings
}

// stateKey 由 设备 + 归一化主题 + 来源（含 Rule 实例）组成。
// 刻意不含事件时间戳、随机值或动态 Data 值。
func stateKey(device, topic string, source map[string]string) string {
	return strings.Join([]string{device, util.NormalizeTopic(topic), util.JSONDump(source)}, "|")
}

// classifyActivity 判断事件是否为「活动 / 明确结束 / 忽略」。
// 优先级：PropertyOperation=Deleted 忽略（不视为结束）→ Motion 结构化字段 →
// Data 中 Is* 布尔键（IsMotion/IsPeople/IsVehicle...）。无布尔信号的事件
// 视为活动但不明确（只能靠静默超时结束），不凭主题名猜测。
func classifyActivity(evt Event) (activity, explicit, ignore bool) {
	if strings.EqualFold(evt.Operation, "Deleted") {
		return false, false, true
	}
	if evt.Motion != nil {
		if evt.Motion.Active {
			return true, true, false
		}
		return false, true, false
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
		return true, true, false
	}
	if falses > 0 {
		return false, true, false
	}
	return true, false, false
}

// processSustained 把原始事件交给状态机：原始事件照常入队落库（不推 webhook），
// 状态转换产生 started/ended 生命周期通知（绕过冷却去重，走 sustained 通道）。
func (d *Dispatcher) processSustained(dev *config.DeviceConfig, raw Event, s sustainedSettings) {
	// 原始事件永远落库（保留原始事件日志，不因持续去重而丢弃）。
	d.enqueueItem(dispatchItem{evt: raw, route: &routeDirect{
		channels: s.channels, names: s.names, matched: true, push: false,
	}}, dev.Name, raw.Topic)

	activity, explicit, ignore := classifyActivity(raw)
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

	var lifecycle Event
	pushLifecycle := false
	// 快照：锁内复制通道/名称等设置，锁外只使用局部快照。
	// st.settings 可能被其他事件协程更新（携带更具体规则的活动事件），
	// 解锁后再读 st 即为数据竞争；started 分支直接用入参 s（值拷贝，安全）。
	var channels []store.WebhookChannel
	var names string
	switch {
	case !st.active && !activity:
		// IDLE + 明确结束 → 忽略
		d.smMu.Unlock()
		return
	case st.active && !activity:
		// ACTIVE + 明确结束 → ended(EXPLICIT_STOP)
		endedAt := now
		lifecycle = d.buildLifecycleEvent(st, raw, PhaseEnded, EndExplicit, endedAt)
		st.active = false
		pushLifecycle = st.settings.notifyEnd
		channels, names = st.settings.channels, st.settings.names
		d.persistState(st, PhaseEnded, EndExplicit, endedAt)
		d.smMu.Unlock()
	case !st.active:
		// IDLE + 活动 → 新一轮 ACTIVE + started
		st.active = true
		st.startedAt = now
		st.lastSeen = now
		st.eventCount = 1
		st.settings = s
		st.lastEvent = raw
		lifecycle = d.buildLifecycleEvent(st, raw, PhaseStarted, "", now)
		pushLifecycle = s.notifyStart
		channels, names = s.channels, s.names
		d.persistState(st, PhaseStarted, "", now)
		d.smMu.Unlock()
	default:
		// ACTIVE + 活动 → 仅刷新，不产生通知；重复事件优先内存，不写库
		st.lastSeen = now
		st.eventCount++
		st.lastEvent = raw
		if explicit && s.timeout != st.settings.timeout {
			st.settings = s // 活动事件携带更具体的规则时同步设置
		}
		d.smMu.Unlock()
		return
	}

	// 锁外入队：lifecycle 通知绕过冷却去重，直接走 sustained 通道。
	// 只使用锁内复制的局部快照，不读 st。
	d.enqueueItem(dispatchItem{evt: lifecycle, route: &routeDirect{
		channels: channels, names: names,
		matched: true, push: pushLifecycle,
	}}, dev.Name, lifecycle.Topic)
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
		out.DurationSeconds = int64(at.Sub(st.startedAt).Seconds())
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

// scanInactivity 把静默超时的 ACTIVE 状态判定为结束（INACTIVITY_TIMEOUT）。
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
		evt := d.buildLifecycleEvent(st, st.lastEvent, PhaseEnded, EndInactivity, now)
		d.persistState(st, PhaseEnded, EndInactivity, now)
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
