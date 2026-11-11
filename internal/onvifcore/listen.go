// Package onvifcore 封装 ONVIF 客户端容错传输、Push 订阅接收端（NotifyServer）
// 与单台摄像头的订阅生命周期管理（CameraWorker）。
package onvifcore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"IpcNotice/internal/config"
	"IpcNotice/internal/store"
	"github.com/mickeyzzc/onvif-go/v2/events"
	"github.com/mickeyzzc/onvif-go/v2/onvif"
)

// WorkerHost 是 CameraWorker 与上层 Manager 之间的解耦接口：worker 只通过它
// 取运行态参数、提交事件、读写数据库与查询回调地址，从而避免 onvifcore 反向
// 依赖 manager 造成的循环引用。Manager 通过实现本接口充当 host。
type WorkerHost interface {
	Context() context.Context
	Logger() *slog.Logger
	Store() *store.Store
	Settings() config.Settings
	ConsumerURL(dev *config.DeviceConfig) string
	Submit(dev *config.DeviceConfig, transport string, msgs []onvif.NotificationMessage)
}

// WorkerStatus 描述一台摄像头当前订阅的运行状态，供「运行态」页面展示。
// 除基本状态外还包含订阅诊断信息：创建次数、最近创建/续约/退订的结果与时间、
// 最近收到回调时间，以及当前已知的设备端订阅管理地址。
type WorkerStatus struct {
	CameraID   int64  `json:"camera_id"`
	Device     string `json:"device"`
	Transport  string `json:"transport"` // push | pull | ""（尚未建立）
	Connected  bool   `json:"connected"`
	LastError  string `json:"last_error"`
	StartedAt  string `json:"started_at"`
	UpdatedAt  string `json:"updated_at"`
	RetryCount int    `json:"retry_count"`

	// ---- 订阅诊断（进程内统计，重启后清零；sub_manager 持久化在 cameras 表）----
	CreateCount  int64  `json:"create_count"`        // 本次运行内订阅创建次数
	LastCreateAt string `json:"last_create_at"`      // 最近一次创建时间
	LastCreateOK bool   `json:"last_create_ok"`      // 最近一次创建是否成功
	LastRenewAt  string `json:"last_renew_at"`       // 最近一次续约时间
	LastRenewOK  bool   `json:"last_renew_ok"`       // 最近一次续约结果
	LastRenewErr string `json:"last_renew_err"`      // 最近一次续约失败原因
	LastUnsubAt  string `json:"last_unsubscribe_at"` // 最近一次退订（含残留清理）时间
	LastUnsubOK  bool   `json:"last_unsubscribe_ok"` // 最近一次退订结果
	SubManager   string `json:"sub_manager"`         // 当前已知的设备端订阅管理地址
	LastNotifyAt string `json:"last_notify_at"`      // 最近收到该设备回调的时间（由 NotifyServer 填充）

	// ---- 设备健康探测（独立于订阅状态：订阅中断但设备可能在线）----
	DeviceOnline bool   `json:"device_online"` // ONVIF 服务可达（连续失败 3 次判定离线）
	LastProbeAt  string `json:"last_probe_at"` // 最近一次探测时间
	ProbeFails   int    `json:"probe_fails"`   // 当前连续探测失败次数
	LastProbeErr string `json:"last_probe_err"`
}

// DeviceHash 用于判断「影响订阅的配置」是否变化，变化时才需重建订阅。
// 注意：设备显示名（dev.Name）不参与哈希——改名只影响展示与路由表的键，
// 回调地址由 token（c<id>）决定，重建订阅反而白白冒僵尸订阅风险；
// 改名时由 Reload 对保留的 worker 原子刷新 dev 指针。
func DeviceHash(dev *config.DeviceConfig, st config.Settings) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%s|%d|%d|%d|%s|%s",
		dev.Endpoint, dev.DeviceUser, dev.DevicePass,
		st.AdvertiseHost, st.SubscriptionSeconds, st.PullTimeoutSeconds,
		st.MessageLimit, st.EventMode, st.NotifyPath)))
	return hex.EncodeToString(sum[:8])
}

// CameraWorker 负责一台设备的订阅生命周期：建立、异常重连、退订。
//
// 并发契约：同一摄像头同一时间只有一个 worker（Manager.Reload 以 id 为键管理，
// 且先 stop() 等待旧 worker 退出再创建新 worker）；retired 标志保证 stop()
// 超时后旧 worker 也不会再发起任何新的订阅创建/续约动作，且停止路径上的
// 清理/退订都有总时间预算（< stopWait），理论上 Stop 不会超时——万一超时，
// Reload 会记录告警：限时收尾可能在新 worker 启动后短暂重叠（仅退订类请求）。
// token（c<id>）只负责「摄像头身份 → 回调路由」；同一摄像头的多个订阅
// （如崩溃残留的旧订阅）可能同时向同一回调 URL 推送，由 NotifyServer 全部
// 接收并交给去重逻辑处理，与订阅实例无关。
type CameraWorker struct {
	host     WorkerHost
	dev      atomic.Pointer[config.DeviceConfig] // 可被 Reload 原子刷新（改名等不重建订阅的变更）
	hash     string
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	cameraID int64

	retired      atomic.Bool // stop() 后置位：不再允许创建/续约订阅
	stopTimedOut atomic.Bool // Stop 等待超时置位：收尾工作尚未结束（Reload 据此知情）

	staleWarnOnce sync.Once // 残留订阅清理失败只告警一次，避免重连风暴刷屏
	startOnce     sync.Once // Start 只允许启动一次 run/healthLoop，防止重复健康检查协程

	// kick 在设备健康探测从离线恢复在线时触发一次，让 run() 提前结束退避等待，
	// 立即尝试重建订阅（容量 1，非阻塞发送，不丢语义——最多合并多次恢复信号）。
	kick chan struct{}

	// 运行时订阅管理地址状态（subMgrMu 保护）：
	// curSubMgr 是当前活跃 Push 订阅的设备端管理地址——订阅成功时更新；
	// staleRefs 是已知待清理的订阅引用（崩溃恢复快照 + 退订失败累积），设备
	// 订阅槽有限且 TTL 实现不规范时，逐个重连重试清理，避免新订阅把唯一保存的
	// 旧地址覆盖掉。数据库仍负责跨进程重启恢复（值为空格分隔的引用列表）。
	//
	// 注意：内存 dev.SubManager 只是 Worker 创建时从数据库读出的静态快照，
	// DB 更新后不会同步——它仅在 Worker 创建时用于初始化 staleRefs 一次，
	// 之后一律以 curSubMgr/staleRefs 为准（清理结果变化后写回 DB），
	// 绝不在每次重连时回读快照，否则已清理成功的旧引用会被反复带回重试。
	subMgrMu  sync.Mutex
	curSubMgr string
	staleRefs []string

	mu     sync.RWMutex
	status WorkerStatus
}

// 健康探测与退避参数。
const (
	probeInterval    = 30 * time.Second  // 每 30 秒探测一次 ONVIF 服务
	probeFailOffline = 3                 // 连续失败 3 次判定设备离线
	probeTimeout     = 10 * time.Second  // 单次探测超时
	backoffInitial   = 3 * time.Second   // 重连退避初值
	backoffMax       = 120 * time.Second // 重连退避上限，恢复成功后重置
	backoffResetAge  = 60 * time.Second  // 订阅存活超过该时长后再次失败，退避重置

	staleCleanBudget = 10 * time.Second // 残留清理总时间预算（须小于 Stop 等待上限）
	staleCleanPerRef = 6 * time.Second  // 单个引用的清理超时（受总预算约束）
	stopWait         = 12 * time.Second // Stop 等待 worker 退出的上限
	renewExitWait    = 3 * time.Second  // 退订前等待续约协程退出的上限（见 runPushLease）
)

func NewCameraWorker(host WorkerHost, dev *config.DeviceConfig, hash string, cameraID int64) *CameraWorker {
	ctx, cancel := context.WithCancel(host.Context())
	w := &CameraWorker{
		host: host, hash: hash, ctx: ctx, cancel: cancel, cameraID: cameraID,
		done: make(chan struct{}),
		kick: make(chan struct{}, 1),
		// 跨进程崩溃恢复：DB 快照只在创建时初始化运行态待清理列表一次，
		// 之后由 curSubMgr/staleRefs 独立管理（见结构体注释）。
		staleRefs: splitSubMgrRefs(dev.SubManager),
		status: WorkerStatus{
			CameraID:     cameraID,
			Device:       dev.Name,
			StartedAt:    time.Now().UTC().Format(time.RFC3339),
			UpdatedAt:    time.Now().UTC().Format(time.RFC3339),
			DeviceOnline: true, // 乐观初始：由探测结果修正
		},
	}
	w.dev.Store(dev)
	return w
}

func (w *CameraWorker) logger() *slog.Logger { return w.host.Logger() }

// Device 返回当前设备配置（Reload 可能已原子刷新为改名后的副本）。
func (w *CameraWorker) Device() *config.DeviceConfig { return w.dev.Load() }

// Hash 返回本 worker 当前配置哈希，供 Manager.Reload 判断是否需要重建订阅。
func (w *CameraWorker) Hash() string { return w.hash }

func (w *CameraWorker) setStatus(transport string, connected bool, errText string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.status.Transport = transport
	w.status.Connected = connected
	w.status.LastError = errText
	w.status.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	if errText != "" {
		w.status.RetryCount++
	}
}

func nowStr() string { return time.Now().UTC().Format(time.RFC3339) }

func (w *CameraWorker) recordRenew(errText string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.status.LastRenewAt = nowStr()
	w.status.LastRenewOK = errText == ""
	w.status.LastRenewErr = errText
}

func (w *CameraWorker) recordUnsub(errText string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.status.LastUnsubAt = nowStr()
	w.status.LastUnsubOK = errText == ""
}

func (w *CameraWorker) recordSubManager(ref string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.status.SubManager = ref
}

// ---- 运行时订阅引用管理 ----

// splitSubMgrRefs 把数据库中的订阅引用列表（空格分隔）拆开。
func splitSubMgrRefs(s string) []string {
	return strings.Fields(s)
}

// joinSubMgrRefs 去重并拼接引用列表（当前活跃订阅在前，空值忽略）。
func joinSubMgrRefs(cur string, stale []string) string {
	seen := map[string]bool{}
	var out []string
	for _, r := range append([]string{cur}, stale...) {
		r = strings.TrimSpace(r)
		if r == "" || seen[r] {
			continue
		}
		seen[r] = true
		out = append(out, r)
	}
	return strings.Join(out, " ")
}

// takeStaleRefs 返回待清理引用的快照（锁内复制）。
func (w *CameraWorker) takeStaleRefs() []string {
	w.subMgrMu.Lock()
	defer w.subMgrMu.Unlock()
	return append([]string(nil), w.staleRefs...)
}

// setCurSubMgr 记录当前活跃 Push 订阅的管理地址（订阅成功时调用）。
func (w *CameraWorker) setCurSubMgr(ref string) {
	w.subMgrMu.Lock()
	w.curSubMgr = ref
	w.subMgrMu.Unlock()
}

// clearCurSubMgr 当前订阅退订成功后清空引用。
func (w *CameraWorker) clearCurSubMgr() {
	w.subMgrMu.Lock()
	w.curSubMgr = ""
	w.subMgrMu.Unlock()
}

// moveCurToStale 退订失败时把当前订阅地址转入待清理列表，随重连重试。
func (w *CameraWorker) moveCurToStale() {
	w.subMgrMu.Lock()
	if w.curSubMgr != "" {
		w.staleRefs = append(w.staleRefs, w.curSubMgr)
		w.curSubMgr = ""
	}
	w.subMgrMu.Unlock()
}

// persistSubMgrState 把「当前订阅 + 待清理引用」整体持久化到数据库。
// 旧版本只存单个地址；新值为空格分隔列表，跨重启恢复时逐个尝试清理。
// stage 标注调用阶段（subscribe/unsubscribe/unsubscribe_fail），便于排查。
// 写库失败只记录错误、不阻断当前订阅运行，但必须可观测——否则新订阅建立
// 成功而持久化失败时，进程崩溃后将丢失该订阅的清理地址（残留占用订阅槽）。
func (w *CameraWorker) persistSubMgrState(stage string) {
	w.subMgrMu.Lock()
	val := joinSubMgrRefs(w.curSubMgr, w.staleRefs)
	dev := w.Device()
	w.subMgrMu.Unlock()
	if err := w.host.Store().SetCameraSubManager(dev.ID, val); err != nil {
		w.logger().Error("订阅管理地址持久化失败（进程重启后无法恢复引用，残留订阅可能占用设备订阅槽）",
			"device", dev.Name, "stage", stage, "err", err)
	}
}

// recordCreate 记录一次订阅创建结果；成功即证明设备可达，同步健康探测状态。
func (w *CameraWorker) recordCreate(at string, ok bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.status.CreateCount++
	w.status.LastCreateAt = at
	w.status.LastCreateOK = ok
	if ok {
		w.status.DeviceOnline = true
		w.status.ProbeFails = 0
		w.status.LastProbeErr = ""
	}
}

// recordProbe 记录一次设备健康探测结果，返回（当前在线, 是否刚从离线恢复）。
func (w *CameraWorker) recordProbe(ok bool, errText string) (online, recovered bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.status.LastProbeAt = nowStr()
	w.status.LastProbeErr = errText
	if ok {
		w.status.ProbeFails = 0
		recovered = !w.status.DeviceOnline
		w.status.DeviceOnline = true
	} else {
		w.status.ProbeFails++
		if w.status.ProbeFails >= probeFailOffline {
			w.status.DeviceOnline = false
		}
	}
	return w.status.DeviceOnline, recovered
}

// probeDevice 做一次轻量 ONVIF 探测（构造客户端 + Initialize 设备信息/对时）。
func (w *CameraWorker) probeDevice() error {
	cli, err := NewDeviceClient(w.Device())
	if err != nil {
		return fmt.Errorf("创建 ONVIF 客户端失败: %w", err)
	}
	pctx, cancel := context.WithTimeout(w.ctx, probeTimeout)
	defer cancel()
	if err := cli.Initialize(pctx); err != nil {
		return fmt.Errorf("ONVIF 初始化失败: %w", err)
	}
	return nil
}

// healthLoop 设备健康探测协程：每 30 秒探测一次 ONVIF 服务，连续失败 3 次
// 判定设备离线；从离线恢复在线时 kick 通知订阅循环提前重试。
// 每个 worker 仅启动一个（由 startOnce 保证），随 ctx 取消退出。
func (w *CameraWorker) healthLoop() {
	ticker := time.NewTicker(probeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
		}
		if w.retired.Load() {
			return
		}
		err := w.probeDevice()
		if err != nil && w.ctx.Err() != nil {
			return // 探测被停止打断，直接退出
		}
		_, recovered := w.recordProbe(err == nil, errTextOf(err))
		switch {
		case err == nil && recovered:
			w.logger().Info("设备健康探测：恢复在线", "device", w.Device().Name)
			select {
			case w.kick <- struct{}{}:
			default:
			}
		case err == nil:
			w.logger().Debug("设备健康探测正常", "device", w.Device().Name)
		default:
			if st := w.Status(); st.ProbeFails == probeFailOffline {
				w.logger().Warn("设备健康探测连续失败，判定设备离线",
					"device", w.Device().Name, "err", err)
			} else {
				w.logger().Debug("设备健康探测失败", "device", w.Device().Name,
					"fails", st.ProbeFails, "err", err)
			}
		}
	}
}

// Status 返回当前订阅运行状态快照。
func (w *CameraWorker) Status() WorkerStatus {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.status
}

// Start 启动 worker 的订阅循环与健康探测协程（各一个，startOnce 保证不重复）。
func (w *CameraWorker) Start() {
	w.startOnce.Do(func() {
		go w.run()
		go w.healthLoop()
	})
}

// RefreshDevice 在改名等不重建订阅的变更后，原子刷新设备配置指针。
func (w *CameraWorker) RefreshDevice(dev *config.DeviceConfig) { w.dev.Store(dev) }

func (w *CameraWorker) run() {
	defer close(w.done)
	backoff := backoffInitial
	for {
		if w.ctx.Err() != nil {
			return
		}
		started := time.Now()
		err := w.serveOnce()
		if w.ctx.Err() != nil {
			return
		}
		// 上一次订阅存活超过阈值（恢复成功运行过一段时间）后再次失败：
		// 视为新故障，退避重置。短时间内的连续失败则继续指数增长。
		if time.Since(started) >= backoffResetAge {
			backoff = backoffInitial
		}
		msg := ""
		if err != nil {
			msg = err.Error()
			w.logger().Warn("设备订阅中断，将自动重连", "device", w.Device().Name, "err", err,
				"backoff", backoff.String())
		}
		w.setStatus("", false, msg)
		// 指数退避等待；设备健康探测从离线恢复在线时提前唤醒，立即重建订阅。
		select {
		case <-w.ctx.Done():
			return
		case <-w.kick:
			w.logger().Info("设备恢复在线，提前重试订阅重建", "device", w.Device().Name)
		case <-time.After(backoff):
		}
		if backoff < backoffMax {
			backoff *= 2
			if backoff > backoffMax {
				backoff = backoffMax
			}
		}
	}
}

func (w *CameraWorker) serveOnce() error {
	if w.retired.Load() {
		return nil // 已被 Reload 停用，不再触碰设备
	}
	dev := w.Device()
	cli, err := NewDeviceClient(dev)
	if err != nil {
		return fmt.Errorf("创建 ONVIF 客户端失败: %w", err)
	}
	initCtx, cancel := context.WithTimeout(w.ctx, 25*time.Second)
	err = cli.Initialize(initCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("设备初始化失败: %w", err)
	}
	if w.retired.Load() {
		return nil
	}

	// 崩溃残留清理：上次运行异常退出时来不及退订，而 TP-Link 固件不遵守订阅 TTL，
	// 残留订阅会持续占用设备订阅槽。建立新订阅前先尽力退订一次。
	// 保守策略：不假定旧订阅已过期，也不因 token 稳定而认为设备端无重复订阅——
	// 退订失败时保留地址重试，并靠下游去重容忍旧订阅的并发推送。
	w.unsubscribeStale(cli)
	if w.retired.Load() {
		return nil
	}

	st := w.host.Settings()
	cfg := st.Event()
	dur := cfg.Duration()
	renewMargin := dur / 3
	if renewMargin < 5*time.Second {
		renewMargin = 5 * time.Second
	}

	// auto 优先 Push；Push 失败（或显式 pull）走 PullPoint。
	if st.EventMode == "auto" || st.EventMode == "push" {
		consumerURL := w.host.ConsumerURL(dev)
		sub, err := cli.Events().Subscribe(w.ctx, consumerURL, dur)
		if err == nil {
			// 记录当前订阅管理地址并持久化（新地址 + 仍未清理的残留引用列表）：
			// 进程崩溃后，下次启动可据此逐个退订残留订阅。
			w.setCurSubMgr(sub.ManagerAddress)
			w.persistSubMgrState("subscribe")
			w.recordCreate(nowStr(), true)
			w.recordSubManager(sub.ManagerAddress)
			w.setStatus("push", true, "")
			w.logger().Info("Push 订阅已建立", "device", dev.Name,
				"consumer_url", consumerURL, "ttl", dur.String(),
				"manager", sub.ManagerAddress)
			w.runPushLease(cli.Events(), sub, dur)
			if w.ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("Push 订阅循环已退出（续订失败或设备重启）")
		}
		w.recordCreate(nowStr(), false)
		w.logger().Warn("Push 订阅失败", "device", dev.Name, "err", err,
			"fallback", st.EventMode == "auto")
		if st.EventMode == "push" {
			return fmt.Errorf("Push 订阅失败: %w", err)
		}
	}
	if w.retired.Load() {
		return nil
	}

	device := dev
	stream, err := cli.Events().SubscribeEvents(w.ctx,
		func(msg onvif.NotificationMessage) {
			w.host.Submit(device, "pull", []onvif.NotificationMessage{msg})
		},
		&onvif.SubscribeEventsOptions{
			SubscriptionDuration: dur,
			RenewMargin:          renewMargin,
			PullTimeout:          cfg.PullTimeout(),
			MessageLimit:         cfg.MessageLimit,
		})
	if err != nil {
		w.recordCreate(nowStr(), false)
		return fmt.Errorf("PullPoint 订阅失败: %w", err)
	}
	w.recordCreate(nowStr(), true)
	w.setStatus("pull", true, "")
	w.logger().Info("PullPoint 订阅已建立", "device", dev.Name, "ttl", dur.String(),
		"pull_timeout", cfg.PullTimeout().String(), "message_limit", cfg.MessageLimit)
	select {
	case <-stream.Done():
	case <-w.ctx.Done():
	}
	u, c := context.WithTimeout(context.WithoutCancel(w.ctx), 8*time.Second)
	_ = stream.Unsubscribe(u)
	c()
	if w.ctx.Err() != nil {
		return nil
	}
	return fmt.Errorf("PullPoint 订阅循环已退出（续订失败或设备重启）")
}

// Stop 先停发新动作，再取消上下文等待 worker 退出。
// 等待上限 stopWait 覆盖了停止路径上的全部收尾动作（残留清理总预算
// staleCleanBudget、退订 8s+3s 回退），正常情况下超时不会触发；一旦触发，
// 置位 stopTimedOut 供 Manager.Reload 知情——retired 已阻止旧 worker 创建/
// 续约订阅，但其限时收尾（退订/清理）可能仍在新 Worker 启动后短暂重叠。
func (w *CameraWorker) Stop() {
	w.retired.Store(true) // 先停发新动作，再取消上下文等待退出
	w.cancel()
	select {
	case <-w.done:
	case <-time.After(stopWait):
		w.stopTimedOut.Store(true)
		w.logger().Warn("设备订阅退出超时（收尾工作可能仍在后台限时进行）",
			"device", w.Device().Name)
	}
	w.setStatus("", false, "已停止")
}

// StopTimedOut 报告最近一次 Stop 是否等待超时。
func (w *CameraWorker) StopTimedOut() bool { return w.stopTimedOut.Load() }

// unsubscribeStale 尽力退订所有已知的残留 Push 订阅。
// 引用来源（去重）：① worker 运行时待清理列表 staleRefs（含创建时从 DB
// 快照初始化的崩溃残留）；② 当前订阅地址 curSubMgr（重连场景下可能仍在）。
// 内存 dev.SubManager 不作为来源——它只是 Worker 创建时的静态快照，DB 更新
// 后不会同步，回读会把已清理成功的旧引用在每次重连时反复带回重试。
// ONVIF 退订必须持有该地址（wsnt:Unsubscribe 发往 SubscribeResponse 返回的
// SubscriptionManager 端点），绝不向回调 URL（ConsumerReference）发退订。
// 清理失败的引用保留在 staleRefs 随重连重试；设备重启后地址会永久失效，
// 此时只能断电重启摄像头清理。
//
// 生命周期约束：整个清理过程受总时间预算 staleCleanBudget 约束（小于
// Stop 的等待上限），且每轮清理前检查 retired——保证旧 Worker 不会越过
// Stop() 边界仍在清理（否则 Reload 超时后启动新 Worker，新旧可能同时对
// 设备退订）。Stop 之后的收尾清理仍尽力完成（WithoutCancel），只是限时。
func (w *CameraWorker) unsubscribeStale(cli *onvif.Client) {
	dev := w.Device()
	w.subMgrMu.Lock()
	refs := append([]string(nil), w.staleRefs...)
	if w.curSubMgr != "" {
		refs = append(refs, w.curSubMgr)
	}
	w.subMgrMu.Unlock()
	seen := map[string]bool{}
	var unique []string
	for _, r := range refs {
		if r == "" || seen[r] {
			continue
		}
		seen[r] = true
		unique = append(unique, r)
	}
	if len(unique) == 0 {
		return
	}

	deadline := time.Now().Add(staleCleanBudget)
	var remaining []string
	cleaned := 0
	for _, ref := range unique {
		// 已停用或预算耗尽：未清理的引用保留待重试（写回 DB 交给后续
		// 重连或下一个 Worker 接手），不再发起新的清理请求。
		if w.retired.Load() {
			remaining = append(remaining, ref)
			continue
		}
		left := time.Until(deadline)
		if left <= 0 {
			remaining = append(remaining, ref)
			continue
		}
		if left > staleCleanPerRef {
			left = staleCleanPerRef
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(w.ctx), left)
		err := cli.Events().UnsubscribePush(ctx, ref)
		cancel()
		if err == nil {
			cleaned++
			continue
		}
		remaining = append(remaining, ref)
	}

	// 更新运行时状态：当前订阅引用清空（建立新订阅前本应为空），
	// 清理失败的转入/保留在待清理列表随重连重试。
	w.subMgrMu.Lock()
	w.curSubMgr = ""
	w.staleRefs = remaining
	w.subMgrMu.Unlock()

	if cleaned > 0 {
		w.logger().Info("已清理设备端残留的 Push 订阅", "device", dev.Name, "count", cleaned)
	}
	if len(remaining) > 0 {
		w.recordUnsub("清理残留订阅失败（保留待重试）")
		w.staleWarnOnce.Do(func() {
			w.logger().Warn("部分残留 Push 订阅清理失败，将随重连重试",
				"device", dev.Name, "refs", len(remaining))
		})
	} else {
		w.recordUnsub("")
	}
	// 数据库只保留仍待清理的引用（新订阅建立后会写入「新地址+待清理列表」）。
	if err := w.host.Store().SetCameraSubManager(dev.ID, strings.Join(remaining, " ")); err != nil {
		w.logger().Error("残留订阅清理结果持久化失败（重启后无法恢复待清理引用）",
			"device", dev.Name, "stage", "unsubscribe_stale", "err", err)
	}
}

// unsubscribeBestEffort 退订一次 Push 订阅并记录诊断结果。
// 优先使用 SubscribeResponse.SubscriptionManager.Address（即设备端订阅管理端点，
// 符合 WS-BaseNotification 语义，也覆盖 TP-Link 等把引用编进 URL 的固件）；
// 部分设备不下发 SubscriptionManager，库已回退为 SubscriptionReference.Address——
// 本函数在此基础上再做一层保险：管理地址退订失败且两者不同时，改用
// SubscriptionReference 再试一次。两次超时合计（8s+3s）小于 Stop 等待上限，
// 保证停止路径的收尾不会越过 Worker 生命周期边界。返回最终是否成功。
func (w *CameraWorker) unsubscribeBestEffort(svc *events.Service, sub *events.PushSubscription) bool {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(w.ctx), 8*time.Second)
	defer cancel()
	err := svc.UnsubscribePush(ctx, sub.ManagerAddress)
	if err != nil && sub.SubscriptionReference != "" &&
		sub.SubscriptionReference != sub.ManagerAddress {
		w.logger().Debug("按订阅管理地址退订失败，回退 SubscriptionReference 重试",
			"device", w.Device().Name, "err", err)
		ctx2, cancel2 := context.WithTimeout(context.WithoutCancel(w.ctx), 3*time.Second)
		defer cancel2()
		err = svc.UnsubscribePush(ctx2, sub.SubscriptionReference)
	}
	if err != nil {
		w.recordUnsub(err.Error())
		return false
	}
	w.recordUnsub("")
	return true
}

// runPushLease 自管一次 Push 订阅的续约与退订（替代库的 SubscribePushEvents，
// 以便拿到订阅管理地址持久化到 DB）。语义与库实现一致：
// 以 granted TTL 一半的间隔续约（下限 2s），续约失败即退出；ctx 取消时主动退订。
func (w *CameraWorker) runPushLease(svc *events.Service, sub *events.PushSubscription, dur time.Duration) {
	granted := dur
	if !sub.TerminationTime.IsZero() {
		if g := time.Until(sub.TerminationTime); g > 0 {
			granted = g
		}
	}
	loopCtx, cancel := context.WithCancel(context.WithoutCancel(w.ctx))
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			delay := granted / 2
			if delay < 2*time.Second {
				delay = 2 * time.Second
			}
			select {
			case <-loopCtx.Done():
				return
			case <-time.After(delay):
			}
			if w.retired.Load() {
				return
			}
			term, err := svc.RenewPush(loopCtx, sub.ManagerAddress, dur)
			w.recordRenew(errTextOf(err))
			if err != nil {
				// 订阅已失效或设备拒绝续约，无限重试只会刷屏——终止循环。
				w.logger().Warn("Push 续约失败，订阅终止", "device", w.Device().Name, "err", err)
				return
			}
			if g := time.Until(term); g > 0 {
				granted = g
			} else {
				granted = dur
			}
		}
	}()

	select {
	case <-done:
	case <-w.ctx.Done():
	}
	cancel()
	// 等续约协程真正退出后再退订，避免 RenewPush 与 UnsubscribePush 同时
	// 操作同一个订阅（cancel() 只是通知，不等于协程已退出）。正常情况下
	// RenewPush 的在途请求会随 loopCtx 取消立即中止、等待瞬间完成（若
	// select 本就走 done 分支则立即返回）；设上限 renewExitWait 防止底层
	// HTTP 不响应 Context 时拖穿 Stop 等待预算，超时则告警后退订。
	select {
	case <-done:
	case <-time.After(renewExitWait):
		w.logger().Warn("续约协程退出超时，继续执行退订（可能与在途续约短暂并发）",
			"device", w.Device().Name)
	}
	// 主动退订；成功则清空当前引用并持久化（仅剩待清理列表），
	// 失败则把地址转入待清理列表，随重连/下次启动重试。
	if w.unsubscribeBestEffort(svc, sub) {
		w.clearCurSubMgr()
		w.persistSubMgrState("unsubscribe")
	} else {
		w.moveCurToStale()
		w.persistSubMgrState("unsubscribe_fail")
		if w.ctx.Err() == nil {
			w.logger().Warn("Push 退订失败，订阅地址转入待清理列表随重连重试",
				"device", w.Device().Name)
		}
	}
}

func errTextOf(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
