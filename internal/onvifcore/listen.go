// Package onvifcore 封装 ONVIF 客户端容错传输、Push 订阅接收端（NotifyServer）
// 与单台摄像头的订阅生命周期管理（CameraWorker）。
package onvifcore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
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
// 超时后旧 worker 也不会再发起任何新的订阅创建/续约动作。
// token（c<id>）只负责「摄像头身份 → 回调路由」；同一摄像头的多个订阅
// （如崩溃残留的旧订阅）可能同时向同一回调 URL 推送，由 NotifyServer 全部
// 接收并交给去重逻辑处理，与订阅实例无关。
type CameraWorker struct {
	host    WorkerHost
	dev     atomic.Pointer[config.DeviceConfig] // 可被 Reload 原子刷新（改名等不重建订阅的变更）
	hash    string
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	cameraID int64

	retired atomic.Bool // stop() 后置位：不再允许创建/续约订阅

	staleWarnOnce sync.Once // 残留订阅清理失败只告警一次，避免重连风暴刷屏

	mu     sync.RWMutex
	status WorkerStatus
}

func NewCameraWorker(host WorkerHost, dev *config.DeviceConfig, hash string, cameraID int64) *CameraWorker {
	ctx, cancel := context.WithCancel(host.Context())
	w := &CameraWorker{
		host: host, hash: hash, ctx: ctx, cancel: cancel, cameraID: cameraID,
		done: make(chan struct{}),
		status: WorkerStatus{
			CameraID:  cameraID,
			Device:    dev.Name,
			StartedAt: time.Now().UTC().Format(time.RFC3339),
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
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

func (w *CameraWorker) recordCreate(at string, ok bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.status.CreateCount++
	w.status.LastCreateAt = at
	w.status.LastCreateOK = ok
}

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

// Status 返回当前订阅运行状态快照。
func (w *CameraWorker) Status() WorkerStatus {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.status
}

// Start 启动 worker 的订阅循环。
func (w *CameraWorker) Start() { go w.run() }

// RefreshDevice 在改名等不重建订阅的变更后，原子刷新设备配置指针。
func (w *CameraWorker) RefreshDevice(dev *config.DeviceConfig) { w.dev.Store(dev) }

func (w *CameraWorker) run() {
	defer close(w.done)
	backoff := 3 * time.Second
	for {
		if w.ctx.Err() != nil {
			return
		}
		err := w.serveOnce()
		if w.ctx.Err() != nil {
			return
		}
		msg := ""
		if err != nil {
			msg = err.Error()
			w.logger().Warn("设备订阅中断，将自动重连", "device", w.Device().Name, "err", err,
				"backoff", backoff.String())
		}
		w.setStatus("", false, msg)
		select {
		case <-w.ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
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
			// 持久化订阅管理地址：进程崩溃后，下次启动可据此退订残留订阅。
			_ = w.host.Store().SetCameraSubManager(dev.ID, sub.ManagerAddress)
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
func (w *CameraWorker) Stop() {
	w.retired.Store(true) // 先停发新动作，再取消上下文等待退出
	w.cancel()
	select {
	case <-w.done:
	case <-time.After(12 * time.Second):
		w.logger().Warn("设备订阅退出超时", "device", w.Device().Name)
	}
	w.setStatus("", false, "已停止")
}

// unsubscribeStale 尽力退订上次运行残留的 Push 订阅。
// 设备端订阅管理地址持久化在 cameras.sub_manager：正常退出时已清空，
// 进程崩溃/被杀时保留。ONVIF 退订必须持有该地址（wsnt:Unsubscribe 发往
// SubscribeResponse 返回的 SubscriptionManager 端点），否则无法定位旧订阅；
// 绝不向回调 URL（ConsumerReference）发退订。
func (w *CameraWorker) unsubscribeStale(cli *onvif.Client) {
	dev := w.Device()
	ref := dev.SubManager
	if ref == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(w.ctx), 6*time.Second)
	defer cancel()
	if err := cli.Events().UnsubscribePush(ctx, ref); err == nil {
		w.logger().Info("已清理上次运行残留的 Push 订阅", "device", dev.Name)
		_ = w.host.Store().SetCameraSubManager(dev.ID, "")
		w.recordUnsub("")
		return
	}
	w.recordUnsub("清理残留订阅失败")
	// 失败时保留地址随重连重试；设备重启后地址会永久失效，此时只能断电重启摄像头清理。
	w.staleWarnOnce.Do(func() {
		w.logger().Warn("清理残留 Push 订阅失败，将随重连重试",
			"device", dev.Name, "manager", ref)
	})
}

// unsubscribeBestEffort 退订一次 Push 订阅并记录诊断结果。
// 优先使用 SubscribeResponse.SubscriptionManager.Address（即设备端订阅管理端点，
// 符合 WS-BaseNotification 语义，也覆盖 TP-Link 等把引用编进 URL 的固件）；
// 部分设备不下发 SubscriptionManager，库已回退为 SubscriptionReference.Address——
// 本函数在此基础上再做一层保险：管理地址退订失败且两者不同时，改用
// SubscriptionReference 再试一次。返回最终是否成功。
func (w *CameraWorker) unsubscribeBestEffort(svc *events.Service, sub *events.PushSubscription) bool {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(w.ctx), 8*time.Second)
	defer cancel()
	err := svc.UnsubscribePush(ctx, sub.ManagerAddress)
	if err != nil && sub.SubscriptionReference != "" &&
		sub.SubscriptionReference != sub.ManagerAddress {
		w.logger().Debug("按订阅管理地址退订失败，回退 SubscriptionReference 重试",
			"device", w.Device().Name, "err", err)
		ctx2, cancel2 := context.WithTimeout(context.WithoutCancel(w.ctx), 8*time.Second)
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
	// 主动退订；仅成功时清空持久化地址，失败则保留供下次启动重试清理。
	if w.unsubscribeBestEffort(svc, sub) {
		_ = w.host.Store().SetCameraSubManager(w.Device().ID, "")
	} else if w.ctx.Err() == nil {
		w.logger().Warn("Push 退订失败，保留订阅管理地址供下次启动清理",
			"device", w.Device().Name)
	}
}

func errTextOf(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
