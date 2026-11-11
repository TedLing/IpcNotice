// Package manager 把数据库配置映射为运行中的 ONVIF 订阅，并在配置变更后热重建。
// 它同时实现 onvifcore.WorkerHost 接口，充当 CameraWorker 与运行时之间的宿主。
package manager

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"IpcNotice/internal/config"
	"IpcNotice/internal/dispatch"
	"IpcNotice/internal/onvifcore"
	"IpcNotice/internal/store"
	"github.com/mickeyzzc/onvif-go/v2/onvif"
)

// RuntimeInfo 是运行态总览，供前端「概览 / 运行态」页面展示。
type RuntimeInfo struct {
	StartedAt     string                  `json:"started_at"`
	AdvertiseHost string                  `json:"advertise_host"`
	NotifyListen  string                  `json:"notify_listen"`
	NotifyPath    string                  `json:"notify_path"`
	ConsumerBase  string                  `json:"consumer_base"`
	EventMode     string                  `json:"event_mode"`
	NotifyRunning bool                    `json:"notify_running"`
	NotifyPosts   map[string]string       `json:"notify_posts"`
	Workers       []onvifcore.WorkerStatus `json:"workers"`
	ActiveRoutes  int                     `json:"active_routes"`
	Subscriptions int64                   `json:"subscriptions"`
	LastReload    string                  `json:"last_reload"`
	LastReloadErr string                  `json:"last_reload_err"`
}

// Manager 管理全部设备的订阅生命周期、Notify 接收端与事件分发器。
type Manager struct {
	ctx        context.Context
	store      *store.Store
	log        *slog.Logger
	notify     *onvifcore.NotifyServer
	dispatcher *dispatch.Dispatcher

	advertiseHost string
	notifyRunning bool
	startedAt     string
	lastReload    string
	lastReloadErr string

	reloadMu sync.Mutex
	mu        sync.RWMutex
	st        config.Settings
	workers   map[int64]*onvifcore.CameraWorker
	cameraIDs map[string]int64
}

// ---- WorkerHost 实现（CameraWorker 通过本接口反向访问运行态） ----

func (m *Manager) Context() context.Context          { return m.ctx }
func (m *Manager) Logger() *slog.Logger             { return m.log }
func (m *Manager) Store() *store.Store              { return m.store }
func (m *Manager) Settings() config.Settings        { return m.settings() }
func (m *Manager) ConsumerURL(dev *config.DeviceConfig) string { return m.notify.ConsumerURL(dev) }
func (m *Manager) Submit(dev *config.DeviceConfig, transport string, msgs []onvif.NotificationMessage) {
	m.dispatcher.Submit(dev, transport, msgs)
}

// NewManager 从数据库加载配置，启动 Notify 接收端、分发器并建立初始订阅。
func NewManager(ctx context.Context, store *store.Store, logger *slog.Logger) (*Manager, error) {
	st, err := store.Settings()
	if err != nil {
		return nil, err
	}
	m := &Manager{
		ctx: ctx, store: store, log: logger, st: st,
		startedAt: time.Now().UTC().Format(time.RFC3339),
		workers:   map[int64]*onvifcore.CameraWorker{},
		cameraIDs: map[string]int64{},
	}

	cameras, err := store.ListCameras()
	if err != nil {
		return nil, err
	}
	devs, firstHost := devicesFromCameras(cameras)
	host, isLocal, err := "", true, error(nil)
	if firstHost == "" {
		// 全新安装时数据库还没有设备，无法按路由自动探测；先用本机回环地址，
		// 待前端新增摄像头后 Reload 会重新计算并提示。
		host = "127.0.0.1"
		if strings.TrimSpace(st.AdvertiseHost) != "" && !strings.EqualFold(st.AdvertiseHost, "auto") {
			host = st.AdvertiseHost
		}
		logger.Warn("数据库尚无摄像头，通告地址暂用占位值；新增设备后会自动重新探测并更新",
			"advertise_host", host)
	} else {
		host, isLocal, err = onvifcore.ResolveAdvertiseHost(st.AdvertiseHost, firstHost)
		if err != nil {
			return nil, err
		}
		if !isLocal {
			logger.Warn("通告地址不是本机地址，摄像头可能无法回调；可填 auto 自动探测",
				"configured", st.AdvertiseHost, "detected_local", host)
		}
	}
	m.advertiseHost = host

	notify, err := onvifcore.NewNotifyServer(st.Server(), host, devs,
		func(dev *config.DeviceConfig, msgs []onvif.NotificationMessage) {
			m.dispatcher.Submit(dev, "push", msgs)
		}, logger)
	if err != nil {
		return nil, err
	}
	m.notify = notify

	d, err := dispatch.NewDispatcher(store, st, logger)
	if err != nil {
		return nil, err
	}
	m.dispatcher = d

	if err := notify.Start(); err != nil {
		logger.Error("Notify 接收端启动失败（Push 模式将不可用；PullPoint 不受影响）", "err", err)
	} else {
		m.notifyRunning = true
	}
	notify.SetDevices(devs)

	if err := m.Reload(); err != nil {
		logger.Warn("首次加载运行配置未完全成功", "err", err)
	}
	return m, nil
}

func devicesFromCameras(cameras []store.Camera) ([]*config.DeviceConfig, string) {
	devs := make([]*config.DeviceConfig, 0, len(cameras))
	firstHost := ""
	for _, c := range cameras {
		if !c.Enabled {
			continue
		}
		dev, err := c.Device()
		if err != nil {
			continue
		}
		devs = append(devs, dev)
		if firstHost == "" {
			firstHost = dev.Host
		}
	}
	return devs, firstHost
}

func (m *Manager) settings() config.Settings {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.st
}

// Reload 从数据库重建路由表并热更新订阅。任何配置变更都应随后调用它。
func (m *Manager) Reload() error {
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()

	st, err := m.store.Settings()
	if err != nil {
		m.setReloadErr(err)
		return err
	}
	cameras, err := m.store.ListCameras()
	if err != nil {
		m.setReloadErr(err)
		return err
	}
	channels, err := m.store.ListChannels()
	if err != nil {
		m.setReloadErr(err)
		return err
	}
	subs, err := m.store.ListSubscriptions(0)
	if err != nil {
		m.setReloadErr(err)
		return err
	}
	// 事件备注用于给 webhook 推送补上中文标题，随订阅一起热更新。
	topicRemarks, err := m.store.ListTopicRemarks()
	if err != nil {
		m.setReloadErr(err)
		return err
	}

	cameraByID := map[int64]store.Camera{}
	idsByName := map[string]int64{}
	devs := []*config.DeviceConfig{}
	devByID := map[int64]*config.DeviceConfig{}
	for _, c := range cameras {
		cameraByID[c.ID] = c
		if !c.Enabled {
			continue
		}
		idsByName[c.Name] = c.ID
		dev, err := c.Device()
		if err != nil {
			m.log.Warn("设备配置非法，已跳过", "device", c.Name, "err", err)
			continue
		}
		devByID[c.ID] = dev
		devs = append(devs, dev)
	}
	channelByID := map[int64]store.WebhookChannel{}
	for _, ch := range channels {
		if ch.Enabled {
			channelByID[ch.ID] = ch
		}
	}

	rt := dispatch.EmptyRouteTable()
	rt.CameraIDs = idsByName
	for _, cam := range cameras {
		if remarks := topicRemarks[cam.ID]; len(remarks) > 0 {
			rt.Remarks[cam.Name] = remarks
		}
	}
	desired := map[int64]bool{}
	activeSubs := int64(0)
	routeCount := 0
	for _, sub := range subs {
		cam, ok := cameraByID[sub.CameraID]
		if !ok || !cam.Enabled || !sub.Enabled {
			continue
		}
		activeSubs++
		desired[sub.CameraID] = true
		picked := []store.WebhookChannel{}
		for _, id := range sub.ChannelIDs {
			if ch, ok := channelByID[id]; ok {
				picked = append(picked, ch)
			}
		}
		rt.ByCamera[cam.Name] = append(rt.ByCamera[cam.Name],
			dispatch.Route{Topic: sub.Topic, Channels: picked})
		routeCount++
	}

	m.mu.Lock()
	m.st = st
	m.cameraIDs = idsByName
	m.lastReload = time.Now().UTC().Format(time.RFC3339)
	m.lastReloadErr = ""
	workers := m.workers
	m.mu.Unlock()

	_ = m.dispatcher.Configure(st)
	m.dispatcher.SetRoutes(rt)
	m.notify.SetDevices(devs)

	// 全新安装时数据库为空，通告地址只是占位值；新增设备后按实际路由重新解析，
	// 否则首次配置的摄像头会拿到 127.0.0.1 的回调地址而无法 Push。
	if len(devs) > 0 {
		configured := strings.TrimSpace(st.AdvertiseHost)
		newHost := m.advertiseHost
		if configured == "" || strings.EqualFold(configured, "auto") {
			if h, _, err := onvifcore.ResolveAdvertiseHost("auto", devs[0].Host); err == nil && h != "" {
				newHost = h
			}
		} else {
			newHost = configured
		}
		if newHost != m.advertiseHost {
			m.mu.Lock()
			m.advertiseHost = newHost
			m.mu.Unlock()
			m.notify.SetBaseURL(newHost)
		}
	}

	// 停止配置已变化或不再需要监听的设备；保留的 worker 原子刷新 dev 指针
	//（如仅改显示名：回调地址由 token 决定，订阅无需重建）。
	for id, w := range workers {
		dev := devByID[id]
		if !desired[id] || dev == nil || w.Hash() != onvifcore.DeviceHash(dev, st) {
			w.Stop()
			delete(workers, id)
		} else {
			w.RefreshDevice(dev)
		}
	}
	for id := range desired {
		if _, ok := workers[id]; ok {
			continue
		}
		dev := devByID[id]
		if dev == nil {
			continue
		}
		w := onvifcore.NewCameraWorker(m, dev, onvifcore.DeviceHash(dev, st), id)
		workers[id] = w
		w.Start()
		m.log.Info("已启动设备事件订阅", "device", dev.Name, "endpoint", dev.Endpoint)
	}

	m.mu.Lock()
	m.workers = workers
	m.mu.Unlock()
	m.log.Info("运行配置已重载", "cameras", len(devs), "active_subscriptions", activeSubs,
		"routes", routeCount, "workers", len(workers), "event_mode", st.EventMode)
	return nil
}

func (m *Manager) setReloadErr(err error) {
	m.mu.Lock()
	m.lastReloadErr = err.Error()
	m.lastReload = time.Now().UTC().Format(time.RFC3339)
	m.mu.Unlock()
}

// Status 返回运行态总览快照。
func (m *Manager) Status() RuntimeInfo {
	m.mu.RLock()
	st := m.st
	workers := make([]*onvifcore.CameraWorker, 0, len(m.workers))
	for _, w := range m.workers {
		workers = append(workers, w)
	}
	started, reloaded, reloadErr := m.startedAt, m.lastReload, m.lastReloadErr
	host, running := m.advertiseHost, m.notifyRunning
	m.mu.RUnlock()

	statuses := make([]onvifcore.WorkerStatus, 0, len(workers))
	for _, w := range workers {
		s := w.Status()
		s.LastNotifyAt = m.notify.LastNotifyAt(s.Device)
		statuses = append(statuses, s)
	}
	active, _ := m.store.ActiveSubscriptionCount()
	return RuntimeInfo{
		StartedAt: started, AdvertiseHost: host,
		NotifyListen: st.NotifyListenAddr, NotifyPath: st.NotifyPath,
		ConsumerBase: m.notify.BaseURL(), EventMode: st.EventMode,
		NotifyRunning: running, NotifyPosts: m.notify.PostCounts(),
		Workers: statuses, ActiveRoutes: m.dispatcher.RouteCount(), Subscriptions: active,
		LastReload: reloaded, LastReloadErr: reloadErr,
	}
}

// Shutdown 停止全部订阅、分发器与 Notify 接收端。
func (m *Manager) Shutdown() {
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()
	m.mu.RLock()
	workers := make([]*onvifcore.CameraWorker, 0, len(m.workers))
	for _, w := range m.workers {
		workers = append(workers, w)
	}
	m.mu.RUnlock()
	for _, w := range workers {
		w.Stop()
	}
	m.dispatcher.Close()
	if m.notifyRunning {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.notify.Shutdown(ctx); err != nil {
			m.log.Warn("Notify 接收端关闭失败", "err", err)
		}
	}
	m.log.Info("运行态已关闭", "notify_posts", fmt.Sprint(m.notify.PostCounts()))
}
