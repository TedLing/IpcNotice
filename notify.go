package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mickeyzzc/onvif-go/v2/onvif"
)

const maxNotifyBodyBytes = 1 << 20 // 1 MiB

// deviceCounter 同时用于「设备能否回调到本机」的探测计数与唤醒。
type deviceCounter struct {
	posts  atomic.Int64
	signal chan struct{}
	lastIP atomic.Value // string
	lastAt atomic.Value // string，最近一次收到 POST 的时间（UTC RFC3339）
}

func newDeviceCounter() *deviceCounter {
	return &deviceCounter{signal: make(chan struct{}, 1)}
}

func (c *deviceCounter) mark(ip string) {
	c.posts.Add(1)
	c.lastIP.Store(ip)
	c.lastAt.Store(time.Now().UTC().Format(time.RFC3339))
	select {
	case c.signal <- struct{}{}:
	default:
	}
}

// NotifyServer 接收摄像头 POST 的 wsnt:Notify，按回调 token 分路。
// consumer URL 形如 http://<advertise-host>:9100/onvif/notify/c1。
// 注意：TP-Link 固件无法正确处理 percent-encoded 的中文路径（会把回调地址损坏成乱码），
// 因此路径段一律使用 ASCII 的设备 token（c<id>），设备名仅用于显示与日志。
type NotifyServer struct {
	cfg      ServerConfig
	baseURL  string
	mux      *http.ServeMux
	server   *http.Server
	mu       sync.RWMutex
	devices  map[string]*DeviceConfig // token 与历史名称路径双键 → 设备
	counters sync.Map                 // name -> *deviceCounter
	onEvent  func(*DeviceConfig, []onvif.NotificationMessage)
	log      *slog.Logger
}

// notifyToken 返回 ASCII 安全的回调路径段。
func notifyToken(d *DeviceConfig) string {
	if d.ID > 0 {
		return fmt.Sprintf("c%d", d.ID)
	}
	return url.PathEscape(d.Name)
}

func deviceKeyMap(devices []*DeviceConfig) map[string]*DeviceConfig {
	m := make(map[string]*DeviceConfig, len(devices)*2)
	for _, d := range devices {
		m[notifyToken(d)] = d
		if _, exists := m[d.Name]; !exists {
			m[d.Name] = d // 兼容仍指向旧名称路径的在途订阅
		}
	}
	return m
}

// NewNotifyServer 构造接收端。baseURL 的 host 必须是摄像头可路由到的地址。
func NewNotifyServer(cfg ServerConfig, advertiseHost string, devices []*DeviceConfig,
	onEvent func(*DeviceConfig, []onvif.NotificationMessage), logger *slog.Logger) (*NotifyServer, error) {

	_, port, err := net.SplitHostPort(cfg.ListenAddr)
	if err != nil {
		return nil, err
	}
	if advertiseHost == "" {
		return nil, errors.New("通告地址为空")
	}

	n := &NotifyServer{
		cfg:     cfg,
		baseURL: (&url.URL{Scheme: "http", Host: net.JoinHostPort(advertiseHost, port), Path: cfg.NotifyPath}).String(),
		mux:     http.NewServeMux(),
		devices: deviceKeyMap(devices),
		onEvent: onEvent,
		log:     logger.With("component", "notify"),
	}
	for _, d := range devices {
		n.devices[d.Name] = d
		n.counters.Store(d.Name, newDeviceCounter())
	}

	pattern := fmt.Sprintf("POST %s/{device}", cfg.NotifyPath)
	n.mux.HandleFunc(pattern, n.handleNotify)
	n.mux.HandleFunc(cfg.NotifyPath+"/", func(w http.ResponseWriter, r *http.Request) {
		n.log.Warn("收到未匹配的 Notify 请求", "method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr)
		http.Error(w, "unknown notify target", http.StatusNotFound)
	})
	n.mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"ok"}`)
	})

	n.server = &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           n.mux,
		ReadHeaderTimeout: time.Duration(cfg.ReadTimeoutSeconds) * time.Second,
	}

	return n, nil
}

// SetBaseURL 在设备/设置变化后更新通告给摄像头的 ConsumerReference 基址。
func (n *NotifyServer) SetBaseURL(host string) {
	_, port, err := net.SplitHostPort(n.cfg.ListenAddr)
	if err != nil || host == "" {
		return
	}
	u := (&url.URL{Scheme: "http", Host: net.JoinHostPort(host, port), Path: n.cfg.NotifyPath}).String()
	n.mu.Lock()
	n.baseURL = u
	n.mu.Unlock()
	n.log.Info("Notify 通告基址已更新", "consumer_base", u)
}

// BaseURL 返回当前通告基址。
func (n *NotifyServer) BaseURL() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.baseURL
}

// SetDevices 在配置变更后替换设备表，使新增/重命名的摄像头无需重启即可回调。
// 设备改名后 token（c<id>）不变，回调地址保持稳定；同时保留名称键兼容在途的旧订阅。
func (n *NotifyServer) SetDevices(devices []*DeviceConfig) {
	fresh := deviceKeyMap(devices)
	n.mu.Lock()
	n.devices = fresh
	n.mu.Unlock()
	for _, d := range devices {
		n.Counter(d.Name)
	}
	n.log.Debug("Notify 设备表已更新", "count", len(devices))
}

func (n *NotifyServer) getDevice(name string) *DeviceConfig {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.devices[name]
}

// Start 立即绑定端口；失败直接返回，避免后续订阅到一个不存在的回调地址。
func (n *NotifyServer) Start() error {
	ln, err := net.Listen("tcp", n.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("Notify 接收端绑定 %s 失败（检查端口占用与防火墙入站规则）: %w", n.cfg.ListenAddr, err)
	}
	n.log.Info("Notify 接收端已启动",
		"listen", n.cfg.ListenAddr,
		"consumer_base", n.baseURL,
		"health", "http://127.0.0.1:"+mustPort(n.cfg.ListenAddr)+"/health")
	go func() {
		if err := n.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			n.log.Error("Notify 接收端异常退出", "err", err)
		}
	}()
	return nil
}

// Shutdown 平滑关闭接收端。
func (n *NotifyServer) Shutdown(ctx context.Context) error {
	return n.server.Shutdown(ctx)
}

// ConsumerURL 返回某台设备专用的回调地址（ASCII token 路径，改名不影响）。
func (n *NotifyServer) ConsumerURL(dev *DeviceConfig) string {
	return n.BaseURL() + "/" + notifyToken(dev)
}

// Counter 返回设备计数（不存在时按需创建，便于记录陌生来源）。
func (n *NotifyServer) Counter(deviceName string) *deviceCounter {
	if v, ok := n.counters.Load(deviceName); ok {
		return v.(*deviceCounter)
	}
	c := newDeviceCounter()
	actual, _ := n.counters.LoadOrStore(deviceName, c)
	return actual.(*deviceCounter)
}

// LastNotifyAt 返回设备最近一次收到 Notify POST 的时间（UTC RFC3339；从未收到返回空串）。
// token 只负责摄像头身份路由：同一摄像头的多个订阅（含崩溃残留的旧订阅）都会
// 推到同一个 token 路径，这里按身份统计，不区分订阅实例。
func (n *NotifyServer) LastNotifyAt(deviceName string) string {
	v, ok := n.counters.Load(deviceName)
	if !ok {
		return ""
	}
	s, _ := v.(*deviceCounter).lastAt.Load().(string)
	return s
}

func (n *NotifyServer) handleNotify(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("device")
	dev := n.getDevice(token)
	name := token
	if dev != nil {
		name = dev.Name
	}
	counter := n.Counter(name)
	counter.mark(remoteIP(r.RemoteAddr))

	body, err := io.ReadAll(io.LimitReader(r.Body, maxNotifyBodyBytes+1))
	if err != nil {
		n.log.Warn("读取 Notify body 失败", "device", name, "err", err)
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(body) > maxNotifyBodyBytes {
		http.Error(w, "notify body too large", http.StatusRequestEntityTooLarge)
		return
	}

	msgs, err := onvif.ParseNotifyEvents(body)
	if err != nil {
		n.log.Warn("Notify 解析失败（回调链路仍然可达）", "device", name, "err", err,
			"body_head", head(body, 300))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if dev == nil {
		n.log.Warn("Notify 来自未配置的设备", "device", name, "remote", r.RemoteAddr, "messages", len(msgs))
		w.WriteHeader(http.StatusOK) // 对未知来源也回 200，避免设备反复重发
		return
	}

	n.log.Debug("收到 Notify", "device", name, "remote", r.RemoteAddr, "messages", len(msgs))
	if n.onEvent != nil {
		n.onEvent(dev, msgs)
	}
	w.WriteHeader(http.StatusOK)
}

func remoteIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

func head(b []byte, n int) string {
	if len(b) > n {
		b = b[:n]
	}
	return string(b)
}

func mustPort(addr string) string {
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		return "?"
	}
	return p
}

// ResolveAdvertiseHost 决定写进 ConsumerReference 的地址。
// "auto" 通过 UDP 路由探测选取访问设备时本机使用的源 IP（设备能回连的地址）。
func ResolveAdvertiseHost(configured string, deviceHost string) (string, bool, error) {
	configured = strings.TrimSpace(configured)
	if configured != "" && !strings.EqualFold(configured, "auto") {
		return configured, isLocalAddr(configured), nil
	}

	if deviceHost == "" {
		return "", false, errors.New("auto 模式下无法确定设备地址")
	}
	conn, err := net.Dial("udp", net.JoinHostPort(deviceHost, "9"))
	if err != nil {
		return "", false, fmt.Errorf("auto 探测本机通告地址失败: %w", err)
	}
	defer conn.Close()

	host, _, err := net.SplitHostPort(conn.LocalAddr().String())
	if err != nil {
		return "", false, err
	}
	return host, true, nil
}

func isLocalAddr(host string) bool {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return false
	}
	for _, a := range addrs {
		ip, _, err := net.ParseCIDR(a.String())
		if err != nil {
			continue
		}
		if ip.String() == host {
			return true
		}
	}
	return false
}

// deviceNameFromCounter 仅用于日志汇总，避免暴露额外状态。
func (n *NotifyServer) PostCounts() map[string]string {
	out := make(map[string]string)
	n.counters.Range(func(k, v any) bool {
		c := v.(*deviceCounter)
		out[k.(string)] = strconv.FormatInt(c.posts.Load(), 10)
		return true
	})
	return out
}

// jsonDump 用于调试输出结构化对象。
func jsonDump(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}
