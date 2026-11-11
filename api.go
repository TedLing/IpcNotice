package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const sessionCookie = "ipcnotice_session"
const sessionTTL = 24 * time.Hour

// APIServer 提供管理界面所需的 REST API 与前端静态资源。
type APIServer struct {
	cfg        *Config
	store      *Store
	mgr        *Manager
	log        *slog.Logger
	distFS     fs.FS // 前端资源：默认内嵌（embed），可用 -web-dir 覆盖为磁盘目录
	distSource string // 日志用：embedded / dir:xxx
	mux        *http.ServeMux
	server     *http.Server

	preview *previewHub // WebRTC 预览会话（RTSP → WebRTC 中继）

	sessMu   sync.Mutex
	sessions map[string]time.Time
}

func NewAPIServer(cfg *Config, store *Store, mgr *Manager, webDir string, logger *slog.Logger) *APIServer {
	s := &APIServer{
		cfg: cfg, store: store, mgr: mgr,
		log: logger.With("component", "api"),
		mux: http.NewServeMux(), sessions: map[string]time.Time{},
		preview: newPreviewHub(logger),
	}
	s.distFS, s.distSource = resolveWebFS(webDir)
	s.routes()
	s.server = &http.Server{
		Addr:              cfg.Addr(),
		Handler:           s.mux,
		ReadHeaderTimeout: 15 * time.Second,
	}
	return s
}

func (s *APIServer) routes() {
	s.mux.HandleFunc("GET /api/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "version": version})
	})
	s.mux.HandleFunc("POST /api/login", s.login)
	s.mux.HandleFunc("POST /api/refresh-token", s.refreshToken)
	s.mux.HandleFunc("POST /api/logout", s.auth(s.logout))
	s.mux.HandleFunc("GET /api/me", s.auth(s.me))

	s.mux.HandleFunc("GET /api/settings", s.auth(s.getSettings))
	s.mux.HandleFunc("PUT /api/settings", s.auth(s.putSettings))

	s.mux.HandleFunc("GET /api/cameras", s.auth(s.listCameras))
	s.mux.HandleFunc("POST /api/cameras", s.auth(s.createCamera))
	s.mux.HandleFunc("PUT /api/cameras/{id}", s.auth(s.updateCamera))
	s.mux.HandleFunc("DELETE /api/cameras/{id}", s.auth(s.deleteCamera))
	s.mux.HandleFunc("POST /api/cameras/{id}/test", s.auth(s.testCamera))
	s.mux.HandleFunc("POST /api/cameras/{id}/discover", s.auth(s.discoverCamera))
	s.mux.HandleFunc("POST /api/cameras/{id}/device-info", s.auth(s.cameraDeviceInfo))
	s.mux.HandleFunc("POST /api/cameras/{id}/preview/offer", s.auth(s.previewOffer))
	s.mux.HandleFunc("DELETE /api/cameras/{id}/preview/{session}", s.auth(s.previewClose))
	s.mux.HandleFunc("GET /api/cameras/{id}/topics", s.auth(s.cameraTopics))
	s.mux.HandleFunc("PUT /api/cameras/{id}/topics", s.auth(s.saveTopicRemarks))
	s.mux.HandleFunc("GET /api/cameras/{id}/subscriptions", s.auth(s.cameraSubscriptions))
	s.mux.HandleFunc("PUT /api/cameras/{id}/subscriptions", s.auth(s.saveSubscriptions))

	s.mux.HandleFunc("GET /api/channels", s.auth(s.listChannels))
	s.mux.HandleFunc("POST /api/channels", s.auth(s.createChannel))
	s.mux.HandleFunc("PUT /api/channels/{id}", s.auth(s.updateChannel))
	s.mux.HandleFunc("DELETE /api/channels/{id}", s.auth(s.deleteChannel))
	s.mux.HandleFunc("POST /api/channels/{id}/test", s.auth(s.testChannel))

	s.mux.HandleFunc("GET /api/subscriptions", s.auth(s.listSubscriptions))
	s.mux.HandleFunc("GET /api/events", s.auth(s.listEvents))
	s.mux.HandleFunc("GET /api/overview", s.auth(s.overview))
	s.mux.HandleFunc("GET /api/runtime", s.auth(s.runtime))
	s.mux.HandleFunc("POST /api/runtime/reload", s.auth(s.reload))

	s.mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		writeErr(w, http.StatusNotFound, "未知接口: "+r.URL.Path)
	})
	s.mux.HandleFunc("/", s.serveStatic)
}

func (s *APIServer) Start() error {
	ln, err := net.Listen("tcp", s.cfg.Addr())
	if err != nil {
		return fmt.Errorf("管理界面监听 %s 失败: %w", s.cfg.Addr(), err)
	}
	s.log.Info("管理界面已启动", "url", "http://127.0.0.1:"+strconv.Itoa(s.cfg.Port),
		"dist", s.distSource)
	go func() {
		if err := s.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("管理界面异常退出", "err", err)
		}
	}()
	return nil
}

func (s *APIServer) Shutdown(ctx context.Context) error {
	// 先释放所有预览的 RTSP 连接与 WebRTC 端口，再关闭 HTTP 服务。
	s.preview.closeAll()
	return s.server.Shutdown(ctx)
}

// ---------------- auth ----------------

func (s *APIServer) login(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := readJSON(r, &body); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	okUser := subtle.ConstantTimeCompare([]byte(body.Username), []byte(s.cfg.Username)) == 1
	okPass := subtle.ConstantTimeCompare([]byte(body.Password), []byte(s.cfg.Password)) == 1
	if !okUser || !okPass {
		s.log.Warn("登录失败", "remote", r.RemoteAddr, "username", body.Username)
		writeErr(w, http.StatusUnauthorized, "用户名或密码错误")
		return
	}
	token := randomToken()
	s.sessMu.Lock()
	now := time.Now()
	for k, exp := range s.sessions {
		if now.After(exp) {
			delete(s.sessions, k)
		}
	}
	s.sessions[token] = now.Add(sessionTTL)
	s.sessMu.Unlock()

	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: token, Path: "/", HttpOnly: true,
		SameSite: http.SameSiteLaxMode, Expires: now.Add(sessionTTL),
	})
	writeJSON(w, http.StatusOK, map[string]any{
		"success": true, "username": s.cfg.Username, "expires_in": int(sessionTTL.Seconds()),
		// 兼容 pure-admin 的 UserResult 结构；真正的鉴权以 HttpOnly 会话 Cookie 为准。
		"data": s.userData(token),
	})
}

// userData 生成 pure-admin 所需的本地用户信息（accessToken 仅用于前端状态，不参与后端鉴权）。
func (s *APIServer) userData(token string) map[string]any {
	return map[string]any{
		"avatar": "", "username": s.cfg.Username, "nickname": "管理员",
		"roles": []string{"admin"}, "permissions": []string{"*:*:*"},
		"accessToken": token, "refreshToken": token,
		"expires": time.Now().Add(sessionTTL).Format("2006/01/02 15:04:05"),
	}
}

func (s *APIServer) refreshToken(w http.ResponseWriter, r *http.Request) {
	if !s.validSession(r) {
		writeErr(w, http.StatusUnauthorized, "会话已过期，请重新登录")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "data": s.userData(randomToken())})
}

func (s *APIServer) logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil {
		s.sessMu.Lock()
		delete(s.sessions, c.Value)
		s.sessMu.Unlock()
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/", MaxAge: -1, HttpOnly: true})
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *APIServer) me(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"username": s.cfg.Username, "role": "admin"})
}

func (s *APIServer) validSession(r *http.Request) bool {
	c, err := r.Cookie(sessionCookie)
	if err != nil || c.Value == "" {
		return false
	}
	s.sessMu.Lock()
	defer s.sessMu.Unlock()
	exp, ok := s.sessions[c.Value]
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		delete(s.sessions, c.Value)
		return false
	}
	return true
}

func (s *APIServer) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.validSession(r) {
			writeErr(w, http.StatusUnauthorized, "未登录或会话已过期")
			return
		}
		next(w, r)
	}
}

func randomToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b)
}

// ---------------- settings ----------------

func (s *APIServer) getSettings(w http.ResponseWriter, _ *http.Request) {
	st, err := s.store.Settings()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *APIServer) putSettings(w http.ResponseWriter, r *http.Request) {
	var st Settings
	if err := readJSON(r, &st); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := st.Normalize(); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	old := s.mgr.settings()
	if err := s.store.SaveSettings(st); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if old.NotifyListenAddr != st.NotifyListenAddr || old.AdvertiseHost != st.AdvertiseHost ||
		old.NotifyPath != st.NotifyPath {
		s.log.Warn("Notify 监听地址/通告地址已变更，需重启程序后才会生效",
			"old", old.NotifyListenAddr, "new", st.NotifyListenAddr)
	}
	if err := s.mgr.Reload(); err != nil {
		writeErr(w, http.StatusInternalServerError, "配置已保存但热重载失败: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, st)
}

// ---------------- cameras ----------------

func (s *APIServer) listCameras(w http.ResponseWriter, _ *http.Request) {
	items, err := s.store.ListCameras()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (s *APIServer) createCamera(w http.ResponseWriter, r *http.Request) {
	var c Camera
	if err := readJSON(r, &c); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if _, err := c.Device(); err != nil {
		writeErr(w, http.StatusBadRequest, "设备参数非法: "+err.Error())
		return
	}
	if err := s.store.CreateCamera(&c); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	s.reloadQuiet()
	writeJSON(w, http.StatusCreated, c)
}

func (s *APIServer) updateCamera(w http.ResponseWriter, r *http.Request) {
	c, err := s.cameraFromRequest(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if _, err := c.Device(); err != nil {
		writeErr(w, http.StatusBadRequest, "设备参数非法: "+err.Error())
		return
	}
	if err := s.store.UpdateCamera(&c); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	s.reloadQuiet()
	writeJSON(w, http.StatusOK, c)
}

func (s *APIServer) deleteCamera(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.store.DeleteCamera(id); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.reloadQuiet()
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *APIServer) cameraFromRequest(r *http.Request) (Camera, error) {
	id, err := pathID(r)
	if err != nil {
		return Camera{}, err
	}
	old, err := s.store.GetCamera(id)
	if err != nil {
		return Camera{}, fmt.Errorf("设备不存在: %w", err)
	}
	var body Camera
	if err := readJSON(r, &body); err != nil {
		return Camera{}, err
	}
	body.ID = id
	body.CreatedAt = old.CreatedAt
	body.Manufacturer = old.Manufacturer
	body.Model = old.Model
	body.Firmware = old.Firmware
	body.Serial = old.Serial
	body.LastTestAt = old.LastTestAt
	body.LastTestOK = old.LastTestOK
	body.LastTestMsg = old.LastTestMsg
	return body, nil
}

// testCamera 只做连接与能力校验，不做事件实抓。
func (s *APIServer) testCamera(w http.ResponseWriter, r *http.Request) {
	c, dev, err := s.cameraDevice(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	res, err := testDevice(r.Context(), dev, s.log, 0)
	if err != nil {
		_ = s.store.UpdateCameraTest(c.ID, false, err.Error(), "", "", "", "")
		writeJSON(w, http.StatusOK, DeviceTestResult{OK: false, Message: err.Error(),
			Topics: []string{}, ObservedTopics: []string{}, Warnings: []string{}})
		return
	}
	_ = s.store.UpdateCameraTest(c.ID, true, res.Message,
		res.Manufacturer, res.Model, res.Firmware, res.Serial)
	writeJSON(w, http.StatusOK, res)
}

// cameraDeviceInfo 通过 ONVIF 逐项采集设备信息，按 基本信息/系统信息/网络信息/分析配置 分组返回。
// 单项操作失败（设备不支持或超时）只影响该项展示，不影响其他操作。
func (s *APIServer) cameraDeviceInfo(w http.ResponseWriter, r *http.Request) {
	_, dev, err := s.cameraDevice(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
	defer cancel()
	res := collectDeviceInfo(ctx, dev, s.log)
	writeJSON(w, http.StatusOK, res)
}

// discoverCamera 「测试通过后抓取全部事件」：连接 + TopicSet 枚举 + 短时实抓。
func (s *APIServer) discoverCamera(w http.ResponseWriter, r *http.Request) {
	c, dev, err := s.cameraDevice(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	var body struct {
		CaptureSeconds int `json:"capture_seconds"`
	}
	_ = readJSON(r, &body)
	if body.CaptureSeconds <= 0 {
		body.CaptureSeconds = 6
	}
	if body.CaptureSeconds > 30 {
		body.CaptureSeconds = 30
	}

	res, err := testDevice(r.Context(), dev, s.log, body.CaptureSeconds)
	if err != nil {
		_ = s.store.UpdateCameraTest(c.ID, false, err.Error(), "", "", "", "")
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	_ = s.store.UpdateCameraTest(c.ID, true, "连接成功，事件抓取完成",
		res.Manufacturer, res.Model, res.Firmware, res.Serial)
	if err := s.store.ReplaceTopics(c.ID, res.Topics, "declared"); err != nil {
		writeErr(w, http.StatusInternalServerError, "保存声明主题失败: "+err.Error())
		return
	}
	if err := s.store.ReplaceTopics(c.ID, res.ObservedTopics, "observed"); err != nil {
		writeErr(w, http.StatusInternalServerError, "保存实抓主题失败: "+err.Error())
		return
	}
	topics, err := s.store.ListTopics(c.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.reloadQuiet()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "message": "事件抓取完成", "test": res, "topics": topics,
	})
}

func (s *APIServer) cameraTopics(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	topics, err := s.store.ListTopics(id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, topics)
}

// saveTopicRemarks 批量保存事件的中文名称/备注；保存后热重载，使后续推送立即带上新标题。
func (s *APIServer) saveTopicRemarks(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if _, err := s.store.GetCamera(id); err != nil {
		writeErr(w, http.StatusBadRequest, "设备不存在")
		return
	}
	var body struct {
		Items []TopicRemarkInput `json:"items"`
	}
	if err := readJSON(r, &body); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.store.UpsertTopicRemarks(id, body.Items); err != nil {
		writeErr(w, http.StatusInternalServerError, "保存事件备注失败: "+err.Error())
		return
	}
	s.reloadQuiet()
	topics, err := s.store.ListTopics(id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, topics)
}

func (s *APIServer) cameraDevice(r *http.Request) (Camera, *DeviceConfig, error) {
	id, err := pathID(r)
	if err != nil {
		return Camera{}, nil, err
	}
	c, err := s.store.GetCamera(id)
	if err != nil {
		return Camera{}, nil, fmt.Errorf("设备不存在: %w", err)
	}
	dev, err := c.Device()
	if err != nil {
		return Camera{}, nil, fmt.Errorf("设备配置非法: %w", err)
	}
	return c, dev, nil
}

// ---------------- subscriptions ----------------

func (s *APIServer) cameraSubscriptions(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	subs, err := s.store.ListSubscriptions(id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, subs)
}

func (s *APIServer) listSubscriptions(w http.ResponseWriter, r *http.Request) {
	cameraID, _ := strconv.ParseInt(r.URL.Query().Get("camera_id"), 10, 64)
	subs, err := s.store.ListSubscriptions(cameraID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, subs)
}

func (s *APIServer) saveSubscriptions(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if _, err := s.store.GetCamera(id); err != nil {
		writeErr(w, http.StatusBadRequest, "设备不存在")
		return
	}
	var body struct {
		Items   []SubscriptionInput `json:"items"`
		Remarks []TopicRemarkInput  `json:"remarks"`
	}
	if err := readJSON(r, &body); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	// 事件名称/备注与订阅在同一个请求里保存，避免用户改了名称却忘记单独保存；
	// remarks 覆盖表格所有行（含未订阅的事件），随后一次 Reload 即可生效。
	if len(body.Remarks) > 0 {
		if err := s.store.UpsertTopicRemarks(id, body.Remarks); err != nil {
			writeErr(w, http.StatusBadRequest, "保存事件名称失败: "+err.Error())
			return
		}
	}
	if err := s.store.ReplaceSubscriptions(id, body.Items); err != nil {
		writeErr(w, http.StatusBadRequest, "保存订阅失败（检查通知渠道是否存在）: "+err.Error())
		return
	}
	s.reloadQuiet()
	subs, _ := s.store.ListSubscriptions(id)
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "subscriptions": subs})
}

// ---------------- channels ----------------

func (s *APIServer) listChannels(w http.ResponseWriter, _ *http.Request) {
	items, err := s.store.ListChannels()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (s *APIServer) createChannel(w http.ResponseWriter, r *http.Request) {
	var c WebhookChannel
	if err := readJSON(r, &c); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateChannel(&c); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.store.CreateChannel(&c); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	s.reloadQuiet()
	writeJSON(w, http.StatusCreated, c)
}

func (s *APIServer) updateChannel(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	old, err := s.store.GetChannel(id)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "通道不存在")
		return
	}
	var c WebhookChannel
	if err := readJSON(r, &c); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateChannel(&c); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	c.ID = id
	c.CreatedAt = old.CreatedAt
	c.LastTestAt, c.LastTestOK, c.LastTestMsg = old.LastTestAt, old.LastTestOK, old.LastTestMsg
	if err := s.store.UpdateChannel(&c); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	s.reloadQuiet()
	writeJSON(w, http.StatusOK, c)
}

func (s *APIServer) deleteChannel(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.store.DeleteChannel(id); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.reloadQuiet()
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func validateChannel(c *WebhookChannel) error {
	c.Name = strings.TrimSpace(c.Name)
	// 通道仅支持 POST
	c.Method = http.MethodPost
	c.BodyTemplate = strings.TrimSpace(c.BodyTemplate)
	if !validBodyTemplate(c.BodyTemplate) {
		return errors.New("请求体模板不是合法 JSON（占位符仅支持 {title} / {content}，且需写在字符串值内）")
	}
	u, err := url.Parse(c.URL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("通道 URL 非法: %q", c.URL)
	}
	if c.Headers == nil {
		c.Headers = map[string]string{}
	}
	return nil
}

func (s *APIServer) testChannel(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	ch, err := s.store.GetChannel(id)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "通道不存在")
		return
	}
	// 用一条与真实推送同构的示例事件渲染请求体，让「测试推送」所见即所得。
	evt := Event{
		ID:         "test-" + randomToken()[:8],
		Device:     "测试摄像头",
		Transport:  "test",
		Topic:      "RuleEngine/CellMotionDetector/Motion",
		Title:      "移动侦测",
		DeviceTime: time.Now().UTC().Format(time.RFC3339Nano),
		ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	var body []byte
	if strings.TrimSpace(ch.BodyTemplate) != "" {
		body = []byte(renderBodyTemplate(ch.BodyTemplate, evt))
	} else {
		body, _ = json.Marshal(evt)
	}
	timeout := ch.TimeoutSeconds
	if timeout <= 0 {
		timeout = 5
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(timeout)*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ch.URL, bytes.NewReader(body))
	if err != nil {
		s.finishChannelTest(w, ch.ID, false, "请求构造失败: "+err.Error(), 0, body)
		return
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("User-Agent", "IpcNotice/0.2")
	for k, v := range ch.Headers {
		req.Header.Set(k, v)
	}
	client := &http.Client{Timeout: time.Duration(timeout) * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		s.finishChannelTest(w, ch.ID, false, "请求失败: "+err.Error(), 0, body)
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	ok := resp.StatusCode >= 200 && resp.StatusCode < 300
	msg := fmt.Sprintf("返回 HTTP %d", resp.StatusCode)
	s.finishChannelTest(w, ch.ID, ok, msg, resp.StatusCode, body)
}

func (s *APIServer) finishChannelTest(w http.ResponseWriter, id int64, ok bool, msg string, status int, body []byte) {
	_ = s.store.UpdateChannelTest(id, ok, msg)
	preview := string(body)
	if len(preview) > 512 {
		preview = preview[:512] + "..."
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": ok, "message": msg, "status": status, "body": preview})
}

// ---------------- monitoring ----------------

func (s *APIServer) overview(w http.ResponseWriter, _ *http.Request) {
	counts, err := s.store.Overview()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"counts": counts, "runtime": s.mgr.Status()})
}

func (s *APIServer) runtime(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.mgr.Status())
}

func (s *APIServer) reload(w http.ResponseWriter, _ *http.Request) {
	if err := s.mgr.Reload(); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, s.mgr.Status())
}

func (s *APIServer) listEvents(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	items, total, err := s.store.ListEventLogs(limit, offset)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"list": items, "total": total})
}

func (s *APIServer) reloadQuiet() {
	if err := s.mgr.Reload(); err != nil {
		s.log.Warn("配置变更后热重载失败", "err", err)
	}
}

// ---------------- static ----------------

// resolveWebFS 选择前端资源来源：若显式指定 -web-dir 且目录存在，则使用磁盘目录；
// 否则使用编译期内嵌的 web/dist（单文件部署）。
func resolveWebFS(webDir string) (fsys fs.FS, source string) {
	if webDir != "" {
		if info, err := os.Stat(webDir); err == nil && info.IsDir() {
			return os.DirFS(webDir), "dir:" + webDir
		}
		slog.Warn("指定的 -web-dir 不存在，回退到内嵌前端", "webDir", webDir)
	}
	sub, err := fs.Sub(webFS, "web/dist")
	if err != nil {
		return nil, "embed-failed:" + err.Error()
	}
	return sub, "embedded"
}

func (s *APIServer) serveStatic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.distFS == nil {
		s.serveNotBuilt(w)
		return
	}
	rel := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
	if rel == "" || rel == "." {
		rel = "index.html"
	}
	// 优先尝试直接打开请求的资源
	if f, err := s.distFS.Open(rel); err == nil {
		if stat, serr := f.Stat(); serr == nil && !stat.IsDir() {
			defer f.Close()
			s.serveFSFile(w, r, rel, f, stat)
			return
		}
		f.Close()
	}
	// SPA 回退：未知路径统一返回 index.html
	if idx, ierr := s.distFS.Open("index.html"); ierr == nil {
		if stat, serr := idx.Stat(); serr == nil {
			defer idx.Close()
			s.serveFSFile(w, r, "index.html", idx, stat)
			return
		}
		idx.Close()
	}
	s.serveNotBuilt(w)
}

// serveFSFile 以正确的 Content-Type 与范围请求支持，流式输出 fs.File。
func (s *APIServer) serveFSFile(w http.ResponseWriter, r *http.Request, name string, f fs.File, stat fs.FileInfo) {
	if ct := contentType(name); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	http.ServeContent(w, r, stat.Name(), stat.ModTime(), f.(io.ReadSeeker))
}

func (s *APIServer) serveNotBuilt(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = io.WriteString(w, `<!doctype html><meta charset="utf-8"><h2>前端尚未构建</h2>
<p>请先在 <code>web/</code> 目录执行 <code>pnpm install &amp;&amp; pnpm build</code> 构建前端，然后重新编译并运行本程序（前端已内嵌到二进制中）。</p>
<p>开发模式可直接在 <code>web/</code> 执行 <code>pnpm dev</code>，已配置 /api 代理。</p>`)
}

// contentType 按扩展名返回合适的 Content-Type；优先内置映射（跨平台更可靠），
// 未知扩展名回退到标准 mime 库。
func contentType(name string) string {
	switch ext := strings.ToLower(filepath.Ext(name)); ext {
	case ".html", ".htm":
		return "text/html; charset=utf-8"
	case ".js", ".mjs", ".cjs":
		return "application/javascript; charset=utf-8"
	case ".css":
		return "text/css; charset=utf-8"
	case ".json":
		return "application/json; charset=utf-8"
	case ".svg":
		return "image/svg+xml"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".ico":
		return "image/x-icon"
	case ".woff2":
		return "font/woff2"
	case ".woff":
		return "font/woff"
	case ".ttf", ".otf":
		return "font/opentype"
	case ".webp":
		return "image/webp"
	case ".map":
		return "application/json; charset=utf-8"
	default:
		return mime.TypeByExtension(ext)
	}
}

// ---------------- helpers ----------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg})
}

func readJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("请求体不是合法 JSON: %w", err)
	}
	return nil
}

func pathID(r *http.Request) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		return 0, errors.New("路径中的 id 非法")
	}
	return id, nil
}
