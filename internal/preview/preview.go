// Package preview 实现 WebRTC 视频预览中继：
// 浏览器 <--WebRTC/H.264--> IpcNotice <--RTSP/H.264--> 摄像头。
// 不转码、不落盘、不入订阅链路；每个预览是独立会话，关闭弹窗即释放设备 RTSP 连接。
//
// 注意：TP-Link 这类设备主码流常为 H.265，而浏览器 WebRTC 普遍只支持 H.264，
// 因此 OpenSource 会遍历所有 ONVIF Profile，自动挑第一个 H.264 码流（通常是子码流）。
package preview

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"IpcNotice/internal/config"
	"IpcNotice/internal/onvifcore"
	"IpcNotice/internal/util"
	"github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v3"
)

const (
	MaxSessions  = 6                // 全局并发预览上限，避免拉爆摄像头 RTSP 连接数
	MaxPerCamera = 2                // 单台摄像头并发预览上限
	MaxLifetime  = 30 * time.Minute // 会话兜底存活时间（前端异常未关闭时自动回收）
	OfferLimit   = 64 << 10         // SDP 请求体上限

	previewSetupTimeout = 10 * time.Second // 单次 RTSP DESCRIBE / SETUP 超时
)

// ---------------- RTSP 源 ----------------

// Source 表示一条已 DESCRIBE + SETUP 视频轨、尚未 PLAY 的 RTSP 连接。
type Source struct {
	client    *gortsplib.Client
	media     *description.Media
	format    *format.H264
	streamURL string // 已遮蔽密码，仅用于日志与前端展示
	profile   string

	closeOnce sync.Once
}

// Profile 返回选定的 ONVIF Profile token。
func (s *Source) Profile() string { return s.profile }

// StreamURL 返回遮蔽密码后的 RTSP 地址（仅展示用）。
func (s *Source) StreamURL() string { return s.streamURL }

// Close 释放底层 RTSP 连接（幂等）。
func (s *Source) Close() {
	s.closeOnce.Do(func() {
		if s.client != nil {
			s.client.Close()
		}
	})
}

// OpenSource 通过 ONVIF GetProfiles/GetStreamUri 找到一条 H.264 码流并建好 RTSP 会话。
// 单项失败只影响该 Profile，不阻断其他 Profile；全部失败时返回可读原因。
func OpenSource(ctx context.Context, dev *config.DeviceConfig, log *slog.Logger) (*Source, error) {
	cli, err := onvifcore.NewDeviceClient(dev)
	if err != nil {
		return nil, fmt.Errorf("创建 ONVIF 客户端失败: %w", err)
	}
	initCtx, initCancel := context.WithTimeout(ctx, 15*time.Second)
	err = cli.Initialize(initCtx)
	initCancel()
	if err != nil {
		return nil, fmt.Errorf("连接设备失败（检查地址、账号与设备时钟）: %w", err)
	}

	profiles, err := cli.Media().GetProfiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("ONVIF GetProfiles 失败: %w", err)
	}
	if len(profiles) == 0 {
		return nil, errors.New("设备未返回任何媒体 Profile")
	}

	var lastErr error
	tried := 0
	for _, p := range profiles {
		if ctx.Err() != nil {
			break
		}
		uriCtx, uriCancel := context.WithTimeout(ctx, previewSetupTimeout)
		mediaURI, err := cli.Media().GetStreamURI(uriCtx, p.Token)
		uriCancel()
		if err != nil {
			lastErr = fmt.Errorf("%s GetStreamUri: %w", p.Token, err)
			continue
		}
		rtspURL, masked, err := RTSPURL(mediaURI.URI, dev)
		if err != nil {
			lastErr = fmt.Errorf("%s 地址非法: %w", p.Token, err)
			continue
		}
		tried++
		src, err := setupStream(rtspURL, masked, p.Token)
		if err != nil {
			lastErr = err
			continue
		}
		log.Info("预览码流已选定", "device", dev.Name, "profile", p.Token,
			"codec", "H.264", "stream", masked,
			"packetization_mode", src.format.PacketizationMode)
		return src, nil
	}
	if lastErr == nil {
		lastErr = errors.New("未找到可用码流")
	}
	return nil, fmt.Errorf("未找到浏览器可播放的 H.264 码流（WebRTC 不支持 H.265；已检查 %d 个 Profile）: %w",
		tried, lastErr)
}

// RTSPURL 把 GetStreamUri 返回值补全凭据（设备通常不在 URI 里带账号密码）。
func RTSPURL(raw string, dev *config.DeviceConfig) (*base.URL, string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, "", err
	}
	if u.Scheme != "rtsp" && u.Scheme != "rtsps" {
		return nil, "", fmt.Errorf("不支持的流协议 %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, "", errors.New("流地址缺少主机")
	}
	if u.User == nil {
		u.User = url.UserPassword(dev.DeviceUser, dev.DevicePass)
	}
	username := ""
	if u.User != nil {
		username = u.User.Username()
	}
	masked := fmt.Sprintf("%s://%s:***@%s%s", u.Scheme, username, u.Host, u.EscapedPath())
	if u.RawQuery != "" {
		masked += "?" + u.RawQuery
	}
	ru, err := base.ParseURL(u.String())
	if err != nil {
		return nil, "", err
	}
	return ru, masked, nil
}

// setupStream 用 RTSP over TCP 取 SDP、找 H.264 轨并 SETUP。
// TCP 交织比 UDP 更容易穿透本机/局域网防火墙，且 TP-Link 实测稳定。
func setupStream(u *base.URL, masked, profile string) (*Source, error) {
	tcp := gortsplib.ProtocolTCP
	c := &gortsplib.Client{
		Scheme:       u.Scheme,
		Host:         u.Host,
		Protocol:     &tcp,
		ReadTimeout:  previewSetupTimeout,
		WriteTimeout: previewSetupTimeout,
		UserAgent:    "IpcNotice",
	}
	if err := c.Start(); err != nil {
		return nil, fmt.Errorf("%s RTSP 连接失败: %w", profile, err)
	}
	desc, _, err := c.Describe(u)
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("%s RTSP DESCRIBE 失败: %w", profile, err)
	}
	var forma *format.H264
	medi := desc.FindFormat(&forma)
	if medi == nil {
		c.Close()
		return nil, fmt.Errorf("%s 不含 H.264 视频轨（多为 H.265，浏览器不支持）", profile)
	}
	if _, err := c.Setup(desc.BaseURL, medi, 0, 0); err != nil {
		c.Close()
		return nil, fmt.Errorf("%s RTSP SETUP 失败: %w", profile, err)
	}
	return &Source{
		client: c, media: medi, format: forma, streamURL: masked, profile: profile,
	}, nil
}

// ---------------- WebRTC 会话管理 ----------------

type Session struct {
	id       string
	cameraID int64
	device   string
	pc       *webrtc.PeerConnection
	src      *Source
	timer    *time.Timer

	closeOnce sync.Once
}

// ID 返回会话标识（前端关闭时回传）。
func (s *Session) ID() string { return s.id }

// Close 释放会话资源：定时器、WebRTC 连接与 RTSP 源（幂等）。
func (s *Session) Close() {
	s.closeOnce.Do(func() {
		if s.timer != nil {
			s.timer.Stop()
		}
		if s.pc != nil {
			_ = s.pc.Close()
		}
		if s.src != nil {
			s.src.Close()
		}
	})
}

// Hub 管理全部预览会话的生命周期与并发上限。
type Hub struct {
	log      *slog.Logger
	mu       sync.Mutex
	sessions map[string]*Session
}

// NewHub 创建预览会话中心。
func NewHub(log *slog.Logger) *Hub {
	return &Hub{log: log.With("component", "preview"), sessions: map[string]*Session{}}
}

// Add 登记一个新会话，超限时返回错误（调用方负责 close 会话）。
func (h *Hub) Add(s *Session) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.sessions) >= MaxSessions {
		return fmt.Errorf("预览会话数已达上限 %d，请关闭其他预览", MaxSessions)
	}
	perCamera := 0
	for _, v := range h.sessions {
		if v.cameraID == s.cameraID {
			perCamera++
		}
	}
	if perCamera >= MaxPerCamera {
		return fmt.Errorf("该摄像头的并发预览已达上限 %d", MaxPerCamera)
	}
	h.sessions[s.id] = s
	return nil
}

// remove 从表中摘除并释放资源；close() 触发的 ICE 状态回调会再次调用，因 id 已删除而不会递归。
func (h *Hub) remove(id string) {
	h.mu.Lock()
	s, ok := h.sessions[id]
	if ok {
		delete(h.sessions, id)
	}
	h.mu.Unlock()
	if ok {
		s.Close()
		h.log.Info("预览会话已结束", "device", s.device, "session", id)
	}
}

// RemoveIf 仅当会话 id 匹配该摄像头时才结束会话（关闭预览接口按设备定位）。
func (h *Hub) RemoveIf(id string, cameraID int64) {
	h.mu.Lock()
	s, ok := h.sessions[id]
	h.mu.Unlock()
	if ok && s.cameraID == cameraID {
		h.remove(id)
	}
}

// CloseAll 结束全部预览会话（服务关闭时调用）。
func (h *Hub) CloseAll() {
	h.mu.Lock()
	all := make([]*Session, 0, len(h.sessions))
	for id, s := range h.sessions {
		all = append(all, s)
		delete(h.sessions, id)
	}
	h.mu.Unlock()
	for _, s := range all {
		s.Close()
	}
}

// Start 完成一次 offer/answer 协商并启动 RTP 转发，返回 answer SDP。
func (h *Hub) Start(
	ctx context.Context,
	dev *config.DeviceConfig,
	offerSDP string,
	src *Source,
) (*Session, string, error) {
	m := &webrtc.MediaEngine{}
	if err := m.RegisterDefaultCodecs(); err != nil {
		return nil, "", fmt.Errorf("初始化 WebRTC 编解码器失败: %w", err)
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(m))
	// 不配置 STUN/TURN：管理界面与摄像头都在局域网内，host candidate 足够；
	// 也避免依赖外网导致 ICE 收集变慢。
	pc, err := api.NewPeerConnection(webrtc.Configuration{ICEServers: []webrtc.ICEServer{}})
	if err != nil {
		return nil, "", fmt.Errorf("创建 WebRTC 连接失败: %w", err)
	}
	cleanup := func() { _ = pc.Close() }

	track, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000},
		"video", "ipcnotice-preview")
	if err != nil {
		cleanup()
		return nil, "", fmt.Errorf("创建视频轨失败: %w", err)
	}
	sender, err := pc.AddTrack(track)
	if err != nil {
		cleanup()
		return nil, "", fmt.Errorf("绑定视频轨失败: %w", err)
	}
	// 必须持续读 RTCP，否则 pion 内部缓冲写满后会影响发送。
	go func() {
		buf := make([]byte, 1500)
		for {
			if _, _, readErr := sender.Read(buf); readErr != nil {
				return
			}
		}
	}()

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer, SDP: offerSDP,
	}); err != nil {
		cleanup()
		return nil, "", fmt.Errorf("解析浏览器 offer（SDP）失败，请确认 offer 非空且完整（常见原因：请求体被截断或缺少结尾换行）: %w", err)
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		cleanup()
		return nil, "", fmt.Errorf("生成 WebRTC answer 失败: %w", err)
	}
	gatherComplete := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		cleanup()
		return nil, "", fmt.Errorf("设置本地 SDP 失败: %w", err)
	}
	select {
	case <-gatherComplete:
	case <-ctx.Done():
		cleanup()
		return nil, "", errors.New("ICE 候选收集被取消")
	case <-time.After(10 * time.Second):
		// 超时仍返回已收集的候选，浏览器侧可继续尝试连接。
	}
	local := pc.LocalDescription()
	if local == nil || local.SDP == "" {
		cleanup()
		return nil, "", errors.New("未能生成 WebRTC answer")
	}

	sess := &Session{
		id: util.RandomToken(), cameraID: dev.ID, device: dev.Name, pc: pc, src: src,
	}
	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		switch state {
		case webrtc.ICEConnectionStateFailed, webrtc.ICEConnectionStateClosed:
			h.remove(sess.id)
		case webrtc.ICEConnectionStateDisconnected:
			// 网络抖动可能短暂断开，给 15s 恢复窗口，避免误杀预览。
			time.AfterFunc(15*time.Second, func() {
				if pc.ICEConnectionState() == webrtc.ICEConnectionStateDisconnected {
					h.remove(sess.id)
				}
			})
		}
	})

	// RTSP 的 RTP 包直接写入 WebRTC 轨：不解码、不转码。
	// pion 的 TrackLocalStaticRTP 会重写 SSRC 与 PayloadType，序列号/时间戳保持不变。
	var injected atomic.Bool
	src.client.OnPacketRTP(src.media, src.format, func(pkt *rtp.Packet) {
		if injected.CompareAndSwap(false, true) {
			if stap := StapA(src.format.SPS, src.format.PPS, pkt); stap != nil {
				_ = track.WriteRTP(stap)
			}
		}
		_ = track.WriteRTP(pkt)
	})
	if _, err := src.client.Play(nil); err != nil {
		cleanup()
		return nil, "", fmt.Errorf("RTSP PLAY 失败: %w", err)
	}

	sess.timer = time.AfterFunc(MaxLifetime, func() { h.remove(sess.id) })
	h.log.Info("预览会话已建立", "device", dev.Name, "session", sess.id,
		"profile", src.profile, "stream", src.streamURL)
	return sess, local.SDP, nil
}

// StapA 用 SDP 里的 SPS/PPS 构造一个 STAP-A 包，插在第一个 RTP 包之前，
// 让浏览器解码器立刻拿到参数集（部分摄像头只在关键帧前才带，启动会黑屏数秒）。
// 序列号取首包 -1，保证单调递增；PayloadType 会被 pion 重写。
func StapA(sps, pps []byte, ref *rtp.Packet) *rtp.Packet {
	if len(sps) == 0 || len(pps) == 0 || ref == nil {
		return nil
	}
	payload := make([]byte, 1+2+len(sps)+2+len(pps))
	payload[0] = 0x78 // F=0, NRI=3, Type=24 (STAP-A)
	binary.BigEndian.PutUint16(payload[1:3], uint16(len(sps)))
	copy(payload[3:], sps)
	off := 3 + len(sps)
	binary.BigEndian.PutUint16(payload[off:off+2], uint16(len(pps)))
	copy(payload[off+2:], pps)

	hdr := ref.Header // 值拷贝，不修改原包
	hdr.SequenceNumber--
	hdr.Marker = false
	hdr.Padding = false
	hdr.Extension = false
	hdr.Extensions = nil
	return &rtp.Packet{Header: hdr, Payload: payload}
}
