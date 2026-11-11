package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const defaultDeviceServicePath = "/onvif/device_service"

// Config 是唯一的本地配置文件内容：登录用户名、密码、管理界面端口。
// 其余运行参数、设备、通道、订阅全部保存在 SQLite，由前端维护。
type Config struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Port     int    `json:"port"`
	Path     string `json:"-"`
}

func defaultConfig() Config {
	// 默认密码只是占位示例，首次启动会告警提示修改；真实凭据只存在被 .gitignore
	// 排除的 config.json 中，切勿把真实密码写进源码或提交到仓库。
	return Config{Username: "admin", Password: "change-me", Port: 8080}
}

// LoadConfig 读取配置；文件不存在时生成一份带默认值的配置并提示修改密码。
func LoadConfig(path string) (*Config, error) {
	cfg := defaultConfig()
	raw, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("读取配置文件 %s 失败: %w", path, err)
		}
		b, _ := json.MarshalIndent(cfg, "", "  ")
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(path, append(b, '\n'), 0o600); err != nil {
			return nil, fmt.Errorf("生成默认配置文件失败: %w", err)
		}
	} else if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件 %s 失败: %w", path, err)
	}

	cfg.Username = strings.TrimSpace(cfg.Username)
	if cfg.Username == "" {
		return nil, errors.New("config.json 中 username 不能为空")
	}
	if cfg.Password == "" {
		return nil, errors.New("config.json 中 password 不能为空")
	}
	if cfg.Port < 1 || cfg.Port > 65535 {
		return nil, fmt.Errorf("config.json 中 port 非法: %d", cfg.Port)
	}
	cfg.Path = filepath.Clean(path)
	return &cfg, nil
}

func (c *Config) Addr() string {
	return fmt.Sprintf("0.0.0.0:%d", c.Port)
}

// ServerConfig 描述 WS-BaseNotification 的 Notify 接收端。
type ServerConfig struct {
	ListenAddr         string `json:"listen_addr"`
	AdvertiseHost      string `json:"advertise_host"`
	NotifyPath         string `json:"notify_path"`
	ReadTimeoutSeconds int    `json:"read_timeout_seconds"`
}

// EventConfig 描述事件订阅参数。Mode: auto | push | pull。
type EventConfig struct {
	Mode                string `json:"mode"`
	SubscriptionSeconds int    `json:"subscription_seconds"`
	PullTimeoutSeconds  int    `json:"pull_timeout_seconds"`
	MessageLimit        int    `json:"message_limit"`
	DedupSeconds        int    `json:"dedup_seconds"`
	EventLogFile        string `json:"event_log_file"`
}

func (c EventConfig) Duration() time.Duration {
	return time.Duration(c.SubscriptionSeconds) * time.Second
}

func (c EventConfig) PullTimeout() time.Duration {
	return time.Duration(c.PullTimeoutSeconds) * time.Second
}

// Settings 是存入 SQLite 的全部系统参数，通过「系统设置」页面维护。
type Settings struct {
	NotifyListenAddr string `json:"notify_listen_addr"`
	AdvertiseHost    string `json:"advertise_host"`
	NotifyPath       string `json:"notify_path"`
	EventMode        string `json:"event_mode"`

	SubscriptionSeconds int `json:"subscription_seconds"`
	PullTimeoutSeconds  int `json:"pull_timeout_seconds"`
	MessageLimit        int `json:"message_limit"`
	DedupSeconds        int `json:"dedup_seconds"`

	WebhookTimeoutSeconds int `json:"webhook_timeout_seconds"`
	WebhookWorkers        int `json:"webhook_workers"`
	QueueSize             int `json:"queue_size"`
	ReadTimeoutSeconds    int `json:"read_timeout_seconds"`
	MaxEventLogs          int `json:"max_event_logs"`

	EventLogFile string `json:"event_log_file"`
	UpdatedAt    string `json:"updated_at,omitempty"`
}

func DefaultSettings() Settings {
	return Settings{
		NotifyListenAddr:      "0.0.0.0:9100",
		AdvertiseHost:         "auto",
		NotifyPath:            "/onvif/notify",
		EventMode:             "auto",
		SubscriptionSeconds:   60,
		PullTimeoutSeconds:    30,
		MessageLimit:          20,
		DedupSeconds:          120,
		WebhookTimeoutSeconds: 5,
		WebhookWorkers:        4,
		QueueSize:             1024,
		ReadTimeoutSeconds:    10,
		MaxEventLogs:          5000,
		EventLogFile:          "logs/events.jsonl",
	}
}

// Normalize 修正非法/缺失值，并返回校验错误。
func (s *Settings) Normalize() error {
	d := DefaultSettings()
	if strings.TrimSpace(s.NotifyListenAddr) == "" {
		s.NotifyListenAddr = d.NotifyListenAddr
	}
	if _, _, err := net.SplitHostPort(s.NotifyListenAddr); err != nil {
		return fmt.Errorf("notify_listen_addr 非法（需 host:port）: %w", err)
	}
	if strings.TrimSpace(s.AdvertiseHost) == "" {
		s.AdvertiseHost = d.AdvertiseHost
	}
	s.NotifyPath = "/" + strings.Trim(strings.TrimSpace(s.NotifyPath), "/")
	if s.NotifyPath == "/" {
		s.NotifyPath = d.NotifyPath
	}
	s.EventMode = strings.ToLower(strings.TrimSpace(s.EventMode))
	if s.EventMode == "" {
		s.EventMode = d.EventMode
	}
	switch s.EventMode {
	case "auto", "push", "pull":
	default:
		return fmt.Errorf("event_mode 必须是 auto/push/pull，当前为 %q", s.EventMode)
	}
	if s.SubscriptionSeconds < 10 {
		s.SubscriptionSeconds = d.SubscriptionSeconds
	}
	if s.PullTimeoutSeconds <= 0 || s.PullTimeoutSeconds >= s.SubscriptionSeconds {
		s.PullTimeoutSeconds = d.PullTimeoutSeconds
	}
	if s.MessageLimit <= 0 {
		s.MessageLimit = d.MessageLimit
	}
	if s.DedupSeconds < 0 {
		s.DedupSeconds = 0
	}
	if s.WebhookTimeoutSeconds <= 0 {
		s.WebhookTimeoutSeconds = d.WebhookTimeoutSeconds
	}
	if s.WebhookWorkers <= 0 {
		s.WebhookWorkers = d.WebhookWorkers
	}
	if s.QueueSize <= 0 {
		s.QueueSize = d.QueueSize
	}
	if s.ReadTimeoutSeconds <= 0 {
		s.ReadTimeoutSeconds = d.ReadTimeoutSeconds
	}
	if s.MaxEventLogs < 100 {
		s.MaxEventLogs = d.MaxEventLogs
	}
	if strings.TrimSpace(s.EventLogFile) == "" {
		s.EventLogFile = d.EventLogFile
	}
	return nil
}

func (s Settings) Server() ServerConfig {
	return ServerConfig{
		ListenAddr:         s.NotifyListenAddr,
		AdvertiseHost:      s.AdvertiseHost,
		NotifyPath:         s.NotifyPath,
		ReadTimeoutSeconds: s.ReadTimeoutSeconds,
	}
}

func (s Settings) Event() EventConfig {
	return EventConfig{
		Mode:                s.EventMode,
		SubscriptionSeconds: s.SubscriptionSeconds,
		PullTimeoutSeconds:  s.PullTimeoutSeconds,
		MessageLimit:        s.MessageLimit,
		DedupSeconds:        s.DedupSeconds,
		EventLogFile:        s.EventLogFile,
	}
}

// DeviceConfig 是一台摄像头。URL 支持 onvif://user:pass@host:port 形式。
// 实例由数据库记录临时构造，不直接落配置文件。
type DeviceConfig struct {
	ID       int64  `json:"-"`
	Name     string `json:"name"`
	URL      string `json:"url"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`

	Endpoint   string `json:"-"`
	Host       string `json:"-"`
	DeviceUser string `json:"-"`
	DevicePass string `json:"-"`
	// SubManager 是上次运行持久化的设备端 Push 订阅管理地址（cameras.sub_manager），
	// 建立新订阅前先用它尽力退订崩溃残留的订阅。
	SubManager string `json:"-"`
}

func (d *DeviceConfig) MaskedURL() string {
	u, err := url.Parse(d.URL)
	if err != nil {
		return "<invalid url>"
	}
	if u.User != nil {
		return fmt.Sprintf("%s://%s:***@%s%s", u.Scheme, u.User.Username(), u.Host, u.Path)
	}
	return u.String()
}

// resolve 把 onvif://、http(s):// 或裸 host:port 统一解析为 device_service URL，
// 并从 userinfo 中提取鉴权信息（显式 username/password 字段优先级更高）。
func (d *DeviceConfig) resolve() error {
	raw := strings.TrimSpace(d.URL)
	if raw == "" {
		return errors.New("连接地址为空")
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("URL 解析失败: %w", err)
	}
	switch u.Scheme {
	case "onvif", "http", "https":
	default:
		return fmt.Errorf("不支持的 scheme %q（支持 onvif/http/https）", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("URL 缺少 host")
	}

	user, pass := d.Username, d.Password
	if u.User != nil {
		if user == "" {
			user = u.User.Username()
		}
		if p, ok := u.User.Password(); ok && pass == "" {
			pass = p
		}
	}
	if user == "" || pass == "" {
		return errors.New("缺少 ONVIF 凭据（写在 URL 的 userinfo 或单独的用户名/密码字段）")
	}

	scheme := u.Scheme
	if scheme == "onvif" {
		scheme = "http"
	}
	path := u.Path
	if path == "" || path == "/" {
		path = defaultDeviceServicePath
	}
	d.Endpoint = (&url.URL{Scheme: scheme, Host: u.Host, Path: path}).String()
	d.Host = u.Hostname()
	d.DeviceUser = user
	d.DevicePass = pass
	return nil
}
