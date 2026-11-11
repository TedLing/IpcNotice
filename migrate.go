package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"os"
)

// legacyConfig 只用于把 beta 第一版 config.json 的内容迁移进数据库。
// 迁移完成后配置文件会被重写为「用户名 / 密码 / 端口」三字段的精简版。
type legacyConfig struct {
	Server struct {
		ListenAddr         string `json:"listen_addr"`
		AdvertiseHost      string `json:"advertise_host"`
		NotifyPath         string `json:"notify_path"`
		ReadTimeoutSeconds int    `json:"read_timeout_seconds"`
	} `json:"server"`
	Webhook struct {
		Enabled        bool              `json:"enabled"`
		URL            string            `json:"url"`
		TimeoutSeconds int               `json:"timeout_seconds"`
		Workers        int               `json:"workers"`
		QueueSize      int               `json:"queue_size"`
		Headers        map[string]string `json:"headers"`
	} `json:"webhook"`
	Event struct {
		Mode                string `json:"mode"`
		SubscriptionSeconds int    `json:"subscription_seconds"`
		PullTimeoutSeconds  int    `json:"pull_timeout_seconds"`
		MessageLimit        int    `json:"message_limit"`
		DedupSeconds        int    `json:"dedup_seconds"`
		EventLogFile        string `json:"event_log_file"`
	} `json:"event"`
	Devices []struct {
		Name    string `json:"name"`
		URL     string `json:"url"`
		Enabled *bool  `json:"enabled"`
	} `json:"devices"`
}

func migrateLegacy(path string, store *Store, logger *slog.Logger) (bool, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var legacy legacyConfig
	if err := json.Unmarshal(raw, &legacy); err != nil {
		return false, nil // 新格式无法解析成旧结构时交给 LoadConfig 报错
	}
	if len(legacy.Devices) == 0 {
		return false, nil // 已是精简版配置
	}

	st := DefaultSettings()
	if legacy.Server.ListenAddr != "" {
		st.NotifyListenAddr = legacy.Server.ListenAddr
	}
	if legacy.Server.AdvertiseHost != "" {
		st.AdvertiseHost = legacy.Server.AdvertiseHost
	}
	if legacy.Server.NotifyPath != "" {
		st.NotifyPath = legacy.Server.NotifyPath
	}
	if legacy.Server.ReadTimeoutSeconds > 0 {
		st.ReadTimeoutSeconds = legacy.Server.ReadTimeoutSeconds
	}
	if legacy.Event.Mode != "" {
		st.EventMode = legacy.Event.Mode
	}
	if legacy.Event.SubscriptionSeconds > 0 {
		st.SubscriptionSeconds = legacy.Event.SubscriptionSeconds
	}
	if legacy.Event.PullTimeoutSeconds > 0 {
		st.PullTimeoutSeconds = legacy.Event.PullTimeoutSeconds
	}
	if legacy.Event.MessageLimit > 0 {
		st.MessageLimit = legacy.Event.MessageLimit
	}
	st.DedupSeconds = legacy.Event.DedupSeconds
	if legacy.Event.EventLogFile != "" {
		st.EventLogFile = legacy.Event.EventLogFile
	}
	if legacy.Webhook.TimeoutSeconds > 0 {
		st.WebhookTimeoutSeconds = legacy.Webhook.TimeoutSeconds
	}
	if legacy.Webhook.Workers > 0 {
		st.WebhookWorkers = legacy.Webhook.Workers
	}
	if legacy.Webhook.QueueSize > 0 {
		st.QueueSize = legacy.Webhook.QueueSize
	}
	_ = store.SaveSettings(st)

	existing, err := store.ListCameras()
	if err != nil {
		return false, err
	}
	if len(existing) == 0 {
		for _, d := range legacy.Devices {
			enabled := d.Enabled == nil || *d.Enabled
			c := Camera{Name: d.Name, URL: d.URL, Enabled: enabled}
			if err := store.CreateCamera(&c); err != nil {
				return false, err
			}
		}
		logger.Info("已从旧版 config.json 迁移摄像头到数据库", "count", len(legacy.Devices))
	}

	if legacy.Webhook.URL != "" {
		channels, err := store.ListChannels()
		if err != nil {
			return false, err
		}
		if len(channels) == 0 {
			ch := WebhookChannel{
				Name: "默认通道", URL: legacy.Webhook.URL, Method: httpMethodPost,
				Headers: legacy.Webhook.Headers, Enabled: legacy.Webhook.Enabled,
				TimeoutSeconds: legacy.Webhook.TimeoutSeconds,
			}
			if ch.TimeoutSeconds <= 0 {
				ch.TimeoutSeconds = 5
			}
			if err := store.CreateChannel(&ch); err != nil {
				return false, err
			}
			logger.Info("已从旧版 config.json 迁移 webhook 通道", "url", ch.URL)
		}
	}

	cfg := defaultConfig()
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return false, err
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o600); err != nil {
		return false, err
	}
	logger.Warn("配置文件已精简为用户名/密码/端口，设备与通道请在管理界面维护",
		"config", path, "default_username", cfg.Username, "default_password_set", true)
	return true, nil
}
