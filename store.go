package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const databaseFile = "data/ipcnotice.db"

// Camera 是一台 ONVIF 摄像头（数据库记录）。
type Camera struct {
	ID           int64  `json:"id"`
	Name         string `json:"name"`
	URL          string `json:"url"`
	Username     string `json:"username"`
	Password     string `json:"password"`
	Enabled      bool   `json:"enabled"`
	Manufacturer string `json:"manufacturer"`
	Model        string `json:"model"`
	Firmware     string `json:"firmware"`
	Serial       string `json:"serial"`
	LastTestAt   string `json:"last_test_at"`
	LastTestOK   bool   `json:"last_test_ok"`
	LastTestMsg  string `json:"last_test_msg"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
	// SubManager 是设备端 Push 订阅的订阅管理地址（Subscribe 响应返回）。
	// 正常退出时清空；进程崩溃时保留，供下次启动先退订残留订阅。
	SubManager string `json:"sub_manager"`
}

// Device 把数据库记录转换为运行期设备配置。
func (c Camera) Device() (*DeviceConfig, error) {
	d := &DeviceConfig{ID: c.ID, Name: c.Name, URL: c.URL, Username: c.Username,
		Password: c.Password, SubManager: c.SubManager}
	if err := d.resolve(); err != nil {
		return nil, err
	}
	return d, nil
}

// WebhookChannel 是一条通知渠道。
type WebhookChannel struct {
	ID      int64             `json:"id"`
	Name    string            `json:"name"`
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	// BodyTemplate 是用户自定义的请求体模板（JSON 文本，占位符仅支持 {title} / {content}）。
	// 留空时推送完整事件 JSON（向后兼容）。
	BodyTemplate   string `json:"body_template"`
	Enabled        bool   `json:"enabled"`
	TimeoutSeconds int    `json:"timeout_seconds"`
	LastTestAt     string `json:"last_test_at"`
	LastTestOK     bool   `json:"last_test_ok"`
	LastTestMsg    string `json:"last_test_msg"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
}

// CameraTopic 是一次事件抓取得到的主题，或运行时观察到的真实推送主题。
// Remark 是用户为该事件起的中文名称/说明，会作为 title 随 webhook 一起推送。
type CameraTopic struct {
	ID           int64  `json:"id"`
	CameraID     int64  `json:"camera_id"`
	Topic        string `json:"topic"`
	Source       string `json:"source"` // declared（TopicSet）| observed（真实推送）| custom（自定义）
	Remark       string `json:"remark"`
	DiscoveredAt string `json:"discovered_at"`
}

// TopicRemarkInput 是前端批量保存事件备注的入参。
type TopicRemarkInput struct {
	Topic  string `json:"topic"`
	Remark string `json:"remark"`
}

// Subscription 是「某台摄像头的某个事件主题」的订阅，含通知渠道。
type Subscription struct {
	ID         int64   `json:"id"`
	CameraID   int64   `json:"camera_id"`
	CameraName string  `json:"camera_name,omitempty"`
	Topic      string  `json:"topic"`
	Enabled    bool    `json:"enabled"`
	ChannelIDs []int64 `json:"channel_ids"`
	CreatedAt  string  `json:"created_at"`
	UpdatedAt  string  `json:"updated_at"`
}

// SubscriptionInput 是前端批量保存订阅的入参。
type SubscriptionInput struct {
	Topic      string  `json:"topic"`
	Enabled    bool    `json:"enabled"`
	ChannelIDs []int64 `json:"channel_ids"`
}

// EventLog 是事件落库记录。
type EventLog struct {
	ID         int64           `json:"id"`
	EventID    string          `json:"event_id"`
	CameraID   int64           `json:"camera_id"`
	Device     string          `json:"device"`
	Topic      string          `json:"topic"`
	Transport  string          `json:"transport"`
	Payload    json.RawMessage `json:"payload"`
	Matched    bool            `json:"matched"`
	Dispatched bool            `json:"dispatched"`
	Channels   string          `json:"channels"`
	CreatedAt  string          `json:"created_at"`
}

// Store 封装 SQLite 访问。单连接 + WAL 足以支撑本应用的写入量，
// 同时避免多线程并发写造成的 SQLITE_BUSY。
type Store struct {
	db *sql.DB
}

func OpenStore(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("创建数据目录失败: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("打开 SQLite 失败: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, pragma := range []string{
		"PRAGMA busy_timeout=10000",
		"PRAGMA foreign_keys=ON",
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
	} {
		if _, err := db.Exec(pragma); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("执行 %s 失败: %w", pragma, err)
		}
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	const schema = `
CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cameras (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  url TEXT NOT NULL,
  username TEXT NOT NULL DEFAULT '',
  password TEXT NOT NULL DEFAULT '',
  enabled INTEGER NOT NULL DEFAULT 1,
  manufacturer TEXT NOT NULL DEFAULT '',
  model TEXT NOT NULL DEFAULT '',
  firmware TEXT NOT NULL DEFAULT '',
  serial TEXT NOT NULL DEFAULT '',
  last_test_at TEXT NOT NULL DEFAULT '',
  last_test_ok INTEGER NOT NULL DEFAULT 0,
  last_test_msg TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  sub_manager TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS webhook_channels (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  url TEXT NOT NULL,
  method TEXT NOT NULL DEFAULT 'POST',
  headers_json TEXT NOT NULL DEFAULT '{}',
  body_template TEXT NOT NULL DEFAULT '',
  enabled INTEGER NOT NULL DEFAULT 1,
  timeout_seconds INTEGER NOT NULL DEFAULT 5,
  last_test_at TEXT NOT NULL DEFAULT '',
  last_test_ok INTEGER NOT NULL DEFAULT 0,
  last_test_msg TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS camera_topics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  camera_id INTEGER NOT NULL REFERENCES cameras(id) ON DELETE CASCADE,
  topic TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'declared',
  remark TEXT NOT NULL DEFAULT '',
  discovered_at TEXT NOT NULL,
  UNIQUE(camera_id, topic)
);
CREATE TABLE IF NOT EXISTS event_subscriptions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  camera_id INTEGER NOT NULL REFERENCES cameras(id) ON DELETE CASCADE,
  topic TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(camera_id, topic)
);
CREATE TABLE IF NOT EXISTS subscription_channels (
  subscription_id INTEGER NOT NULL REFERENCES event_subscriptions(id) ON DELETE CASCADE,
  channel_id INTEGER NOT NULL REFERENCES webhook_channels(id) ON DELETE CASCADE,
  PRIMARY KEY(subscription_id, channel_id)
);
CREATE TABLE IF NOT EXISTS event_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL,
  camera_id INTEGER NOT NULL DEFAULT 0,
  device TEXT NOT NULL,
  topic TEXT NOT NULL,
  transport TEXT NOT NULL,
  payload TEXT NOT NULL,
  matched INTEGER NOT NULL DEFAULT 0,
  dispatched INTEGER NOT NULL DEFAULT 0,
  channels TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_event_logs_created ON event_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_topics_camera ON camera_topics(camera_id);
CREATE INDEX IF NOT EXISTS idx_subs_camera ON event_subscriptions(camera_id);
`
	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("初始化数据库表失败: %w", err)
	}
	// 旧库的 camera_topics 没有 remark 列，CREATE TABLE IF NOT EXISTS 不会补列。
	if err := s.ensureTopicRemarkColumn(); err != nil {
		return err
	}
	if err := s.ensureChannelBodyTemplateColumn(); err != nil {
		return err
	}
	if err := s.ensureCameraSubManagerColumn(); err != nil {
		return err
	}
	if _, err := s.Settings(); err != nil {
		_ = s.SaveSettings(DefaultSettings())
	}
	return nil
}

// ensureTopicRemarkColumn 幂等地为旧数据库补上 camera_topics.remark。
func (s *Store) ensureTopicRemarkColumn() error {
	rows, err := s.db.Query(`PRAGMA table_info(camera_topics)`)
	if err != nil {
		return fmt.Errorf("读取 camera_topics 表结构失败: %w", err)
	}
	found := false
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt any
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			_ = rows.Close()
			return err
		}
		if name == "remark" {
			found = true
		}
	}
	err = rows.Err()
	_ = rows.Close() // 单连接：必须关闭后才能执行下面的 ALTER
	if err != nil {
		return err
	}
	if found {
		return nil
	}
	if _, err := s.db.Exec(`ALTER TABLE camera_topics ADD COLUMN remark TEXT NOT NULL DEFAULT ''`); err != nil {
		return fmt.Errorf("为 camera_topics 增加 remark 列失败: %w", err)
	}
	// 旧库已有主题补一次中文名；之后不再回填，避免覆盖用户主动清空的备注。
	return s.backfillTopicRemarks()
}

// ensureChannelBodyTemplateColumn 幂等地为旧数据库补上 webhook_channels.body_template。
func (s *Store) ensureChannelBodyTemplateColumn() error {
	rows, err := s.db.Query(`PRAGMA table_info(webhook_channels)`)
	if err != nil {
		return fmt.Errorf("读取 webhook_channels 表结构失败: %w", err)
	}
	found := false
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt any
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			_ = rows.Close()
			return err
		}
		if name == "body_template" {
			found = true
		}
	}
	err = rows.Err()
	_ = rows.Close() // 单连接：必须关闭后才能执行下面的 ALTER
	if err != nil {
		return err
	}
	if found {
		return nil
	}
	if _, err := s.db.Exec(`ALTER TABLE webhook_channels ADD COLUMN body_template TEXT NOT NULL DEFAULT ''`); err != nil {
		return fmt.Errorf("为 webhook_channels 增加 body_template 列失败: %w", err)
	}
	return nil
}

// ensureCameraSubManagerColumn 幂等地为旧数据库补上 cameras.sub_manager。
// 该列持久化设备端 Push 订阅管理地址，服务异常退出后下次启动可先据此退订残留订阅。
func (s *Store) ensureCameraSubManagerColumn() error {
	rows, err := s.db.Query(`PRAGMA table_info(cameras)`)
	if err != nil {
		return fmt.Errorf("读取 cameras 表结构失败: %w", err)
	}
	found := false
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt any
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			_ = rows.Close()
			return err
		}
		if name == "sub_manager" {
			found = true
		}
	}
	err = rows.Err()
	_ = rows.Close() // 单连接：必须关闭后才能执行下面的 ALTER
	if err != nil {
		return err
	}
	if found {
		return nil
	}
	if _, err := s.db.Exec(`ALTER TABLE cameras ADD COLUMN sub_manager TEXT NOT NULL DEFAULT ''`); err != nil {
		return fmt.Errorf("为 cameras 增加 sub_manager 列失败: %w", err)
	}
	return nil
}

// backfillTopicRemarks 仅用于旧库升级：为还没有备注的历史主题填一次默认中文名。
func (s *Store) backfillTopicRemarks() error {
	rows, err := s.db.Query(`SELECT id, topic FROM camera_topics WHERE remark=''`)
	if err != nil {
		return err
	}
	type item struct {
		id     int64
		remark string
	}
	updates := []item{}
	for rows.Next() {
		var id int64
		var topic string
		if err := rows.Scan(&id, &topic); err != nil {
			_ = rows.Close()
			return err
		}
		if r := defaultTopicRemark(topic); r != "" {
			updates = append(updates, item{id, r})
		}
	}
	err = rows.Err()
	_ = rows.Close() // 单连接：必须关闭后才能执行写操作
	if err != nil || len(updates) == 0 {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`UPDATE camera_topics SET remark=? WHERE id=?`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, u := range updates {
		if _, err := stmt.Exec(u.remark, u.id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func now() string { return time.Now().UTC().Format(time.RFC3339) }

func b2i(v bool) int {
	if v {
		return 1
	}
	return 0
}

// ---------------- settings ----------------

func (s *Store) Settings() (Settings, error) {
	var raw string
	err := s.db.QueryRow(`SELECT value FROM settings WHERE key='app'`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return DefaultSettings(), nil
	}
	if err != nil {
		return Settings{}, err
	}
	st := DefaultSettings()
	if err := json.Unmarshal([]byte(raw), &st); err != nil {
		return Settings{}, fmt.Errorf("settings 记录损坏: %w", err)
	}
	if err := st.Normalize(); err != nil {
		return Settings{}, err
	}
	return st, nil
}

func (s *Store) SaveSettings(st Settings) error {
	if err := st.Normalize(); err != nil {
		return err
	}
	st.UpdatedAt = now()
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT INTO settings(key,value,updated_at) VALUES('app',?,?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at`,
		string(b), st.UpdatedAt)
	return err
}

// ---------------- cameras ----------------

const cameraColumns = `id,name,url,username,password,enabled,manufacturer,model,firmware,serial,
 last_test_at,last_test_ok,last_test_msg,created_at,updated_at,sub_manager`

func scanCamera(row interface{ Scan(...any) error }) (Camera, error) {
	var c Camera
	var enabled, testOK int
	err := row.Scan(&c.ID, &c.Name, &c.URL, &c.Username, &c.Password, &enabled,
		&c.Manufacturer, &c.Model, &c.Firmware, &c.Serial,
		&c.LastTestAt, &testOK, &c.LastTestMsg, &c.CreatedAt, &c.UpdatedAt, &c.SubManager)
	c.Enabled = enabled != 0
	c.LastTestOK = testOK != 0
	return c, err
}

func (s *Store) ListCameras() ([]Camera, error) {
	rows, err := s.db.Query(`SELECT ` + cameraColumns + ` FROM cameras ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Camera{}
	for rows.Next() {
		c, err := scanCamera(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) GetCamera(id int64) (Camera, error) {
	return scanCamera(s.db.QueryRow(`SELECT `+cameraColumns+` FROM cameras WHERE id=?`, id))
}

func (s *Store) CreateCamera(c *Camera) error {
	c.Name = strings.TrimSpace(c.Name)
	if c.Name == "" {
		return errors.New("设备名称不能为空")
	}
	if c.URL == "" {
		return errors.New("连接地址不能为空")
	}
	c.CreatedAt, c.UpdatedAt = now(), now()
	res, err := s.db.Exec(`INSERT INTO cameras
		(name,url,username,password,enabled,manufacturer,model,firmware,serial,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		c.Name, c.URL, c.Username, c.Password, b2i(c.Enabled),
		c.Manufacturer, c.Model, c.Firmware, c.Serial, c.CreatedAt, c.UpdatedAt)
	if err != nil {
		return err
	}
	c.ID, err = res.LastInsertId()
	return err
}

func (s *Store) UpdateCamera(c *Camera) error {
	c.Name = strings.TrimSpace(c.Name)
	if c.Name == "" {
		return errors.New("设备名称不能为空")
	}
	c.UpdatedAt = now()
	_, err := s.db.Exec(`UPDATE cameras SET name=?,url=?,username=?,password=?,enabled=?,
		manufacturer=?,model=?,firmware=?,serial=?,updated_at=? WHERE id=?`,
		c.Name, c.URL, c.Username, c.Password, b2i(c.Enabled),
		c.Manufacturer, c.Model, c.Firmware, c.Serial, c.UpdatedAt, c.ID)
	return err
}

func (s *Store) DeleteCamera(id int64) error {
	_, err := s.db.Exec(`DELETE FROM cameras WHERE id=?`, id)
	return err
}

// UpdateCameraTest 记录一次连接测试结果；info 为空时只更新状态文本。
func (s *Store) UpdateCameraTest(id int64, ok bool, msg, manufacturer, model, firmware, serial string) error {
	_, err := s.db.Exec(`UPDATE cameras SET last_test_at=?,last_test_ok=?,last_test_msg=?,
		manufacturer=?,model=?,firmware=?,serial=?,updated_at=? WHERE id=?`,
		now(), b2i(ok), msg, manufacturer, model, firmware, serial, now(), id)
	return err
}

// SetCameraSubManager 记录或清除设备端 Push 订阅管理地址（不动 updated_at）。
func (s *Store) SetCameraSubManager(id int64, ref string) error {
	_, err := s.db.Exec(`UPDATE cameras SET sub_manager=? WHERE id=?`, ref, id)
	return err
}

// ---------------- webhook channels ----------------

func scanChannel(row interface{ Scan(...any) error }) (WebhookChannel, error) {
	var c WebhookChannel
	var headers string
	var enabled, testOK int
	err := row.Scan(&c.ID, &c.Name, &c.URL, &c.Method, &headers, &c.BodyTemplate, &enabled,
		&c.TimeoutSeconds, &c.LastTestAt, &testOK, &c.LastTestMsg, &c.CreatedAt, &c.UpdatedAt)
	if err != nil {
		return c, err
	}
	c.Enabled = enabled != 0
	c.LastTestOK = testOK != 0
	c.Headers = map[string]string{}
	if headers != "" {
		_ = json.Unmarshal([]byte(headers), &c.Headers)
	}
	if c.Headers == nil {
		c.Headers = map[string]string{}
	}
	return c, nil
}

const channelColumns = `id,name,url,method,headers_json,body_template,enabled,timeout_seconds,
 last_test_at,last_test_ok,last_test_msg,created_at,updated_at`

func (s *Store) ListChannels() ([]WebhookChannel, error) {
	rows, err := s.db.Query(`SELECT ` + channelColumns + ` FROM webhook_channels ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []WebhookChannel{}
	for rows.Next() {
		c, err := scanChannel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) GetChannel(id int64) (WebhookChannel, error) {
	return scanChannel(s.db.QueryRow(`SELECT `+channelColumns+` FROM webhook_channels WHERE id=?`, id))
}

func (s *Store) CreateChannel(c *WebhookChannel) error {
	c.Name = strings.TrimSpace(c.Name)
	if c.Name == "" {
		return errors.New("通道名称不能为空")
	}
	if c.URL == "" {
		return errors.New("通道 URL 不能为空")
	}
	if c.Method == "" {
		c.Method = httpMethodPost
	}
	if c.TimeoutSeconds <= 0 {
		c.TimeoutSeconds = 5
	}
	headers, _ := json.Marshal(c.Headers)
	c.CreatedAt, c.UpdatedAt = now(), now()
	res, err := s.db.Exec(`INSERT INTO webhook_channels
		(name,url,method,headers_json,body_template,enabled,timeout_seconds,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?)`,
		c.Name, c.URL, c.Method, string(headers), c.BodyTemplate, b2i(c.Enabled), c.TimeoutSeconds,
		c.CreatedAt, c.UpdatedAt)
	if err != nil {
		return err
	}
	c.ID, err = res.LastInsertId()
	return err
}

func (s *Store) UpdateChannel(c *WebhookChannel) error {
	headers, _ := json.Marshal(c.Headers)
	c.UpdatedAt = now()
	_, err := s.db.Exec(`UPDATE webhook_channels SET name=?,url=?,method=?,headers_json=?,body_template=?,
		enabled=?,timeout_seconds=?,updated_at=? WHERE id=?`,
		c.Name, c.URL, c.Method, string(headers), c.BodyTemplate, b2i(c.Enabled), c.TimeoutSeconds, c.UpdatedAt, c.ID)
	return err
}

func (s *Store) DeleteChannel(id int64) error {
	_, err := s.db.Exec(`DELETE FROM webhook_channels WHERE id=?`, id)
	return err
}

func (s *Store) UpdateChannelTest(id int64, ok bool, msg string) error {
	_, err := s.db.Exec(`UPDATE webhook_channels SET last_test_at=?,last_test_ok=?,last_test_msg=?,updated_at=? WHERE id=?`,
		now(), b2i(ok), msg, now(), id)
	return err
}

// ---------------- topics ----------------

// topicRemarkLabels 为新抓取到的事件预填中文名称，用户可在界面「事件订阅」里随时修改。
// 匹配 normalizeTopic 后的路径段（从末段往前），命中第一个即返回。
var topicRemarkLabels = map[string]string{
	"motion":               "移动侦测",
	"motionalarm":          "移动侦测告警",
	"cellmotiondetector":   "移动侦测",
	"motiondetector":       "移动侦测",
	"intrusion":            "区域入侵检测",
	"intrusiondetector":    "区域入侵检测",
	"fielddetector":        "区域入侵检测",
	"people":               "人行检测",
	"peopledetector":       "人行检测",
	"peopledetection":      "人行检测",
	"humandetect":          "人形检测",
	"pedestrian":           "人行检测",
	"linecross":            "越界检测",
	"linecrossdetector":    "越界检测",
	"linecrossing":         "越界检测",
	"linecrossingdetector": "越界检测",
	"linedetector":         "越界检测",
	"tamper":               "遮挡告警",
	"tamperdetector":       "遮挡告警",
	"tpsmartevent":         "智能事件",
	"tpsmarteventdetector": "智能事件",
	"smartevent":           "智能事件",
	"face":                 "人脸检测",
	"facedetection":        "人脸检测",
	"audio":                "声音检测",
	"audiodetection":       "声音检测",
	"alarm":                "报警输入",
	"digitalinput":         "报警输入",
	"videoloss":            "视频丢失",
}

func defaultTopicRemark(topic string) string {
	norm := strings.Trim(normalizeTopic(topic), "/")
	// 只有根节点本身才命名，避免未知子主题都被泛化成「规则引擎」。
	if strings.EqualFold(norm, "ruleengine") {
		return "规则引擎"
	}
	parts := strings.Split(norm, "/")
	for i := len(parts) - 1; i >= 0; i-- {
		seg := strings.ToLower(strings.TrimSpace(parts[i]))
		seg = strings.TrimSuffix(seg, "*")
		if label, ok := topicRemarkLabels[seg]; ok {
			return label
		}
	}
	return ""
}

// ReplaceTopics 保存一次抓取得到的全部主题。source='declared' 会替换旧的声明主题，
// 但保留运行时观察到的 'observed' 主题；用户已编辑的 remark 必须原样保留，
// 新主题则预填中文名称。
func (s *Store) ReplaceTopics(cameraID int64, topics []string, source string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	remarks := map[string]string{}
	rows, err := tx.Query(`SELECT topic, remark FROM camera_topics WHERE camera_id=?`, cameraID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var topic, remark string
		if err := rows.Scan(&topic, &remark); err != nil {
			_ = rows.Close()
			return err
		}
		remarks[topic] = remark
	}
	err = rows.Err()
	_ = rows.Close() // 单连接：rows 必须关闭后才能继续 Exec
	if err != nil {
		return err
	}

	if _, err := tx.Exec(`DELETE FROM camera_topics WHERE camera_id=? AND source=?`, cameraID, source); err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT OR IGNORE INTO camera_topics(camera_id,topic,source,remark,discovered_at) VALUES(?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	ts := now()
	for _, raw := range topics {
		t := strings.TrimSpace(raw)
		if t == "" {
			continue
		}
		// 只有全新主题才预填中文名；已存在的记录即使备注为空，也保留用户主动清空的意图。
		remark, exists := remarks[t]
		if !exists {
			remark = defaultTopicRemark(t)
		}
		if _, err := stmt.Exec(cameraID, t, source, remark, ts); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// UpsertObservedTopic 记录真实推送中出现的主题（TP-Link 实际主题与 TopicSet 不完全一致）。
// 返回 true 表示这是新主题，调用方可据此触发运行时刷新。
func (s *Store) UpsertObservedTopic(cameraID int64, topic string) (bool, error) {
	res, err := s.db.Exec(`INSERT OR IGNORE INTO camera_topics(camera_id,topic,source,remark,discovered_at) VALUES(?,?,?,?,?)`,
		cameraID, topic, "observed", defaultTopicRemark(topic), now())
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// UpsertTopicRemarks 批量保存用户为事件起的名称/备注；主题不存在时补一条 custom 记录。
// 备注为空表示用户主动清空（推送 title 会回退为主题末段），不会被默认值覆盖。
func (s *Store) UpsertTopicRemarks(cameraID int64, items []TopicRemarkInput) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`INSERT INTO camera_topics(camera_id,topic,source,remark,discovered_at)
		VALUES(?,?,'custom',?,?)
		ON CONFLICT(camera_id,topic) DO UPDATE SET remark=excluded.remark`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	ts := now()
	for _, item := range items {
		topic := normalizeTopic(item.Topic)
		if topic == "" {
			continue
		}
		remark := strings.TrimSpace(item.Remark)
		if r := []rune(remark); len(r) > 200 {
			remark = string(r[:200])
		}
		if _, err := stmt.Exec(cameraID, topic, remark, ts); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ListTopicRemarks 一次性读出全部非空备注，供 Manager.Reload 构建运行期标题表。
func (s *Store) ListTopicRemarks() (map[int64]map[string]string, error) {
	rows, err := s.db.Query(`SELECT camera_id, topic, remark FROM camera_topics WHERE remark<>''`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]map[string]string{}
	for rows.Next() {
		var id int64
		var topic, remark string
		if err := rows.Scan(&id, &topic, &remark); err != nil {
			return nil, err
		}
		if out[id] == nil {
			out[id] = map[string]string{}
		}
		out[id][normalizeTopic(topic)] = remark
	}
	return out, rows.Err()
}

func (s *Store) ListTopics(cameraID int64) ([]CameraTopic, error) {
	rows, err := s.db.Query(`SELECT id,camera_id,topic,source,remark,discovered_at FROM camera_topics
		WHERE camera_id=? ORDER BY source, topic`, cameraID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []CameraTopic{}
	for rows.Next() {
		var t CameraTopic
		if err := rows.Scan(&t.ID, &t.CameraID, &t.Topic, &t.Source, &t.Remark, &t.DiscoveredAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// ---------------- subscriptions ----------------

func (s *Store) ListSubscriptions(cameraID int64) ([]Subscription, error) {
	query := `SELECT s.id,s.camera_id,c.name,s.topic,s.enabled,s.created_at,s.updated_at
		FROM event_subscriptions s JOIN cameras c ON c.id=s.camera_id`
	args := []any{}
	if cameraID > 0 {
		query += ` WHERE s.camera_id=?`
		args = append(args, cameraID)
	}
	query += ` ORDER BY c.name, s.topic`
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	out := []Subscription{}
	for rows.Next() {
		var sub Subscription
		var enabled int
		if err := rows.Scan(&sub.ID, &sub.CameraID, &sub.CameraName, &sub.Topic,
			&enabled, &sub.CreatedAt, &sub.UpdatedAt); err != nil {
			_ = rows.Close()
			return nil, err
		}
		sub.Enabled = enabled != 0
		sub.ChannelIDs = []int64{}
		out = append(out, sub)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	// 单连接模式：必须先释放 rows 占用的连接，再发起后续查询，否则会自锁。
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for i := range out {
		ids, err := s.channelIDsForSubscription(out[i].ID)
		if err != nil {
			return nil, err
		}
		out[i].ChannelIDs = ids
	}
	return out, nil
}

func (s *Store) channelIDsForSubscription(id int64) ([]int64, error) {
	rows, err := s.db.Query(`SELECT channel_id FROM subscription_channels WHERE subscription_id=? ORDER BY channel_id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []int64{}
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		ids = append(ids, v)
	}
	return ids, rows.Err()
}

// ReplaceSubscriptions 以 camera 为单位全量覆盖订阅列表（前端一次性保存勾选结果）。
func (s *Store) ReplaceSubscriptions(cameraID int64, items []SubscriptionInput) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 保留既有 enabled/created_at 之外的语义：全量重建，简单且无歧义。
	if _, err := tx.Exec(`DELETE FROM event_subscriptions WHERE camera_id=?`, cameraID); err != nil {
		return err
	}
	ts := now()
	for _, item := range items {
		topic := strings.TrimSpace(item.Topic)
		if topic == "" {
			continue
		}
		res, err := tx.Exec(`INSERT INTO event_subscriptions(camera_id,topic,enabled,created_at,updated_at)
			VALUES(?,?,?,?,?)`, cameraID, topic, b2i(item.Enabled), ts, ts)
		if err != nil {
			return err
		}
		subID, err := res.LastInsertId()
		if err != nil {
			return err
		}
		for _, chID := range item.ChannelIDs {
			if _, err := tx.Exec(`INSERT OR IGNORE INTO subscription_channels(subscription_id,channel_id) VALUES(?,?)`,
				subID, chID); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// ActiveSubscriptionCount 返回启用的订阅条数。
func (s *Store) ActiveSubscriptionCount() (int64, error) {
	var n int64
	err := s.db.QueryRow(`SELECT COUNT(*) FROM event_subscriptions WHERE enabled=1`).Scan(&n)
	return n, err
}

// ---------------- event logs ----------------

func (s *Store) InsertEventLog(e EventLog) error {
	_, err := s.db.Exec(`INSERT INTO event_logs
		(event_id,camera_id,device,topic,transport,payload,matched,dispatched,channels,created_at)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		e.EventID, e.CameraID, e.Device, e.Topic, e.Transport, string(e.Payload),
		b2i(e.Matched), b2i(e.Dispatched), e.Channels, e.CreatedAt)
	return err
}

func (s *Store) ListEventLogs(limit, offset int) ([]EventLog, int64, error) {
	var total int64
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM event_logs`).Scan(&total); err != nil {
		return nil, 0, err
	}
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	rows, err := s.db.Query(`SELECT id,event_id,camera_id,device,topic,transport,payload,
		matched,dispatched,channels,created_at FROM event_logs ORDER BY id DESC LIMIT ? OFFSET ?`, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := []EventLog{}
	for rows.Next() {
		var e EventLog
		var payload string
		var matched, dispatched int
		if err := rows.Scan(&e.ID, &e.EventID, &e.CameraID, &e.Device, &e.Topic, &e.Transport,
			&payload, &matched, &dispatched, &e.Channels, &e.CreatedAt); err != nil {
			return nil, 0, err
		}
		e.Payload = json.RawMessage(payload)
		e.Matched = matched != 0
		e.Dispatched = dispatched != 0
		out = append(out, e)
	}
	return out, total, rows.Err()
}

func (s *Store) TrimEventLogs(max int) error {
	if max <= 0 {
		return nil
	}
	_, err := s.db.Exec(`DELETE FROM event_logs WHERE id NOT IN
		(SELECT id FROM event_logs ORDER BY id DESC LIMIT ?)`, max)
	return err
}

func (s *Store) Overview() (map[string]int64, error) {
	out := map[string]int64{
		"cameras": 0, "cameras_enabled": 0, "channels": 0,
		"subscriptions": 0, "topics": 0, "events": 0, "events_today": 0,
	}
	queries := map[string]string{
		"cameras":         `SELECT COUNT(*) FROM cameras`,
		"cameras_enabled": `SELECT COUNT(*) FROM cameras WHERE enabled=1`,
		"channels":        `SELECT COUNT(*) FROM webhook_channels WHERE enabled=1`,
		"subscriptions":   `SELECT COUNT(*) FROM event_subscriptions WHERE enabled=1`,
		"topics":          `SELECT COUNT(*) FROM camera_topics`,
		"events":          `SELECT COUNT(*) FROM event_logs`,
		"events_today":    `SELECT COUNT(*) FROM event_logs WHERE created_at >= ?`,
	}
	for k, q := range queries {
		var n int64
		var err error
		if k == "events_today" {
			err = s.db.QueryRow(q, time.Now().UTC().Format("2006-01-02T00:00:00Z")).Scan(&n)
		} else {
			err = s.db.QueryRow(q).Scan(&n)
		}
		if err != nil {
			return nil, err
		}
		out[k] = n
	}
	return out, nil
}

const httpMethodPost = "POST"
