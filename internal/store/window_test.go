package store

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestNormalizeWindow(t *testing.T) {
	c := &WebhookChannel{}
	if err := c.NormalizeWindow(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.Weekdays, []int{1, 2, 3, 4, 5, 6, 7}) ||
		c.StartTime != DefaultStartTime || c.EndTime != DefaultEndTime {
		t.Fatalf("空值应回退为全周全天: %+v", c)
	}

	c2 := &WebhookChannel{Weekdays: []int{5, 1, 5, 7}, StartTime: " 09:30:00 ", EndTime: "18:00:00"}
	if err := c2.NormalizeWindow(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c2.Weekdays, []int{1, 5, 7}) || c2.StartTime != "09:30:00" {
		t.Fatalf("归一化结果不符: %+v", c2)
	}
	// 幂等
	if err := c2.NormalizeWindow(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c2.Weekdays, []int{1, 5, 7}) {
		t.Fatal("重复归一化不应改变结果")
	}

	for _, bad := range []*WebhookChannel{
		{Weekdays: []int{0}},
		{Weekdays: []int{8}},
		{StartTime: "9:00:00"},
		{EndTime: "24:00:00"},
		{StartTime: "18:00:00", EndTime: "09:00:00"}, // 跨天暂不支持
	} {
		if err := bad.NormalizeWindow(); err == nil {
			t.Errorf("应拒绝非法窗口: %+v", bad)
		}
	}
}

func TestChannelWindowRoundTrip(t *testing.T) {
	s, err := OpenStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	c := &WebhookChannel{
		Name: "ch-1", URL: "http://192.0.2.10/webhook", Enabled: true,
		Weekdays: []int{1, 3, 5}, StartTime: "08:00:00", EndTime: "22:00:00",
	}
	if err := s.CreateChannel(c); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetChannel(c.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Weekdays, []int{1, 3, 5}) ||
		got.StartTime != "08:00:00" || got.EndTime != "22:00:00" {
		t.Fatalf("时间窗口未正确持久化: %+v", got)
	}

	got.Weekdays = []int{2, 4, 6, 7}
	got.StartTime, got.EndTime = "", "" // 清空后应回退全天
	if err := s.UpdateChannel(&got); err != nil {
		t.Fatal(err)
	}
	got2, err := s.GetChannel(c.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got2.Weekdays, []int{2, 4, 6, 7}) ||
		got2.StartTime != DefaultStartTime || got2.EndTime != DefaultEndTime {
		t.Fatalf("时间窗口更新不符: %+v", got2)
	}
}

// TestEnsureChannelWindowColumns 模拟旧库：表存在但没有 3 个窗口列，
// 迁移后旧通道应保持「全周全天」（行为不变）。
func TestEnsureChannelWindowColumns(t *testing.T) {
	s, err := OpenStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.db.Exec(`DROP TABLE webhook_channels`); err != nil {
		t.Fatal(err)
	}
	oldSchema := `CREATE TABLE webhook_channels (
  id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, url TEXT NOT NULL,
  method TEXT NOT NULL DEFAULT 'POST', headers_json TEXT NOT NULL DEFAULT '{}',
  body_template TEXT NOT NULL DEFAULT '', enabled INTEGER NOT NULL DEFAULT 1,
  timeout_seconds INTEGER NOT NULL DEFAULT 5, last_test_at TEXT NOT NULL DEFAULT '',
  last_test_ok INTEGER NOT NULL DEFAULT 0, last_test_msg TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`
	if _, err := s.db.Exec(oldSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO webhook_channels (name,url,created_at,updated_at)
		VALUES('old-ch','http://192.0.2.10/webhook','2026-01-01','2026-01-01')`); err != nil {
		t.Fatal(err)
	}
	if err := s.ensureChannelWindowColumns(); err != nil {
		t.Fatal(err)
	}
	if err := s.ensureChannelWindowColumns(); err != nil { // 幂等
		t.Fatal(err)
	}
	ch, err := s.GetChannel(1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ch.Weekdays, []int{1, 2, 3, 4, 5, 6, 7}) ||
		ch.StartTime != DefaultStartTime || ch.EndTime != DefaultEndTime {
		t.Fatalf("旧通道升级后应保持全周全天: %+v", ch)
	}
	// 脏数据兜底：空/非法字符串读出来也必须回退默认值。
	if _, err := s.db.Exec(`UPDATE webhook_channels SET weekdays='bad', start_time='', end_time='' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	ch, err = s.GetChannel(1)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join([]string{ch.StartTime, ch.EndTime}, "|") != "00:00:00|23:59:59" {
		t.Fatalf("脏数据未回退: %+v", ch)
	}
}
