package manager

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"

	"IpcNotice/internal/config"
	"IpcNotice/internal/store"
)

// newTestManager 构造基于临时数据库的 Manager（无设备，订阅循环不会真正启动）。
func newTestManager(t *testing.T) *Manager {
	t.Helper()
	st, err := store.OpenStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("打开临时数据库失败: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	m, err := NewManager(context.Background(), st, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("创建 Manager 失败: %v", err)
	}
	return m
}

// TestReloadStatusConcurrent 回归：Reload 曾在锁外直接修改 m.workers 引用的
// 同一映射，与 Status()/deviceHealthy() 的 RLock 读遍历构成并发 Map 读写。
// 修复后 Reload 在副本上增删并整体替换，-race 下并发读写不再竞争。
func TestReloadStatusConcurrent(t *testing.T) {
	m := newTestManager(t)

	// 交替增删一台假设备（RFC5737 文档地址，不会真正连通），
	// 使 Reload 反复创建/停止 worker，覆盖映射增删与读遍历并发路径。
	cam := store.Camera{
		Name: "cam-1", URL: "onvif://user:pass@192.0.2.1:80", Enabled: true,
	}
	if err := m.store.CreateCamera(&cam); err != nil {
		t.Fatalf("创建测试摄像头失败: %v", err)
	}
	ch := store.WebhookChannel{Name: "ch-1", URL: "http://127.0.0.1:9/webhook", Enabled: true}
	if err := m.store.CreateChannel(&ch); err != nil {
		t.Fatalf("创建测试通道失败: %v", err)
	}
	if err := m.store.ReplaceSubscriptions(cam.ID, []store.SubscriptionInput{{
		Topic: "tns1:RuleEngine/CellMotionDetector/Motion", Enabled: true,
		ChannelIDs: []int64{ch.ID},
	}}); err != nil {
		t.Fatalf("创建测试订阅失败: %v", err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 读者：Status 与 deviceHealthy 并发遍历 workers
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = m.Status()
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = m.deviceHealthy("cam-1")
			}
		}
	}()

	// 写者：反复 Reload；中途删除摄像头触发 worker 停止与删除路径
	for i := 0; i < 30; i++ {
		if err := m.Reload(); err != nil {
			t.Fatalf("Reload 失败: %v", err)
		}
		if i == 15 {
			if err := m.store.DeleteCamera(cam.ID); err != nil {
				t.Fatalf("删除测试摄像头失败: %v", err)
			}
		}
	}
	close(stop)
	wg.Wait()

	// settings 热更新（configure 路径）也应无竞争
	st := config.DefaultSettings()
	if err := m.dispatcher.Configure(st); err != nil {
		t.Fatalf("Configure 失败: %v", err)
	}
	m.Shutdown()
}
