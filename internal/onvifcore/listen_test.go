package onvifcore

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"IpcNotice/internal/config"
	"IpcNotice/internal/store"
	"github.com/mickeyzzc/onvif-go/v2/events"
	"github.com/mickeyzzc/onvif-go/v2/onvif"
)

func TestDeviceHashIgnoresDisplayName(t *testing.T) {
	st := config.DefaultSettings()
	a := &config.DeviceConfig{Name: "摄像头 A", Endpoint: "http://192.0.2.10:2020/onvif/device_service"}
	b := &config.DeviceConfig{Name: "ipc-a", Endpoint: "http://192.0.2.10:2020/onvif/device_service"}
	if DeviceHash(a, st) != DeviceHash(b, st) {
		t.Fatal("改显示名不应改变订阅哈希（不重建订阅）")
	}
	c := &config.DeviceConfig{Name: "摄像头 A", Endpoint: "http://192.0.2.20:2020/onvif/device_service"}
	if DeviceHash(a, st) == DeviceHash(c, st) {
		t.Fatal("设备地址变化必须改变订阅哈希（重建订阅）")
	}
}

// fakeHost 是测试用的最小 WorkerHost，挂在临时 SQLite 库上。
type fakeHost struct {
	ctx   context.Context
	log   *slog.Logger
	store *store.Store
}

func (h *fakeHost) Context() context.Context                   { return h.ctx }
func (h *fakeHost) Logger() *slog.Logger                       { return h.log }
func (h *fakeHost) Store() *store.Store                        { return h.store }
func (h *fakeHost) Settings() config.Settings                  { return config.DefaultSettings() }
func (h *fakeHost) ConsumerURL(*config.DeviceConfig) string    { return "http://192.0.2.2:9100/onvif/notify/c1" }
func (h *fakeHost) Submit(*config.DeviceConfig, string, []onvif.NotificationMessage) {}

// TestNewCameraWorkerInitializesStaleRefsFromSnapshot 验证：dev.SubManager
// （DB 快照）只在 Worker 创建时初始化运行态待清理列表一次；此后运行态由
// curSubMgr/staleRefs 独立管理，清理/持久化不再回读内存快照。
func TestNewCameraWorkerInitializesStaleRefsFromSnapshot(t *testing.T) {
	st, err := store.OpenStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	cam := &store.Camera{Name: "ipc-test", URL: "http://192.0.2.10:2020/onvif/device_service"}
	if err := st.CreateCamera(cam); err != nil {
		t.Fatal(err)
	}
	// 模拟上次进程崩溃残留的两个订阅引用。
	if err := st.SetCameraSubManager(cam.ID, "http://192.0.2.10/onvif/sub_a http://192.0.2.10/onvif/sub_b"); err != nil {
		t.Fatal(err)
	}

	h := &fakeHost{ctx: context.Background(), log: slog.Default(), store: st}
	// DeviceConfig.SubManager 由 Camera.Device() 从 DB 列填充，此处直接模拟。
	dev := &config.DeviceConfig{ID: cam.ID, Name: cam.Name, Endpoint: cam.URL,
		SubManager: "http://192.0.2.10/onvif/sub_a http://192.0.2.10/onvif/sub_b"}
	w := NewCameraWorker(h, dev, "hash", cam.ID)

	got := w.takeStaleRefs()
	if len(got) != 2 || got[0] != "http://192.0.2.10/onvif/sub_a" || got[1] != "http://192.0.2.10/onvif/sub_b" {
		t.Fatalf("启动时应从 DB 快照初始化待清理列表，got %v", got)
	}

	// 模拟建立新订阅：当前地址 + 残留列表应整体持久化，且当前地址在前。
	w.setCurSubMgr("http://192.0.2.10/onvif/sub_cur")
	w.persistSubMgrState("subscribe")
	dbCam, err := st.GetCamera(cam.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := "http://192.0.2.10/onvif/sub_cur http://192.0.2.10/onvif/sub_a http://192.0.2.10/onvif/sub_b"
	if dbCam.SubManager != want {
		t.Fatalf("持久化值不符\n want %q\n got  %q", want, dbCam.SubManager)
	}

	// 运行态清理（此处直接改状态模拟清理完成）：DB 与运行态应一致清空，
	// 且不依赖内存 dev.SubManager（其快照仍含旧值也不会被带回）。
	w.subMgrMu.Lock()
	w.curSubMgr = ""
	w.staleRefs = nil
	w.subMgrMu.Unlock()
	w.persistSubMgrState("unsubscribe")
	dbCam, err = st.GetCamera(cam.ID)
	if err != nil {
		t.Fatal(err)
	}
	if dbCam.SubManager != "" {
		t.Fatalf("清理完成后 DB 应清空引用，got %q", dbCam.SubManager)
	}
	if w.Device().SubManager != "http://192.0.2.10/onvif/sub_a http://192.0.2.10/onvif/sub_b" {
		t.Fatal("内存快照按设计保持创建时的静态副本（不被更新，也不再回读）")
	}
}

func TestJoinSubMgrRefs(t *testing.T) {
	got := joinSubMgrRefs("b", []string{"a", "b", "", "c", "a"})
	if got != "b a c" {
		t.Fatalf("去重拼接结果不符（当前在前）：got %q", got)
	}
	if joinSubMgrRefs("", nil) != "" {
		t.Fatal("空引用应得到空字符串")
	}
}

// TestPushTeardownBudgetClosed 验证收尾预算体系：各阶段共享同一绝对
// 截止时间，总预算严格小于 Stop 等待上限（阶段上限相加不再顶穿 stopWait）。
func TestPushTeardownBudgetClosed(t *testing.T) {
	if pushTeardownBudget >= stopWait {
		t.Fatalf("Push 收尾总预算 %s 必须小于 Stop 等待上限 %s", pushTeardownBudget, stopWait)
	}
	deadline := time.Now().Add(pushTeardownBudget)
	if got := budgetFor(deadline, renewExitWait); got > renewExitWait {
		t.Fatalf("阶段预算不得超过自身上限: %s", got)
	}
	if got := budgetFor(time.Now().Add(time.Second), unsubPrimaryMax); got > time.Second {
		t.Fatalf("剩余不足时应只取剩余预算: %s", got)
	}
	if got := budgetFor(time.Now().Add(-time.Second), unsubFallbackMax); got != 0 {
		t.Fatalf("截止时间已过应返回 0: %s", got)
	}
	// 极端情况：等待续约耗满 3s、首次退订耗满 8s 时，备用退订已无预算可用
	//（因此必须由 budgetFor 统一裁剪，不能用固定的 3s 独立超时）。
	lateDeadline := time.Now().Add(pushTeardownBudget - renewExitWait - unsubPrimaryMax)
	if got := budgetFor(lateDeadline, unsubFallbackMax); got > 0 {
		t.Fatalf("前两阶段耗尽预算后备用退订不应再获得时间: %s", got)
	}
}

func TestWaitRenewExit(t *testing.T) {
	w := NewCameraWorker(&fakeHost{ctx: context.Background(), log: slog.Default()},
		&config.DeviceConfig{ID: 1, Name: "ipc-test"}, "hash", 1)
	sub := &events.PushSubscription{ManagerAddress: "http://192.0.2.10/onvif/sub", SubscriptionReference: "http://192.0.2.10/onvif/ref"}

	done := make(chan struct{})
	close(done)
	if !w.waitRenewExit(done, time.Now().Add(pushTeardownBudget), sub) {
		t.Fatal("已退出的续约协程应立即返回 true")
	}
	if w.waitRenewExit(make(chan struct{}), time.Now().Add(-time.Second), sub) {
		t.Fatal("预算耗尽时应返回 false")
	}
	// 超时路径：有限等待后放弃，不让 Worker 永远无法退出。
	start := time.Now()
	if w.waitRenewExit(make(chan struct{}), time.Now().Add(20*time.Millisecond), sub) {
		t.Fatal("底层不响应取消时应超时返回 false")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("超时等待不应超过设定预算: %s", elapsed)
	}
}
