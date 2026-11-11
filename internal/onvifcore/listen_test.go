package onvifcore

import (
	"testing"

	"IpcNotice/internal/config"
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
