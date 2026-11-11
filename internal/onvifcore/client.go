// client.go 提供带鉴权梯队与时钟偏差校正的 ONVIF 客户端构造。
package onvifcore

import (
	"time"

	"IpcNotice/internal/config"
	"github.com/mickeyzzc/onvif-go/v2/onvif"
)

// NewDeviceClient 构造带鉴权梯队与时钟偏差校正的 ONVIF 客户端。
func NewDeviceClient(dev *config.DeviceConfig) (*onvif.Client, error) {
	timeout := 12 * time.Second
	return onvif.NewClient(
		dev.Endpoint,
		onvif.WithHTTPClient(NewONVIFHTTPClient(timeout)),
		onvif.WithCredentials(dev.DeviceUser, dev.DevicePass),
		onvif.WithTimeout(timeout),
		onvif.WithAutoClockSkew(),
		onvif.WithAuthFallback(onvif.AuthPasswordText, onvif.AuthHTTPBasic),
	)
}
