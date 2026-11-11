package preview

import (
	"encoding/binary"
	"strings"
	"testing"

	"IpcNotice/internal/config"
	"github.com/pion/rtp"
)

func TestPreviewRTSPURL(t *testing.T) {
	dev := &config.DeviceConfig{DeviceUser: "admin", DevicePass: "p@ss:word"}
	u, masked, err := RTSPURL("rtsp://192.0.2.10:554/stream2", dev)
	if err != nil {
		t.Fatal(err)
	}
	if u.User == nil || u.User.Username() != "admin" {
		t.Fatalf("凭据未注入: %v", u.User)
	}
	if pass, _ := u.User.Password(); pass != "p@ss:word" {
		t.Fatalf("密码转义错误: %q", pass)
	}
	if masked != "rtsp://admin:***@192.0.2.10:554/stream2" {
		t.Fatalf("遮蔽后的地址错误: %q", masked)
	}
	if strings.Contains(masked, "p@ss") {
		t.Fatal("遮蔽后仍含密码")
	}
	// 设备已带凭据时不覆盖，但仍需遮蔽
	u2, masked2, err := RTSPURL("rtsp://u:secret@192.0.2.11/stream1", dev)
	if err != nil {
		t.Fatal(err)
	}
	if p, _ := u2.User.Password(); p != "secret" {
		t.Fatalf("覆盖了设备自带凭据: %q", p)
	}
	if strings.Contains(masked2, "secret") {
		t.Fatal("遮蔽后仍含密码")
	}
	if _, _, err := RTSPURL("http://192.0.2.10/live", dev); err == nil {
		t.Error("非 RTSP 协议应被拒绝")
	}
}

func TestPreviewStapA(t *testing.T) {
	sps := []byte{0x67, 0x42, 0x00, 0x1e}
	pps := []byte{0x68, 0xce, 0x3c}
	ref := &rtp.Packet{
		Header:  rtp.Header{Version: 2, PayloadType: 96, SequenceNumber: 100, Timestamp: 90000, Marker: true},
		Payload: []byte{0x65, 0x88},
	}
	stap := StapA(sps, pps, ref)
	if stap == nil {
		t.Fatal("应生成 STAP-A 包")
	}
	if stap.Header.SequenceNumber != 99 || stap.Header.Marker || stap.Header.Timestamp != 90000 {
		t.Fatalf("STAP-A 头部错误: %+v", stap.Header)
	}
	if stap.Payload[0] != 0x78 {
		t.Fatalf("NAL 类型不是 STAP-A: 0x%02x", stap.Payload[0])
	}
	if int(binary.BigEndian.Uint16(stap.Payload[1:3])) != len(sps) {
		t.Fatal("SPS 长度字段错误")
	}
	off := 3 + len(sps)
	if int(binary.BigEndian.Uint16(stap.Payload[off:off+2])) != len(pps) {
		t.Fatal("PPS 长度字段错误")
	}
	if len(stap.Payload) != 1+2+len(sps)+2+len(pps) {
		t.Fatalf("STAP-A 载荷长度错误: %d", len(stap.Payload))
	}
	// 原包不能被改写
	if !ref.Header.Marker || ref.Header.SequenceNumber != 100 {
		t.Fatal("构造 STAP-A 修改了原始 RTP 包")
	}
	if StapA(nil, pps, ref) != nil || StapA(sps, pps, nil) != nil {
		t.Error("缺少参数集或参考包时不应生成 STAP-A")
	}
}
