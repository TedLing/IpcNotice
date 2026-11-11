package config

import (
	"strings"
	"testing"
)

func TestResolveOnvifURL(t *testing.T) {
	cases := []struct {
		url, wantEndpoint, wantUser, wantPass string
	}{
		{"onvif://admin:examplePass1@192.0.2.10:2020", "http://192.0.2.10:2020/onvif/device_service", "admin", "examplePass1"},
		{"onvif://admin:examplePass2@192.0.2.20:2020", "http://192.0.2.20:2020/onvif/device_service", "admin", "examplePass2"},
		{"http://10.0.0.2/onvif/device_service", "http://10.0.0.2/onvif/device_service", "u", "p"},
		{"192.0.2.5:2020", "http://192.0.2.5:2020/onvif/device_service", "u", "p"},
	}
	for _, tc := range cases {
		d := &DeviceConfig{URL: tc.url, Username: "u", Password: "p"}
		if err := d.Resolve(); err != nil {
			t.Fatalf("%s: %v", tc.url, err)
		}
		if tc.wantUser == "admin" {
			// url 内的凭据优先级低于显式字段，这里单独验证 userinfo 提取。
			d2 := &DeviceConfig{URL: tc.url}
			if err := d2.Resolve(); err != nil {
				t.Fatalf("%s: %v", tc.url, err)
			}
			if d2.DeviceUser != tc.wantUser || d2.DevicePass != tc.wantPass {
				t.Errorf("userinfo: got %s/%s, want %s/%s", d2.DeviceUser, d2.DevicePass, tc.wantUser, tc.wantPass)
			}
		}
		if d.Endpoint != tc.wantEndpoint {
			t.Errorf("%s: endpoint = %s, want %s", tc.url, d.Endpoint, tc.wantEndpoint)
		}
	}
}

func TestMaskedURL(t *testing.T) {
	d := &DeviceConfig{URL: "onvif://admin:secret@192.0.2.10:2020"}
	if strings.Contains(d.MaskedURL(), "secret") {
		t.Fatalf("密码未被遮蔽: %s", d.MaskedURL())
	}
}
