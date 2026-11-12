package dispatch

import (
	"testing"
	"time"

	"IpcNotice/internal/store"
)

// cstAt 按北京时间（与推送窗口判断一致）构造测试时刻。
func cstAt(s string) time.Time {
	t, err := time.ParseInLocation("2006-01-02 15:04:05", s, cst)
	if err != nil {
		panic(err)
	}
	return t
}

func TestChannelWindowAllows(t *testing.T) {
	workdays := store.WebhookChannel{
		Weekdays:  []int{1, 2, 3, 4, 5},
		StartTime: "09:00:00",
		EndTime:   "18:00:00",
	}
	cases := []struct {
		name string
		ch   store.WebhookChannel
		at   time.Time
		want bool
	}{
		{"空字段回退全周全天", store.WebhookChannel{}, cstAt("2026-11-11 10:00:00"), true}, // 周三
		{"工作日时段内", workdays, cstAt("2026-11-11 10:00:00"), true},
		{"周六不在允许星期", workdays, cstAt("2026-11-14 10:00:00"), false},
		{"工作日但早于起点", workdays, cstAt("2026-11-11 08:59:59"), false},
		{"起点闭区间", workdays, cstAt("2026-11-11 09:00:00"), true},
		{"终点闭区间", workdays, cstAt("2026-11-11 18:00:00"), true},
		{"晚于终点", workdays, cstAt("2026-11-11 18:00:01"), false},
		{"仅周日且默认全天", store.WebhookChannel{Weekdays: []int{7}}, cstAt("2026-11-15 23:59:59"), true},
		{"仅周日但周三", store.WebhookChannel{Weekdays: []int{7}}, cstAt("2026-11-11 12:00:00"), false},
		// UTC 02:00 = 北京 10:00，判断必须按 cst 而非机器本地时区。
		{"UTC 输入按北京时间换算", workdays, time.Date(2026, 11, 11, 2, 0, 0, 0, time.UTC), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ChannelWindowAllows(tc.ch, tc.at); got != tc.want {
				t.Fatalf("ChannelWindowAllows = %v, want %v (at=%s weekday=%s)",
					got, tc.want, tc.at.In(cst).Format(time.RFC3339), tc.at.In(cst).Weekday())
			}
		})
	}
}

func TestChannelWindowAllowsBothConditions(t *testing.T) {
	ch := store.WebhookChannel{
		Weekdays:  []int{3},       // 仅周三
		StartTime: "10:00:00",
		EndTime:   "11:00:00",
	}
	if !ChannelWindowAllows(ch, cstAt("2026-11-11 10:30:00")) {
		t.Fatal("星期与时段同时满足时应允许推送")
	}
	if ChannelWindowAllows(ch, cstAt("2026-11-12 10:30:00")) {
		t.Fatal("星期不满足时不应推送（勿扰）")
	}
	if ChannelWindowAllows(ch, cstAt("2026-11-11 12:30:00")) {
		t.Fatal("时段不满足时不应推送（勿扰）")
	}
}
