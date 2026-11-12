package dispatch

import (
	"time"

	"IpcNotice/internal/store"
)

// ChannelWindowAllows 判断 at 时刻是否落在该通道「允许推送」的时间窗口内。
// 两个条件必须同时满足：① 星期被勾选；② 时刻在 [start_time, end_time] 闭区间。
// 窗口外即勿扰时段——事件照常落库与记录日志，只是不发起 HTTP 推送。
//
// 时区固定为北京时间（cst，UTC+8），与推送内容和事件日志里的触发时间
// 展示保持一致，不随运行机器的本地时区变化；调用方传入 at 便于测试注入。
// 字段为空（旧数据/缺省）一律回退为全周全天，保证向后兼容。
func ChannelWindowAllows(ch store.WebhookChannel, at time.Time) bool {
	local := at.In(cst)
	day := int(local.Weekday()) // Go: 周日=0；转换成 ISO: 周一=1 ... 周日=7
	if day == 0 {
		day = 7
	}
	if len(ch.Weekdays) > 0 {
		allowed := false
		for _, d := range ch.Weekdays {
			if d == day {
				allowed = true
				break
			}
		}
		if !allowed {
			return false
		}
	}
	start, end := ch.StartTime, ch.EndTime
	if start == "" {
		start = store.DefaultStartTime
	}
	if end == "" {
		end = store.DefaultEndTime
	}
	hhmmss := local.Format("15:04:05")
	return hhmmss >= start && hhmmss <= end
}
