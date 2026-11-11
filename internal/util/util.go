// Package util 收拢被多个内部包共享的叶子函数（无内部依赖）。
package util

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// JSONDump 用于调试输出结构化对象；序列化失败时回退到 %v。
func JSONDump(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// NormalizeTopic 去掉各段命名空间前缀（tns1: / wstop: 等），
// 使 TopicSet 枚举出的层级路径与真实 Notify 主题可以互相匹配。
func NormalizeTopic(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	parts := strings.Split(s, "/")
	for i, p := range parts {
		if idx := strings.LastIndex(p, ":"); idx >= 0 {
			p = p[idx+1:]
		}
		parts[i] = strings.TrimSpace(p)
	}
	return strings.Trim(strings.Join(parts, "/"), "/")
}

// MatchTopic 支持以 /* 结尾的通配订阅，其余按规范化后的路径精确匹配。
func MatchTopic(pattern, actual string) bool {
	p, a := NormalizeTopic(pattern), NormalizeTopic(actual)
	if p == "" || a == "" {
		return false
	}
	if p == a || p == "*" {
		return true
	}
	if strings.HasSuffix(p, "/*") {
		return strings.HasPrefix(a, strings.TrimSuffix(p, "*"))
	}
	return false
}

// RandomToken 生成 32 字节随机十六进制串，用于会话标识与测试事件 ID。
// 随机数不可用时回退到纳秒时间戳，保证不会返回空串。
func RandomToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b)
}
