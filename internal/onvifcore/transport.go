package onvifcore

import (
	"bytes"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

// TP-Link 部分固件在 GetEventProperties 的 TopicSet 中输出未加引号的属性值：
//
//	<Motion wstop:topic=true>
//
// 这是非法 XML，标准 encoding/xml 会在解析 SOAP 信封时直接报
// "unquoted or missing attribute value in element"，连库内置的
// GetEventProperties 都无法使用。xmlSanitizeTransport 在 SOAP 客户端
// 解密响应之前只修正这一类已知缺陷，其余响应原样透传。
var unquotedTopicAttrRe = regexp.MustCompile(`(?i)(wstop:topic\s*=\s*)(true|false|0|1)([\s/>])`)

const maxSanitizeBytes = 4 << 20 // 4 MiB

type xmlSanitizeTransport struct {
	base http.RoundTripper
}

func (t *xmlSanitizeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil || resp == nil || resp.Body == nil {
		return resp, err
	}

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxSanitizeBytes+1))
	_ = resp.Body.Close()
	if readErr != nil {
		return nil, readErr
	}
	if len(body) > maxSanitizeBytes {
		resp.Body = io.NopCloser(bytes.NewReader(body))
		return resp, nil
	}

	fixed := body
	if unquotedTopicAttrRe.Match(body) {
		fixed = unquotedTopicAttrRe.ReplaceAll(body, []byte(`${1}"${2}"${3}`))
	}
	resp.Body = io.NopCloser(bytes.NewReader(fixed))
	resp.ContentLength = int64(len(fixed))
	resp.Header.Set("Content-Length", strconv.Itoa(len(fixed)))
	return resp, nil
}

// NewONVIFHTTPClient 构造带容错传输层的 HTTP 客户端。
// 注意：WithHTTPClient 必须在 WithTimeout 之前应用，否则超时设置会被覆盖。
func NewONVIFHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &xmlSanitizeTransport{
			base: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				MaxIdleConns:          10,
				MaxIdleConnsPerHost:   5,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: time.Second,
			},
		},
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse // 防止 http 被静默升级为 https
		},
	}
}
