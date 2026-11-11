// embed.go 内嵌前端构建产物（仓库内的 web/dist，由 vite 在 web/ 下 pnpm build 生成）。
//
// 这样运行时不再依赖外部的 web/dist 目录，编译出的二进制自带管理界面，实现单文件部署。
// 注意：go build 之前必须先构建前端，否则 web/dist 不存在会导致编译失败
//（builder.bat / GitHub Actions 均先构建前端再编译后端）。
// 如需用磁盘上的前端目录覆盖内嵌资源（例如开发调试或自定义界面），可用 -web-dir 指定。
package main

import "embed"

// webFS 内嵌的前端资源根（含 web/dist 子目录），交给 internal/server 托管。
//
//go:embed web/dist
var webFS embed.FS
