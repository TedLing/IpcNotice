package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

// version 可由发版工作流通过 -ldflags "-X main.version=<tag>" 注入。
var version = "0.2.0"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		configPath = flag.String("config", "config.json", "配置文件路径（仅用户名/密码/端口）")
		dbPath     = flag.String("db", databaseFile, "SQLite 数据库文件路径")
		webDir     = flag.String("web-dir", "web/dist", "前端构建产物目录")
		logFile    = flag.String("log-file", "", "额外写入日志文件（追加）")
		port       = flag.Int("port", 0, "覆盖配置文件中的管理界面端口")
		debug      = flag.Bool("debug", false, "输出 DEBUG 级日志")
		showVer    = flag.Bool("version", false, "输出版本号")
	)
	flag.Parse()

	if *showVer {
		fmt.Println("IpcNotice", version)
		return nil
	}
	logger, closer, err := setupLogger(*logFile, *debug)
	if err != nil {
		return err
	}
	if closer != nil {
		defer closer.Close()
	}

	store, err := OpenStore(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()

	migrated, err := migrateLegacy(*configPath, store, logger)
	if err != nil {
		return fmt.Errorf("迁移旧版配置失败: %w", err)
	}
	cfg, err := LoadConfig(*configPath)
	if err != nil {
		return err
	}
	if *port > 0 {
		if *port > 65535 {
			return fmt.Errorf("-port 非法: %d", *port)
		}
		cfg.Port = *port
	}

	cameras, err := store.ListCameras()
	if err != nil {
		return err
	}
	channels, err := store.ListChannels()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mgr, err := NewManager(ctx, store, logger)
	if err != nil {
		return err
	}
	defer mgr.Shutdown()

	api := NewAPIServer(cfg, store, mgr, *webDir, logger)
	if err := api.Start(); err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := api.Shutdown(shutdownCtx); err != nil {
			logger.Warn("管理界面关闭失败", "err", err)
		}
	}()

	logger.Info("IpcNotice 启动完成",
		"version", version, "config", cfg.Path, "db", *dbPath,
		"url", fmt.Sprintf("http://127.0.0.1:%d", cfg.Port),
		"cameras", len(cameras), "channels", len(channels),
		"migrated_legacy_config", migrated,
		"notify_listen", mgr.settings().NotifyListenAddr,
		"advertise_host", mgr.advertiseHost)
	if cfg.Password == defaultConfig().Password {
		logger.Warn("当前仍在使用默认登录密码，请修改 config.json 中的 password",
			"username", cfg.Username)
	}

	<-ctx.Done()
	logger.Info("收到退出信号，正在停止订阅并保存状态...")
	return nil
}

func setupLogger(logFile string, debug bool) (*slog.Logger, io.Closer, error) {
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	writers := []io.Writer{os.Stdout}
	var closer io.Closer
	if logFile != "" {
		if err := os.MkdirAll(filepath.Dir(logFile), 0o755); err != nil {
			return nil, nil, fmt.Errorf("创建日志目录失败: %w", err)
		}
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, nil, fmt.Errorf("打开日志文件失败: %w", err)
		}
		writers = append(writers, f)
		closer = f
	}
	h := slog.NewTextHandler(io.MultiWriter(writers...), &slog.HandlerOptions{Level: level})
	lg := slog.New(h)
	slog.SetDefault(lg)
	return lg, closer, nil
}
