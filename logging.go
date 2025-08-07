package sync

import (
	"log/slog"
	"os"
)

var syncLogger *slog.Logger

func init() {
	// 创建自定义的logger配置
	opts := &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: true,
	}

	// 使用JSON格式的handler
	handler := slog.NewJSONHandler(os.Stdout, opts)
	syncLogger = slog.New(handler)

	// 设置为默认logger（可选）
	slog.SetDefault(syncLogger)
}
