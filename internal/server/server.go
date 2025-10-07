package server

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"net/http"
	"time"

	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/config"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/cache"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/controller/router"
)

type Server struct {
	cfg      *config.Config
	Cache    cache.Cache
	HTTPPort string
	logger   *zap.Logger
	server   *http.Server
}

func New(cfg *config.Config, cache cache.Cache, logger *zap.Logger) (*Server, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if cache == nil {
		return nil, fmt.Errorf("cache cannot be nil")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	httpPort := fmt.Sprintf("%s:%s", cfg.App.Host, cfg.App.Port)

	return &Server{
		cfg:      cfg,
		Cache:    cache,
		HTTPPort: httpPort,
		logger:   logger,
	}, nil
}

func (s *Server) Launch() error {
	// Создаем контроллер с логгером
	controller := router.NewController(s.Cache, s.logger)
	r := controller.SetupRouter()

	// Настраиваем HTTP сервер с таймаутами
	s.server = &http.Server{
		Addr:         s.HTTPPort,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	s.logger.Info("Starting HTTP server",
		zap.String("address", s.HTTPPort),
		zap.String("host", s.cfg.App.Host),
		zap.String("port", s.cfg.App.Port))

	// Запускаем сервер
	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to launch server: %w", err)
	}

	return nil
}

// Shutdown gracefully останавливает сервер
func (s *Server) Shutdown() error {
	if s.server != nil {
		s.logger.Info("Shutting down HTTP server")

		// Создаем контекст с таймаутом для graceful shutdown
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := s.server.Shutdown(ctx); err != nil {
			s.logger.Error("Failed to shutdown server gracefully", zap.Error(err))
			return fmt.Errorf("failed to shutdown server: %w", err)
		}

		s.logger.Info("HTTP server shut down successfully")
	}
	return nil
}

// GetHealth возвращает статус здоровья сервера
func (s *Server) GetHealth() map[string]interface{} {
	return map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().Format(time.RFC3339),
		"service":   "order-cache-api",
		"version":   "1.0.0",
	}
}
