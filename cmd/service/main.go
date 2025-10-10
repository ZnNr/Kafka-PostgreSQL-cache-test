package main

import (
	"context"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/config"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/cache"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/consumer"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/repository"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/server"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/migrations"
	"go.uber.org/zap"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var (
	cfgPath = "config/config.yaml"
)

func main() {
	logger := initializeLogger()
	defer func() {
		_ = logger.Sync()
	}()

	cfg := loadConfig(cfgPath, logger)

	ordersRepo := initializeRepository(cfg, logger)
	defer closeRepository(ordersRepo, logger)

	//инициализация всех таблиц через одну схему
	err := migrations.InitializeDatabaseSchema(ordersRepo.DB, logger)
	if err != nil {
		logger.Fatal("Failed to run migrations", zap.Error(err))
	}

	//инициализация таблиц бд через миграцию с версионированием
	//migrationManager := migrations.NewMigrationManager(ordersRepo.DB, logger)
	//if err := migrationManager.Up(); err != nil {
	//	logger.Fatal("Failed to run migrations", zap.Error(err))
	//}

	appCache := initializeCache(cfg, ordersRepo, logger)
	defer closeCache(appCache, logger)

	httpServer := initializeController(cfg, appCache, logger)
	startServer(httpServer, logger)

	// Канал для системных сигналов
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM)

	// Kafka consumer
	subscribeToKafka(cfg, appCache, ordersRepo, logger, sigchan)

	logger.Info("Application shutting down")
}

func startServer(server *server.Server, logger *zap.Logger) {

	go func() {
		if err := server.Launch(); err != nil {
			logger.Fatal("Server error", zap.Error(err))
		}
	}()

	logger.Info("Server started successfully")
}

func initializeLogger() *zap.Logger {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	return logger
}

func loadConfig(cfgPath string, logger *zap.Logger) *config.Config {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		logger.Fatal("Failed to load config", zap.Error(err))
	}
	logger.Info("Configuration loaded successfully")
	return cfg
}

func initializeRepository(cfg *config.Config, logger *zap.Logger) *repository.OrdersRepo {
	ordersRepo, err := repository.New(cfg)
	if err != nil {
		logger.Fatal("Failed to initialize repository", zap.Error(err))
	}
	logger.Info("Repository initialized successfully",
		zap.String("host", cfg.DB.Host),
		zap.String("port", cfg.DB.Port),
		zap.String("db", cfg.DB.Name),
		zap.String("user", cfg.DB.User),
	)
	return ordersRepo
}

func closeRepository(repo *repository.OrdersRepo, logger *zap.Logger) {
	if err := repo.DB.Close(); err != nil {
		logger.Error("Error closing repository", zap.Error(err))
	} else {
		logger.Info("Repository closed successfully")
	}
}

func initializeCache(cfg *config.Config, ordersRepo *repository.OrdersRepo, logger *zap.Logger) cache.Cache {
	// Создаем кэш на основе конфигурации
	appCache, err := cache.New(cfg.Cache.ToCacheConfig())
	if err != nil {
		logger.Fatal("Failed to initialize cache", zap.Error(err))
	}

	// Загружаем заказы из БД в кэш
	orders, err := ordersRepo.GetOrders()
	if err != nil {
		logger.Fatal("Orders Load error", zap.Error(err))
	}

	for _, order := range orders {
		if err := appCache.SaveOrder(order); err != nil {
			logger.Error("Failed to save order to cache",
				zap.String("order_uid", order.OrderUID),
				zap.Error(err))
		} else {
			logger.Info("Order cached successfully",
				zap.String("order_uid", order.OrderUID))
		}
	}

	logger.Info("Cache initialized successfully",
		zap.Int("orders_loaded", len(orders)),
		zap.String("cache_type", string(cfg.Cache.Type)))

	return appCache
}

func closeCache(appCache cache.Cache, logger *zap.Logger) {
	if err := appCache.Close(); err != nil {
		logger.Error("Error closing cache", zap.Error(err))
	} else {
		logger.Info("Cache closed successfully")
	}
}

func initializeController(cfg *config.Config, appCache cache.Cache, logger *zap.Logger) *server.Server {
	httpServer, err := server.New(cfg, appCache, logger)
	if err != nil {
		logger.Fatal("Controller initialization error", zap.Error(err))
	}
	logger.Info("Controller initialized successfully")
	return httpServer
}

func subscribeToKafka(cfg *config.Config, cache cache.Cache, repo *repository.OrdersRepo, logger *zap.Logger, sigchan chan os.Signal) {
	// Создаем контекст с отменой, чтобы корректно завершать работу
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		// Добавляем brokers как шестой параметр
		if err := consumer.Subscribe(
			ctx,
			cache,
			repo,
			logger,
			&wg,
			cfg.Kafka.Brokers,
			cfg.Kafka.DlqTopic, // ← передаём DLQ топик
		); err != nil {
			logger.Error("Consumer error", zap.Error(err))
		} else {
			logger.Info("Consumer started successfully")
		}
	}()

	sig := <-sigchan
	logger.Info("Received signal, shutting down...", zap.String("signal", sig.String()))
	cancel()

	wg.Wait() // Ждем завершения работы go-рутины
	logger.Info("Consumer shut down gracefully")
}
