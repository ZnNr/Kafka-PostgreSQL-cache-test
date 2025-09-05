package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/cache"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/repository"
	"go.uber.org/zap"
	"sync"
	"time"
)

const (
	orderTopic       = "orders"
	operationTimeout = 30 * time.Second
	reconnectDelay   = 5 * time.Second
)

// Subscribe подписывается на сообщения Kafka и обрабатывает их.
func Subscribe(ctx context.Context, appCache cache.Cache, db *repository.OrdersRepo, logger *zap.Logger, wg *sync.WaitGroup, brokers []string) error {
	defer wg.Done()

	if appCache == nil {
		return fmt.Errorf("cache cannot be nil")
	}
	if db == nil {
		return fmt.Errorf("database repository cannot be nil")
	}
	if logger == nil {
		return fmt.Errorf("logger cannot be nil")
	}

	// Восстанавливаем кеш из БД при старте
	logger.Info("Restoring cache from database...")
	if err := restoreCacheFromDB(ctx, appCache, db, logger); err != nil {
		return fmt.Errorf("failed to restore cache from DB: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			logger.Info("Consumer shutting down due to context cancellation")
			return nil
		default:
			if err := runConsumer(ctx, appCache, db, logger, brokers); err != nil {
				logger.Error("Consumer error, reconnecting", zap.Error(err), zap.Duration("delay", reconnectDelay))
				time.Sleep(reconnectDelay)
				continue
			}
			return nil
		}
	}
}

// runConsumer запускает основной цикл потребителя
func runConsumer(ctx context.Context, appCache cache.Cache, db *repository.OrdersRepo, logger *zap.Logger, brokers []string) error {
	// Подключаемся к Kafka
	consumer, err := sarama.NewConsumer(brokers, createConsumerConfig())
	if err != nil {
		return fmt.Errorf("failed to connect consumer: %w", err)
	}
	defer safeClose(consumer, "consumer", logger)

	partitionConsumer, err := consumer.ConsumePartition(orderTopic, 0, sarama.OffsetNewest)
	if err != nil {
		return fmt.Errorf("failed to consume partition: %w", err)
	}
	defer safeClose(partitionConsumer, "partition consumer", logger)

	logger.Info("Consumer subscribed to Kafka",
		zap.String("topic", orderTopic),
		zap.Strings("brokers", brokers))

	for {
		select {
		case <-ctx.Done():
			logger.Info("Consumer shutting down")
			return nil

		case err := <-partitionConsumer.Errors():
			if err != nil {
				logger.Error("Kafka consumer error", zap.Error(err))
			}

		case msg, ok := <-partitionConsumer.Messages():
			if !ok {
				logger.Info("Partition consumer channel closed")
				return fmt.Errorf("partition consumer channel closed")
			}
			if msg != nil {
				handleMessage(ctx, msg, appCache, db, logger)
			}
		}
	}
}

// restoreCacheFromDB восстанавливает кеш из базы данных при старте
func restoreCacheFromDB(ctx context.Context, appCache cache.Cache, db *repository.OrdersRepo, logger *zap.Logger) error {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()

	orders, err := db.GetOrders()
	if err != nil {
		return fmt.Errorf("failed to get orders from DB: %w", err)
	}

	if clearable, ok := appCache.(interface{ Clear() error }); ok {
		if err := clearable.Clear(); err != nil {
			logger.Warn("Failed to clear cache", zap.Error(err))
		}
	}

	for _, order := range orders {
		if err := appCache.SaveOrder(order); err != nil {
			logger.Error("Failed to restore order to cache",
				zap.String("order_uid", order.OrderUID),
				zap.Error(err))
			continue
		}
	}

	logger.Info("Cache restored from database",
		zap.Int("orders_count", len(orders)))
	return nil
}

// handleMessage обрабатывает сообщение из Kafka.
func handleMessage(ctx context.Context, msg *sarama.ConsumerMessage, appCache cache.Cache, db *repository.OrdersRepo, logger *zap.Logger) {
	if msg == nil || len(msg.Value) == 0 {
		logger.Warn("Received empty message, skipping")
		return
	}

	// Таймаут для обработки сообщения
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()

	var order models.Order
	if err := json.Unmarshal(msg.Value, &order); err != nil {
		logger.Error("Failed to unmarshal message",
			zap.Error(err),
			zap.Int64("offset", msg.Offset),
			zap.Int32("partition", msg.Partition))
		return
	}

	exists, err := appCache.OrderExists(order.OrderUID)
	if err != nil {
		logger.Warn("Failed to check order in cache, proceeding anyway",
			zap.String("order_uid", order.OrderUID),
			zap.Error(err))
	} else if exists {
		logger.Debug("Order already exists in cache, skipping",
			zap.String("order_uid", order.OrderUID))
		return
	}

	// Сохр в БД
	if err := db.AddOrder(order); err != nil {
		logger.Error("Failed to save order to DB",
			zap.Error(err),
			zap.String("order_uid", order.OrderUID))
		return
	}

	// Сохр в кеш
	if err := appCache.SaveOrder(order); err != nil {
		logger.Error("Failed to save order to cache",
			zap.Error(err),
			zap.String("order_uid", order.OrderUID))
	}

	logger.Info("Successfully processed order",
		zap.String("order_uid", order.OrderUID),
		zap.Int64("offset", msg.Offset),
		zap.Int32("partition", msg.Partition))
}

// createConsumerConfig создает конфигурацию для consumer'а
func createConsumerConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_1_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	return config
}

// safeClose безопасно закрывает ресурсы
func safeClose(closer interface{}, resourceName string, logger *zap.Logger) {
	if closer == nil {
		return
	}

	var err error
	switch c := closer.(type) {
	case sarama.Consumer:
		err = c.Close()
	case sarama.PartitionConsumer:
		err = c.Close()
	}

	if err != nil {
		logger.Error("Failed to close resource",
			zap.String("resource", resourceName),
			zap.Error(err))
	}
}
