// internal/consumer/kafka.go
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
)

const (
	// Топик для заказов
	orderTopic = "orders"

	// Таймаут для операций
	//operationTimeout = 30 * time.Second
)

var connectConsumer = ConnectConsumer

// Subscribe подписывается на сообщения Kafka и обрабатывает их.
func Subscribe(ctx context.Context, appCache cache.Cache, db *repository.OrdersRepo, logger *zap.Logger, wg *sync.WaitGroup) error {
	defer wg.Done() // Убедимся, что wait group завершится
	if appCache == nil {
		return fmt.Errorf("cache cannot be nil")
	}
	if db == nil {
		return fmt.Errorf("database repository cannot be nil")
	}
	if logger == nil {
		return fmt.Errorf("logger cannot be nil")
	}

	// Подключаемся к Kafka
	consumer, err := ConnectConsumer([]string{"localhost:9092"})
	if err != nil {
		return fmt.Errorf("failed to connect consumer: %w", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			logger.Error("Failed to close consumer", zap.Error(err))
		}
	}()

	// Подписываемся на партицию
	partitionConsumer, err := consumer.ConsumePartition(orderTopic, 0, sarama.OffsetNewest)
	if err != nil {
		return fmt.Errorf("failed to consume partition: %w", err)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			logger.Error("Failed to close partition consumer", zap.Error(err))
		}
	}()

	logger.Info("Consumer subscribed to Kafka!",
		zap.String("topic", orderTopic),
		zap.String("brokers", "localhost:9092"))

	// Основной цикл обработки сообщений
	for {
		select {
		case <-ctx.Done():
			logger.Info("Consumer shutting down due to context cancellation")
			return nil

		case err := <-partitionConsumer.Errors():
			if err != nil {
				logger.Error("Kafka consumer error", zap.Error(err))
			}

		case msg := <-partitionConsumer.Messages():
			if msg != nil {
				handleMessage(ctx, msg, appCache, db, logger)
			}
		}
	}
}

// handleMessage обрабатывает сообщение из Kafka.
func handleMessage(ctx context.Context, msg *sarama.ConsumerMessage, appCache cache.Cache, db *repository.OrdersRepo, logger *zap.Logger) {
	// Проверяем, что сообщение не пустое
	if msg == nil || len(msg.Value) == 0 {
		logger.Warn("Received empty or nil message, skipping")
		return
	}

	// Десериализуем сообщение
	var order models.Order
	if err := json.Unmarshal(msg.Value, &order); err != nil {
		logger.Error("Failed to unmarshal message",
			zap.Error(err),
			zap.ByteString("message", msg.Value),
			zap.Int64("offset", msg.Offset),
			zap.Int32("partition", msg.Partition))
		return
	}

	// Проверяем, есть ли заказ уже в кэше
	exists, err := checkOrderExistsInCache(appCache, order.OrderUID, logger)
	if err != nil {
		logger.Error("Failed to check order existence in cache",
			zap.Error(err),
			zap.String("order_uid", order.OrderUID))
		// Продолжаем обработку, даже если проверка кэша не удалась
	} else if exists {
		logger.Debug("Order already exists in cache, skipping",
			zap.String("order_uid", order.OrderUID))
		return
	}

	// Сохраняем заказ в базу данных
	if err := db.AddOrder(order); err != nil {
		logger.Error("Failed to save order to DB",
			zap.Error(err),
			zap.String("order_uid", order.OrderUID))
		return
	}

	// Сохраняем заказ в кэш
	if err := saveOrderToCache(appCache, order, logger); err != nil {
		logger.Error("Failed to save order to cache",
			zap.Error(err),
			zap.String("order_uid", order.OrderUID))
		// Не возвращаем ошибку, так как заказ уже сохранен в БД
	}

	logger.Info("Successfully processed order",
		zap.String("order_uid", order.OrderUID),
		zap.Int64("offset", msg.Offset),
		zap.Int32("partition", msg.Partition))
}

// checkOrderExistsInCache проверяет существование заказа в кэше с обработкой ошибок
func checkOrderExistsInCache(appCache cache.Cache, orderUID string, logger *zap.Logger) (bool, error) {
	// Пытаемся вызвать метод OrderExists
	exists, err := appCache.OrderExists(orderUID)
	if err != nil {
		// Если метод OrderExists возвращает ошибку, попробуем GetOrder
		_, found, getErr := appCache.GetOrder(orderUID)
		if getErr != nil {
			return false, fmt.Errorf("both OrderExists and GetOrder failed: OrderExists err: %w, GetOrder err: %w", err, getErr)
		}
		return found, nil
	}
	return exists, nil
}

// saveOrderToCache сохраняет заказ в кэш с обработкой ошибок
func saveOrderToCache(appCache cache.Cache, order models.Order, logger *zap.Logger) error {
	// Пытаемся вызвать SaveOrder напрямую
	err := appCache.SaveOrder(order)
	if err != nil {
		// Если SaveOrder возвращает ошибку, логируем и возвращаем ошибку
		return fmt.Errorf("failed to save order in cache: %w", err)
	}
	return nil
}

// ConnectConsumer создает подключение к Kafka consumer.
func ConnectConsumer(brokers []string) (sarama.Consumer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("brokers list cannot be empty")
	}

	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0 // Более новая версия
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	return consumer, nil
}
