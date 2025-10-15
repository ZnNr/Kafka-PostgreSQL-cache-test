package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/cache"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/repository"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/service"
	"go.uber.org/zap"
)

const (
	orderTopic = "orders"
	//dlqTopic         = "orders.dlq"
	operationTimeout = 30 * time.Second
	reconnectDelay   = 5 * time.Second
)

// Subscribe подписывается на сообщения Kafka и обрабатывает их.
func Subscribe(
	ctx context.Context,
	appCache cache.Cache,
	db *repository.OrdersRepo,
	logger *zap.Logger,
	brokers []string,
	dlqTopic string, // ← новая топика для DLQ
) error {

	if appCache == nil {
		return fmt.Errorf("cache cannot be nil")
	}
	if db == nil {
		return fmt.Errorf("database repository cannot be nil")
	}
	if logger == nil {
		return fmt.Errorf("logger cannot be nil")
	}
	// Создаем валидатор
	validator := service.NewOrderValidator()
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
			if err := runConsumer(ctx, appCache, db, logger, brokers, validator, dlqTopic); err != nil {
				logger.Error("Consumer error, reconnecting", zap.Error(err), zap.Duration("delay", reconnectDelay))
				time.Sleep(reconnectDelay)
				continue
			}
			return nil
		}
	}
}

// runConsumer запускает основной цикл потребителя
func runConsumer(
	ctx context.Context,
	appCache cache.Cache,
	db *repository.OrdersRepo,
	logger *zap.Logger,
	brokers []string,
	validator *service.OrderValidator,
	dlqTopic string,
) error {
	// Создаем асинхронного продюсера для DLQ
	producer, err := sarama.NewAsyncProducer(brokers, createProducerConfig())
	if err != nil {
		return fmt.Errorf("failed to create DLQ producer: %w", err)
	}
	defer safeClose(producer, "dlq producer", logger)
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
		zap.String("dlq_topic", dlqTopic),
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
				handleMessage(ctx, msg, appCache, db, logger, validator, producer, dlqTopic)
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
	successCount := 0
	for _, order := range orders {
		if err := appCache.SaveOrder(order); err != nil {
			logger.Error("Failed to restore order to cache",
				zap.String("order_uid", order.OrderUID),
				zap.Error(err))
			continue
		}
		successCount++
	}

	logger.Info("Cache restored from database",
		zap.Int("successful_restorations", successCount),
		zap.Int("orders_count", len(orders)))
	return nil
}

// handleMessage обрабатывает сообщение из Kafka.
func handleMessage(
	ctx context.Context,
	msg *sarama.ConsumerMessage,
	appCache cache.Cache,
	db *repository.OrdersRepo,
	logger *zap.Logger,
	validator *service.OrderValidator,
	producer sarama.AsyncProducer,
	dlqTopic string,
) {
	// Проверяем, что сообщение не пустое
	if msg == nil || len(msg.Value) == 0 {
		sendToDLQ(producer, dlqTopic, msg, "empty message")
		return
	}

	var order models.Order
	if err := json.Unmarshal(msg.Value, &order); err != nil {
		logger.Error("Failed to unmarshal message", zap.Error(err), zap.ByteString("raw", msg.Value))
		sendToDLQ(producer, dlqTopic, msg, fmt.Sprintf("unmarshal error: %v", err))
		return
	}

	// Валидация OrderUID
	if order.OrderUID == "" {
		sendToDLQ(producer, dlqTopic, msg, "empty OrderUID")
		return
	}

	// Валидация структуры
	if !validator.ValidateOrder(order) {
		sendToDLQ(producer, dlqTopic, msg, "invalid order data")
		return
	}

	// Проверка дубликата
	exists, err := appCache.OrderExists(order.OrderUID)
	if err != nil {
		logger.Warn("Failed to check cache",
			zap.String("order_uid", order.OrderUID), zap.Error(err))
	} else if exists {
		logger.Debug("Duplicate order skipped",
			zap.String("order_uid", order.OrderUID))
		return
	}

	// Сохранение в БД и кеш
	if err := db.AddOrder(order); err != nil {
		logger.Error("Failed to save to DB",
			zap.Error(err),
			zap.String("order_uid", order.OrderUID))
		// Не отправляем в DLQ — возможно временная ошибка (повтор может помочь)
		// Можно добавить retry, но не DLQ сразу
		return
	}

	if err := appCache.SaveOrder(order); err != nil {
		logger.Error("Failed to save to cache",
			zap.Error(err),
			zap.String("order_uid", order.OrderUID))
		// То же — можно не в DLQ, если критично
	}

	logger.Info("Successfully processed order", zap.String("order_uid", order.OrderUID))
}

// createConsumerConfig создает конфигурацию для consumer
func createConsumerConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_1_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	return config
}

func sendToDLQ(producer sarama.AsyncProducer, dlqTopic string, msg *sarama.ConsumerMessage, reason string) {
	var keyEncoder sarama.Encoder
	if msg.Key != nil {
		keyEncoder = sarama.ByteEncoder(msg.Key)
	}

	headers := make([]sarama.RecordHeader, 0, len(msg.Headers)+1)
	for _, h := range msg.Headers {
		if h != nil {
			headers = append(headers, sarama.RecordHeader{
				Key:   h.Key,
				Value: h.Value,
			})
		}
	}
	headers = append(headers, sarama.RecordHeader{
		Key:   []byte("dlq_reason"),
		Value: []byte(reason),
	})

	dlqMsg := &sarama.ProducerMessage{
		Topic:   dlqTopic,
		Key:     keyEncoder,
		Value:   sarama.ByteEncoder(msg.Value),
		Headers: headers,
	}

	zap.L().Warn("🔄 Attempting to send to DLQ",
		zap.String("topic", dlqTopic),
		zap.String("reason", reason),
		zap.Int64("offset", msg.Offset))

	// Отправляем с обработкой ошибок
	select {
	case producer.Input() <- dlqMsg:
		zap.L().Info("✅ Message queued for DLQ",
			zap.String("topic", dlqTopic),
			zap.String("reason", reason))

		// Ждем подтверждения или ошибки
		select {
		case success := <-producer.Successes():
			zap.L().Info("✅ Message confirmed sent to DLQ",
				zap.String("topic", success.Topic),
				zap.Int32("partition", success.Partition),
				zap.Int64("offset", success.Offset))

		case err := <-producer.Errors():
			zap.L().Error("❌ Failed to send to DLQ",
				zap.Error(err),
				zap.String("reason", reason))

		case <-time.After(5 * time.Second):
			zap.L().Error("⏰ DLQ confirmation timeout",
				zap.String("reason", reason))
		}

	case <-time.After(3 * time.Second):
		zap.L().Error("⏰ DLQ producer input timeout - producer may be blocked",
			zap.String("reason", reason))

	case err := <-producer.Errors():
		zap.L().Error("❌ DLQ producer error",
			zap.Error(err),
			zap.String("reason", reason))
	}
}

func createProducerConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_1_0
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Retry.Max = 3
	config.Producer.Timeout = 10 * time.Second
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
	case sarama.AsyncProducer:
		err = c.Close()
	default:
		logger.Warn("Unknown resource type for closing", zap.String("resource", resourceName))
		return
	}

	if err != nil {
		logger.Error("Failed to close resource",
			zap.String("resource", resourceName),
			zap.Error(err))
	} else {
		logger.Debug("Resource closed successfully", zap.String("resource", resourceName))
	}
}
