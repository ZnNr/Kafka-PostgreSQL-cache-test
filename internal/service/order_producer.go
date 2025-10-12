// internal/service/order_producer.go
package service

import (
	"log"

	"github.com/IBM/sarama"
)

// OrderProducer отвечает за отправку сообщений в Kafka
type OrderProducer struct {
	producer sarama.SyncProducer
	topic    string
}

// NewOrderProducer создаёт новый продюсер для заказов
func NewOrderProducer(producer sarama.SyncProducer, topic string) *OrderProducer {
	return &OrderProducer{
		producer: producer,
		topic:    topic,
	}
}

// Send отправляет сериализованный заказ в Kafka
func (op *OrderProducer) Send(data []byte) error {
	return op.PushToQueue(data)
}

// PushToQueue отправляет сообщение в Kafka с обработкой ошибок
func (op *OrderProducer) PushToQueue(message []byte) error {
	if op.producer == nil {
		return ErrProducerNotInitialized
	}
	if len(message) == 0 {
		return ErrEmptyMessage
	}
	if op.topic == "" {
		return ErrTopicRequired
	}

	msg := &sarama.ProducerMessage{
		Topic: op.topic,
		Value: sarama.ByteEncoder(message),
	}

	partition, offset, err := op.producer.SendMessage(msg)
	if err != nil {
		log.Printf("Failed to send message: %v", err)
		return ErrSendMessage{Err: err}
	}

	log.Printf("Message delivered to topic=%s partition=%d offset=%d",
		op.topic, partition, offset)
	return nil
}

// Ошибки
var (
	ErrProducerNotInitialized = &AppError{"producer is not initialized"}
	ErrEmptyMessage           = &AppError{"message is empty"}
	ErrTopicRequired          = &AppError{"topic is required"}
)

// AppError — пользовательская ошибка
type AppError struct {
	Message string
}

func (e *AppError) Error() string {
	return e.Message
}

// ErrSendMessage — ошибка отправки
type ErrSendMessage struct {
	Err error
}

func (e ErrSendMessage) Error() string {
	return "failed to send message: " + e.Err.Error()
}
