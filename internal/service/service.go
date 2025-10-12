// internal/service/service.go
package service

import "github.com/IBM/sarama"

// Services объединяет все сервисы приложения
type Service struct {
	OrderProducer *OrderProducer
}

// NewServices создаёт экземпляр всех сервисов
func NewService(producer sarama.SyncProducer, kafkaTopic string) *Service {
	return &Service{
		OrderProducer: NewOrderProducer(producer, kafkaTopic),
	}
}
