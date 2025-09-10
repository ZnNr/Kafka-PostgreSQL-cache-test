package cache

import "github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models"

// Cache определяет контракт для кэша
type Cache interface {
	SaveOrder(order models.Order) error
	GetOrder(orderUID string) (models.Order, bool, error)
	OrderExists(orderUID string) (bool, error)
	RemoveOrder(orderUID string) error
	Clear() error
	GetAllOrders() ([]models.Order, error)
	Close() error
}
