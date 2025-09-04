// internal/cache/mock_cache.go
package cache

import (
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models"
	"sync"
	"time"
)

// MockCache мок реализация для тестирования без Redis
type MockCache struct {
	mu     sync.RWMutex
	orders map[string]models.Order
}

// NewMock создает новый mock кэш
func NewMock() *MockCache {
	return &MockCache{
		orders: make(map[string]models.Order),
	}
}

func (m *MockCache) SaveOrder(order models.Order) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.orders[order.OrderUID] = order
	return nil
}

func (m *MockCache) GetOrder(orderUID string) (models.Order, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	order, exists := m.orders[orderUID]
	return order, exists, nil
}

func (m *MockCache) OrderExists(orderUID string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exists := m.orders[orderUID]
	return exists, nil
}

func (m *MockCache) RemoveOrder(orderUID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.orders, orderUID)
	return nil
}

func (m *MockCache) Clear() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.orders = make(map[string]models.Order)
	return nil
}

func (m *MockCache) GetAllOrders() ([]models.Order, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	orders := make([]models.Order, 0, len(m.orders))
	for _, order := range m.orders {
		orders = append(orders, order)
	}
	return orders, nil
}

func (m *MockCache) Close() error {
	return nil
}

func (m *MockCache) Set(order models.Order) error {
	return m.SaveOrder(order)
}

func (m *MockCache) Exists(orderUID string) (bool, error) {
	return m.OrderExists(orderUID)
}

func (m *MockCache) Get(orderUID string) (models.Order, bool, error) {
	return m.GetOrder(orderUID)
}

func (m *MockCache) SaveOrderWithTTL(order models.Order, ttl time.Duration) error {
	return m.SaveOrder(order)
}
