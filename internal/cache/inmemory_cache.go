package cache

import (
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models"
	"sync"
)

type InMemoryCache struct {
	mu     sync.RWMutex
	orders map[string]models.Order
}

// NewInMemoryCache создает новый in-memory кэш
func NewInMemoryCache(initialCapacity int) *InMemoryCache {
	return &InMemoryCache{
		orders: make(map[string]models.Order, initialCapacity),
	}
}

// SaveOrder сохраняет заказ в кэш
func (c *InMemoryCache) SaveOrder(order models.Order) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.orders[order.OrderUID] = order
	return nil
}

// GetOrder получает заказ из кэша по UID
func (c *InMemoryCache) GetOrder(orderUID string) (models.Order, bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	order, ok := c.orders[orderUID]
	return order, ok, nil
}

// OrderExists проверяет существование заказа в кэше
func (c *InMemoryCache) OrderExists(orderUID string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, exists := c.orders[orderUID]
	return exists, nil
}

// RemoveOrder удаляет заказ из кэша по UID
func (c *InMemoryCache) RemoveOrder(orderUID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.orders, orderUID)
	return nil
}

// Clear очищает кэш
func (c *InMemoryCache) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.orders = make(map[string]models.Order)
	return nil
}

// GetAllOrders возвращает список всех заказов
func (c *InMemoryCache) GetAllOrders() ([]models.Order, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	orders := make([]models.Order, 0, len(c.orders))
	for _, order := range c.orders {
		orders = append(orders, order)
	}
	return orders, nil
}

// Close закрывает кэш
func (c *InMemoryCache) Close() error {
	return nil
}

// Ensure InMemoryCache implements Cache interface
var _ Cache = (*InMemoryCache)(nil)
