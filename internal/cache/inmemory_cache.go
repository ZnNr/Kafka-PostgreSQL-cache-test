// Package cache предоставляет реализации in-memory кэша.
package cache

import (
	"container/list"
	"sync"
	"time"

	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models"
)

// entry — элемент кэша, хранящий заказ, его ключ и время истечения срока жизни.
type entry struct {
	key   string
	value models.Order
	exp   time.Time
}

// InMemoryCache — потокобезопасный in-memory кэш с поддержкой TTL и LRU-вытеснения.
type InMemoryCache struct {
	mu       sync.RWMutex
	capacity int                      // Максимальное количество записей в кэше.
	ttl      time.Duration            // Время жизни записи (0 — без ограничения).
	cache    map[string]*list.Element // Отображение ключа на элемент двусвязного списка.
	lruList  *list.List               // Двусвязный список: голова — самый свежий, хвост — самый старый.
	stopCh   chan struct{}            // Канал для остановки фоновой горутины очистки.
}

// NewInMemoryCache создаёт новый in-memory кэш с заданным размером и временем жизни записей.
// capacity — максимальное количество записей (ограничение LRU).
// ttl — время жизни записи; если 0, записи не устаревают автоматически.
func NewInMemoryCache(capacity int, ttl time.Duration) *InMemoryCache {
	c := &InMemoryCache{
		capacity: capacity,
		ttl:      ttl,
		cache:    make(map[string]*list.Element, capacity),
		lruList:  list.New(),
		stopCh:   make(chan struct{}),
	}

	// Запускаем фоновую горутину для периодической очистки просроченных записей.
	if ttl > 0 {
		go c.startCleanup()
	}

	return c
}

// startCleanup запускает фоновую горутину, которая периодически удаляет просроченные записи.
func (c *InMemoryCache) startCleanup() {
	ticker := time.NewTicker(c.ttl / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanupExpired()
		case <-c.stopCh:
			return
		}
	}
}

// cleanupExpired удаляет все просроченные записи из кэша.
func (c *InMemoryCache) cleanupExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	var keysToRemove []string

	// Собираем ключи просроченных записей.
	for key, elem := range c.cache {
		if now.After(elem.Value.(*entry).exp) {
			keysToRemove = append(keysToRemove, key)
		}
	}

	// Удаляем просроченные записи из списка и карты.
	for _, key := range keysToRemove {
		elem := c.cache[key]
		c.lruList.Remove(elem)
		delete(c.cache, key)
	}
}

// evictIfNeeded удаляет наименее недавно использованные записи, если превышен лимит ёмкости.
func (c *InMemoryCache) evictIfNeeded() {
	for c.lruList.Len() > c.capacity && c.capacity > 0 {
		oldest := c.lruList.Back()
		if oldest == nil {
			break
		}
		ent := oldest.Value.(*entry)
		delete(c.cache, ent.key)
		c.lruList.Remove(oldest)
	}
}

// SaveOrder сохраняет заказ в кэш с установленным временем жизни.
func (c *InMemoryCache) SaveOrder(order models.Order) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	exp := time.Now().Add(c.ttl)
	newEntry := &entry{
		key:   order.OrderUID,
		value: order,
		exp:   exp,
	}

	// Если запись с таким ключом уже существует — удаляем её.
	if elem, exists := c.cache[order.OrderUID]; exists {
		c.lruList.Remove(elem)
	}

	// Добавляем новую запись в начало списка (помечаем как недавно использованную).
	elem := c.lruList.PushFront(newEntry)
	c.cache[order.OrderUID] = elem

	// При необходимости удаляем старые записи, чтобы не превысить лимит ёмкости.
	c.evictIfNeeded()

	return nil
}

// GetOrder возвращает заказ по UID, если он существует и не просрочен.
// Возвращает заказ, флаг существования и ошибку (всегда nil в текущей реализации).
func (c *InMemoryCache) GetOrder(orderUID string) (models.Order, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.cache[orderUID]
	if !ok {
		return models.Order{}, false, nil
	}

	ent := elem.Value.(*entry)
	now := time.Now()

	// Если запись просрочена — удаляем её и возвращаем "не найдено".
	if now.After(ent.exp) {
		c.lruList.Remove(elem)
		delete(c.cache, orderUID)
		return models.Order{}, false, nil
	}

	// Обновляем позицию записи в списке (помечаем как недавно использованную).
	c.lruList.MoveToFront(elem)
	return ent.value, true, nil
}

// OrderExists проверяет, существует ли в кэше не просроченный заказ с указанным UID.
func (c *InMemoryCache) OrderExists(orderUID string) (bool, error) {
	_, ok, _ := c.GetOrder(orderUID)
	return ok, nil
}

// RemoveOrder удаляет заказ из кэша по его UID.
func (c *InMemoryCache) RemoveOrder(orderUID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.cache[orderUID]; ok {
		c.lruList.Remove(elem)
		delete(c.cache, orderUID)
	}
	return nil
}

// Clear полностью очищает кэш.
func (c *InMemoryCache) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[string]*list.Element, c.capacity)
	c.lruList = list.New()
	return nil
}

// GetAllOrders возвращает все не просроченные заказы из кэша.
func (c *InMemoryCache) GetAllOrders() ([]models.Order, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	now := time.Now()
	orders := make([]models.Order, 0, c.lruList.Len())
	for e := c.lruList.Front(); e != nil; e = e.Next() {
		ent := e.Value.(*entry)
		if now.Before(ent.exp) {
			orders = append(orders, ent.value)
		}
	}
	return orders, nil
}

// Close останавливает фоновую горутину очистки и очищает кэш.
func (c *InMemoryCache) Close() error {
	close(c.stopCh)
	c.Clear()
	return nil
}

// Проверка на соответствие интерфейсу Cache.
var _ Cache = (*InMemoryCache)(nil)
