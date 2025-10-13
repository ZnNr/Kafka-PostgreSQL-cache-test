package cache

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/datagenerators"
)

// setupTestInMemoryCache создаёт тестовый кэш с заданной ёмкостью и TTL = 1 секунда
func setupTestInMemoryCache(t *testing.T, capacity int) *InMemoryCache {
	cache := NewInMemoryCache(capacity, 10)
	require.NotNil(t, cache)
	err := cache.Clear()
	require.NoError(t, err)
	return cache
}

func TestNewInMemoryCache(t *testing.T) {
	cache := NewInMemoryCache(100, 10*time.Minute)
	assert.NotNil(t, cache)
	assert.Equal(t, 100, cache.capacity)
	assert.Equal(t, 10*time.Minute, cache.ttl)
}

func TestInMemoryCache_SaveAndGetOrder(t *testing.T) {
	cache := setupTestInMemoryCache(t, 100)

	order := datagenerators.GenerateOrder()
	err := cache.SaveOrder(order)
	require.NoError(t, err)

	retrievedOrder, exists, err := cache.GetOrder(order.OrderUID)
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, order, retrievedOrder)
}

func TestInMemoryCache_GetOrder_NonExistent(t *testing.T) {
	cache := setupTestInMemoryCache(t, 100)

	_, exists, err := cache.GetOrder("non-existent-order-uid")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestInMemoryCache_OrderExists(t *testing.T) {
	cache := setupTestInMemoryCache(t, 100)

	order := datagenerators.GenerateOrder()
	err := cache.SaveOrder(order)
	require.NoError(t, err)

	exists, err := cache.OrderExists(order.OrderUID)
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = cache.OrderExists("non-existent")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestInMemoryCache_RemoveOrder(t *testing.T) {
	cache := setupTestInMemoryCache(t, 100)

	order := datagenerators.GenerateOrder()
	err := cache.SaveOrder(order)
	require.NoError(t, err)

	err = cache.RemoveOrder(order.OrderUID)
	require.NoError(t, err)

	_, exists, err := cache.GetOrder(order.OrderUID)
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestInMemoryCache_Clear(t *testing.T) {
	cache := setupTestInMemoryCache(t, 100)

	order1 := datagenerators.GenerateOrder()
	order2 := datagenerators.GenerateOrder()
	err := cache.SaveOrder(order1)
	require.NoError(t, err)
	err = cache.SaveOrder(order2)
	require.NoError(t, err)

	err = cache.Clear()
	require.NoError(t, err)

	orders, err := cache.GetAllOrders()
	require.NoError(t, err)
	assert.Empty(t, orders)
}

func TestInMemoryCache_TTL_Expiry(t *testing.T) {
	cache := NewInMemoryCache(100, 100*time.Millisecond)

	order := datagenerators.GenerateOrder()
	err := cache.SaveOrder(order)
	require.NoError(t, err)

	// Проверяем, что запись есть
	_, exists, err := cache.GetOrder(order.OrderUID)
	require.NoError(t, err)
	assert.True(t, exists)

	// Ждём истечения TTL
	time.Sleep(150 * time.Millisecond)

	// Теперь запись должна быть удалена
	_, exists, err = cache.GetOrder(order.OrderUID)
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestInMemoryCache_BackgroundCleanup(t *testing.T) {
	cache := NewInMemoryCache(100, 100*time.Millisecond)

	order := datagenerators.GenerateOrder()
	err := cache.SaveOrder(order)
	require.NoError(t, err)

	// Ждём, пока фоновая горутина удалит запись
	time.Sleep(300 * time.Millisecond)

	orders, err := cache.GetAllOrders()
	require.NoError(t, err)
	assert.Empty(t, orders, "Фоновая очистка должна удалить просроченные записи")
}

func TestInMemoryCache_Close(t *testing.T) {
	cache := setupTestInMemoryCache(t, 100)

	order := datagenerators.GenerateOrder()
	err := cache.SaveOrder(order)
	require.NoError(t, err)

	err = cache.Close()
	require.NoError(t, err)

	// После Close кэш должен быть пуст
	orders, err := cache.GetAllOrders()
	require.NoError(t, err)
	assert.Empty(t, orders)
}

func TestInMemoryCache_ConcurrentAccess(t *testing.T) {
	cache := setupTestInMemoryCache(t, 1000)
	order := datagenerators.GenerateOrder()

	const goroutines = 10
	const operations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < operations; j++ {
				// Сохраняем
				err := cache.SaveOrder(order)
				assert.NoError(t, err)

				// Получаем
				_, _, err = cache.GetOrder(order.OrderUID)
				assert.NoError(t, err)

				// Проверяем существование
				_, err = cache.OrderExists(order.OrderUID)
				assert.NoError(t, err)
			}
		}()
	}

	wg.Wait()

	// Убедимся, что запись всё ещё на месте
	_, exists, err := cache.GetOrder(order.OrderUID)
	require.NoError(t, err)
	assert.True(t, exists)
}
