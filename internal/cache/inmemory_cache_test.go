package cache

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/datagenerators"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models" // ← обязательно!
)

// setupTestInMemoryCache создаёт тестовый кэш с заданной ёмкостью и TTL = 1 секунда
func setupTestInMemoryCache(t *testing.T, capacity int) *InMemoryCache {
	cache := NewInMemoryCache(capacity, 1*time.Second)
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

func TestInMemoryCache_GetAllOrders(t *testing.T) {
	cache := setupTestInMemoryCache(t, 100)

	order1 := datagenerators.GenerateOrder()
	order2 := datagenerators.GenerateOrder()
	err := cache.SaveOrder(order1)
	require.NoError(t, err)
	err = cache.SaveOrder(order2)
	require.NoError(t, err)

	orders, err := cache.GetAllOrders()
	require.NoError(t, err)
	assert.Len(t, orders, 2)
	uids := map[string]bool{
		order1.OrderUID: true,
		order2.OrderUID: true,
	}
	for _, o := range orders {
		assert.True(t, uids[o.OrderUID])
	}
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

func TestInMemoryCache_LRU_Eviction(t *testing.T) {
	capacity := 2
	cache := setupTestInMemoryCache(t, capacity)

	// Сохраняем capacity + 1 записей
	orders := make([]models.Order, capacity+1) // ← ИСПРАВЛЕНО: models.Order
	for i := 0; i < len(orders); i++ {
		orders[i] = datagenerators.GenerateOrder()
		err := cache.SaveOrder(orders[i])
		require.NoError(t, err)
	}

	// Проверяем, что первая запись вытеснена (LRU)
	_, exists, err := cache.GetOrder(orders[0].OrderUID)
	require.NoError(t, err)
	assert.False(t, exists, "Первая запись должна быть вытеснена по LRU")

	// Последние две записи должны быть на месте
	for i := 1; i < len(orders); i++ {
		_, exists, err := cache.GetOrder(orders[i].OrderUID)
		require.NoError(t, err)
		assert.True(t, exists, "Запись %d должна существовать", i)
	}
}

func TestInMemoryCache_UpdateOrder_ResetsLRU(t *testing.T) {
	cache := setupTestInMemoryCache(t, 2)

	order1 := datagenerators.GenerateOrder()
	order2 := datagenerators.GenerateOrder()
	order3 := datagenerators.GenerateOrder()

	err := cache.SaveOrder(order1)
	require.NoError(t, err)
	err = cache.SaveOrder(order2)
	require.NoError(t, err)

	// Получаем order1 — это обновит его позицию в LRU
	_, _, err = cache.GetOrder(order1.OrderUID)
	require.NoError(t, err)

	// Теперь сохраняем order3 — должна вытесниться order2 (а не order1)
	err = cache.SaveOrder(order3)
	require.NoError(t, err)

	// Проверяем: order1 и order3 есть, order2 — нет
	_, exists, _ := cache.GetOrder(order1.OrderUID)
	assert.True(t, exists)
	_, exists, _ = cache.GetOrder(order3.OrderUID)
	assert.True(t, exists)
	_, exists, _ = cache.GetOrder(order2.OrderUID)
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
