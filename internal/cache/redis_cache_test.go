// internal/cache/redis_cache_test.go
package cache

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/datagenerators"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// isRedisAvailable проверяет доступность Redis
func isRedisAvailable() bool {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	ctx := context.Background()
	_, err := client.Ping(ctx).Result()
	client.Close()
	return err == nil
}

// setupTestRedisCache создает тестовый Redis кэш
func setupTestRedisCache(t *testing.T) *RedisCache {
	if !isRedisAvailable() {
		t.Skip("Redis is not available")
	}

	cache, err := NewRedisCache("localhost:6379", "", 1) // Используем DB 1 для тестов
	require.NoError(t, err)
	require.NotNil(t, cache)

	// Очищаем тестовую БД перед началом теста
	err = cache.Clear()
	require.NoError(t, err)

	t.Cleanup(func() {
		if cache != nil {
			cache.Clear()
			cache.Close()
		}
	})

	return cache
}

func TestNewRedisCache(t *testing.T) {
	if !isRedisAvailable() {
		t.Skip("Redis is not available")
	}

	// Тест с валидными параметрами
	cache, err := NewRedisCache("localhost:6379", "", 0)
	assert.NoError(t, err)
	assert.NotNil(t, cache)
	cache.Close()

	// Тест с неверным адресом
	_, err = NewRedisCache("invalid:6379", "", 0)
	assert.Error(t, err)
}

func TestRedisCache_SaveAndGetOrder(t *testing.T) {
	cache := setupTestRedisCache(t)

	// Генерируем тестовый заказ
	order := datagenerators.GenerateOrder()

	// Сохраняем заказ
	err := cache.SaveOrder(order)
	assert.NoError(t, err)

	// Получаем заказ
	retrievedOrder, exists, err := cache.GetOrder(order.OrderUID)
	assert.NoError(t, err)
	assert.True(t, exists)

	// Проверяем основные поля
	assert.Equal(t, order.OrderUID, retrievedOrder.OrderUID)
	assert.Equal(t, order.TrackNumber, retrievedOrder.TrackNumber)
	assert.Equal(t, order.EntryPoint, retrievedOrder.EntryPoint)
	assert.Equal(t, order.LocaleCode, retrievedOrder.LocaleCode)
	assert.Equal(t, order.CustomerId, retrievedOrder.CustomerId)

	// Проверяем Delivery
	assert.Equal(t, order.Delivery.Name, retrievedOrder.Delivery.Name)
	assert.Equal(t, order.Delivery.Phone, retrievedOrder.Delivery.Phone)
	assert.Equal(t, order.Delivery.Email, retrievedOrder.Delivery.Email)

	// Проверяем Payment
	assert.Equal(t, order.Payment.TransactionUID, retrievedOrder.Payment.TransactionUID)
	assert.Equal(t, order.Payment.CurrencyCode, retrievedOrder.Payment.CurrencyCode)
	assert.Equal(t, order.Payment.AmountTotal, retrievedOrder.Payment.AmountTotal)

	// Проверяем Items
	assert.Len(t, retrievedOrder.Items, len(order.Items))
	if len(order.Items) > 0 {
		assert.Equal(t, order.Items[0].ProductName, retrievedOrder.Items[0].ProductName)
		assert.Equal(t, order.Items[0].BrandName, retrievedOrder.Items[0].BrandName)
	}
}

func TestRedisCache_GetOrder_NonExistent(t *testing.T) {
	cache := setupTestRedisCache(t)

	// Пытаемся получить несуществующий заказ
	_, exists, err := cache.GetOrder("non-existent-order-uid")
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestRedisCache_OrderExists(t *testing.T) {
	cache := setupTestRedisCache(t)

	// Генерируем тестовый заказ
	order := datagenerators.GenerateOrder()

	// Проверяем несуществующий заказ
	exists, err := cache.OrderExists(order.OrderUID)
	assert.NoError(t, err)
	assert.False(t, exists)

	// Сохраняем заказ
	err = cache.SaveOrder(order)
	assert.NoError(t, err)

	// Проверяем существующий заказ
	exists, err = cache.OrderExists(order.OrderUID)
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestRedisCache_RemoveOrder(t *testing.T) {
	cache := setupTestRedisCache(t)

	// Генерируем тестовый заказ
	order := datagenerators.GenerateOrder()

	// Сохраняем заказ
	err := cache.SaveOrder(order)
	assert.NoError(t, err)

	// Проверяем, что заказ существует
	exists, err := cache.OrderExists(order.OrderUID)
	assert.NoError(t, err)
	assert.True(t, exists)

	// Удаляем заказ
	err = cache.RemoveOrder(order.OrderUID)
	assert.NoError(t, err)

	// Проверяем, что заказ удален
	exists, err = cache.OrderExists(order.OrderUID)
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestRedisCache_Clear(t *testing.T) {
	cache := setupTestRedisCache(t)

	// Добавляем несколько заказов
	for i := 0; i < 3; i++ {
		order := datagenerators.GenerateOrder()
		err := cache.SaveOrder(order)
		assert.NoError(t, err)
	}

	// Очищаем кэш
	err := cache.Clear()
	assert.NoError(t, err)

	// Проверяем, что кэш пуст
	orders, err := cache.GetAllOrders()
	assert.NoError(t, err)
	assert.Empty(t, orders)
}

func TestRedisCache_GetAllOrders(t *testing.T) {
	cache := setupTestRedisCache(t)

	// Добавляем несколько заказов
	expectedOrders := make([]models.Order, 3)
	for i := 0; i < 3; i++ {
		order := datagenerators.GenerateOrder()
		expectedOrders[i] = order
		err := cache.SaveOrder(order)
		assert.NoError(t, err)
	}

	// Получаем все заказы
	retrievedOrders, err := cache.GetAllOrders()
	assert.NoError(t, err)
	assert.Len(t, retrievedOrders, 3)

	// Создаем мапу для проверки
	orderMap := make(map[string]models.Order)
	for _, order := range retrievedOrders {
		orderMap[order.OrderUID] = order
	}

	// Проверяем, что все заказы присутствуют
	for _, expectedOrder := range expectedOrders {
		retrieved, exists := orderMap[expectedOrder.OrderUID]
		assert.True(t, exists, "Order %s should exist", expectedOrder.OrderUID)
		assert.Equal(t, expectedOrder.OrderUID, retrieved.OrderUID)
		assert.Equal(t, expectedOrder.TrackNumber, retrieved.TrackNumber)
	}
}

func TestRedisCache_SaveOrderWithTTL(t *testing.T) {
	if !isRedisAvailable() {
		t.Skip("Redis is not available")
	}

	// Создаем кэш
	cache, err := NewRedisCache("localhost:6379", "", 2)
	require.NoError(t, err)
	defer cache.Close()

	// Очищаем перед тестом
	cache.Clear()

	// Генерируем тестовый заказ
	order := datagenerators.GenerateOrder()

	// Сохраняем заказ с коротким TTL
	err = cache.saveOrderWithTTL(order, 1*time.Second)
	assert.NoError(t, err)

	// Проверяем, что заказ существует сразу после сохранения
	exists, err := cache.OrderExists(order.OrderUID)
	assert.NoError(t, err)
	assert.True(t, exists)

	// Ждем истечения TTL
	time.Sleep(2 * time.Second)

	// Проверяем, что заказ истек
	exists, err = cache.OrderExists(order.OrderUID)
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestRedisCache_MultipleOperations(t *testing.T) {
	cache := setupTestRedisCache(t)

	// Генерируем и сохраняем несколько заказов
	orders := make(map[string]models.Order)
	for i := 0; i < 5; i++ {
		order := datagenerators.GenerateOrder()
		orders[order.OrderUID] = order

		err := cache.SaveOrder(order)
		assert.NoError(t, err)
	}

	// Проверяем каждый заказ
	for uid, expectedOrder := range orders {
		retrievedOrder, exists, err := cache.GetOrder(uid)
		assert.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, expectedOrder.OrderUID, retrievedOrder.OrderUID)
		assert.Equal(t, expectedOrder.TrackNumber, retrievedOrder.TrackNumber)
	}

	// Проверяем получение всех заказов
	allOrders, err := cache.GetAllOrders()
	assert.NoError(t, err)
	assert.Len(t, allOrders, 5)

	// Удаляем один заказ
	firstOrderUID := ""
	for uid := range orders {
		firstOrderUID = uid
		break
	}
	err = cache.RemoveOrder(firstOrderUID)
	assert.NoError(t, err)

	// Проверяем, что заказ удален
	exists, err := cache.OrderExists(firstOrderUID)
	assert.NoError(t, err)
	assert.False(t, exists)

	// Проверяем количество оставшихся заказов
	allOrders, err = cache.GetAllOrders()
	assert.NoError(t, err)
	assert.Len(t, allOrders, 4)
}

func TestRedisCache_Close(t *testing.T) {
	if !isRedisAvailable() {
		t.Skip("Redis is not available")
	}

	cache, err := NewRedisCache("localhost:6379", "", 3)
	require.NoError(t, err)

	// Добавляем заказ
	order := datagenerators.GenerateOrder()
	err = cache.SaveOrder(order)
	assert.NoError(t, err)

	// Закрываем соединение
	err = cache.Close()
	assert.NoError(t, err)

	// Попытка выполнить операцию после закрытия должна вернуть ошибку
	_, _, err = cache.GetOrder(order.OrderUID)
	assert.Error(t, err)
}

func TestRedisCache_ComplexOrderStructure(t *testing.T) {
	cache := setupTestRedisCache(t)

	// Генерируем несколько заказов с разной структурой
	for i := 0; i < 10; i++ {
		order := datagenerators.GenerateOrder()

		// Сохраняем заказ
		err := cache.SaveOrder(order)
		assert.NoError(t, err, "Failed to save order %s", order.OrderUID)

		// Немедленно проверяем сохранение
		retrieved, exists, err := cache.GetOrder(order.OrderUID)
		assert.NoError(t, err, "Failed to get order %s", order.OrderUID)
		assert.True(t, exists, "Order %s should exist", order.OrderUID)

		// Проверяем целостность данных
		assert.Equal(t, order.OrderUID, retrieved.OrderUID)
		assert.Equal(t, order.TrackNumber, retrieved.TrackNumber)
		assert.Equal(t, len(order.Items), len(retrieved.Items))

		if len(order.Items) > 0 {
			assert.Equal(t, order.Items[0].ProductName, retrieved.Items[0].ProductName)
			assert.Equal(t, order.Items[0].BrandName, retrieved.Items[0].BrandName)
		}

		// Проверяем delivery
		assert.Equal(t, order.Delivery.Name, retrieved.Delivery.Name)
		assert.Equal(t, order.Delivery.Email, retrieved.Delivery.Email)

		// Проверяем payment
		assert.Equal(t, order.Payment.TransactionUID, retrieved.Payment.TransactionUID)
		assert.Equal(t, order.Payment.AmountTotal, retrieved.Payment.AmountTotal)
	}
}

func TestRedisCache_ConcurrentAccess(t *testing.T) {
	cache := setupTestRedisCache(t)

	// Количество горутин
	numGoroutines := 10
	numOrdersPerGoroutine := 5

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*numOrdersPerGoroutine*2)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()

			for j := 0; j < numOrdersPerGoroutine; j++ {
				order := datagenerators.GenerateOrder()

				// Сохраняем заказ
				if err := cache.SaveOrder(order); err != nil {
					errors <- fmt.Errorf("goroutine %d failed to save order %s: %w", routineID, order.OrderUID, err)
					return
				}

				// Немедленно читаем заказ
				_, exists, err := cache.GetOrder(order.OrderUID)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d failed to get order %s: %w", routineID, order.OrderUID, err)
					return
				}
				if !exists {
					errors <- fmt.Errorf("goroutine %d: order %s not found after save", routineID, order.OrderUID)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Проверяем ошибки
	for err := range errors {
		assert.NoError(t, err, "Concurrent access error")
	}

	// Проверяем общее количество заказов
	allOrders, err := cache.GetAllOrders()
	assert.NoError(t, err)
	assert.Len(t, allOrders, numGoroutines*numOrdersPerGoroutine)
}
