package cache

import (
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/datagenerators"
	"github.com/stretchr/testify/assert"
)

// setupTestInMemoryCache создает тестовый in-memory кэш
func setupTestInMemoryCache(t *testing.T) *InMemoryCache {
	cache := NewInMemoryCache(100)
	require.NotNil(t, cache)

	// Очищаем кэш перед началом теста
	err := cache.Clear()
	require.NoError(t, err)

	return cache
}

func TestNewInMemoryCache(t *testing.T) {
	cache := NewInMemoryCache(100)
	assert.NotNil(t, cache)
	assert.Equal(t, 0, len(cache.orders))
}

func TestInMemoryCache_SaveAndGetOrder(t *testing.T) {
	cache := setupTestInMemoryCache(t)

	order := datagenerators.GenerateOrder()

	err := cache.SaveOrder(order)
	assert.NoError(t, err)

	retrievedOrder, exists, err := cache.GetOrder(order.OrderUID)
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, order, retrievedOrder)
}

func TestInMemoryCache_GetOrder_NonExistent(t *testing.T) {
	cache := setupTestInMemoryCache(t)

	_, exists, err := cache.GetOrder("non-existent-order-uid")
	assert.NoError(t, err)
	assert.False(t, exists)
}
