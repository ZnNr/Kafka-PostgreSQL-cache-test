// internal/cache/redis_cache.go
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models"
	"github.com/go-redis/redis/v8"
)

type RedisCache struct {
	client *redis.Client
	ctx    context.Context
}

// NewRedisCache создает новый Redis кэш
func NewRedisCache(redisAddr string, password string, db int) (*RedisCache, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: password,
		DB:       db,
	})

	ctx := context.Background()

	// Проверяем подключение к Redis
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisCache{
		client: client,
		ctx:    ctx,
	}, nil
}

// SaveOrder сохраняет заказ в Redis (использует TTL по умолчанию 24 часа)
func (c *RedisCache) SaveOrder(order models.Order) error {
	return c.saveOrderWithTTL(order, 24*time.Hour)
}

// saveOrderWithTTL сохраняет заказ в Redis с указанным TTL
func (c *RedisCache) saveOrderWithTTL(order models.Order, ttl time.Duration) error {
	orderJSON, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("failed to marshal order: %w", err)
	}

	key := c.getOrderKey(order.OrderUID)
	err = c.client.Set(c.ctx, key, orderJSON, ttl).Err()
	if err != nil {
		return fmt.Errorf("failed to save order to Redis: %w", err)
	}

	return nil
}

// GetOrder получает заказ из Redis по UID
func (c *RedisCache) GetOrder(orderUID string) (models.Order, bool, error) {
	key := c.getOrderKey(orderUID)
	orderJSON, err := c.client.Get(c.ctx, key).Result()

	if err == redis.Nil {
		return models.Order{}, false, nil
	}
	if err != nil {
		return models.Order{}, false, fmt.Errorf("failed to get order from Redis: %w", err)
	}

	var order models.Order
	err = json.Unmarshal([]byte(orderJSON), &order)
	if err != nil {
		return models.Order{}, false, fmt.Errorf("failed to unmarshal order: %w", err)
	}

	return order, true, nil
}

// OrderExists проверяет существование заказа в Redis
func (c *RedisCache) OrderExists(orderUID string) (bool, error) {
	key := c.getOrderKey(orderUID)
	exists, err := c.client.Exists(c.ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check order existence: %w", err)
	}

	return exists > 0, nil
}

// RemoveOrder удаляет заказ из Redis по UID
func (c *RedisCache) RemoveOrder(orderUID string) error {
	key := c.getOrderKey(orderUID)
	err := c.client.Del(c.ctx, key).Err()
	if err != nil {
		return fmt.Errorf("failed to remove order from Redis: %w", err)
	}
	return nil
}

// Clear очищает все ключи заказов из Redis
func (c *RedisCache) Clear() error {
	pattern := c.getOrderKey("*")
	iter := c.client.Scan(c.ctx, 0, pattern, 0).Iterator()

	for iter.Next(c.ctx) {
		err := c.client.Del(c.ctx, iter.Val()).Err()
		if err != nil {
			return fmt.Errorf("failed to delete key %s: %w", iter.Val(), err)
		}
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("failed to scan keys: %w", err)
	}

	return nil
}

// GetAllOrders возвращает список всех заказов из Redis
func (c *RedisCache) GetAllOrders() ([]models.Order, error) {
	pattern := c.getOrderKey("*")
	keys, err := c.client.Keys(c.ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get keys: %w", err)
	}

	var orders []models.Order
	for _, key := range keys {
		orderJSON, err := c.client.Get(c.ctx, key).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to get order %s: %w", key, err)
		}

		var order models.Order
		err = json.Unmarshal([]byte(orderJSON), &order)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal order %s: %w", key, err)
		}

		orders = append(orders, order)
	}

	return orders, nil
}

// Close закрывает соединение с Redis
func (c *RedisCache) Close() error {
	return c.client.Close()
}

// getOrderKey формирует ключ для хранения заказа в Redis
func (c *RedisCache) getOrderKey(orderUID string) string {
	return fmt.Sprintf("order:%s", orderUID)
}

// Ensure RedisCache implements Cache interface
var _ Cache = (*RedisCache)(nil)
