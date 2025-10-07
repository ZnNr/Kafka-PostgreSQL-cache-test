package cache

import (
	"fmt"
	"time"
)

// CacheType тип кэша
type CacheType string

const (
	CacheTypeRedis    CacheType = "redis"
	CacheTypeInMemory CacheType = "inmemory"
)

// Config конфигурация кэша
type Config struct {
	Type     CacheType
	Redis    RedisConfig
	Capacity int           // для in-memory кэша: максимальное количество записей
	TTL      time.Duration // для in-memory кэша: время жизни записи
}
type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

// New создает кэш в зависимости от конфигурации
func New(config Config, err error) (Cache, error) {
	switch config.Type {
	case CacheTypeRedis:
		return NewRedisCache(config.Redis.Addr, config.Redis.Password, config.Redis.DB)
	case CacheTypeInMemory:
		if config.Capacity <= 0 {
			return nil, fmt.Errorf("in-memory cache capacity must be > 0")
		}
		// Если TTL не задан — используем разумное значение по умолчанию, например 1 час
		ttl := config.TTL
		if ttl == 0 {
			ttl = 1 * time.Hour
		}
		return NewInMemoryCache(config.Capacity, ttl), nil
	default:
		return nil, fmt.Errorf("unknown cache type: %s", config.Type)
	}
}
