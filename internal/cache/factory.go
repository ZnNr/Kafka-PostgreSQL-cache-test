package cache

import (
	"fmt"
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
	Capacity int // для in-memory кэша
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

// New создает кэш в зависимости от конфигурации
func New(config Config) (Cache, error) {
	switch config.Type {
	case CacheTypeRedis:
		return NewRedisCache(config.Redis.Addr, config.Redis.Password, config.Redis.DB)
	case CacheTypeInMemory:
		return NewInMemoryCache(config.Capacity), nil
	default:
		return nil, fmt.Errorf("unknown cache type: %s", config.Type)
	}
}
