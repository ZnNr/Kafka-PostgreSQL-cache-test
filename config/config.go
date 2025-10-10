// config/config.go
package config

import (
	"fmt"
	"os"
	"time"

	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/cache"
	"github.com/ilyakaznacheev/cleanenv"
)

// Config полная конфигурацию приложения
type Config struct {
	DB    ConfigDB    `yaml:"db"`
	App   ConfigApp   `yaml:"app"`
	Kafka KafkaConfig `yaml:"kafka"`
	Cache CacheConfig `yaml:"cache,omitempty"`
}

// ConfigApp конфигурация HTTP сервера
type ConfigApp struct {
	Host string `yaml:"host" env:"APP_HOST" env-default:"localhost"`
	Port string `yaml:"port" env:"APP_PORT" env-default:"8080"`
}

// ConfigDB конфигурация базы данных
type ConfigDB struct {
	Port     string `yaml:"port" env:"DB_PORT" env-default:"5432"`
	Host     string `yaml:"host" env:"DB_HOST" env-default:"localhost"`
	Name     string `yaml:"name" env:"DB_NAME" env-default:"orders_db"`
	User     string `yaml:"user" env:"DB_USER" env-default:"my_user"`
	Password string `yaml:"password" env:"DB_PASSWORD" env-default:"my_password"`
}

// KafkaConfig конфигурация Kafka
type KafkaConfig struct {
	Brokers  []string `yaml:"brokers" env:"KAFKA_BROKERS" env-separator:"," env-default:"localhost:9092"`
	Topic    string   `yaml:"topic" env:"KAFKA_TOPIC" env-default:"orders"`
	DlqTopic string   `yaml:"dlq_topic" env:"KAFKA_DLQ_TOPIC" env-default:"orders.dlq"`
}

// CacheConfig конфигурация кэша
type CacheConfig struct {
	Type     cache.CacheType `yaml:"type" env:"CACHE_TYPE" env-default:"redis"`
	Host     string          `yaml:"host" env:"CACHE_HOST" env-default:"localhost"`
	Port     string          `yaml:"port" env:"CACHE_PORT" env-default:"6379"`
	Password string          `yaml:"password" env:"CACHE_PASSWORD" env-default:""`
	DB       int             `yaml:"db" env:"CACHE_DB" env-default:"0"`
	Capacity int             `yaml:"capacity" env:"CACHE_CAPACITY" env-default:"1000"`
	TTL      string          `yaml:"ttl" env:"CACHE_TTL" env-default:"30m"`
}

// Load загружает конфигурацию из файла
func Load(cfgPath string) (*Config, error) {
	if _, err := os.Stat(cfgPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file not found: %s", cfgPath)
	}

	var cfg Config
	if err := cleanenv.ReadConfig(cfgPath, &cfg); err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	// Валидация обязательных полей
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &cfg, nil
}

// LoadFromEnv загружает конфигурацию из переменных окружения
func LoadFromEnv() (*Config, error) {
	var cfg Config
	if err := cleanenv.ReadEnv(&cfg); err != nil {
		return nil, fmt.Errorf("error reading config from environment: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &cfg, nil
}

// Validate проверяет корректность конфигурации
func (c *Config) Validate() error {
	if c.App.Host == "" {
		return fmt.Errorf("app.host is required")
	}
	if c.App.Port == "" {
		return fmt.Errorf("app.port is required")
	}
	if c.DB.Host == "" {
		return fmt.Errorf("db.host is required")
	}
	if c.DB.Name == "" {
		return fmt.Errorf("db.name is required")
	}
	if c.DB.User == "" {
		return fmt.Errorf("db.user is required")
	}
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers is required")
	}
	if c.Kafka.Topic == "" {
		return fmt.Errorf("kafka.topic is required")
	}
	if c.Kafka.DlqTopic == "" { // ← новая проверка
		return fmt.Errorf("kafka.dlq_topic is required")
	}

	if err := c.Cache.Validate(); err != nil {
		return fmt.Errorf("cache validation failed: %w", err)
	}

	return nil
}

// Validate проверяет корректность конфигурации кэша
func (c *CacheConfig) Validate() error {
	switch c.Type {
	case cache.CacheTypeRedis:
		if c.Host == "" {
			return fmt.Errorf("cache.host is required for redis cache")
		}
		if c.Port == "" {
			return fmt.Errorf("cache.port is required for redis cache")
		}
	case cache.CacheTypeInMemory:
		if c.Capacity <= 0 {
			return fmt.Errorf("cache.capacity must be positive for in-memory cache")
		}
		// Проверяем, что TTL парсится
		if c.TTL != "" {
			if _, err := time.ParseDuration(c.TTL); err != nil {
				return fmt.Errorf("invalid cache.ttl format: %w", err)
			}
		}
	default:
		return fmt.Errorf("unknown cache type: %s", c.Type)
	}
	return nil
}

// GetDBConnectionString возвращает строку подключения к базе данных
func (c *ConfigDB) GetDBConnectionString() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		c.Host, c.Port, c.User, c.Password, c.Name)
}

// GetAppAddress возвращает адрес приложения в формате host:port
func (c *ConfigApp) GetAppAddress() string {
	return fmt.Sprintf("%s:%s", c.Host, c.Port)
}

// GetCacheAddress возвращает адрес кэша в формате host:port (только для Redis)
func (c *CacheConfig) GetCacheAddress() string {
	if c.Type != cache.CacheTypeRedis {
		return ""
	}
	return fmt.Sprintf("%s:%s", c.Host, c.Port)
}

// ToCacheConfig преобразует в конфиг для фабрики кэша
func (c *CacheConfig) ToCacheConfig() (cache.Config, error) {
	var ttl time.Duration
	var err error

	if c.TTL != "" {
		ttl, err = time.ParseDuration(c.TTL)
		if err != nil {
			return cache.Config{}, fmt.Errorf("invalid cache.ttl format: %w", err)
		}
	} else {
		// Значение по умолчанию, если TTL пустой (хотя env-default задан)
		ttl = 30 * time.Minute
	}

	return cache.Config{
		Type: c.Type,
		Redis: cache.RedisConfig{
			Addr:     c.GetCacheAddress(),
			Password: c.Password,
			DB:       c.DB,
		},
		Capacity: c.Capacity,
		TTL:      ttl,
	}, nil
}
