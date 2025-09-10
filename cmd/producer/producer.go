package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/config"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/datagenerators"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/repository"
)

var (
	cfgPath = "config/config.yaml"
)

func main() {
	topic := "orders"

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Подключение к базе данных
	ordersRepo, err := repository.New(cfg)
	if err != nil {
		log.Fatalf("Connection to DB failed: %v", err)
	}
	defer func() {
		if err := ordersRepo.DB.Close(); err != nil {
			log.Fatalf("Failed to close database connection: %v", err)
		}
	}()

	// Проверка наличия брокеров
	brokers := cfg.Kafka.Brokers
	if len(brokers) == 0 {
		log.Fatalf("No Kafka brokers configured")
	}

	// Создание продюсера
	producer, err := ConnectProducer(brokers)
	if err != nil {
		log.Fatalf("Failed to connect to Kafka: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalf("Failed to close Kafka producer: %v", err)
		}
	}()

	log.Println("Producer is launched!")
	log.Printf("Connected to Kafka brokers: %v", brokers)

	orders, err := loadOrdersFromDB(ordersRepo)
	if err != nil {
		log.Fatalf("Failed to load orders: %v", err)
	}

	for {
		fmt.Println("\n=== Order Producer ===")
		fmt.Println("s - Generate and send new order")
		fmt.Println("c - Send copy of existing order")
		fmt.Println("r - Refresh orders list from database")
		fmt.Println("exit - Quit program")
		fmt.Print("Choose option: ")

		var input string
		fmt.Scanln(&input)

		switch input {
		case "exit":
			fmt.Println("Exiting the program...")
			return

		case "r":
			orders, err = loadOrdersFromDB(ordersRepo)
			if err != nil {
				log.Printf("Failed to refresh orders: %v", err)
			}
			continue

		case "s", "c":
			// Обработка генерации или выбора заказа
			if err := processOrder(input, orders, ordersRepo, producer, topic); err != nil {
				log.Printf("Error processing order: %v", err)
			}

		default:
			fmt.Println("Invalid input. Please choose s, c, r or exit")
		}
	}
}

// loadOrdersFromDB загружает заказы из базы данных
func loadOrdersFromDB(repo *repository.OrdersRepo) ([]models.Order, error) {
	orders, err := repo.GetOrders()
	if err != nil {
		return nil, fmt.Errorf("failed to get orders from DB: %w", err)
	}
	log.Printf("Loaded %d orders from database", len(orders))
	return orders, nil
}

// processOrder обрабатывает команду генерации или выбора заказа
func processOrder(command string, orders []models.Order, repo *repository.OrdersRepo, producer sarama.SyncProducer, topic string) error {
	var order models.Order
	var err error

	switch command {
	case "s":
		// Генерация нового заказа
		order = datagenerators.GenerateOrder()
		log.Printf("Generated new order: %s", order.OrderUID)

	case "c":
		if len(orders) == 0 {
			return fmt.Errorf("no orders available to select")
		}

		fmt.Println("Available orders:")
		for i, o := range orders {
			fmt.Printf("%d: %s (Created: %s)\n", i, o.OrderUID, o.DateCreated.Format("2006-01-02 15:04"))
		}

		fmt.Print("Select order number: ")
		var input string
		fmt.Scanln(&input)

		index, err := strconv.Atoi(input)
		if err != nil {
			return fmt.Errorf("invalid number: %w", err)
		}

		if index < 0 || index >= len(orders) {
			return fmt.Errorf("index out of range (0-%d)", len(orders)-1)
		}

		order = orders[index]
		log.Printf("Selected order: %s", order.OrderUID)
	}

	// Валидация заказа перед отправкой
	if err := validateOrder(order); err != nil {
		return fmt.Errorf("order validation failed: %w", err)
	}

	orderJSON, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("failed to marshal order: %w", err)
	}

	if err := PushOrderToQueue(producer, topic, orderJSON); err != nil {
		return fmt.Errorf("failed to send to Kafka: %w", err)
	}

	log.Printf("Successfully sent order %s to Kafka", order.OrderUID)
	return nil
}

// validateOrder выполняет базовую валидацию заказа
func validateOrder(order models.Order) error {
	if order.OrderUID == "" {
		return fmt.Errorf("order UID is required")
	}
	if order.TrackNumber == "" {
		return fmt.Errorf("track number is required")
	}
	if len(order.Items) == 0 {
		return fmt.Errorf("order must contain at least one item")
	}
	return nil
}

// ConnectProducer создает надежного продюсера Kafka
func ConnectProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()

	// Настройки для надежной доставки
	config.Producer.RequiredAcks = sarama.WaitForAll // Ждем подтверждения от всех реплик
	config.Producer.Retry.Max = 5                    // Максимальное количество попыток
	config.Producer.Retry.Backoff = 1 * time.Second  // Задержка между попытками
	config.Producer.Return.Successes = true          // Получаем подтверждения
	config.Producer.Timeout = 30 * time.Second       // Таймаут операций
	config.Producer.MaxMessageBytes = 1000000        // Максимальный размер сообщения 1MB

	// Настройки сжатия для эффективности
	config.Producer.Compression = sarama.CompressionSnappy

	// Идентификатор клиента для мониторинга
	config.ClientID = "order-producer"

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return producer, nil
}

// PushOrderToQueue отправляет сообщение в Kafka с обработкой ошибок
func PushOrderToQueue(producer sarama.SyncProducer, topic string, message []byte) error {
	if producer == nil {
		return fmt.Errorf("producer is not initialized")
	}
	if len(message) == 0 {
		return fmt.Errorf("message is empty")
	}
	if topic == "" {
		return fmt.Errorf("topic is required")
	}

	// Создание сообщения с ключом для партиционирования
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	log.Printf("Message successfully delivered to topic %s, partition %d, offset %d",
		topic, partition, offset)

	return nil
}
