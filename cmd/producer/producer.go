package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
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

	log.Println("✅ Producer is launched!")
	log.Printf("📡 Connected to Kafka brokers: %v", brokers)

	// Загружаем заказы из БД
	orders, err := loadOrdersFromDB(ordersRepo)
	if err != nil {
		log.Fatalf("Failed to load orders: %v", err)
	}

	// Основной цикл
	for {
		fmt.Println("\n=== Order Producer ===")
		fmt.Println("s - Generate and send new VALID order")
		fmt.Println("c - Send copy of existing order from DB")
		fmt.Println("i - Send INVALID order (empty OrderUID)")
		fmt.Println("n - Send INVALID order (negative amount)")
		fmt.Println("e - Send INVALID order (invalid email)")
		fmt.Println("b - Send INVALID order (sale >100%)")
		fmt.Println("m - Send MALFORMED JSON (unparsable)")
		fmt.Println("r - Refresh orders list from database")
		fmt.Println("exit - Quit program")
		fmt.Print("Choose option: ")

		var input string
		_, err := fmt.Scanln(&input)
		if err != nil {
			log.Printf("Input error: %v", err)
			continue
		}

		switch input {
		case "exit":
			fmt.Println("👋 Exiting the program...")
			return

		case "r":
			orders, err = loadOrdersFromDB(ordersRepo)
			if err != nil {
				log.Printf("❌ Failed to refresh orders: %v", err)
			} else {
				log.Printf("🔁 Refreshed: loaded %d orders from DB", len(orders))
			}

		case "s":
			order := datagenerators.GenerateOrder()
			log.Printf("✅ Generated NEW valid order: %s", order.OrderUID)

			if err := validateOrder(order); err != nil {
				log.Printf("❌ Validation failed: %v", err)
				continue
			}

			data, err := json.Marshal(order)
			if err != nil {
				log.Printf("❌ Marshal error: %v", err)
				continue
			}

			if err := PushOrderToQueue(producer, topic, data); err != nil {
				log.Printf("❌ Failed to send to Kafka: %v", err)
			} else {
				log.Printf("📤 Sent valid order %s to '%s'", order.OrderUID, topic)
			}

		case "c":
			if len(orders) == 0 {
				fmt.Println("🚫 No orders available in database.")
				continue
			}

			fmt.Println("Available orders:")
			for i, o := range orders {
				fmt.Printf("%d: %s (Created: %s)\n", i, o.OrderUID, o.DateCreated.Format("2006-01-02 15:04"))
			}

			fmt.Print("Select order number: ")
			var idxStr string
			fmt.Scanln(&idxStr)

			idx, err := strconv.Atoi(idxStr)
			if err != nil || idx < 0 || idx >= len(orders) {
				log.Printf("❌ Invalid selection: %s", idxStr)
				continue
			}

			selected := orders[idx]
			log.Printf("📋 Selected order: %s", selected.OrderUID)

			if err := validateOrder(selected); err != nil {
				log.Printf("❌ Validation failed: %v", err)
				continue
			}

			data, err := json.Marshal(selected)
			if err != nil {
				log.Printf("❌ Marshal error: %v", err)
				continue
			}

			if err := PushOrderToQueue(producer, topic, data); err != nil {
				log.Printf("❌ Failed to send to Kafka: %v", err)
			} else {
				log.Printf("📤 Sent existing order %s to '%s'", selected.OrderUID, topic)
			}

		// === Невалидные заказы ===
		case "i":
			order := datagenerators.GenerateInvalidOrder_EmptyUID()
			log.Printf("❌ Generated invalid order (empty UID): %s", order.OrderUID)
			data, _ := json.Marshal(order)
			if err := PushOrderToQueue(producer, topic, data); err != nil {
				log.Printf("❌ Failed to send: %v", err)
			} else {
				log.Printf("📤 Sent to Kafka (expect DLQ): %s", getUID(order))
			}

		case "n":
			order := datagenerators.GenerateInvalidOrder_NegativeAmount()
			log.Printf("❌ Generated invalid order (negative amount): %s", order.OrderUID)
			data, _ := json.Marshal(order)
			if err := PushOrderToQueue(producer, topic, data); err != nil {
				log.Printf("❌ Failed to send: %v", err)
			} else {
				log.Printf("📤 Sent to Kafka (expect DLQ): %s", getUID(order))
			}

		case "e":
			order := datagenerators.GenerateInvalidOrder_InvalidEmail()
			log.Printf("❌ Generated invalid order (invalid email): %s", order.OrderUID)
			data, _ := json.Marshal(order)
			if err := PushOrderToQueue(producer, topic, data); err != nil {
				log.Printf("❌ Failed to send: %v", err)
			} else {
				log.Printf("📤 Sent to Kafka (expect DLQ): %s", getUID(order))
			}

		case "b":
			order := datagenerators.GenerateInvalidOrder_BigSalePercent()
			log.Printf("❌ Generated invalid order (sale >100%%): %s", order.OrderUID)
			data, _ := json.Marshal(order)
			if err := PushOrderToQueue(producer, topic, data); err != nil {
				log.Printf("❌ Failed to send: %v", err)
			} else {
				log.Printf("📤 Sent to Kafka (expect DLQ): %s", getUID(order))
			}

		case "m":
			rawMessage := datagenerators.GenerateMalformedJSON_ReturnsBytes()
			if len(rawMessage) == 0 {
				log.Printf("⚠️  Empty malformed message generated")
				continue
			}
			if err := PushOrderToQueue(producer, topic, rawMessage); err != nil {
				log.Printf("❌ Failed to send malformed JSON: %v", err)
			} else {
				log.Printf("📤 Sent %d-byte MALFORMED JSON (expect unmarshal error)", len(rawMessage))
			}

		default:
			fmt.Println("❌ Invalid input. Please choose:")
			fmt.Println("  s, c — валидные заказы")
			fmt.Println("  i, n, e, b — невалидные")
			fmt.Println("  m — битый JSON")
			fmt.Println("  r — обновить")
			fmt.Println("  exit — выйти")
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
// processOrder обрабатывает команду пользователя: генерация, выбор или отправка невалидных данных
func processOrder(command string, orders []models.Order, repo *repository.OrdersRepo, producer sarama.SyncProducer, topic string) error {
	var order models.Order
	var orderJSON []byte
	var err error
	switch command {
	case "s":
		// Генерация нового валидного заказа
		order = datagenerators.GenerateOrder()
		log.Printf("✅ Generated NEW valid order: %s", order.OrderUID)

	case "c":
		// Выбор существующего заказа из БД
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
		log.Printf("📋 Selected existing order: %s", order.OrderUID)

	case "i":
		// Невалидный: пустой OrderUID
		order = datagenerators.GenerateInvalidOrder_EmptyUID()
		log.Printf("❌ Generated INVALID order (empty OrderUID): %s", order.OrderUID)

	case "n":
		// Невалидный: отрицательная сумма
		order = datagenerators.GenerateInvalidOrder_NegativeAmount()
		log.Printf("❌ Generated INVALID order (negative amount): %s", order.OrderUID)

	case "e":
		// Невалидный: некорректный email
		order = datagenerators.GenerateInvalidOrder_InvalidEmail()
		log.Printf("❌ Generated INVALID order (invalid email): %s", order.OrderUID)

	case "b":
		// Невалидный: скидка >100%
		order = datagenerators.GenerateInvalidOrder_BigSalePercent()
		log.Printf("❌ Generated INVALID order (sale >100%%): %s", order.OrderUID)

	default:
		return fmt.Errorf("unknown command: %s", command)
	}

	// Для всех случаев, кроме "m", преобразуем в JSON
	orderJSON, err = json.Marshal(order)
	if err != nil {
		return fmt.Errorf("failed to marshal order: %w", err)
	}

	// Пропускаем валидацию только для явно невалидных заказов
	if !strings.HasPrefix(command, "i") && !strings.HasPrefix(command, "n") &&
		!strings.HasPrefix(command, "e") && !strings.HasPrefix(command, "b") {
		// Валидируем только валидные/существующие заказы
		if err := validateOrder(order); err != nil {
			return fmt.Errorf("order validation failed: %w", err)
		}
	}

	// Отправляем в Kafka
	if err := PushOrderToQueue(producer, topic, orderJSON); err != nil {
		return fmt.Errorf("failed to send to Kafka: %w", err)
	}

	log.Printf("📤 Successfully sent order %s to Kafka topic '%s'", getUID(order), topic)
	return nil
}

func getUID(order models.Order) string {
	if order.OrderUID != "" {
		return order.OrderUID
	}
	return "(empty)"
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
