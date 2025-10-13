package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/cmd/ui/menu"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/service"

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

	// Подключение к БД
	ordersRepo, err := repository.New(cfg)
	if err != nil {
		log.Fatalf("Connection to DB failed: %v", err)
	}
	defer ordersRepo.DB.Close()

	// Проверка брокеров
	brokers := cfg.Kafka.Brokers
	if len(brokers) == 0 {
		log.Fatalf("No Kafka brokers configured")
	}

	// Создание продюсера
	producer, err := ConnectProducer(brokers)
	if err != nil {
		log.Fatalf("Failed to connect to Kafka: %v", err)
	}
	defer producer.Close()

	// Создание OrderProducer
	orderProducer := service.NewOrderProducer(producer, topic)

	log.Println("Producer is launched!")
	log.Printf("📡 Connected to Kafka brokers: %v", brokers)

	// Загружаем заказы
	orders, err := loadOrdersFromDB(ordersRepo)
	if err != nil {
		log.Fatalf("Failed to load orders: %v", err)
	}

	// Основной цикл с меню
	for {
		menuInstance := menu.NewMenu(orderProducer, orders)
		menuInstance.SetReader(bufio.NewReader(os.Stdin)) // stdin
		menuInstance.Run()

		// Если меню вернуло ошибку "refresh_signal" — обновляем заказы
		var errRefresh error
		orders, errRefresh = loadOrdersFromDB(ordersRepo)
		if errRefresh != nil {
			log.Printf("Failed to refresh orders: %v", errRefresh)
		} else {
			log.Printf("Refreshed: loaded %d orders from DB", len(orders))
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

// processOrder обрабатывает команду пользователя: генерация, выбор или отправка невалидных данных
func processOrder(command string, orders []models.Order, repo *repository.OrdersRepo, orderProducer *service.OrderProducer) error {
	var order models.Order
	var orderJSON []byte
	var err error

	switch command {
	case "s":
		// Генерация нового валидного заказа
		order = datagenerators.GenerateOrder()
		log.Printf("Generated NEW valid order: %s", order.OrderUID)

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
		log.Printf("Selected existing order: %s", order.OrderUID)

	case "i":
		// Невалидный: пустой OrderUID
		order = datagenerators.GenerateInvalidOrder_EmptyUID()
		log.Printf("Generated INVALID order (empty OrderUID): %s", order.OrderUID)

	case "n":
		// Невалидный: отрицательная сумма
		order = datagenerators.GenerateInvalidOrder_NegativeAmount()
		log.Printf("Generated INVALID order (negative amount): %s", order.OrderUID)

	case "e":
		// Невалидный: некорректный email
		order = datagenerators.GenerateInvalidOrder_InvalidEmail()
		log.Printf("Generated INVALID order (invalid email): %s", order.OrderUID)

	case "b":
		// Невалидный: скидка >100%
		order = datagenerators.GenerateInvalidOrder_BigSalePercent()
		log.Printf("Generated INVALID order (sale >100%%): %s", order.OrderUID)

	default:
		return fmt.Errorf("unknown command: %s", command)
	}

	// Для всех случаев преобразуем в JSON
	orderJSON, err = json.Marshal(order)
	if err != nil {
		return fmt.Errorf("failed to marshal order: %w", err)
	}

	// Валидация только для валидных типов (s, c)
	if command == "s" || command == "c" {
		if err := validateOrder(order); err != nil {
			return fmt.Errorf("validation failed: %w", err)
		}
	}

	// Отправляем в Kafka через OrderProducer
	if err := orderProducer.Send(orderJSON); err != nil {
		return fmt.Errorf("failed to send to Kafka: %w", err)
	}

	log.Printf("Successfully sent order %s to Kafka topic", getUID(order))
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
