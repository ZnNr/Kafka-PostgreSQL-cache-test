// internal/datagenerators/orders_generator.go
package datagenerators

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models"
	"github.com/google/uuid"
)

func GenerateOrder() models.Order {

	// Генерируем дату создания
	dateCreated := time.Now()

	order := models.Order{
		OrderUID:          uuid.New().String(),
		TrackNumber:       randomString(15),
		EntryPoint:        randomString(4),
		LocaleCode:        generateLocale(),
		InternalSignature: randomString(16),
		CustomerId:        randomString(8),
		DeliveryService:   randomString(6),
		ShardKey:          fmt.Sprintf("%d", rand.Intn(10)),
		StateMachineID:    rand.Intn(100),
		DateCreated:       dateCreated,
		OOFShard:          fmt.Sprintf("%d", rand.Intn(5)),

		Delivery: generateDelivery(),
		Payment:  generatePayment(),
		Items:    generateItems(),
	}

	// Устанавливаем OrderUID в Delivery
	order.Delivery.OrderUID = order.OrderUID

	return order
}

func generateDelivery() models.Delivery {
	return models.Delivery{
		Name:    randomFullName(),
		Phone:   randomPhone(),
		Zip:     randomZip(),
		City:    randomString(10),
		Address: randomString(20),
		Region:  randomString(12),
		Email:   randomEmail(),
	}
}

func generatePayment() models.Payment {
	currencies := []string{"USD", "RUB", "EUR", "CNY"}
	providers := []string{"wbpay", "yookassa", "stripe", "paypal"}
	banks := []string{"alpha", "sber", "vtb", "raiffeisen"}

	return models.Payment{
		TransactionUID:  uuid.New().String(),
		RequestID:       uuid.New().String(),
		CurrencyCode:    currencies[rand.Intn(len(currencies))],
		PaymentProvider: providers[rand.Intn(len(providers))],
		AmountTotal:     float64(rand.Intn(1000000)) / 100, // до 10000.00
		PaymentDateTime: int(time.Now().Unix()),            // Изменить на time.Time
		BankCode:        banks[rand.Intn(len(banks))],
		DeliveryCost:    float64(rand.Intn(50000)) / 100, // до 500.00
		GoodsTotal:      float64(rand.Intn(1000000)) / 100,
		CustomFee:       float64(rand.Intn(10000)) / 100, // до 100.00
	}
}

func generateItems() []models.OrderItem {
	// Генерируем от 1 до 5 товаров
	count := rand.Intn(5) + 1
	items := make([]models.OrderItem, count)

	for i := 0; i < count; i++ {
		items[i] = models.OrderItem{
			ChartID:     rand.Intn(1000000),
			TrackNumber: randomString(15),
			UnitPrice:   float64(rand.Intn(1000)),
			RID:         uuid.New().String(),
			ProductName: randomProductName(),
			SalePercent: float64(rand.Intn(100)),
			SizeCode:    randomSize(),
			LineTotal:   float64(rand.Intn(1000)),
			ProductID:   int64(rand.Intn(1000000)),
			BrandName:   randomBrand(),
			StatusCode:  rand.Intn(300) + 200, // статусы 200-499
		}
	}

	return items
}

func generateLocale() string {
	locales := []string{"en", "ru", "zh", "es", "de"}
	return locales[rand.Intn(len(locales))]
}

func randomString(length int) string {
	charset := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}
	return string(result)
}

func randomFullName() string {
	firstNames := []string{"John", "Jane", "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"}
	lastNames := []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"}
	return firstNames[rand.Intn(len(firstNames))] + " " + lastNames[rand.Intn(len(lastNames))]
}

func randomPhone() string {
	// Генерируем российский номер телефона
	return fmt.Sprintf("+7%010d", rand.Int63n(10000000000))
}

func randomZip() string {
	return fmt.Sprintf("%06d", rand.Intn(1000000))
}

func randomEmail() string {
	domains := []string{"gmail.com", "yahoo.com", "hotmail.com", "example.com"}
	return randomString(8) + "@" + domains[rand.Intn(len(domains))]
}

func randomSize() string {
	sizes := []string{"XS", "S", "M", "L", "XL", "XXL", "XXXL", "36", "38", "40", "42", "44"}
	return sizes[rand.Intn(len(sizes))]
}

func randomProductName() string {
	products := []string{
		"T-Shirt", "Jeans", "Sneakers", "Jacket", "Dress", "Shirt",
		"Skirt", "Shorts", "Sweater", "Coat", "Boots", "Sandals",
		"Hat", "Scarf", "Gloves", "Socks", "Underwear", "Swimsuit",
	}
	return products[rand.Intn(len(products))]
}

func randomBrand() string {
	brands := []string{
		"Nike", "Adidas", "Puma", "Reebok", "New Balance", "Converse",
		"Zara", "H&M", "Uniqlo", "Gucci", "Prada", "Louis Vuitton",
		"Chanel", "Dior", "Versace", "Balenciaga", "Off-White", "Supreme",
	}
	return brands[rand.Intn(len(brands))]
}
