package datagenerators

import (
	"math/rand"
	"time"

	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models"
	"github.com/brianvoe/gofakeit/v6"
)

// GenerateOrder — генерирует валидный заказ (как у тебя)
func GenerateOrder() models.Order {
	gofakeit.Seed(time.Now().UnixNano()) // случайность при каждом запуске

	order := models.Order{
		OrderUID:          gofakeit.UUID(),
		TrackNumber:       gofakeit.LetterN(15),
		EntryPoint:        gofakeit.DomainName(),
		LocaleCode:        gofakeit.RandomString([]string{"en", "ru", "zh", "es", "de"}),
		InternalSignature: gofakeit.Password(true, true, true, true, true, 16),
		CustomerId:        gofakeit.Username(),
		DeliveryService:   gofakeit.Company(),
		ShardKey:          gofakeit.DigitN(1),
		StateMachineID:    gofakeit.Number(1, 100),
		DateCreated:       gofakeit.Date(),
		OOFShard:          gofakeit.DigitN(1),
	}

	order.Delivery = models.Delivery{
		Name:    gofakeit.Name(),
		Phone:   gofakeit.Phone(),
		Zip:     gofakeit.Zip(),
		City:    gofakeit.City(),
		Address: gofakeit.Address().Address,
		Region:  gofakeit.State(),
		Email:   gofakeit.Email(),
	}
	order.Delivery.OrderUID = order.OrderUID

	currencies := []string{"USD", "RUB", "EUR", "CNY", "GBP", "JPY"}
	banks := []string{"Alpha Bank", "Sberbank", "VTB", "Raiffeisen", "Tinkoff"}

	order.Payment = models.Payment{
		TransactionUID:  gofakeit.UUID(),
		RequestID:       gofakeit.UUID(),
		CurrencyCode:    gofakeit.RandomString(currencies),
		PaymentProvider: gofakeit.RandomString([]string{"wbpay", "yookassa", "stripe", "paypal"}),
		AmountTotal:     float64(gofakeit.Number(100, 1000000)) / 100,
		PaymentDateTime: int(gofakeit.Date().Unix()),
		BankCode:        gofakeit.RandomString(banks),
		DeliveryCost:    float64(gofakeit.Number(1000, 50000)) / 100,
		GoodsTotal:      float64(gofakeit.Number(10000, 10000000)) / 100,
		CustomFee:       float64(gofakeit.Number(100, 10000)) / 100,
	}

	itemCount := gofakeit.Number(1, 5)
	order.Items = make([]models.OrderItem, itemCount)
	sizes := []string{"XS", "S", "M", "L", "XL", "XXL"}
	brands := []string{"Nike", "Adidas", "Zara", "H&M", "Uniqlo", "Gucci"}

	for i := 0; i < itemCount; i++ {
		order.Items[i] = models.OrderItem{
			ChartID:     gofakeit.Number(1000, 999999),
			TrackNumber: gofakeit.LetterN(15),
			UnitPrice:   float64(gofakeit.Number(1000, 100000)) / 100,
			RID:         gofakeit.UUID(),
			ProductName: gofakeit.ProductName(),
			SalePercent: float64(gofakeit.Number(0, 99)),
			SizeCode:    gofakeit.RandomString(sizes),
			LineTotal:   float64(gofakeit.Number(1000, 100000)) / 100,
			ProductID:   int64(gofakeit.Number(1, 999999)),
			BrandName:   gofakeit.RandomString(brands),
			StatusCode:  gofakeit.Number(200, 499),
		}
	}

	return order
}

// --- ГЕНЕРАТОРЫ НЕВАЛИДНЫХ ДАННЫХ ---

// GenerateInvalidOrder_EmptyUID — заказ без OrderUID
func GenerateInvalidOrder_EmptyUID() models.Order {
	order := GenerateOrder()
	order.OrderUID = ""
	return order
}

// GenerateInvalidOrder_NegativeAmount — отрицательная сумма платежа
func GenerateInvalidOrder_NegativeAmount() models.Order {
	order := GenerateOrder()
	order.Payment.AmountTotal = -100.0
	return order
}

// GenerateInvalidOrder_MissingDelivery — нет данных о доставке
func GenerateInvalidOrder_MissingDelivery() models.Order {
	order := GenerateOrder()
	order.Delivery = models.Delivery{} // пустая структура
	return order
}

// GenerateInvalidOrder_InvalidEmail — невалидный email
func GenerateInvalidOrder_InvalidEmail() models.Order {
	order := GenerateOrder()
	order.Delivery.Email = "not-an-email" // намеренно битый
	return order
}

// GenerateInvalidOrder_BigSalePercent — скидка > 100%
func GenerateInvalidOrder_BigSalePercent() models.Order {
	order := GenerateOrder()
	for i := range order.Items {
		order.Items[i].SalePercent = 150.0 // >100% — недопустимо
	}
	return order
}

// GenerateMalformedJSON_ReturnsBytes — возвращает битые JSON как []byte
func GenerateMalformedJSON_ReturnsBytes() []byte {
	// Примеры битых JSON
	malformed := []string{
		`{"order_uid": "abc123",}`,                       // лишняя запятая
		`{"order_uid": "abc123", "price": "not-number"}`, // строка вместо числа
		`{"order_uid": "", "amount": 100}`,               // пустой UID
		`{ "order_uid": null }`,                          // null
		`{"track_number": 12345}`,                        // число вместо строки
		`{"items": [{}]}`,                                // пустой item
		`{ "extra": #invalid }`,                          // синтаксис не JSON
		"",                                               // пусто
		randString(10000),                                // мусор
	}

	return []byte(malformed[rand.Intn(len(malformed))])
}

// GenerateInvalidOrdersBatch — генерирует несколько невалидных заказов
func GenerateInvalidOrdersBatch(count int) []models.Order {
	var orders []models.Order
	generators := []func() models.Order{
		GenerateInvalidOrder_EmptyUID,
		GenerateInvalidOrder_NegativeAmount,
		GenerateInvalidOrder_MissingDelivery,
		GenerateInvalidOrder_InvalidEmail,
		GenerateInvalidOrder_BigSalePercent,
	}

	for i := 0; i < count; i++ {
		gen := generators[i%len(generators)]
		orders = append(orders, gen())
	}

	return orders
}

// Вспомогательная функция: случайная строка (для мусора)
func randString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
