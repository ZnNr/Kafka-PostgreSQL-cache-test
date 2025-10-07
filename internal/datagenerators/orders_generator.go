package datagenerators

import (
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models"
	"github.com/brianvoe/gofakeit/v6"
)

func GenerateOrder() models.Order {
	// Опционально: зафиксировать seed для воспроизводимости в тестах
	// gofakeit.Seed(123)

	order := models.Order{
		OrderUID:          gofakeit.UUID(),
		TrackNumber:       gofakeit.LetterN(15),
		EntryPoint:        gofakeit.DomainName(),
		LocaleCode:        gofakeit.RandomString([]string{"en", "ru", "zh", "es", "de"}), // LanguageCode нет → делаем вручную
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

	// Валюта: gofakeit.Currency() возвращает структуру, но у неё нет .Code
	// Используем список кодов вручную
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

	// Генерация товаров
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
