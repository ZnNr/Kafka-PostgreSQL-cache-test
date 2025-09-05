package datagenerators

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestGenerateOrder(t *testing.T) {
	order := GenerateOrder()

	assert.NotEmpty(t, order.OrderUID)
	assert.NotEmpty(t, order.TrackNumber)
	assert.NotEmpty(t, order.EntryPoint)
	assert.NotEmpty(t, order.LocaleCode)
	assert.NotEmpty(t, order.CustomerId)
	assert.NotEmpty(t, order.DeliveryService)
	assert.NotEmpty(t, order.ShardKey)
	assert.NotEmpty(t, order.OOFShard)
	assert.NotZero(t, order.StateMachineID)
	assert.NotZero(t, order.DateCreated.Unix())

	assert.True(t, isValidUUID(order.OrderUID))

	assert.NotEmpty(t, order.Delivery.Name)
	assert.NotEmpty(t, order.Delivery.Phone)
	assert.NotEmpty(t, order.Delivery.Zip)
	assert.NotEmpty(t, order.Delivery.City)
	assert.NotEmpty(t, order.Delivery.Address)
	assert.NotEmpty(t, order.Delivery.Region)
	assert.NotEmpty(t, order.Delivery.Email)

	assert.Equal(t, order.OrderUID, order.Delivery.OrderUID)

	assert.NotEmpty(t, order.Payment.TransactionUID)
	assert.NotEmpty(t, order.Payment.CurrencyCode)
	assert.NotEmpty(t, order.Payment.PaymentProvider)
	assert.NotEmpty(t, order.Payment.BankCode)
	assert.NotZero(t, order.Payment.PaymentDateTime)

	assert.NotEmpty(t, order.Items)
	assert.True(t, len(order.Items) >= 1 && len(order.Items) <= 5)

	for _, item := range order.Items {
		assert.NotZero(t, item.ChartID)
		assert.NotEmpty(t, item.TrackNumber)
		assert.NotZero(t, item.UnitPrice)
		assert.NotEmpty(t, item.RID)
		assert.NotEmpty(t, item.ProductName)
		assert.NotEmpty(t, item.SizeCode)
		assert.NotZero(t, item.LineTotal)
		assert.NotZero(t, item.ProductID)
		assert.NotEmpty(t, item.BrandName)
		assert.True(t, item.StatusCode >= 200 && item.StatusCode <= 499)
	}
}

func TestGenerateMultipleOrders(t *testing.T) {
	orders := make(map[string]bool)

	for i := 0; i < 100; i++ {
		order := GenerateOrder()
		assert.False(t, orders[order.OrderUID], "Order UID should be unique")
		orders[order.OrderUID] = true
	}

	assert.Equal(t, 100, len(orders))
}

func TestRandomString(t *testing.T) {
	for _, length := range []int{5, 10, 15, 20} {
		result := randomString(length)
		assert.Equal(t, length, len(result))
		assert.Regexp(t, "^[a-zA-Z0-9]+$", result)
	}
}

func TestRandomPhone(t *testing.T) {
	for i := 0; i < 10; i++ {
		phone := randomPhone()
		assert.Regexp(t, `^\+7\d{10}$`, phone)
	}
}

func TestRandomZip(t *testing.T) {
	for i := 0; i < 10; i++ {
		zip := randomZip()
		assert.Regexp(t, `^\d{6}$`, zip)
	}
}

func TestRandomEmail(t *testing.T) {
	validDomains := []string{"gmail.com", "yahoo.com", "hotmail.com", "example.com"}

	for i := 0; i < 20; i++ {
		email := randomEmail()
		assert.Contains(t, email, "@")

		parts := strings.Split(email, "@")
		assert.Equal(t, 2, len(parts))
		assert.NotEmpty(t, parts[0])
		assert.Contains(t, validDomains, parts[1])
	}
}

func TestRandomSize(t *testing.T) {
	validSizes := []string{"XS", "S", "M", "L", "XL", "XXL", "XXXL", "36", "38", "40", "42", "44"}

	for i := 0; i < 20; i++ {
		size := randomSize()
		assert.Contains(t, validSizes, size)
	}
}

func TestRandomProductName(t *testing.T) {
	validProducts := []string{
		"T-Shirt", "Jeans", "Sneakers", "Jacket", "Dress", "Shirt",
		"Skirt", "Shorts", "Sweater", "Coat", "Boots", "Sandals",
		"Hat", "Scarf", "Gloves", "Socks", "Underwear", "Swimsuit",
	}

	for i := 0; i < 20; i++ {
		product := randomProductName()
		assert.Contains(t, validProducts, product)
	}
}

func TestRandomBrand(t *testing.T) {
	validBrands := []string{
		"Nike", "Adidas", "Puma", "Reebok", "New Balance", "Converse",
		"Zara", "H&M", "Uniqlo", "Gucci", "Prada", "Louis Vuitton",
		"Chanel", "Dior", "Versace", "Balenciaga", "Off-White", "Supreme",
	}

	for i := 0; i < 20; i++ {
		brand := randomBrand()
		assert.Contains(t, validBrands, brand)
	}
}

func TestRandomFullName(t *testing.T) {
	validFirstNames := []string{"John", "Jane", "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"}
	validLastNames := []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"}

	for i := 0; i < 20; i++ {
		fullName := randomFullName()
		parts := strings.Split(fullName, " ")
		assert.Equal(t, 2, len(parts))
		assert.Contains(t, validFirstNames, parts[0])
		assert.Contains(t, validLastNames, parts[1])
	}
}

func TestGenerateLocale(t *testing.T) {
	validLocales := []string{"en", "ru", "zh", "es", "de"}

	for i := 0; i < 20; i++ {
		locale := generateLocale()
		assert.Contains(t, validLocales, locale)
	}
}

func TestGenerateItems(t *testing.T) {
	for i := 0; i < 20; i++ {
		items := generateItems()
		assert.True(t, len(items) >= 1 && len(items) <= 5)

		for _, item := range items {
			// Проверяем диапазоны значений
			assert.True(t, item.ChartID >= 0 && item.ChartID <= 1000000)
			assert.NotEmpty(t, item.TrackNumber)
			assert.True(t, item.UnitPrice >= 0 && item.UnitPrice <= 10000)
			assert.NotEmpty(t, item.RID)
			assert.NotEmpty(t, item.ProductName)
			assert.NotEmpty(t, item.SizeCode)
			assert.True(t, item.LineTotal >= 0 && item.LineTotal <= 50000)
			assert.True(t, item.ProductID >= 0 && item.ProductID <= 1000000)
			assert.NotEmpty(t, item.BrandName)
			assert.True(t, item.StatusCode >= 200 && item.StatusCode <= 499)
		}
	}
}

func TestGeneratePayment(t *testing.T) {
	validCurrencies := []string{"USD", "RUB", "EUR", "CNY"}
	validProviders := []string{"wbpay", "yookassa", "stripe", "paypal"}
	validBanks := []string{"alpha", "sber", "vtb", "raiffeisen"}

	for i := 0; i < 20; i++ {
		payment := generatePayment()

		assert.NotEmpty(t, payment.TransactionUID)
		assert.NotEmpty(t, payment.CurrencyCode)
		assert.Contains(t, validCurrencies, payment.CurrencyCode)

		assert.NotEmpty(t, payment.PaymentProvider)
		assert.Contains(t, validProviders, payment.PaymentProvider)

		assert.True(t, payment.AmountTotal >= 0 && payment.AmountTotal <= 10000.00)
		assert.NotZero(t, payment.PaymentDateTime)

		assert.NotEmpty(t, payment.BankCode)
		assert.Contains(t, validBanks, payment.BankCode)

		assert.True(t, payment.DeliveryCost >= 0 && payment.DeliveryCost <= 500.00)
		assert.True(t, payment.GoodsTotal >= 0 && payment.GoodsTotal <= 10000.00)
		assert.True(t, payment.CustomFee >= 0 && payment.CustomFee <= 100.00)
	}
}

func TestGenerateDelivery(t *testing.T) {
	for i := 0; i < 10; i++ {
		delivery := generateDelivery()

		assert.NotEmpty(t, delivery.Name)
		assert.NotEmpty(t, delivery.Phone)
		assert.Regexp(t, `^\+7\d{10}$`, delivery.Phone)

		assert.NotEmpty(t, delivery.Zip)
		assert.Regexp(t, `^\d{6}$`, delivery.Zip)

		assert.NotEmpty(t, delivery.City)
		assert.NotEmpty(t, delivery.Address)
		assert.NotEmpty(t, delivery.Region)
		assert.NotEmpty(t, delivery.Email)
		assert.Contains(t, delivery.Email, "@")
	}
}

func isValidUUID(uuid string) bool {
	if len(uuid) != 36 {
		return false
	}

	parts := strings.Split(uuid, "-")
	if len(parts) != 5 {
		return false
	}

	// Проверяем длины частей
	if len(parts[0]) != 8 || len(parts[1]) != 4 || len(parts[2]) != 4 || len(parts[3]) != 4 || len(parts[4]) != 12 {
		return false
	}

	for _, part := range parts {
		for _, char := range part {
			if !(char >= '0' && char <= '9') && !(char >= 'a' && char <= 'f') {
				return false
			}
		}
	}

	return true
}
