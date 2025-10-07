// internal/validation/order_validator.go
package validation

import (
	"time"

	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models"
)

// OrderValidator предоставляет методы для валидации заказов
type OrderValidator struct{}

// NewOrderValidator создает новый валидатор заказов
func NewOrderValidator() *OrderValidator {
	return &OrderValidator{}
}

// ValidateOrder проверяет валидность данных заказа
func (v *OrderValidator) ValidateOrder(order models.Order) bool {
	return v.validateRequiredFields(order) &&
		v.validateDelivery(order.Delivery) &&
		v.validatePayment(order.Payment) &&
		v.validateItems(order.Items) &&
		v.validateDate(order.DateCreated)
}

// validateRequiredFields проверяет обязательные поля заказа
func (v *OrderValidator) validateRequiredFields(order models.Order) bool {
	return order.OrderUID != "" &&
		order.TrackNumber != "" &&
		order.EntryPoint != "" &&
		order.LocaleCode != "" &&
		order.CustomerId != "" &&
		order.DeliveryService != "" &&
		order.ShardKey != "" &&
		order.OOFShard != ""
}

// validateDelivery проверяет валидность данных доставки
func (v *OrderValidator) validateDelivery(delivery models.Delivery) bool {
	return delivery.Name != "" &&
		delivery.Phone != "" &&
		delivery.Zip != "" &&
		delivery.City != "" &&
		delivery.Address != "" &&
		delivery.Region != "" &&
		delivery.Email != ""
}

// validatePayment проверяет валидность данных платежа
func (v *OrderValidator) validatePayment(payment models.Payment) bool {
	return payment.TransactionUID != "" &&
		payment.CurrencyCode != "" &&
		payment.PaymentProvider != "" &&
		payment.BankCode != "" &&
		payment.AmountTotal >= 0 &&
		payment.DeliveryCost >= 0 &&
		payment.GoodsTotal >= 0 &&
		payment.CustomFee >= 0
}

// validateItems проверяет валидность списка товаров
func (v *OrderValidator) validateItems(items []models.OrderItem) bool {
	if len(items) == 0 {
		return false
	}

	for _, item := range items {
		if !v.validateItem(item) {
			return false
		}
	}

	return true
}

// validateItem проверяет валидность отдельного товара
func (v *OrderValidator) validateItem(item models.OrderItem) bool {
	return item.ChartID != 0 &&
		item.TrackNumber != "" &&
		item.UnitPrice >= 0 &&
		item.RID != "" &&
		item.ProductName != "" &&
		item.SizeCode != "" &&
		item.LineTotal >= 0 &&
		item.ProductID != 0 &&
		item.BrandName != "" &&
		item.SalePercent >= 0
}

// validateDate проверяет валидность даты создания
func (v *OrderValidator) validateDate(date time.Time) bool {
	return !date.IsZero()
}

// ValidateOrderUID проверяет валидность OrderUID
func (v *OrderValidator) ValidateOrderUID(orderUID string) bool {
	return orderUID != ""
}

// ValidateTrackNumber проверяет валидность TrackNumber
func (v *OrderValidator) ValidateTrackNumber(trackNumber string) bool {
	return trackNumber != ""
}

// ValidateEntryPoint проверяет валидность EntryPoint
func (v *OrderValidator) ValidateEntryPoint(entryPoint string) bool {
	return entryPoint != ""
}

// ValidateLocaleCode проверяет валидность LocaleCode
func (v *OrderValidator) ValidateLocaleCode(localeCode string) bool {
	return localeCode != ""
}

// ValidateCustomerId проверяет валидность CustomerId
func (v *OrderValidator) ValidateCustomerId(customerId string) bool {
	return customerId != ""
}

// ValidateDeliveryService проверяет валидность DeliveryService
func (v *OrderValidator) ValidateDeliveryService(deliveryService string) bool {
	return deliveryService != ""
}

// ValidateShardKey проверяет валидность ShardKey
func (v *OrderValidator) ValidateShardKey(shardKey string) bool {
	return shardKey != ""
}

// ValidateStateMachineID проверяет валидность StateMachineID
func (v *OrderValidator) ValidateStateMachineID(smID int) bool {
	return smID >= 0
}

// ValidateOOFShard проверяет валидность OOFShard
func (v *OrderValidator) ValidateOOFShard(oofShard string) bool {
	return oofShard != ""
}

// ValidatePaymentDateTime проверяет валидность PaymentDateTime
func (v *OrderValidator) ValidatePaymentDateTime(paymentDT int) bool {
	return paymentDT > 0
}

// ValidateAmountTotal проверяет валидность AmountTotal
func (v *OrderValidator) ValidateAmountTotal(amount float64) bool {
	return amount >= 0
}

// ValidateDeliveryCost проверяет валидность DeliveryCost
func (v *OrderValidator) ValidateDeliveryCost(cost float64) bool {
	return cost >= 0
}

// ValidateGoodsTotal проверяет валидность GoodsTotal
func (v *OrderValidator) ValidateGoodsTotal(total float64) bool {
	return total >= 0
}

// ValidateCustomFee проверяет валидность CustomFee
func (v *OrderValidator) ValidateCustomFee(fee float64) bool {
	return fee >= 0
}

// ValidateChartID проверяет валидность ChartID
func (v *OrderValidator) ValidateChartID(chartID int) bool {
	return chartID > 0
}

// ValidateUnitPrice проверяет валидность UnitPrice
func (v *OrderValidator) ValidateUnitPrice(price float64) bool {
	return price >= 0
}

// ValidateRID проверяет валидность RID
func (v *OrderValidator) ValidateRID(rid string) bool {
	return rid != ""
}

// ValidateProductName проверяет валидность ProductName
func (v *OrderValidator) ValidateProductName(name string) bool {
	return name != ""
}

// ValidateSizeCode проверяет валидность SizeCode
func (v *OrderValidator) ValidateSizeCode(size string) bool {
	return size != ""
}

// ValidateLineTotal проверяет валидность LineTotal
func (v *OrderValidator) ValidateLineTotal(total float64) bool {
	return total >= 0
}

// ValidateProductID проверяет валидность ProductID
func (v *OrderValidator) ValidateProductID(productID int64) bool {
	return productID > 0
}

// ValidateBrandName проверяет валидность BrandName
func (v *OrderValidator) ValidateBrandName(brand string) bool {
	return brand != ""
}

// ValidateStatusCode проверяет валидность StatusCode
func (v *OrderValidator) ValidateStatusCode(status int) bool {
	return status >= 0
}

// ValidateSalePercent проверяет валидность SalePercent
func (v *OrderValidator) ValidateSalePercent(sale float64) bool {
	return sale >= 0
}
