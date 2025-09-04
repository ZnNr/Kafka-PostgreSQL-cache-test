package models

import "time"

type Order struct {
	OrderUID          string    `json:"order_uid"`          // Уникальный идентификатор заказа
	TrackNumber       string    `json:"track_number"`       // Номер отслеживания доставки
	EntryPoint        string    `json:"entry"`              // Точка входа/источник заказа
	LocaleCode        string    `json:"locale"`             // Код локализации
	InternalSignature string    `json:"internal_signature"` // Внутренняя подпись/хэш
	CustomerId        string    `json:"customer_id"`        // Идентификатор клиента
	DeliveryService   string    `json:"delivery_service"`   // Служба доставки
	ShardKey          string    `json:"shardkey"`           // Ключ шардинга БД
	StateMachineID    int       `json:"sm_id"`              // ID конечного автомата Маршрутизации заказа по бизнес-процессам Трекинга статуса выполнения Логирования этапов обработки Автоматизации действий на каждом этапе
	DateCreated       time.Time `json:"date_created"`       // Время создания заказа
	OOFShard          string    `json:"oof_shard"`          // Привязка к шарду Out Of Flow указывает, в какой шард (подсистему/регион/кластер) должен быть направлен заказ, OofShard — это шард переполнения/исключений

	Delivery Delivery    `json:"delivery"` // Информация о получателе
	Payment  Payment     `json:"payment"`  // Платежная информация
	Items    []OrderItem `json:"items"`    // Список товаров в заказе
}

type Delivery struct {
	OrderUID string `json:"order_uid"`
	Name     string `json:"name"`    // имя получателя
	Phone    string `json:"phone"`   // Контактный телефон
	Zip      string `json:"zip"`     // Почтовый индекс
	City     string `json:"city"`    // Название города
	Address  string `json:"address"` // Адрес доставки
	Region   string `json:"region"`  // Название региона
	Email    string `json:"email"`   // Контактный email
}

type Payment struct {
	TransactionUID  string  `json:"transaction"`
	RequestID       string  `json:"request_id"`
	CurrencyCode    string  `json:"currency"`
	PaymentProvider string  `json:"provider"`
	AmountTotal     float64 `json:"amount"`
	PaymentDateTime int     `json:"payment_dt"` //Дата/время оплаты Unix timestamp
	BankCode        string  `json:"bank"`
	DeliveryCost    float64 `json:"delivery_cost"`
	GoodsTotal      float64 `json:"goods_total"`
	CustomFee       float64 `json:"custom_fee"`
}

type OrderItem struct {
	ChartID     int     `json:"chrt_id"`      // ID чарта/каталога товара
	TrackNumber string  `json:"track_number"` // Трек-номер доставки
	UnitPrice   float64 `json:"price"`        // Цена за единицу
	RID         string  `json:"rid"`          // Уникальный ID товара в системе
	ProductName string  `json:"name"`         // Название товара
	SalePercent float64 `json:"sale"`         // Процент скидки
	SizeCode    string  `json:"size"`         // Код размера
	LineTotal   float64 `json:"total_price"`  // Итоговая сумма позиции
	ProductID   int64   `json:"nm_id"`        // ID товара в номенклатуре
	BrandName   string  `json:"brand"`        // Название бренда
	StatusCode  int     `json:"status"`       // Код статуса товара
}
