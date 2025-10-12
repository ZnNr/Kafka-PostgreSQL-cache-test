// main_test.go
package main

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/config"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/repository"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockProducer имитирует sarama.SyncProducer
type MockProducer struct {
	mock.Mock
}

func (m *MockProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	args := m.Called(msg)
	return int32(args.Int(0)), int64(args.Int(1)), args.Error(2)
}

func (m *MockProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	args := m.Called(msgs)
	return args.Error(0)
}

func (m *MockProducer) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockProducer) TxnStatus() sarama.ProducerTxnStatusFlag {
	args := m.Called()
	return sarama.ProducerTxnStatusFlag(args.Int(0))
}

func (m *MockProducer) IsTransactional() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockProducer) BeginTxn() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockProducer) CommitTxn() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockProducer) AbortTxn() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockProducer) AddMessageToTxn(msg *sarama.ConsumerMessage, groupId string, metadata *string) error {
	args := m.Called(msg, groupId, metadata)
	return args.Error(0)
}

func (m *MockProducer) AddOffsetsToTxn(offsets map[string][]*sarama.PartitionOffsetMetadata, groupId string) error {
	args := m.Called(offsets, groupId)
	return args.Error(0)
}

// MockOrderProducerWrapper обертка для тестирования processOrder
type MockOrderProducerWrapper struct {
	mock.Mock
}

func (m *MockOrderProducerWrapper) Send(data []byte) error {
	args := m.Called(data)
	return args.Error(0)
}

// MockOrdersRepo имитирует repository.OrdersRepo
type MockOrdersRepo struct {
	mock.Mock
}

func (m *MockOrdersRepo) GetOrders() ([]models.Order, error) {
	args := m.Called()
	return args.Get(0).([]models.Order), args.Error(1)
}

func (m *MockOrdersRepo) DB() *repository.OrdersRepo {
	return &repository.OrdersRepo{}
}

func (m *MockOrdersRepo) Close() error {
	args := m.Called()
	return args.Error(0)
}

// createValidTestOrder создает валидный тестовый заказ с правильными названиями полей
func createValidTestOrder() models.Order {
	return models.Order{
		OrderUID:          "test-order-123",
		TrackNumber:       "TRACK123",
		EntryPoint:        "WBIL",
		LocaleCode:        "en",
		InternalSignature: "",
		CustomerId:        "test",
		DeliveryService:   "meest",
		ShardKey:          "9",
		StateMachineID:    99,
		DateCreated:       time.Now(),
		OOFShard:          "1",
		Delivery: models.Delivery{
			OrderUID: "test-order-123",
			Name:     "Test Testov",
			Phone:    "+9720000000",
			Zip:      "2639809",
			City:     "Kiryat Mozkin",
			Address:  "Ploshad Mira 15",
			Region:   "Kraiot",
			Email:    "test@gmail.com",
		},
		Payment: models.Payment{
			TransactionUID:  "test-order-123",
			RequestID:       "",
			CurrencyCode:    "USD",
			PaymentProvider: "wbpay",
			AmountTotal:     1817,
			PaymentDateTime: 1637907727,
			BankCode:        "alpha",
			DeliveryCost:    1500,
			GoodsTotal:      317,
			CustomFee:       0,
		},
		Items: []models.OrderItem{
			{
				ChartID:     9934930,
				TrackNumber: "TRACK123",
				UnitPrice:   453,
				RID:         "ab4219087a764ae0btest",
				ProductName: "Mascaras",
				SalePercent: 30,
				SizeCode:    "0",
				LineTotal:   317,
				ProductID:   2389212,
				BrandName:   "Vivienne Sabo",
				StatusCode:  202,
			},
		},
	}
}

// createMockOrderProducer создает OrderProducer с мок-продюсером для тестов
func createMockOrderProducer(success bool) *service.OrderProducer {
	mockSaramaProducer := new(MockProducer)
	if success {
		mockSaramaProducer.On("SendMessage", mock.AnythingOfType("*sarama.ProducerMessage")).
			Return(0, 0, nil)
	} else {
		mockSaramaProducer.On("SendMessage", mock.AnythingOfType("*sarama.ProducerMessage")).
			Return(0, 0, errors.New("kafka error"))
	}
	return service.NewOrderProducer(mockSaramaProducer, "orders")
}

// TestProcessOrder
func TestProcessOrder_ValidNewOrder(t *testing.T) {
	orderProducer := createMockOrderProducer(true)

	err := processOrder("s", []models.Order{}, nil, orderProducer)

	assert.NoError(t, err)
}

func TestProcessOrder_SelectExistingOrder(t *testing.T) {
	orders := []models.Order{createValidTestOrder()}
	orderProducer := createMockOrderProducer(true)

	// Мокаем ввод
	defer mockStdin("0\n")()

	err := processOrder("c", orders, nil, orderProducer)

	assert.NoError(t, err)
}

func TestProcessOrder_InvalidCommands(t *testing.T) {
	tests := []struct {
		name    string
		command string
		wantErr bool
		errMsg  string
	}{
		{"EmptyUID", "i", false, ""},
		{"NegativeAmount", "n", false, ""},
		{"InvalidEmail", "e", false, ""},
		{"BigSalePercent", "b", false, ""},
		{"UnknownCommand", "x", true, "unknown command"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orderProducer := createMockOrderProducer(true)

			err := processOrder(tt.command, []models.Order{}, nil, orderProducer)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestProcessOrder_NoOrdersAvailable(t *testing.T) {
	orderProducer := createMockOrderProducer(true)

	err := processOrder("c", []models.Order{}, nil, orderProducer)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no orders available to select")
}

func TestProcessOrder_InvalidIndex(t *testing.T) {
	orders := []models.Order{createValidTestOrder()}
	orderProducer := createMockOrderProducer(true)

	defer mockStdin("99\n")()

	err := processOrder("c", orders, nil, orderProducer)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "index out of range")
}

func TestProcessOrder_InvalidNumberInput(t *testing.T) {
	orders := []models.Order{createValidTestOrder()}
	orderProducer := createMockOrderProducer(true)

	defer mockStdin("invalid\n")()

	err := processOrder("c", orders, nil, orderProducer)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid number")
}

func TestProcessOrder_ValidationFailure(t *testing.T) {
	invalidOrder := models.Order{
		OrderUID:    "invalid",
		TrackNumber: "TRACK123",
		// Нет Items - должно провалить валидацию
	}
	orders := []models.Order{invalidOrder}
	orderProducer := createMockOrderProducer(true)

	defer mockStdin("0\n")()

	err := processOrder("c", orders, nil, orderProducer)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "order must contain at least one item")
}

func TestProcessOrder_SendError(t *testing.T) {
	orderProducer := createMockOrderProducer(false) // Создаем продюсер, который возвращает ошибку

	err := processOrder("s", []models.Order{}, nil, orderProducer)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to send to Kafka")
}

// TestValidateOrder
func TestValidateOrder(t *testing.T) {
	tests := []struct {
		name    string
		order   models.Order
		wantErr bool
	}{
		{
			name:    "ValidOrder",
			order:   createValidTestOrder(),
			wantErr: false,
		},
		{
			name: "EmptyUID",
			order: func() models.Order {
				o := createValidTestOrder()
				o.OrderUID = ""
				return o
			}(),
			wantErr: true,
		},
		{
			name: "EmptyTrackNumber",
			order: func() models.Order {
				o := createValidTestOrder()
				o.TrackNumber = ""
				return o
			}(),
			wantErr: true,
		},
		{
			name: "NoItems",
			order: func() models.Order {
				o := createValidTestOrder()
				o.Items = nil
				return o
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateOrder(tt.order)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestServiceOrderProducer - тесты для service.OrderProducer
func TestServiceOrderProducer_Send(t *testing.T) {
	mockSaramaProducer := new(MockProducer)
	orderProducer := service.NewOrderProducer(mockSaramaProducer, "orders")

	tests := []struct {
		name        string
		message     []byte
		setupMock   func()
		wantErr     bool
		errContains string
	}{
		{
			name:    "Success",
			message: []byte(`{"order_uid":"test"}`),
			setupMock: func() {
				mockSaramaProducer.On("SendMessage", mock.AnythingOfType("*sarama.ProducerMessage")).
					Return(0, 0, nil)
			},
			wantErr: false,
		},
		{
			name:    "SendError",
			message: []byte(`{"order_uid":"test"}`),
			setupMock: func() {
				mockSaramaProducer.On("SendMessage", mock.AnythingOfType("*sarama.ProducerMessage")).
					Return(0, 0, errors.New("kafka error"))
			},
			wantErr:     true,
			errContains: "failed to send message",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Сбрасываем мок для каждого подтеста
			mockSaramaProducer.ExpectedCalls = nil
			mockSaramaProducer.Calls = nil

			tt.setupMock()
			err := orderProducer.Send(tt.message)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				assert.NoError(t, err)
			}
			mockSaramaProducer.AssertExpectations(t)
		})
	}
}

func TestServiceOrderProducer_ErrorScenarios(t *testing.T) {
	tests := []struct {
		name        string
		producer    *service.OrderProducer
		message     []byte
		wantErr     bool
		errContains string
	}{
		{
			name:        "NilProducer",
			producer:    service.NewOrderProducer(nil, "orders"),
			message:     []byte(`{"order_uid":"test"}`),
			wantErr:     true,
			errContains: "producer is not initialized",
		},
		{
			name:        "EmptyMessage",
			producer:    service.NewOrderProducer(new(MockProducer), "orders"),
			message:     []byte{},
			wantErr:     true,
			errContains: "message is empty",
		},
		{
			name:        "EmptyTopic",
			producer:    service.NewOrderProducer(new(MockProducer), ""),
			message:     []byte(`{"order_uid":"test"}`),
			wantErr:     true,
			errContains: "topic is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.producer.Send(tt.message)

			assert.Error(t, err)
			if tt.errContains != "" {
				assert.Contains(t, err.Error(), tt.errContains)
			}
		})
	}
}

// TestConnectProducer
func TestConnectProducer(t *testing.T) {
	tests := []struct {
		name        string
		brokers     []string
		wantErr     bool
		errContains string
	}{
		{
			name:        "EmptyBrokers",
			brokers:     []string{},
			wantErr:     true,
			errContains: "failed to create producer",
		},
		{
			name:        "InvalidBroker",
			brokers:     []string{"invalid-broker:9092"},
			wantErr:     true,
			errContains: "failed to create producer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			producer, err := ConnectProducer(tt.brokers)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, producer)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, producer)
				if producer != nil {
					producer.Close()
				}
			}
		})
	}
}

// TestLoadOrdersFromDB
func TestLoadOrdersFromDB(t *testing.T) {
	// Этот тест требует реальной БД, поэтому пропускаем
	t.Skip("Skipping test - requires database connection")

	// Создаем реальный репозиторий с тестовой конфигурацией
	cfg, err := config.Load("../../config/config.yaml")
	if err != nil {
		t.Skip("Skipping test - cannot load config")
	}

	repo, err := repository.New(cfg)
	if err != nil {
		t.Skip("Skipping test - cannot connect to database")
	}
	defer repo.DB.Close()

	orders, err := loadOrdersFromDB(repo)

	// Не проверяем конкретные значения, так как БД может быть пустой
	assert.NoError(t, err)
	assert.NotNil(t, orders)
}

// TestGetUID
func TestGetUID(t *testing.T) {
	tests := []struct {
		name     string
		order    models.Order
		expected string
	}{
		{
			name:     "WithUID",
			order:    models.Order{OrderUID: "test-123"},
			expected: "test-123",
		},
		{
			name:     "EmptyUID",
			order:    models.Order{OrderUID: ""},
			expected: "(empty)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getUID(tt.order)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestMainIntegration - интеграционный тест основных сценариев
func TestMainIntegrationScenarios(t *testing.T) {
	tests := []struct {
		name        string
		command     string
		orders      []models.Order
		stdinInput  string
		expectError bool
	}{
		{
			name:        "GenerateValidOrder",
			command:     "s",
			orders:      []models.Order{},
			expectError: false,
		},
		{
			name:        "SelectValidOrder",
			command:     "c",
			orders:      []models.Order{createValidTestOrder()},
			stdinInput:  "0\n",
			expectError: false,
		},
		{
			name:        "SendMalformedOrder",
			command:     "i",
			orders:      []models.Order{},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orderProducer := createMockOrderProducer(true)

			if tt.stdinInput != "" {
				defer mockStdin(tt.stdinInput)()
			}

			err := processOrder(tt.command, tt.orders, nil, orderProducer)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestCommandValidationInProcessOrder - тест валидации команд
func TestCommandValidationInProcessOrder(t *testing.T) {
	tests := []struct {
		name    string
		command string
		isValid bool
	}{
		{"Valid_s", "s", true},
		{"Valid_c", "c", true},
		{"Valid_i", "i", true},
		{"Valid_n", "n", true},
		{"Valid_e", "e", true},
		{"Valid_b", "b", true},
		{"Invalid_x", "x", false},
		{"Invalid_empty", "", false},
		{"Invalid_number", "1", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orderProducer := createMockOrderProducer(true)

			err := processOrder(tt.command, []models.Order{}, nil, orderProducer)

			if tt.isValid && tt.command != "c" {
				assert.NoError(t, err)
			} else if !tt.isValid {
				assert.Error(t, err)
			}
		})
	}
}

// TestOrderJSONMarshaling - тест правильности JSON маршалинга
func TestOrderJSONMarshaling(t *testing.T) {
	order := createValidTestOrder()

	jsonData, err := json.Marshal(order)
	assert.NoError(t, err)

	// Проверяем, что JSON содержит правильные поля
	var jsonMap map[string]interface{}
	err = json.Unmarshal(jsonData, &jsonMap)
	assert.NoError(t, err)

	// Проверяем основные поля
	assert.Equal(t, order.OrderUID, jsonMap["order_uid"])
	assert.Equal(t, order.TrackNumber, jsonMap["track_number"])
	assert.Equal(t, order.EntryPoint, jsonMap["entry"])
	assert.Equal(t, order.LocaleCode, jsonMap["locale"])

	// Проверяем вложенные структуры
	delivery, ok := jsonMap["delivery"].(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, order.Delivery.Name, delivery["name"])
	assert.Equal(t, order.Delivery.Email, delivery["email"])

	payment, ok := jsonMap["payment"].(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, order.Payment.TransactionUID, payment["transaction"])
	assert.Equal(t, order.Payment.CurrencyCode, payment["currency"])

	items, ok := jsonMap["items"].([]interface{})
	assert.True(t, ok)
	assert.Greater(t, len(items), 0)
}

// TestOrderFieldTypes - тест типов полей в заказе
func TestOrderFieldTypes(t *testing.T) {
	order := createValidTestOrder()

	// Проверяем типы числовых полей
	assert.IsType(t, float64(0), order.Payment.AmountTotal)
	assert.IsType(t, float64(0), order.Payment.DeliveryCost)
	assert.IsType(t, float64(0), order.Payment.GoodsTotal)
	assert.IsType(t, float64(0), order.Payment.CustomFee)

	// Проверяем типы полей Items
	if len(order.Items) > 0 {
		item := order.Items[0]
		assert.IsType(t, int(0), item.ChartID)
		assert.IsType(t, float64(0), item.UnitPrice)
		assert.IsType(t, float64(0), item.SalePercent)
		assert.IsType(t, float64(0), item.LineTotal)
		assert.IsType(t, int64(0), item.ProductID)
		assert.IsType(t, int(0), item.StatusCode)
	}
}

// TestOrderProducerIntegration - интеграционный тест с реальным OrderProducer
func TestOrderProducerIntegration(t *testing.T) {
	mockSaramaProducer := new(MockProducer)
	orderProducer := service.NewOrderProducer(mockSaramaProducer, "orders")

	order := createValidTestOrder()
	orderJSON, err := json.Marshal(order)
	assert.NoError(t, err)

	// Настраиваем мок для успешной отправки
	mockSaramaProducer.On("SendMessage", mock.AnythingOfType("*sarama.ProducerMessage")).
		Return(0, 0, nil)

	// Отправляем заказ
	err = orderProducer.Send(orderJSON)

	assert.NoError(t, err)
	mockSaramaProducer.AssertExpectations(t)
}

// Вспомогательная функция для мока stdin
func mockStdin(input string) func() {
	oldStdin := os.Stdin
	r, w, _ := os.Pipe()
	go func() {
		w.WriteString(input)
		w.Close()
	}()
	os.Stdin = r

	return func() { os.Stdin = oldStdin }
}
