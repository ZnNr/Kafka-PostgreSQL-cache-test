// internal/consumer/kafka_test.go
package consumer

import (
	"errors"
	"github.com/IBM/sarama"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap/zaptest"
	"testing"
)

// MockConsumer мок для sarama.Consumer
type MockConsumer struct {
	mock.Mock
}

func (m *MockConsumer) Partitions(topic string) ([]int32, error) {
	args := m.Called(topic)
	return args.Get(0).([]int32), args.Error(1)
}

func (m *MockConsumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	args := m.Called(topic, partition, offset)
	return args.Get(0).(sarama.PartitionConsumer), args.Error(1)
}

func (m *MockConsumer) HighWaterMarks() map[string]map[int32]int64 {
	args := m.Called()
	return args.Get(0).(map[string]map[int32]int64)
}

func (m *MockConsumer) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockPartitionConsumer мок для sarama.PartitionConsumer
type MockPartitionConsumer struct {
	mock.Mock
	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
}

func (m *MockPartitionConsumer) AsyncClose() {
	m.Called()
}

func (m *MockPartitionConsumer) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockPartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return m.messages
}

func (m *MockPartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return m.errors
}

func (m *MockPartitionConsumer) HighWaterMarkOffset() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}

// MockCache мок для cache.Cache
type MockCache struct {
	mock.Mock
}

func (m *MockCache) SaveOrder(order models.Order) error {
	args := m.Called(order)
	return args.Error(0)
}

func (m *MockCache) GetOrder(orderUID string) (models.Order, bool, error) {
	args := m.Called(orderUID)
	return args.Get(0).(models.Order), args.Bool(1), args.Error(2)
}

func (m *MockCache) OrderExists(orderUID string) (bool, error) {
	args := m.Called(orderUID)
	return args.Bool(0), args.Error(1)
}

func (m *MockCache) RemoveOrder(orderUID string) error {
	args := m.Called(orderUID)
	return args.Error(0)
}

func (m *MockCache) Clear() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockCache) GetAllOrders() ([]models.Order, error) {
	args := m.Called()
	return args.Get(0).([]models.Order), args.Error(1)
}

func (m *MockCache) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockOrdersRepo мок для repository.OrdersRepo
type MockOrdersRepo struct {
	mock.Mock
}

func (m *MockOrdersRepo) AddOrder(order models.Order) error {
	args := m.Called(order)
	return args.Error(0)
}

func (m *MockOrdersRepo) GetOrder(orderUID string) (models.Order, error) {
	args := m.Called(orderUID)
	return args.Get(0).(models.Order), args.Error(1)
}

func (m *MockOrdersRepo) GetOrders() ([]models.Order, error) {
	args := m.Called()
	return args.Get(0).([]models.Order), args.Error(1)
}

func (m *MockOrdersRepo) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestConnectConsumer(t *testing.T) {
	tests := []struct {
		name      string
		brokers   []string
		wantError bool
	}{
		{
			name:      "empty brokers",
			brokers:   []string{},
			wantError: true,
		},
		{
			name:      "valid brokers",
			brokers:   []string{"localhost:9092"},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			consumer, err := ConnectConsumer(tt.brokers)

			if tt.wantError {
				assert.Error(t, err)
				assert.Nil(t, consumer)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, consumer)
				if consumer != nil {
					consumer.Close()
				}
			}
		})
	}
}

func TestCheckOrderExistsInCache(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockCache := &MockCache{}

	tests := []struct {
		name           string
		orderUID       string
		mockExists     bool
		mockExistsErr  error
		mockGetFound   bool
		mockGetErr     error
		expectedExists bool
		expectedError  bool
	}{
		{
			name:           "exists returns true",
			orderUID:       "test1",
			mockExists:     true,
			mockExistsErr:  nil,
			expectedExists: true,
			expectedError:  false,
		},
		{
			name:           "exists returns false",
			orderUID:       "test2",
			mockExists:     false,
			mockExistsErr:  nil,
			expectedExists: false,
			expectedError:  false,
		},
		{
			name:           "exists error, get success found",
			orderUID:       "test3",
			mockExistsErr:  errors.New("exists error"),
			mockGetFound:   true,
			mockGetErr:     nil,
			expectedExists: true,
			expectedError:  false,
		},
		{
			name:           "exists error, get success not found",
			orderUID:       "test4",
			mockExistsErr:  errors.New("exists error"),
			mockGetFound:   false,
			mockGetErr:     nil,
			expectedExists: false,
			expectedError:  false,
		},
		{
			name:          "both exists and get error",
			orderUID:      "test5",
			mockExistsErr: errors.New("exists error"),
			mockGetErr:    errors.New("get error"),
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCache.On("OrderExists", tt.orderUID).Return(tt.mockExists, tt.mockExistsErr)

			if tt.mockExistsErr != nil {
				mockCache.On("GetOrder", tt.orderUID).Return(models.Order{}, tt.mockGetFound, tt.mockGetErr)
			}

			exists, err := checkOrderExistsInCache(mockCache, tt.orderUID, logger)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedExists, exists)
			}

			mockCache.AssertCalled(t, "OrderExists", tt.orderUID)
			if tt.mockExistsErr != nil {
				mockCache.AssertCalled(t, "GetOrder", tt.orderUID)
			}
		})
	}
}

func TestSaveOrderToCache(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockCache := &MockCache{}

	order := models.Order{OrderUID: "test123"}

	tests := []struct {
		name          string
		mockSetErr    error
		expectedError bool
	}{
		{
			name:          "success",
			mockSetErr:    nil,
			expectedError: false,
		},
		{
			name:          "error",
			mockSetErr:    errors.New("set error"),
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCache.On("SaveOrder", order).Return(tt.mockSetErr)

			err := saveOrderToCache(mockCache, order, logger)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockCache.AssertCalled(t, "SaveOrder", order)
		})
	}
}
