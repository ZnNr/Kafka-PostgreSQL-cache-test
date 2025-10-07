package router

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/handlers"
	"go.uber.org/zap"

	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/cache"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models"
	"github.com/gorilla/mux"
)

type Controller struct {
	Cache  cache.Cache
	logger *zap.Logger
}

// Функция для инициализации контроллера с кэшем
func NewController(cache cache.Cache, logger *zap.Logger) *Controller {
	return &Controller{
		Cache:  cache,
		logger: logger,
	}
}

// Настройка маршрутизатора
func (c *Controller) SetupRouter() *mux.Router {
	r := mux.NewRouter()

	// Настройка CORS
	corsOptions := handlers.AllowedOrigins([]string{"*"})
	corsMethods := handlers.AllowedMethods([]string{"GET", "POST", "PUT", "DELETE", "OPTIONS"})
	corsHeaders := handlers.AllowedHeaders([]string{"X-Requested-With", "Content-Type", "Authorization"})

	// middleware для CORS
	r.Use(handlers.CORS(corsOptions, corsMethods, corsHeaders))
	r.Use(c.preflightHandler)
	r.Use(c.loggingMiddleware)

	// Маршруты
	r.HandleFunc("/order/{order_uid}", c.HandleGetOrder).Methods(http.MethodGet, http.MethodOptions)
	r.HandleFunc("/order/{order_uid}", c.HandleDeleteOrder).Methods(http.MethodDelete, http.MethodOptions)
	r.HandleFunc("/delorders", c.HandleClearOrders).Methods(http.MethodDelete, http.MethodOptions)
	r.HandleFunc("/orders", c.HandleGetAllOrders).Methods(http.MethodGet, http.MethodOptions)
	// Health check
	r.HandleFunc("/health", c.HandleHealthCheck).Methods(http.MethodGet)
	return r
}

// Middleware для обработки предварительных запросов
func (c *Controller) preflightHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Middleware для логирования запросов
func (c *Controller) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.logger.Info("HTTP request",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.String("remote_addr", r.RemoteAddr),
			zap.String("user_agent", r.UserAgent()))
		next.ServeHTTP(w, r)
	})
}

// HandleHealthCheck обработчик для проверки здоровья сервиса
func (c *Controller) HandleHealthCheck(w http.ResponseWriter, r *http.Request) {
	c.writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "order-cache"})
}

// HandleGetOrder Обработчик для получения заказа по order_uid
func (c *Controller) HandleGetOrder(w http.ResponseWriter, r *http.Request) {
	orderUID := mux.Vars(r)["order_uid"]

	// Валидация order_uid
	if orderUID == "" {
		c.writeError(w, http.StatusBadRequest, "OrderUID is required")
		return
	}

	if len(orderUID) > 50 {
		c.writeError(w, http.StatusBadRequest, "OrderUID is too long")
		return
	}

	order, exists, err := c.Cache.GetOrder(orderUID)
	if err != nil {
		c.logger.Error("Failed to get order from cache",
			zap.String("order_uid", orderUID),
			zap.Error(err))
		c.writeError(w, http.StatusInternalServerError, "Internal server error")
		return
	}

	if !exists {
		c.writeError(w, http.StatusNotFound, fmt.Sprintf("Order with UID '%s' not found", orderUID))
		return
	}

	c.logger.Info("Order retrieved successfully",
		zap.String("order_uid", orderUID))
	c.writeJSON(w, http.StatusOK, order)
}

// HandleDeleteOrder Обработчик для удаления заказа по order_uid
func (c *Controller) HandleDeleteOrder(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	orderUID := vars["order_uid"]

	// Валидация order_uid
	if orderUID == "" {
		c.writeError(w, http.StatusBadRequest, "OrderUID is required")
		return
	}

	exists, err := c.Cache.OrderExists(orderUID)
	if err != nil {
		c.logger.Error("Failed to check order existence",
			zap.String("order_uid", orderUID),
			zap.Error(err))
		c.writeError(w, http.StatusInternalServerError, "Internal server error")
		return
	}

	if !exists {
		c.writeError(w, http.StatusNotFound, fmt.Sprintf("Order with UID '%s' not found", orderUID))
		return
	}

	if err := c.Cache.RemoveOrder(orderUID); err != nil {
		c.logger.Error("Failed to remove order from cache",
			zap.String("order_uid", orderUID),
			zap.Error(err))
		c.writeError(w, http.StatusInternalServerError, "Failed to delete order")
		return
	}

	c.logger.Info("Order deleted successfully",
		zap.String("order_uid", orderUID))
	c.writeJSON(w, http.StatusOK, map[string]string{
		"message": fmt.Sprintf("Order with UID '%s' successfully deleted", orderUID),
	})
}

// HandleClearOrders Обработчик для очистки всех заказов
func (c *Controller) HandleClearOrders(w http.ResponseWriter, r *http.Request) {
	if err := c.Cache.Clear(); err != nil {
		c.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Error clearing orders: %v", err))
		return
	}
	c.writeJSON(w, http.StatusOK, map[string]string{"message": "All orders successfully cleared"})
}

// HandleGetAllOrders обработчик для получения всех заказов
func (c *Controller) HandleGetAllOrders(w http.ResponseWriter, r *http.Request) {
	orders, err := c.Cache.GetAllOrders()
	if err != nil {
		c.logger.Error("Failed to get all orders from cache", zap.Error(err))
		c.writeError(w, http.StatusInternalServerError, "Failed to retrieve orders")
		return
	}

	if len(orders) == 0 {
		c.logger.Info("No orders found in cache")
		c.writeJSON(w, http.StatusOK, []models.Order{})
		return
	}

	c.logger.Info("Retrieved all orders from cache",
		zap.Int("order_count", len(orders)))
	c.writeJSON(w, http.StatusOK, orders)
}

// Приватные методы для записи JSON и ошибок
func (c *Controller) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		c.logger.Error("Failed to encode JSON response",
			zap.Error(err),
			zap.Any("data", data))
		// Если уже отправили статус, не можем изменить его, только логируем
	}
}

func (c *Controller) writeError(w http.ResponseWriter, status int, message string) {
	c.logger.Warn("HTTP error response",
		zap.Int("status", status),
		zap.String("message", message))
	c.writeJSON(w, status, map[string]string{"error": message})
}
