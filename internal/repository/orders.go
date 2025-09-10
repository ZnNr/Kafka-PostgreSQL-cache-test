package repository

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/config"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/repository/database"
	_ "github.com/lib/pq"
)

const (
	addOrderQuery     = `INSERT INTO orders("order_uid", "track_number", "entry", "locale", "internal_signature", "customer_id", "delivery_service", "shardkey", "sm_id", "date_created", "oof_shard") VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
	getOrderQuery     = "SELECT order_uid, track_number, entry, delivery, payment, items, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard FROM orders WHERE order_uid = $1"
	getAllOrdersQuery = "SELECT * FROM orders"
)

type OrdersRepo struct {
	DB *sql.DB
}

func New(cfg *config.Config) (*OrdersRepo, error) {
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.DB.Host, cfg.DB.Port, cfg.DB.User, cfg.DB.Password, cfg.DB.Name)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	return &OrdersRepo{DB: db}, nil
}

func (o *OrdersRepo) OrderExists(orderUID string) (bool, error) {
	var exists bool
	err := o.DB.QueryRow("SELECT EXISTS(SELECT 1 FROM orders WHERE order_uid = $1)", orderUID).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}
func (o *OrdersRepo) AddOrder(order models.Order) error {
	// существует ли заказ?
	exists, err := o.OrderExists(order.OrderUID)
	if err != nil {
		return fmt.Errorf("failed to check if order exists: %w", err)
	}

	if exists {
		return fmt.Errorf("order with order_uid %s already exists", order.OrderUID)
	}

	// Вставляем заказ в базу данных
	_, err = o.DB.Exec(
		addOrderQuery,
		order.OrderUID,
		order.TrackNumber,
		order.EntryPoint,
		order.LocaleCode,
		order.InternalSignature,
		order.CustomerId,
		order.DeliveryService,
		order.ShardKey,
		order.StateMachineID,
		order.DateCreated,
		order.OOFShard,
	)
	if err != nil {
		return fmt.Errorf("failed to insert order: %w", err)
	}

	// Проверка существования платежа и добавление при необходимости.
	if err := o.processPayment(order); err != nil {
		return fmt.Errorf("failed to process payment: %w", err)
	}

	// Добавление предметов заказа
	if err := database.AddItems(
		o.DB,
		order.Items,
		order.OrderUID,
	); err != nil {
		return fmt.Errorf("failed to insert items: %w", err)
	}

	// Добавление доставки
	statusMessage, err := database.AddDelivery(
		o.DB,
		order.Delivery,
		order.OrderUID,
	)
	if err != nil {
		return fmt.Errorf("failed to insert delivery: %w", err)
	}

	fmt.Println(statusMessage)

	return nil
}

// processPayment проверяет существование платежа и добавляет новый, если его нет.
func (o *OrdersRepo) processPayment(order models.Order) error {
	exists, err := database.PaymentExists(
		o.DB,
		order.OrderUID,
	)
	if err != nil {
		return fmt.Errorf("failed to check if payment exists: %w", err)
	}

	if !exists {
		if err := database.AddPayment(o.DB, order.Payment, order.OrderUID); err != nil {
			return fmt.Errorf("failed to insert payment: %w", err)
		}
	}

	return nil
}

func (o *OrdersRepo) GetOrder(orderUID string) (*models.Order, error) {
	var order models.Order

	if err := o.DB.QueryRow(getOrderQuery, orderUID).Scan(
		&order.OrderUID,
		&order.TrackNumber,
		&order.EntryPoint,
		&order.LocaleCode,
		&order.InternalSignature,
		&order.CustomerId,
		&order.DeliveryService,
		&order.ShardKey,
		&order.StateMachineID,
		&order.DateCreated,
		&order.OOFShard,
	); err != nil {

		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get order: %w", err)
	}

	if err := populateOrderDetails(o.DB, &order); err != nil { // Изменено здесь
		return nil, fmt.Errorf("failed to populate order details: %w", err)
	}

	return &order, nil
}

func populateOrderDetails(db *sql.DB, order *models.Order) error {
	delivery, err := database.GetDelivery(db, order.OrderUID)
	if err != nil {
		return err
	}
	order.Delivery = *delivery

	payment, err := database.GetPayment(db, order.OrderUID)
	if err != nil {
		return err
	}
	order.Payment = *payment

	items, err := database.GetItems(db, order.OrderUID)
	if err != nil {
		return err
	}
	order.Items = items

	return nil
}

func (o *OrdersRepo) GetOrders() ([]models.Order, error) {
	rows, err := o.DB.Query(getAllOrdersQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to get orders: %w", err)
	}
	defer rows.Close()

	var orders []models.Order
	for rows.Next() {
		var order models.Order
		if err := rows.Scan(
			&order.OrderUID,
			&order.TrackNumber,
			&order.EntryPoint,
			&order.LocaleCode,
			&order.InternalSignature,
			&order.CustomerId,
			&order.DeliveryService,
			&order.ShardKey,
			&order.StateMachineID,
			&order.DateCreated,
			&order.OOFShard,
		); err != nil {
			return nil, fmt.Errorf("failed to scan order row: %w", err)
		}

		if err := populateOrderDetails(o.DB, &order); err != nil { // Изменено здесь
			return nil, fmt.Errorf("failed to get details for order %s: %w", order.OrderUID, err)
		}

		orders = append(orders, order)
	}

	return orders, nil
}
