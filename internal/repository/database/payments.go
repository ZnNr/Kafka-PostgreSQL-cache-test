package database

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models"
)

const (
	getPaymentQuery = `SELECT 
    "transaction", 
    "request_id", 
    "currency", 
    "provider", 
    "amount", 
    "payment_dt", 
    "bank", 
    "delivery_cost", 
    "goods_total", 
    "custom_fee" 
FROM payments WHERE order_uid = $1`
	addPaymentQuery = `INSERT INTO payments
        ("transaction", "request_id", "currency", "provider", "amount",
        "payment_dt", "bank", "delivery_cost", "goods_total", "custom_fee", "order_uid")
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
)

// AddPayment добавляет платеж в базу данных.
func AddPayment(db *sql.DB, payment models.Payment, orderUID string) error {
	_, err := db.Exec(
		addPaymentQuery,
		payment.TransactionUID,
		payment.RequestID,
		payment.CurrencyCode,
		payment.PaymentProvider,
		payment.AmountTotal,
		payment.PaymentDateTime,
		payment.BankCode,
		payment.DeliveryCost,
		payment.GoodsTotal,
		payment.CustomFee,
		orderUID,
	)
	if err != nil {
		return fmt.Errorf("failed to add payment: %w", err) // Улучшено сообщение об ошибке
	}
	return nil
}

// GetPayment получает платеж из базы данных по orderUID.
func GetPayment(db *sql.DB, orderUID string) (*models.Payment, error) {
	row := db.QueryRow(getPaymentQuery, orderUID) // Используем tx
	var payment models.Payment

	err := row.Scan(
		&payment.TransactionUID,
		&payment.RequestID,
		&payment.CurrencyCode,
		&payment.PaymentProvider,
		&payment.AmountTotal,
		&payment.PaymentDateTime,
		&payment.BankCode,
		&payment.DeliveryCost,
		&payment.GoodsTotal,
		&payment.CustomFee,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("payment not found: %w", err)
		}
		return nil, fmt.Errorf("failed to receive payment: %w", err)
	}
	return &payment, nil
}

// PaymentExists проверяет существование платежа в базе данных по orderUID.
func PaymentExists(tx *sql.DB, orderUID string) (bool, error) {
	var exists bool
	err := tx.QueryRow("SELECT EXISTS(SELECT 1 FROM payments WHERE order_uid = $1)", orderUID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("could not verify the existence of the payment: %w", err) // Улучшено сообщение об ошибке
	}
	return exists, nil
}
