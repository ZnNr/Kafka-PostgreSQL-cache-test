// internal/repository/database
package database

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models"
)

const (
	addDeliveryQuery = `INSERT INTO deliveries
    ("name", "phone", "zip", "city", "address", "region", "email", "order_uid")
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    ON CONFLICT (order_uid) DO UPDATE SET
        name = EXCLUDED.name,
        phone = EXCLUDED.phone,
        zip = EXCLUDED.zip,
        city = EXCLUDED.city,
        address = EXCLUDED.address,
        region = EXCLUDED.region,
        email = EXCLUDED.email
    RETURNING order_uid`

	getDeliveryQuery = `SELECT * FROM deliveries WHERE order_uid = $1`
)

func AddDelivery(db *sql.DB, delivery models.Delivery, orderUID string) (string, error) {
	// Проверяем, существует ли доставка с данным order_uid
	existingDelivery, err := GetDelivery(db, orderUID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("не удалось получить доставку: %w", err)
	}

	var operationMessage string
	var returnedOrderUID string

	err = db.QueryRow(
		addDeliveryQuery,
		delivery.Name,
		delivery.Phone,
		delivery.Zip,
		delivery.City,
		delivery.Address,
		delivery.Region,
		delivery.Email,
		orderUID,
	).Scan(&returnedOrderUID)

	if err != nil {
		return "", fmt.Errorf("failed to insert/update delivery: %w", err)
	}

	if existingDelivery != nil {
		// Если существующая доставка найдена
		operationMessage = fmt.Sprintf("Доставка с order_uid %s успешно обновлена", returnedOrderUID)
	} else {
		// Если доставка новая
		operationMessage = fmt.Sprintf("Создана новая доставка с order_uid %s", returnedOrderUID)
	}

	return operationMessage, nil
}

func GetDelivery(db *sql.DB, orderUID string) (*models.Delivery, error) {
	row := db.QueryRow(getDeliveryQuery, orderUID)

	var delivery models.Delivery
	err := row.Scan(
		&delivery.OrderUID,
		&delivery.Name,
		&delivery.Phone,
		&delivery.Zip,
		&delivery.City,
		&delivery.Address,
		&delivery.Region,
		&delivery.Email,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil // Запись не найдена
		}
		return nil, fmt.Errorf("failed to get delivery: %w", err)
	}

	return &delivery, nil
}
