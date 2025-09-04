// internal/repository/database
package database

import (
	"database/sql"
	"fmt"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models"
	"strconv"
)

const (
	addItemQuery = `INSERT INTO items ("chrt_id", "track_number", "price", "rid", "name", "sale", "size", "total_price", "nm_id", "brand", "status", "order_uid") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) ON CONFLICT (chrt_id) DO NOTHING`

	getAllItemsQuery = "SELECT chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status FROM items WHERE order_uid = $1"
)

// AddItems сохраняет список элементов заказа в БД, пропуская существующие элементы
func AddItems(db *sql.DB, items []models.OrderItem, orderUID string) error {
	for _, item := range items {
		exists, err := ItemExists(db, strconv.Itoa(item.ChartID), orderUID) // Проверка существования
		if err != nil {
			return fmt.Errorf("failed to check existence: %w", err)
		}

		if !exists {
			err = AddItem(db, item, orderUID)
			if err != nil {
				return fmt.Errorf("failed to add item: %w", err)
			}
		}
	}
	return nil
}

// ItemExists проверяет, существует ли элемент в БД
func ItemExists(db *sql.DB, chrtID string, orderUID string) (bool, error) {
	var exists bool
	err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM items WHERE chrt_id = $1 AND order_uid = $2)`, chrtID, orderUID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check if item exists: %w", err)
	}
	return exists, nil
}

// AddItem добавляет новый элемент в БД
func AddItem(db *sql.DB, item models.OrderItem, orderUID string) error {
	_, err := db.Exec(
		addItemQuery,
		&item.ChartID,
		&item.TrackNumber,
		&item.UnitPrice,
		&item.RID,
		&item.ProductName,
		&item.SalePercent,
		&item.SizeCode,
		&item.LineTotal,
		&item.ProductID,
		&item.BrandName,
		&item.StatusCode,
		orderUID,
	)
	if err != nil {
		return fmt.Errorf("failed to execute add item query: %w", err)
	}
	return nil
}

// GetItems получает все элементы из БД по идентификатору заказа
func GetItems(db *sql.DB, orderUID string) ([]models.OrderItem, error) {
	rows, err := db.Query(getAllItemsQuery, orderUID)
	if err != nil {
		return nil, fmt.Errorf("get items failed: %w", err)
	}
	defer rows.Close()

	var items []models.OrderItem
	for rows.Next() {
		var item models.OrderItem
		err := rows.Scan(
			&item.ChartID,
			&item.TrackNumber,
			&item.UnitPrice,
			&item.RID,
			&item.ProductName,
			&item.SalePercent,
			&item.SizeCode,
			&item.LineTotal,
			&item.ProductID,
			&item.BrandName,
			&item.StatusCode,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		items = append(items, item)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iteration over rows failed: %w", err)
	}

	if len(items) == 0 {
		// Логируем ненайденный orderUID
		fmt.Printf("No items found for order UID %s\n", orderUID)
		return nil, fmt.Errorf("items not found for order UID %s", orderUID)
	}

	return items, nil
}
