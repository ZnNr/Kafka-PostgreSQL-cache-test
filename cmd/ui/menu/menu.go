package menu

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/service"

	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/datagenerators"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/models"
)

// Option представляет пункт меню
type Option struct {
	Key         string
	Description string
	Action      func() error
}

// Menu управляет интерактивным меню
type Menu struct {
	Title         string
	Options       []Option
	Reader        *bufio.Reader
	OrderProducer *service.OrderProducer
	Orders        []models.Order
}

// NewMenu создаёт новое меню
func NewMenu(orderProducer *service.OrderProducer, orders []models.Order) *Menu {
	menu := &Menu{
		Title:         "=== Order Producer ===",
		Reader:        bufio.NewReader(strings.NewReader("")), // будет заменён на os.Stdin
		OrderProducer: orderProducer,
		Orders:        orders,
	}

	options := []Option{
		{"s", "Generate and send new VALID order", func() error {
			order := datagenerators.GenerateOrder()
			log.Printf("Generated NEW valid order: %s", order.OrderUID)
			return menu.sendOrder(order)
		}},
		{"c", "Send copy of existing order from DB", func() error {
			if len(menu.Orders) == 0 {
				fmt.Println("No orders available in database.")
				return nil
			}
			fmt.Println("Available orders:")
			for i, o := range menu.Orders {
				fmt.Printf("%d: %s (Created: %s)\n", i, o.OrderUID, o.DateCreated.Format("2006-01-02 15:04"))
			}
			fmt.Print("Select order number: ")
			input, _ := menu.Reader.ReadString('\n')
			idxStr := strings.TrimSpace(input)
			idx, err := strconv.Atoi(idxStr)
			if err != nil || idx < 0 || idx >= len(menu.Orders) {
				return fmt.Errorf("invalid selection: %s", idxStr)
			}
			return menu.sendOrder(menu.Orders[idx])
		}},
		{"i", "Send INVALID order (empty OrderUID)", func() error {
			order := datagenerators.GenerateInvalidOrder_EmptyUID()
			log.Printf("Generated invalid order (empty UID): %s", order.OrderUID)
			return menu.sendOrder(order)
		}},
		{"n", "Send INVALID order (negative amount)", func() error {
			order := datagenerators.GenerateInvalidOrder_NegativeAmount()
			log.Printf("Generated invalid order (negative amount): %s", order.OrderUID)
			return menu.sendOrder(order)
		}},
		{"e", "Send INVALID order (invalid email)", func() error {
			order := datagenerators.GenerateInvalidOrder_InvalidEmail()
			log.Printf("Generated invalid order (invalid email): %s", order.OrderUID)
			return menu.sendOrder(order)
		}},
		{"b", "Send INVALID order (sale >100%)", func() error {
			order := datagenerators.GenerateInvalidOrder_BigSalePercent()
			log.Printf("Generated invalid order (sale >100%%): %s", order.OrderUID)
			return menu.sendOrder(order)
		}},
		{"m", "Send MALFORMED JSON (unparsable)", func() error {
			rawMessage := datagenerators.GenerateMalformedJSON_ReturnsBytes()
			if len(rawMessage) == 0 {
				log.Printf("Empty malformed message generated")
				return nil
			}
			err := menu.OrderProducer.Send(rawMessage)
			if err != nil {
				log.Printf("Failed to send malformed JSON: %v", err)
			} else {
				log.Printf("Sent %d-byte MALFORMED JSON", len(rawMessage))
			}
			return err
		}},
		{"r", "Refresh orders list from database", func() error {
			// Это действие будет обработано в main
			return fmt.Errorf("refresh_signal")
		}},
	}

	menu.Options = options
	return menu
}

// Run запускает цикл меню
func (m *Menu) Run() {
	for {
		fmt.Println("\n" + m.Title)
		for _, opt := range m.Options {
			fmt.Printf("%s - %s\n", opt.Key, opt.Description)
		}
		fmt.Println("exit - Quit program")
		fmt.Print("Choose option: ")

		input, err := m.Reader.ReadString('\n')
		if err != nil {
			log.Printf("Input error: %v", err)
			continue
		}

		key := strings.TrimSpace(input)

		switch key {
		case "exit":
			fmt.Println("👋 Exiting the program...")
			return
		default:
			found := false
			for _, opt := range m.Options {
				if opt.Key == key {
					err := opt.Action()
					if err != nil {
						if err.Error() == "refresh_signal" {
							return // signal to refresh
						}
						log.Printf("Action error: %v", err)
					}
					found = true
					break
				}
			}
			if !found {
				fmt.Println("Invalid input. Please choose a valid option.")
			}
		}
	}
}

// SetReader устанавливает источник ввода (для тестов)
func (m *Menu) SetReader(reader *bufio.Reader) {
	m.Reader = reader
}

// sendOrder — вспомогательная функция для отправки заказа
func (m *Menu) sendOrder(order models.Order) error {
	data, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}
	return m.OrderProducer.Send(data)
}
