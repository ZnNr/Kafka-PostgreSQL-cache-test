// cmd/producer/producer.go
package main

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/config"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/datagenerators"
	"github.com/ZnNr/Kafka-PostgreSQL-cache-test/internal/repository"
	"log"
	"strconv"
)

var (
	cfgPath = "config/config.yaml"
)

func main() {
	topic := "orders"

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// подключ к базе данных
	ordersRepo, err := repository.New(cfg)
	if err != nil {
		log.Fatalf("Connection to DB failed: %v", err)
	}
	defer func() {
		if err := ordersRepo.DB.Close(); err != nil {
			log.Fatalf("Failed to close database connection: %v", err)
		}
	}()
	brokers := cfg.Kafka.Brokers
	producer, err := ConnectProducer(brokers)
	if err != nil {
		log.Fatalf("Failed to connect to Kafka: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalf("Failed to close Kafka producer: %v", err)
		}
	}()

	log.Println("Producer is launched!")
	// Получаем старые заказы
	orders, err := ordersRepo.GetOrders()
	if err != nil {
		log.Fatalf("Failed to get old orders from DB: %v", err)
	}
	// Логи загруженных заказов
	log.Printf("Loaded %d orders from the database", len(orders))

	// Основной цикл ввода
	for {
		log.Println("Type 's' to generate a new order")
		log.Println("Type 'c' to select and send a copy of an existing order")
		log.Println("Type 'exit' to quit the program")
		var input string
		var orderJSON []byte
		fmt.Scanln(&input)

		if input == "exit" {
			fmt.Println("Exiting the program...")
			break
		}

		if input == "s" {
			orderGenerated := datagenerators.GenerateOrder()
			orderJSON, err = json.Marshal(orderGenerated)
			if err != nil {
				log.Printf("Failed to convert order to JSON: %s", err)
				continue
			}
		}

		if input == "c" {
			if len(orders) == 0 {
				log.Println("No orders available to select.")
				continue
			}
			log.Println("Choose one of old orders:")
			for i := 0; i < len(orders); i++ {
				fmt.Println(i, orders[i].OrderUID)
			}
			var indstr string
			fmt.Scanln(&indstr)
			ind, err := strconv.Atoi(indstr)
			if err != nil {
				log.Println("Entered is not a number!")
				continue
			}
			if ind < 0 || ind > len(orders) {
				log.Println("Entered number isn't in range of orders!")
				continue
			}
			orderJSON, err = json.Marshal(orders[ind])
			if err != nil {
				log.Printf("Failed to convert order to JSON: %s", err)
				continue
			}
		}

		err = PushOrderToQueue(producer, topic, orderJSON)
		if err != nil {
			log.Printf("Failed to send message to Kafka: %s", err)
			continue
		}

		log.Printf("Successfully sent order")
	}
}

func ConnectProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	return sarama.NewSyncProducer(brokers, config)
}

func PushOrderToQueue(producer sarama.SyncProducer, topic string, message []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	log.Printf("Order is stored in topic(%s)/partition(%d)/offset(%d)\n",
		topic,
		partition,
		offset,
	)

	return nil
}
