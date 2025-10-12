package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"localhost:9092"}
	topic := "orders.dlq"

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to start partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Watching DLQ topic:", topic)
	fmt.Print("Press Ctrl+C to stop.\n")

consumeLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			// Ищем заголовок dlq_reason
			reason := "unknown reason"
			for _, h := range msg.Headers {
				if string(h.Key) == "dlq_reason" { // ← []byte → string
					reason = string(h.Value)
					break
				}
			}

			fmt.Printf("❌ INVALID ORDER (offset %d) → REASON: %s\n", msg.Offset, reason)
			fmt.Printf("   Payload: %s\n\n", string(msg.Value))

		case err := <-partitionConsumer.Errors():
			log.Printf("Error: %v", err)

		case <-signals:
			break consumeLoop
		}
	}

	fmt.Println("DLQ watcher stopped.")
}
