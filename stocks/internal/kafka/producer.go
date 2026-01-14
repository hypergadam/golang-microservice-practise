package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

//go:generate mkdir -p mock
//go:generate minimock -o ./mock/ -s .go -g

const (
	flushTimeout = 5000
)

type (
	StocksEventProducer interface {
		ProduceSKUCreated(ctx context.Context, payload SKUCreatedAndStockChangedPayload)
		ProduceStockChanged(ctx context.Context, payload SKUCreatedAndStockChangedPayload)
		Close()
	}
)

type (
	EventModel struct {
		Type      string      `json:"type"`
		Service   string      `json:"service"`
		Timestamp time.Time   `json:"timestamp"`
		Payload   interface{} `json:"payload"`
	}

	SKUCreatedAndStockChangedPayload struct {
		SKU   string `json:"sku"`
		Price uint32 `json:"price"`
		Count uint16 `json:"count"`
	}
)

var _ StocksEventProducer = (*stocksEventProducer)(nil)

type stocksEventProducer struct {
	producer *kafka.Producer
	topic    string
}

func NewStocksServiceEventProducer(address []string) (*stocksEventProducer, error) {
	conf := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(address, ","),
	}

	prod, err := kafka.NewProducer(conf)
	if err != nil {
		return nil, fmt.Errorf("error creating stocks service kafka producer: %w", err)
	}

	// i started goroutine for delivery report here.
	go func() {
		for e := range prod.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("[stocksService]delivery failed for topic %s [partition %d]: %v\n", *ev.TopicPartition.Topic,
						ev.TopicPartition.Partition, ev.TopicPartition.Error,
					)
				} else {
					log.Printf("[stocksService]delivered message to topic %s [partition %d]: %v\n", *ev.TopicPartition.Topic,
						ev.TopicPartition.Partition, ev.TopicPartition.Error,
					)
				}
			case kafka.Error:
				log.Printf("stocks service kafka producer error: %v\n", ev)
			}
		}
	}()

	return &stocksEventProducer{
		producer: prod,
		topic:    "metrics",
	}, nil
}

func (sp *stocksEventProducer) ProduceSKUCreated(ctx context.Context, payload SKUCreatedAndStockChangedPayload) {
	event := EventModel{
		Type:      "sku_created",
		Service:   "stock",
		Timestamp: time.Now(),
		Payload:   payload,
	}

	eventBytes, err := json.Marshal(event)
	if err != nil {
		log.Printf("failed to marshal sku_created event: %v\n", err.Error())
	}

	sp.produce(ctx, eventBytes, "sku_created_key", 1)
}

func (sp *stocksEventProducer) ProduceStockChanged(ctx context.Context, payload SKUCreatedAndStockChangedPayload) {
	event := EventModel{
		Type:      "stock_changed",
		Service:   "stock",
		Timestamp: time.Now(),
		Payload:   payload,
	}

	eventBytes, err := json.Marshal(event)
	if err != nil {
		log.Printf("failed to marshal sku_created event: %v\n", err.Error())
	}

	sp.produce(ctx, eventBytes, "stock_changed_key", 1)
}

func (sp *stocksEventProducer) produce(ctx context.Context, message []byte, key string, partition int32) {
	kafkaMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &sp.topic,
			Partition: partition,
		},
		Value:     message,
		Key:       []byte(key),
		Timestamp: time.Now(),
	}

	kafkaChan := make(chan kafka.Event, 1)
	if err := sp.producer.Produce(kafkaMessage, kafkaChan); err != nil {
		log.Printf("failed to produce message with key %s: %v\n", key, err)
	}

	go func() {
		select {
		case e := <-kafkaChan:
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("[stocksService_kafkaProducer]: delivery failed for key %s: %v\n", key,
						ev.TopicPartition.Error,
					)
				} else {
					log.Printf("[stocksService_kafkaProducer]: delivery message with key %s to topic %s [partition %d] at offset %v\n",
						key, *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset,
					)
				}
			case kafka.Error:
				log.Printf("[stocksService_kafkaProducer]: error for key %s: %v\n", key, ev)
			}
		case <-ctx.Done():
			log.Printf("context cancelled for message with key %s: %v\n", key, ctx.Err())
		}
	}()
}

func (sp *stocksEventProducer) Close() {
	sp.producer.Flush(flushTimeout)
	sp.producer.Close()
}
