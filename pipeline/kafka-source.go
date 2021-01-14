package pipeline

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaSource struct {
	ctx      context.Context
	consumer *kafka.Consumer
}

func NewKafkaSource() Source {
	return &KafkaSource{}
}

func (k *KafkaSource) Initialize(ctx context.Context) {

	k.ctx = ctx
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"test-events"}, nil)
	k.consumer = c

}

func (k *KafkaSource) StartStream() <-chan interface{} {

	outStream := make(chan interface{})

	go func() {
		defer k.consumer.Close()
		defer close(outStream)

		for {
			msg, err := k.consumer.ReadMessage(-1)
			if err == nil {
				fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))

				select {
				case <-k.ctx.Done():
					return
				case outStream <- msg:
				}

			} else {
				// The client will automatically try to recover from all errors.
				fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			}
		}
	}()

	return outStream
}
