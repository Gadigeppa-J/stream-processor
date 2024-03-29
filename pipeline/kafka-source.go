package pipeline

import (
	"container/list"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

type KafkaSource struct {
	ctx        context.Context
	consumer   *kafka.Consumer
	sinkStream <-chan interface{}
}

func NewKafkaSource() Source {
	return &KafkaSource{}
}

func (k *KafkaSource) Initialize(ctx context.Context) {

	k.ctx = ctx
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost",
		"group.id":           "myGroup",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
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

		/*
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
		*/
		barrierTick := time.Tick(time.Second * 5)
		var lastEmittedMsg *LastEmittedMessage

		run := true
		for run == true {

			ev := k.consumer.Poll(0)

			switch e := ev.(type) {

			case *kafka.Message:
				fmt.Printf("Message on %s: %s\n", e.TopicPartition, string(e.Value))
				msg := Message{
					meta: e,
					data: e,
				}
				select {
				case <-k.ctx.Done():
					return
				case outStream <- msg:
					lastEmittedMsg = &LastEmittedMessage{e}
					select {
					case <-k.ctx.Done():
						return
					case <-barrierTick:
						fmt.Println("Reached barrier")
						barrierEvt := BarrierEvent{uuid.New().String(), lastEmittedMsg.msg, 1}
						select {
						case <-k.ctx.Done():
							return
						case outStream <- barrierEvt:
							fmt.Println("Injected barrier event: ", barrierEvt)
							lastEmittedMsg = nil
						}
					default:
					}
				}

			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false
			default:
				//fmt.Printf("Ignored %v\n", e)
				select {
				case <-k.ctx.Done():
					return
				case <-barrierTick:
					if lastEmittedMsg != nil {
						fmt.Println("Reached barrier")
						barrierEvt := BarrierEvent{uuid.New().String(), lastEmittedMsg.msg, 1}
						select {
						case <-k.ctx.Done():
							return
						case outStream <- barrierEvt:
							fmt.Println("Injected barrier event: ", barrierEvt)
							lastEmittedMsg = nil
						}
					}
				default:
				}
			}
		}

	}()

	return outStream
}

func (k *KafkaSource) ConsumeSinkStream(inStream <-chan interface{}) {

	go func() {

		barriersMap := make(map[string]*BarrierEvent)
		barriersQueue := list.New()

		for {
			select {
			case <-k.ctx.Done():
				return
			case msg := <-inStream:
				switch msg.(type) {
				case BarrierEvent:
					be := msg.(BarrierEvent)
					fmt.Println("Received Barrier event in source operator: ", be)
					bme, ok := barriersMap[be.id]
					if !ok {
						beCopy := be
						barriersQueue.PushBack(&beCopy)
						barriersMap[be.id] = &beCopy
						bme = barriersMap[be.id]
					}

					bme.chunkCount--
					curr := barriersQueue.Front()

					for curr != nil {

						if curr.Value.(*BarrierEvent).chunkCount > 0 {
							break
						}

						barrierEvt := curr.Value.(*BarrierEvent)

						fmt.Println("Commiting offset for message: ", barrierEvt.msg)
						k.consumer.CommitMessage(barrierEvt.msg)

						next := curr.Next()

						// clean up
						delete(barriersMap, barrierEvt.id)
						barriersQueue.Remove(curr)

						curr = next
					}

				case Message:
					err := msg.(Message).err
					if err != nil {
						fmt.Println("Error: ", err)
					}

				default:
				}
			}
		}
	}()

}

type Message struct {
	meta interface{}
	data interface{}
	err  error
}

type LastEmittedMessage struct {
	msg *kafka.Message
}

type BarrierEvent struct {
	id         string
	msg        *kafka.Message
	chunkCount int
}
