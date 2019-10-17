package main

import (
	"context"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

func main() {
	pubSub := gochannel.NewGoChannel(
		gochannel.Config{},
		watermill.NewStdLogger(false, false),
	)

	messagesA, err := pubSub.Subscribe(context.Background(), "example.topic.A")
	if err != nil {
		panic(err)
	}
	messagesB, err := pubSub.Subscribe(context.Background(), "example.topic.B")
	if err != nil {
		panic(err)
	}

	go receiveMessages(messagesA)
	go receiveMessages(messagesB)

	publishMessages(pubSub)

	log.Println("finish!")
}

func publishMessages(pubSub *gochannel.GoChannel) error {
	publishMessage := func(publisher message.Publisher, topic string, msgStr string) error {
		msg := message.NewMessage(watermill.NewUUID(), []byte(msgStr))
		return publisher.Publish(topic, msg)
	}

	for i := 0; i < 5; i++ {
		if err := publishMessage(pubSub, "example.topic.A", "Hello, message A!"); err != nil {
			return err
		}
		if err := publishMessage(pubSub, "example.topic.B", "Hello, message B!"); err != nil {
			return err
		}
		time.Sleep(time.Second)
	}
	return nil
}

func receiveMessages(messages <-chan *message.Message) {
	for msg := range messages {
		log.Printf("received message: %s, payload: %s", msg.UUID, string(msg.Payload))
		msg.Ack() // Ack sends message's acknowledgement.
	}
}
