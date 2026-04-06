package main

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
	)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()

	record := &kgo.Record{
		Topic: "my-first-topic",
		Value: []byte("Hello from Go producer!"),
	}

	if err := client.ProduceSync(ctx, record).FirstErr(); err != nil {
		panic(err)
	}

	fmt.Println("✅ Message sent!")
}
