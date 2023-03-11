package main

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"os"
	"os/signal"
)

func main() {
	err := realMain()
	if err != nil {
		log.Fatal("main: ", err)
	}
}

func realMain() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	kafkaClient, err := kgo.NewClient(
		kgo.AllowAutoTopicCreation(),
		kgo.ConsumeTopics("test"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)),
	)
	err = kafkaClient.Ping(ctx)
	if err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	err = tail(ctx, kafkaClient)
	if err != nil {
		return fmt.Errorf("tail: %w", err)
	}
	return nil
}

func tail(ctx context.Context, client *kgo.Client) error {
	// Start consuming messages.
	for ctx.Err() == nil {
		fetches := client.PollFetches(ctx)
		if fetches.Err() != nil {
			return fmt.Errorf("poll fetches: %w", fetches.Err())
		}
		log.Printf("pollFetches got %d records", fetches.NumRecords())
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			fmt.Println(record.Topic, ": ", string(record.Value))
		}
	}
	return nil
}
