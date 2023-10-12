package kafka

import (
	"encoding/json"
	"fmt"

	aulogging "github.com/StephanHCB/go-autumn-logging"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/net/context"
)

type Consumer[E any] struct {
	client *kgo.Client

	receiveCallback func(ctx context.Context, event E) error
}

func CreateConsumer[E any](
	ctx context.Context,
	config TopicConfig,
	receiveCallback func(context.Context, E) error,
	customOpts ...kgo.Opt,
) (Consumer[E], error) {
	opts := defaultTopicOptions(config)
	opts = append(opts, customOpts...)
	opts = append(opts, kgo.ConsumerGroup(*config.ConsumerGroup), kgo.ConsumeTopics(config.Topic))

	client, err := kgo.NewClient(opts...)
	if err != nil {
		aulogging.Logger.Ctx(ctx).Error().WithErr(err).Printf("failed to connect to topic %s", config.Topic)
		return Consumer[E]{}, err
	}

	consumer := Consumer[E]{
		client:          client,
		receiveCallback: receiveCallback,
	}

	go consumer.run(ctx)
	return consumer, nil
}

func (c *Consumer[E]) Stop() {
	c.client.Close()
}

func (c *Consumer[E]) run(ctx context.Context) error {
	for {
		fetches := c.client.PollFetches(context.Background())
		if fetches.IsClientClosed() {
			aulogging.Logger.NoCtx().Info().Print("receive loop ending, kafka client was closed")
			return nil
		}
		aulogging.Logger.NoCtx().Debug().Printf("receive loop found %d fetches", len(fetches))

		var firstError error = nil
		fetches.EachError(func(t string, p int32, err error) {
			if firstError == nil {
				firstError = fmt.Errorf("receive loop fetch error topic %s partition %d: %v", t, p, err)
			}
		})
		if firstError != nil {
			aulogging.Logger.NoCtx().Error().WithErr(firstError).Print("receive loop terminated abnormally: %v", firstError)
			return firstError
		}

		fetches.EachRecord(func(record *kgo.Record) {
			aulogging.Logger.NoCtx().Info().Printf("received kafka message: %s", string(record.Value))
			var event E
			if err := json.Unmarshal(record.Value, &event); err != nil {
				aulogging.Logger.Ctx(ctx).Warn().WithErr(err).Printf("failed to parse kafka message: %s", string(record.Value))
				c.receiveCallback(context.TODO(), event)
			}
		})
	}
}
