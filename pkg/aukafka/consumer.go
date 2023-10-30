package aukafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	aulogging "github.com/StephanHCB/go-autumn-logging"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Consumer[E any] struct {
	client          *kgo.Client
	receiveCallback func(ctx context.Context, key *string, event *E, timestamp time.Time) error
}

func CreateConsumer[E any](
	ctx context.Context,
	config TopicConfig,
	receiveCallback func(context.Context, *string, *E, time.Time) error,
	customOpts ...kgo.Opt,
) (*Consumer[E], error) {
	opts := defaultTopicOptions(fmt.Sprintf("%s consumer", config.Topic), config)
	opts = append(opts, customOpts...)
	opts = append(opts, kgo.ConsumerGroup(*config.ConsumerGroup), kgo.ConsumeTopics(config.Topic))

	client, err := kgo.NewClient(opts...)
	if err != nil {
		aulogging.Logger.Ctx(ctx).Error().WithErr(err).Printf("failed to connect to topic %s", config.Topic)
		return nil, err
	}

	consumer := Consumer[E]{
		client:          client,
		receiveCallback: receiveCallback,
	}

	go consumer.run(ctx)
	return &consumer, nil
}

func (c *Consumer[E]) Stop() {
	c.client.Close()
}

func (c *Consumer[E]) run(
	ctx context.Context,
) {
	for {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			aulogging.Logger.NoCtx().Info().Print("kafka client closed, stopping consumer")
			return
		}
		aulogging.Logger.NoCtx().Debug().Printf("received %s fetches", len(fetches))

		fetches.EachError(func(t string, p int32, err error) {
			aulogging.Logger.NoCtx().Error().WithErr(err).Printf("fetch error occurred for partition %d of topic %s", p, t)
		})

		fetches.EachRecord(func(record *kgo.Record) {
			key := new(string)
			if record.Key != nil {
				*key = string(record.Key)
			}
			event := new(E)
			if record.Value != nil {
				if err := json.Unmarshal(record.Value, &event); err != nil {
					aulogging.Logger.Ctx(ctx).Warn().WithErr(err).Printf("failed to unmarshal event")
					return
				}
			}
			if err := c.receiveCallback(ctx, key, event, record.Timestamp); err != nil {
				aulogging.Logger.Ctx(ctx).Warn().WithErr(err).Printf("failed to perform event callback")
			}
		})
	}
}
