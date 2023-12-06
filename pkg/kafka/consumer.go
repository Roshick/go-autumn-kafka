package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	aulogging "github.com/StephanHCB/go-autumn-logging"
)

type Consumer[E any] struct {
	client          sarama.ConsumerGroup
	topicConfig     TopicConfig
	receiveCallback func(ctx context.Context, key *string, event *E, timestamp time.Time) error
}

func NewConsumer[E any](
	ctx context.Context,
	topicConfig TopicConfig,
	receiveCallback func(context.Context, *string, *E, time.Time) error,
	configPreset *sarama.Config,
) (*Consumer[E], error) {
	if topicConfig.ConsumerGroup == nil || *topicConfig.ConsumerGroup == "" {
		return nil, fmt.Errorf("failed to create consumer group client: consumer group is missing or empty")
	}

	clientConfig, err := mergeConfigWithPreset(topicConfig, configPreset)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group client: %s", err.Error())
	}

	client, err := sarama.NewConsumerGroup(topicConfig.Brokers, *topicConfig.ConsumerGroup, clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group client: %s", err.Error())
	}

	consumer := Consumer[E]{
		client:          client,
		topicConfig:     topicConfig,
		receiveCallback: receiveCallback,
	}

	go consumer.run(ctx)
	return &consumer, nil
}

func (c *Consumer[E]) Close(ctx context.Context) {
	err := c.client.Close()
	if err != nil {
		aulogging.Logger.Ctx(ctx).Warn().WithErr(err).Print("failed to close kafka consumer")
	}
}

func (c *Consumer[E]) run(ctx context.Context) {
	defer func() {
		r := recover()
		if err, ok := r.(error); ok {
			aulogging.Logger.Ctx(ctx).Error().WithErr(err).Print("caught panic in kafka consumer")
		}
	}()

	for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := c.client.Consume(ctx, []string{c.topicConfig.Topic}, c); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return
			}
			aulogging.Logger.Ctx(ctx).Error().WithErr(err).Print("kafka consumer returned with error")
		}
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *Consumer[E]) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *Consumer[E]) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (c *Consumer[E]) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				aulogging.Logger.Ctx(session.Context()).Info().Print("message channel was closed")
				return nil
			}
			timestamp := message.Timestamp.UTC()
			key := new(string)
			if message.Key != nil {
				*key = string(message.Key)
			}
			event := new(E)
			if message.Value != nil {
				if err := json.Unmarshal(message.Value, &event); err != nil {
					aulogging.Logger.Ctx(session.Context()).Warn().WithErr(err).
						Printf("failed to unmarshal event key = %s, value = %s, timestamp = %v, topic = %s",
							key, event, timestamp, message.Topic)
				}
			}
			if err := c.receiveCallback(session.Context(), key, event, timestamp); err != nil {
				aulogging.Logger.Ctx(session.Context()).Warn().WithErr(err).
					Printf("failed to perform callback on event key = %s, value = %s, timestamp = %v, topic = %s",
						key, event, timestamp, message.Topic)
			} else {
				session.MarkMessage(message, "")
			}
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}
