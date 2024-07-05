package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	aulogging "github.com/StephanHCB/go-autumn-logging"
)

type PostSendCallback func(ctx context.Context, message sarama.ProducerMessage, err error)

func NoopCallback(_ context.Context, _ sarama.ProducerMessage, _ error) {}

var _ PostSendCallback = NoopCallback

func LogErrorCallback(ctx context.Context, _ sarama.ProducerMessage, err error) {
	if err != nil {
		aulogging.Logger.Ctx(ctx).Warn().WithErr(err).Print("failed to send kafka message")
	}
}

var _ PostSendCallback = LogErrorCallback

type AsyncProducer[V any] struct {
	client sarama.SyncProducer
	topic  string
}

func CreateAsyncProducer[V any](
	_ context.Context,
	topicConfig TopicConfig,
	configPreset *sarama.Config,
) (*AsyncProducer[V], error) {
	clientConfig, err := mergeConfigWithPreset(topicConfig, configPreset)
	if err != nil {
		return nil, fmt.Errorf("failed to create sync producer client: %s", err.Error())
	}
	clientConfig.Producer.Return.Successes = true

	client, err := sarama.NewSyncProducer(topicConfig.Brokers, clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create sync producer client: %s", err.Error())
	}

	return &AsyncProducer[V]{
		client: client,
		topic:  topicConfig.Topic,
	}, err
}

func (p *AsyncProducer[V]) Produce(
	ctx context.Context,
	key *string,
	value *V,
	postSendCallback PostSendCallback,
) error {
	var keyBytes []byte
	if key != nil {
		keyBytes = []byte(*key)
	}
	var valueBytes []byte
	if value != nil {
		var err error
		valueBytes, err = json.Marshal(*value)
		if err != nil {
			return err
		}
	}
	message := sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.ByteEncoder(keyBytes),
		Value: sarama.ByteEncoder(valueBytes),
	}

	go func() {
		_, _, err := p.client.SendMessage(&message)
		if postSendCallback != nil {
			postSendCallback(ctx, message, err)
		}
	}()
	return nil
}

func (p *AsyncProducer[E]) Close(ctx context.Context) {
	err := p.client.Close()
	if err != nil {
		aulogging.Logger.Ctx(ctx).Warn().WithErr(err).Print("failed to close kafka producer")
	}
}
