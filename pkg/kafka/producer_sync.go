package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	aulogging "github.com/StephanHCB/go-autumn-logging"
)

type SyncProducer[V any] struct {
	client sarama.SyncProducer
	topic  string
}

func NewSyncProducer[V any](
	_ context.Context,
	topicConfig TopicConfig,
	configPreset *sarama.Config,
) (*SyncProducer[V], error) {
	clientConfig, err := mergeConfigWithPreset(topicConfig, configPreset)
	if err != nil {
		return nil, fmt.Errorf("failed to create sync producer client: %s", err.Error())
	}
	clientConfig.Producer.Return.Successes = true

	client, err := sarama.NewSyncProducer(topicConfig.Brokers, clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create sync producer client: %s", err.Error())
	}

	return &SyncProducer[V]{
		client: client,
		topic:  topicConfig.Topic,
	}, err
}

func (p *SyncProducer[V]) Produce(
	_ context.Context,
	key *string,
	value *V,
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

	if _, _, err := p.client.SendMessage(&message); err != nil {
		return err
	}
	return nil
}

func (p *SyncProducer[E]) Close(ctx context.Context) {
	err := p.client.Close()
	if err != nil {
		aulogging.Logger.Ctx(ctx).Warn().WithErr(err).Print("failed to close kafka producer")
	}
}
