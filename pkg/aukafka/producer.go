package aukafka

import (
	"context"
	"encoding/json"

	aulogging "github.com/StephanHCB/go-autumn-logging"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer[V any] struct {
	client *kgo.Client
}

func CreateProducer[V any](
	ctx context.Context,
	config TopicConfig,
	customOpts ...kgo.Opt,
) (*Producer[V], error) {
	opts := defaultTopicOptions(config)
	opts = append(opts, customOpts...)
	opts = append(opts, kgo.DefaultProduceTopic(config.Topic), kgo.ProducerBatchCompression(kgo.NoCompression()))

	client, err := kgo.NewClient(opts...)
	if err != nil {
		aulogging.Logger.Ctx(ctx).Error().WithErr(err).Printf("failed to connect to topic %s", config.Topic)
		return nil, err
	}

	return &Producer[V]{
		client: client,
	}, err
}

func (p *Producer[V]) ProduceSync(
	ctx context.Context,
	key *string,
	value *V,
) error {
	var keyBytes []byte
	if key != nil {
		keyBytes = []byte(*key)
	}
	var eventBytes []byte
	if value != nil {
		var err error
		eventBytes, err = json.Marshal(*value)
		if err != nil {
			return err
		}
	}
	record := &kgo.Record{
		Key:   keyBytes,
		Value: eventBytes,
	}
	result := p.client.ProduceSync(ctx, record)
	return result.FirstErr()
}

func (p *Producer[E]) Close() {
	p.client.Close()
}
