package kafka

import (
	"encoding/json"

	aulogging "github.com/StephanHCB/go-autumn-logging"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/net/context"
)

type Producer[E any] struct {
	client *kgo.Client
}

func CreateProducer[E any](
	ctx context.Context,
	config TopicConfig,
	customOpts ...kgo.Opt,
) (Producer[E], error) {
	opts := defaultTopicOptions(config)
	opts = append(opts, customOpts...)
	opts = append(opts, kgo.DefaultProduceTopic(config.Topic), kgo.ProducerBatchCompression(kgo.NoCompression()))

	client, err := kgo.NewClient(opts...)
	if err != nil {
		aulogging.Logger.Ctx(ctx).Error().WithErr(err).Printf("failed to connect to topic %s", config.Topic)
		return Producer[E]{}, err
	}

	return Producer[E]{
		client: client,
	}, err
}

func (p *Producer[E]) ProduceSync(ctx context.Context, event E) error {
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return err
	}
	record := &kgo.Record{Value: eventBytes}
	result := p.client.ProduceSync(ctx, record)
	return result.FirstErr()
}

func (p *Producer[E]) Close() {
	p.client.Close()
}
