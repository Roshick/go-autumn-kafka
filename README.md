# go-autumn-kafka

Inspired by the go-autumn framework, go-autumn-kafka offers a lightweight Kafka wrapper for type-safe producers and consumers, ensuring a more secure and predictable messaging system.

## Features

- **Type Safe**: Ensures type safety for both producers and consumers.
- **ConfigLoader Compatibility**: Provides config compatible with [go-autumn-configloader](https://github.com/Roshick/go-autumn-configloader).

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Producer](#producer)
  - [Consumer](#consumer)
- [Configuration](#configuration)
- [Contributing](#contributing)
- [License](#license)

## Installation

Install go-autumn-kafka with the following command:

```sh
go get github.com/Roshick/go-autumn-kafka
```

## Usage

### Producer

Creating a type-safe Kafka producer is straightforward:

```go
package main

import (
	"context"

	"github.com/Roshick/go-autumn-kafka/pkg/kafka"
)

type Message struct {
	Example string
}

func main() {
	config := kafka.TopicConfig{
		Topic:    "some-topic",
		Username: "some-username",
		Password: "some-password",
		Brokers:  []string{"localhost:9092"},
	}

	producer, err := kafka.CreateSyncProducer[Message](context.TODO(), config, nil)
	if err != nil {
		panic("failed to instantiate kafka producer: " + err.Error())
	}

	err = producer.Produce(context.TODO(), nil, &Message{
		Example: "Hello World",
	})
	if err != nil {
		panic("failed to produce message: " + err.Error())
	}
}
```

### Consumer

Here’s an example of a type-safe Kafka consumer:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Roshick/go-autumn-kafka/pkg/kafka"
)

type Message struct {
  Example string
}

func main() {
  config := kafka.TopicConfig{
    Topic:         "some-topic",
    Username:      "some-username",
    Password:      "some-password",
    ConsumerGroup: ptr("some-consumer-group"),
    Brokers:       []string{"localhost:9092"},
  }

  callback := func(ctx context.Context, key *string, msg *Message, timestamp time.Time) error {
    if msg != nil {
      fmt.Printf("Message %v received at timestamp %v\n", *msg, timestamp)
    }
    return nil
  }

  // Create consumer and start receive loop asynchronously
  consumer, err := kafka.CreateConsumer[Message](context.TODO(), config, callback, nil)
  if err != nil {
    panic("failed to instantiate kafka consumer: " + err.Error())
  }
  defer consumer.Close(context.TODO())

  // Keep process alive
  time.Sleep(60 * time.Second)
}

func ptr[E any](e E) *E {
  return &e
}
```

## Configuration

Use the [go-autumn-configloader](https://github.com/Roshick/go-autumn-configloader) to manage and load your Kafka configuration settings. Here's an example configuration file (`config.yaml`):

```yaml
KAFKA_TOPICS_CONFIG: |
  {
    "some-topic-config": {
      "topic": "some-topic",
      "brokers": [
        "localhost:9092"
      ],
      "username": "some-username",
      "password": "some-password"
    }
  }
```

To load and access this configuration, use the `configloader` in your application:

```go
package main

import (
	"context"
	
	"github.com/Roshick/go-autumn-configloader/pkg/configloader"
	"github.com/Roshick/go-autumn-kafka/pkg/kafka"
)

type Message struct {
	Example string
}

func main() {
	config := kafka.NewDefaultConfig()
	yamlProvider := configloader.CreateYAMLConfigFileProvider("config.yaml")

	configLoader := configloader.New()
	if err := configLoader.LoadConfig(config, yamlProvider); err != nil {
		panic("failed to load config values: " + err.Error())
	}

	topicConfig, ok := config.TopicConfigs("some-topic-config")
	if !ok {
		panic("failed to find topic config for some-topic-config")
	}
	producer, err := kafka.CreateSyncProducer[Message](context.TODO(), topicConfig, nil)
	if err != nil {
		panic("failed to instantiate kafka producer: " + err.Error())
	}

	err = producer.Produce(context.TODO(), nil, &Message{
		Example: "Hello World",
	})
	if err != nil {
		panic("failed to produce message: " + err.Error())
	}
}
```

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request, or open an issue to report bugs or request new features.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
