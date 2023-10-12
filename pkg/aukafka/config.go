package aukafka

import (
	"encoding/json"
	"fmt"

	auconfigapi "github.com/StephanHCB/go-autumn-config-api"
	auconfigenv "github.com/StephanHCB/go-autumn-config-env"
)

const KeyKafkaTopicsConfig = "KAFKA_TOPICS_CONFIG"

type rawTopicConfig struct {
	Topic          string   `json:"topic"`
	Brokers        []string `json:"brokers"`
	Username       string   `json:"username"`
	PasswordEnvVar string   `json:"passwordEnvVar"`
	ConsumerGroup  *string  `json:"consumerGroup,omitempty"`
	AuthType       string   `json:"authType"`
}

type TopicConfig struct {
	Topic         string
	Brokers       []string
	Username      string
	Password      string
	ConsumerGroup *string
	AuthType      string
}

type Config struct {
	vTopicConfigs map[string]TopicConfig
}

func NewConfig() *Config {
	return new(Config)
}

func (c *Config) TopicConfigs() map[string]TopicConfig {
	return c.vTopicConfigs
}

func (c *Config) ConfigItems() []auconfigapi.ConfigItem {
	return []auconfigapi.ConfigItem{
		{
			Key:         KeyKafkaTopicsConfig,
			EnvName:     KeyKafkaTopicsConfig,
			Default:     "{}",
			Description: "configuration consisting of topic keys (not necessarily the topic name, rather the key used by the application to produce events for or consume of specific topics) and their respective authentication",
			Validate: func(key string) error {
				value := auconfigenv.Get(key)
				_, err := parseTopicConfigs(value)
				return err
			},
		},
	}
}

func (c *Config) Obtain(getter func(key string) string) {
	c.vTopicConfigs, _ = parseTopicConfigs(getter(KeyKafkaTopicsConfig))
}

func parseTopicConfigs(jsonString string) (map[string]TopicConfig, error) {
	rawConfigs := make(map[string]rawTopicConfig)
	if err := json.Unmarshal([]byte(jsonString), &rawConfigs); err != nil {
		return nil, err
	}

	topicConfigs := make(map[string]TopicConfig)
	for key, rawConfig := range rawConfigs {
		password := auconfigenv.Get(rawConfig.PasswordEnvVar)
		// We do not support accessing topics without a password
		if password == "" {
			return nil, fmt.Errorf("kafka-topic %s password variable %s is empty", rawConfig.Topic, rawConfig.PasswordEnvVar)
		}

		topicConfigs[key] = TopicConfig{
			Topic:         rawConfig.Topic,
			Brokers:       rawConfig.Brokers,
			Username:      rawConfig.Username,
			Password:      password,
			ConsumerGroup: rawConfig.ConsumerGroup,
			AuthType:      rawConfig.AuthType,
		}
	}
	return topicConfigs, nil
}
