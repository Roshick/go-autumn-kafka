package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"

	auconfigapi "github.com/StephanHCB/go-autumn-config-api"
	auconfigenv "github.com/StephanHCB/go-autumn-config-env"
)

const (
	DefaultKeyKafkaTopicsConfig = "KAFKA_TOPICS_CONFIG"
)

type rawTopicConfig struct {
	Topic          string   `json:"topic"`
	Brokers        []string `json:"brokers"`
	Username       string   `json:"username"`
	Password       *string  `json:"password,omitempty"`
	PasswordEnvVar *string  `json:"passwordEnvVar,omitempty"`
	ConsumerGroup  *string  `json:"consumerGroup,omitempty"`
	AuthType       string   `json:"authType"`
}

type TopicConfig struct {
	Topic         string
	Brokers       []string
	Username      string
	Password      string
	ConsumerGroup *string
	AuthType      sarama.SASLMechanism
}

func mergeConfigWithPreset(
	topicConfig TopicConfig,
	configPreset *sarama.Config,
) (*sarama.Config, error) {
	var clientConfig *sarama.Config
	if configPreset != nil {
		clientConfig = configPreset
	} else {
		clientConfig = sarama.NewConfig()
	}
	clientConfig.Net.SASL.User = topicConfig.Username
	clientConfig.Net.SASL.Password = topicConfig.Password
	clientConfig.Net.SASL.Enable = true
	clientConfig.Net.SASL.Mechanism = topicConfig.AuthType
	if topicConfig.AuthType == sarama.SASLTypeSCRAMSHA256 && clientConfig.Net.SASL.SCRAMClientGeneratorFunc == nil {
		clientConfig.Net.SASL.SCRAMClientGeneratorFunc = NewSha256ScramClient
	}
	if topicConfig.AuthType == sarama.SASLTypeSCRAMSHA512 && clientConfig.Net.SASL.SCRAMClientGeneratorFunc == nil {
		clientConfig.Net.SASL.SCRAMClientGeneratorFunc = NewSha512ScramClient
	}
	return clientConfig, nil
}

type DefaultConfigImpl struct {
	vTopicConfigs map[string]TopicConfig
}

func NewDefaultConfig() *DefaultConfigImpl {
	return &DefaultConfigImpl{}
}

func (c *DefaultConfigImpl) TopicConfigs(key string) (TopicConfig, bool) {
	value, ok := c.vTopicConfigs[key]
	return value, ok
}

func (c *DefaultConfigImpl) ConfigItems() []auconfigapi.ConfigItem {
	return []auconfigapi.ConfigItem{
		{
			Key:         DefaultKeyKafkaTopicsConfig,
			EnvName:     DefaultKeyKafkaTopicsConfig,
			Default:     "{}",
			Description: "configuration consisting of topic keys (not necessarily the topic name, rather the key used by the application to produce events for or consume of specific topics) and their respective authentication",
			Validate:    auconfigapi.ConfigNeedsNoValidation,
		},
	}
}

func (c *DefaultConfigImpl) ObtainValues(getter func(string) string) error {
	if vTopicConfigs, err := parseTopicConfigs(getter(DefaultKeyKafkaTopicsConfig), getter); err != nil {
		return err
	} else {
		c.vTopicConfigs = vTopicConfigs
	}

	return nil
}

func parseTopicConfigs(jsonString string, getter func(string) string) (map[string]TopicConfig, error) {
	rawConfigs := make(map[string]rawTopicConfig)
	if err := json.Unmarshal([]byte(jsonString), &rawConfigs); err != nil {
		return nil, err
	}

	topicConfigs := make(map[string]TopicConfig)
	for key, rawConfig := range rawConfigs {
		var password string
		// We do not support accessing topics without a password
		if rawConfig.PasswordEnvVar != nil {
			password = getter(*rawConfig.PasswordEnvVar)
			if password == "" {
				return nil, fmt.Errorf("kafka topic %s password environment variable %s is empty", rawConfig.Topic, *rawConfig.PasswordEnvVar)
			}
		} else if rawConfig.Password != nil {
			password = getter(*rawConfig.PasswordEnvVar)
			if password == "" {
				return nil, fmt.Errorf("kafka topic %s password value is empty", rawConfig.Topic)
			}
		} else {
			return nil, fmt.Errorf("kafka topic %s neither password environment variable or password value is set", rawConfig.Topic)
		}

		topicConfigs[key] = TopicConfig{
			Topic:         rawConfig.Topic,
			Brokers:       rawConfig.Brokers,
			Username:      rawConfig.Username,
			Password:      password,
			ConsumerGroup: rawConfig.ConsumerGroup,
			AuthType:      sarama.SASLMechanism(rawConfig.AuthType),
		}
	}
	return topicConfigs, nil
}

// DEPRECATED

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
			Key:         DefaultKeyKafkaTopicsConfig,
			EnvName:     DefaultKeyKafkaTopicsConfig,
			Default:     "{}",
			Description: "configuration consisting of topic keys (not necessarily the topic name, rather the key used by the application to produce events for or consume of specific topics) and their respective authentication",
			Validate: func(key string) error {
				value := auconfigenv.Get(key)
				_, err := ParseTopicConfigs(value)
				return err
			},
		},
	}
}

func (c *Config) Obtain(getter func(key string) string) {
	c.vTopicConfigs, _ = ParseTopicConfigs(getter(DefaultKeyKafkaTopicsConfig))
}

func ParseTopicConfigs(jsonString string) (map[string]TopicConfig, error) {
	rawConfigs := make(map[string]rawTopicConfig)
	if err := json.Unmarshal([]byte(jsonString), &rawConfigs); err != nil {
		return nil, err
	}

	topicConfigs := make(map[string]TopicConfig)
	for key, rawConfig := range rawConfigs {
		var password string
		// We do not support accessing topics without a password
		if rawConfig.PasswordEnvVar != nil {
			password = auconfigenv.Get(*rawConfig.PasswordEnvVar)
			if password == "" {
				return nil, fmt.Errorf("kafka topic %s password environment variable %s is empty", rawConfig.Topic, *rawConfig.PasswordEnvVar)
			}
		} else if rawConfig.Password != nil {
			password = auconfigenv.Get(*rawConfig.PasswordEnvVar)
			if password == "" {
				return nil, fmt.Errorf("kafka topic %s password value is empty", rawConfig.Topic)
			}
		} else {
			return nil, fmt.Errorf("kafka topic %s neither password environment variable or password value is set", rawConfig.Topic)
		}

		topicConfigs[key] = TopicConfig{
			Topic:         rawConfig.Topic,
			Brokers:       rawConfig.Brokers,
			Username:      rawConfig.Username,
			Password:      password,
			ConsumerGroup: rawConfig.ConsumerGroup,
			AuthType:      sarama.SASLMechanism(rawConfig.AuthType),
		}
	}
	return topicConfigs, nil
}
