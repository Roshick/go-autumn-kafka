package aukafka

import "github.com/IBM/sarama"

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
