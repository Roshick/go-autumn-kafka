package kafka

import "github.com/IBM/sarama"

func CreateAzureEventHubConfig() *sarama.Config {
	config := sarama.NewConfig()

	config.Net.TLS.Enable = true
	config.Net.KeepAlive = 180000

	config.Producer.Compression = sarama.CompressionNone
	config.Producer.Timeout = 60000
	config.Producer.MaxMessageBytes = 1000000

	return config
}
