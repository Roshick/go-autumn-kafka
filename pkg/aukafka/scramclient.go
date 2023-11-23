package aukafka

import (
	"crypto/sha256"
	"crypto/sha512"
	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func NewSha256ScramClient() sarama.SCRAMClient {
	return &XDGSCRAMClient{HashGeneratorFcn: sha256.New}
}

func NewSha512ScramClient() sarama.SCRAMClient {
	return &XDGSCRAMClient{HashGeneratorFcn: sha512.New}
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}
