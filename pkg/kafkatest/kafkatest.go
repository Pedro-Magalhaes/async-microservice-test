package kafkatest

import (
	"errors"
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Kafka struct {
	server         string
	kafkaProducer  *kafka.Producer
	kafkaConsumer  *kafka.Consumer
	kafkaAdmClient *kafka.AdminClient
	// Colocar array para mensagens de cada tópico? O que ocorreria em um topico muito movimentado
}

func NewKafka(kConfig kafka.ConfigMap) (*Kafka, error) {
	s, err := kConfig.Get("bootstrap.servers", "")
	if err != nil {
		return nil, err
	}
	server, ok := s.(string)

	if !ok {
		return nil, errors.New("incorrect config, check the bootstrap.servers atribute")
	}

	if strings.Compare(server, "") == 0 {
		server = defaultServer()
	}
	k := Kafka{
		server: server,
	}
	k.kafkaAdmClient, err = kafka.NewAdminClient(&kConfig)
	if err != nil {
		fmt.Println("Erro criando adm do kafka")
		return nil, err
	}

	k.kafkaProducer, err = kafka.NewProducer(&kConfig)
	if err != nil {
		fmt.Println("Erro criando producer do kafka")
		return nil, err
	}
	return &k, nil
}

func defaultServer() string {
	return "localhost:9092"
}
