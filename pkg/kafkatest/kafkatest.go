package kafkatest

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/Pedro-Magalhaes/async-microservice-test/pkg/topic"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	OffsetEarliest = "earliest"
	OffsetLatest   = "latest"
)

type KafkaHelper struct {
	server         string
	kConfig        kafka.ConfigMap
	kafkaProducer  *kafka.Producer
	kafkaConsumer  *kafka.Consumer
	kafkaAdmClient *kafka.AdminClient
	// Colocar array para mensagens de cada tópico? O que ocorreria em um topico muito movimentado
}

func NewKafka(kConfig kafka.ConfigMap) (*KafkaHelper, error) {
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
	k := KafkaHelper{
		server:  server,
		kConfig: kConfig,
	}
	k.kafkaAdmClient, err = kafka.NewAdminClient(&kConfig)
	if err != nil {
		log.Println("Erro criando adm do kafka")
		return nil, err
	}

	k.kafkaProducer, err = kafka.NewProducer(&kConfig)
	if err != nil {
		log.Println("Erro criando producer do kafka")
		return nil, err
	}

	return &k, nil
}

func (k *KafkaHelper) NewConsumer(groupId, offsetConfig string) (*kafka.Consumer, error) {
	consumerConfig := cloneConfigMap(k.kConfig)
	consumerConfig.SetKey("group.id", groupId)
	consumerConfig.SetKey("auto.offset.reset", offsetConfig)
	// auto.offset.reset
	return kafka.NewConsumer(&consumerConfig)
}

func (k *KafkaHelper) Produce(topic, msg string, confirmDelivery chan kafka.Event) error {
	return k.kafkaProducer.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic}, Value: []byte(msg)}, confirmDelivery)
}

func (k KafkaHelper) CreateTopics(t *topic.TopicConfig) {
	var topicNames []string = make([]string, len(t.Topics))
	var specifications []kafka.TopicSpecification = make([]kafka.TopicSpecification, len(t.Topics))
	for i, v := range t.Topics {
		topicNames[i] = v.Name
		specifications[i] = kafka.TopicSpecification{
			Topic:             v.Name,
			NumPartitions:     v.NumPartitions,
			ReplicationFactor: 1,
		}
	}
	var _, e = k.kafkaAdmClient.DeleteTopics(context.Background(), topicNames)
	if e != nil {
		log.Println("Não foi possivel deletar os topicos")
		log.Println(topicNames)
		panic(e)
	}

	// TODO: como aguardar a deleção dos tópicos?
	time.Sleep(time.Second * 1)

	var topics, err = k.kafkaAdmClient.CreateTopics(context.Background(), specifications)
	if err != nil {
		log.Println("Não foi possivel criar os tópicos")
		log.Println(topicNames)
		panic(err)
	}

	log.Printf("topics: %v\n", topics)

	// TODO: como ver se o tópico está pronto para receber mensagens?
	time.Sleep(time.Second * 2)

	for _, v := range t.Topics {
		if len(v.Messages) > 0 {
			log.Println("Deveria colocar as mensagens: ")
			log.Println(v.Messages)
		}
		deliveryChan := make(chan kafka.Event)
		for _, m := range v.Messages {
			err := k.kafkaProducer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &v.Name, Partition: int32(m.Partition)},
				Value:          []byte(m.Message),
			}, deliveryChan)
			if err != nil {
				panic(err)
			}
			<-deliveryChan
		}
	}
}

func defaultServer() string {
	return "localhost:9092"
}

func cloneConfigMap(original kafka.ConfigMap) kafka.ConfigMap {
	m2 := make(kafka.ConfigMap)
	for k, v := range original {
		m2[k] = v
	}
	return m2
}
