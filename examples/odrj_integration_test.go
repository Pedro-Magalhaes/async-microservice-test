package examples

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/Pedro-Magalhaes/async-microservice-test/pkg/stage"
	"github.com/Pedro-Magalhaes/async-microservice-test/pkg/topic"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	docker "github.com/fsouza/go-dockerclient"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type FileChunkMsg struct {
	Msg    string `json:"msg"`
	Offset int64  `json:"offset"`
	Lenth  int    `json:"lenth"`
}

var client *docker.Client
var containers []*docker.Container
var err error
var kafkaAdmClient *kafka.AdminClient
var kafkaProducer *kafka.Producer
var kafkaConsumer *kafka.Consumer
var messageMutex sync.Mutex
var kafkaMessages map[string][]string
var linhasArquivo1 []string

func internalSetup() {
	// AdmClient para criar topicos
	kafkaAdmClient, err = kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092",
		"acks": "all"})
	if err != nil {
		fmt.Println("Erro criando adm do kafka")
		panic(err)
	}
	// Producer para mandar mensagens
	kafkaProducer, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092",
		"acks": "all"})
	if err != nil {
		fmt.Println("Erro criando producer do kafka")
		panic(err)
	}

	// Consumer do kafka
	kafkaConsumer, err = kafka.NewConsumer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092", "group.id": "consumer-teste-id",
		"acks": "all"})
	if err != nil {
		fmt.Println("Erro criando consumer do kafka")
		panic(err)
	}
	// Docker client para executar imagens
	client, err = docker.NewClientFromEnv()
	if err != nil {
		fmt.Println("Erro criado cliente do kafka")
		panic(err)
	}
	kafkaMessages = make(map[string][]string)
	linhasArquivo1 = []string{"Primeira linha\n", "Segunda linha\n", "Ultima linha"}
}

func setup() {
	var t, e = topic.LoadTopicConfig("topics.json")
	if e != nil {
		fmt.Println("Erro ao importar arquivo json de topicos")
		panic(e)
	}
	createTopics(t)
}

// Publica uma mensagem em determinado topico
func addKafkaMessage(topic, message string) {
	messageMutex.Lock()
	if kafkaMessages[topic] == nil {
		kafkaMessages[topic] = make([]string, 0)
	}
	kafkaMessages[topic] = append(kafkaMessages[topic], message)
	messageMutex.Unlock()
}

// Pega a última mensagem de um tópico monitorado
func getKafkaLastMessage(topic string) string {
	return getKafkaMessage(topic, len(kafkaMessages[topic]))
}

// Pega uma mensagem de um tópico monitorado pela posição
func getKafkaMessage(topic string, pos int) string {
	messageMutex.Lock()
	message := kafkaMessages[topic][pos]
	messageMutex.Unlock()
	return message
}

// Pega todas as mensagens recebidas em um determinado tópico
func getKafkaMessagesArray(topic string) []string {
	var messages []string
	messageMutex.Lock()
	if kafkaMessages[topic] != nil {
		messages = kafkaMessages[topic]

	}
	messageMutex.Unlock()
	return messages
}

func handleMessages() {
	// Process messages
	for {
		ev, err := kafkaConsumer.ReadMessage(100 * time.Millisecond)
		if err != nil {
			// Errors are informational and automatically handled by the consumer
			continue
		}
		// *&ev.TopicPartition.Offset
		// vou ignorar as keys por enquanto
		fmt.Printf("Consumed event from topic %s: key = %-10s value = %s\n",
			*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))

		addKafkaMessage(*ev.TopicPartition.Topic, string(ev.Value))
	}
}

func startConsumer(topics []string) {
	println("Iniciando consumer com os seguintes topicos: ", topics)
	err := kafkaConsumer.SubscribeTopics(topics, nil)
	if err != nil {
		fmt.Println("Erro subscrevendo aos topicos")
		panic(err)
	}
	go handleMessages()
}

func createTopics(t *topic.TopicConfig) {
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
	var _, e = kafkaAdmClient.DeleteTopics(context.Background(), topicNames)
	if e != nil {
		fmt.Println("Não foi possivel deletar os topicos")
		fmt.Println(topicNames)
		panic(e)
	}

	// TODO: como aguardar a deleção dos tópicos?
	time.Sleep(time.Second * 1)

	var topics, err = kafkaAdmClient.CreateTopics(context.Background(), specifications)
	if err != nil {
		fmt.Println("Não foi possivel criar os tópicos")
		fmt.Println(topicNames)
		panic(err)
	}

	fmt.Printf("topics: %v\n", topics)

	// TODO: como ver se o tópico está pronto para receber mensagens?
	time.Sleep(time.Second * 1)

	for _, v := range t.Topics {
		if len(v.Messages) > 0 {
			fmt.Println("Deveria colocar as mensagens: ")
			fmt.Println(v.Messages)
		}
		for _, m := range v.Messages {
			kafkaProducer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &v.Name, Partition: int32(m.Partition)},
				Value:          []byte(m.Message),
			}, nil)
		}
	}

}

func teardown() {
	// for _, container := range containers {
	// 	if container != nil {
	// 		fmt.Printf("removendo container: %v\n", container.ID)
	// 		remove(client, container)
	// 	} else {
	// 		fmt.Printf("container nulo: %v\n", container)
	// 	}
	// }

	if kafkaAdmClient != nil {
		println("removendo kafka Adm")
		kafkaAdmClient.Close()
	}
	if kafkaProducer != nil {
		println("removendo kafka Producer")
		kafkaProducer.Close()
	}
	if kafkaConsumer != nil {
		println("removendo kafka Consumer")
		kafkaConsumer.Close()
	}

}

func TestIncorrectMessage(t *testing.T) {
	wasTimeout := false
	// topics := []string{"resposta-complementacao-judicial"}
	l := log.Default()
	l.Println("Iniciando testes")
	// defer teardown()
	// internalSetup()
	// setup()
	// startConsumer(topics)
	ctx := context.Background()
	stages := stage.CreateStages()
	st1 := stage.CreateStage("estágio 0")
	var container testcontainers.Container
	st1.AddJob(func(c *chan bool) {
		l.Println("st1j0")
		req := testcontainers.ContainerRequest{
			Image:        "a863582a7457",
			ExposedPorts: []string{"8080/tcp"},
			WaitingFor: wait.ForAll(wait.ForHTTP("/q/health/live").WithPort("8080/tcp").WithStatusCodeMatcher(func(status int) bool {
				if status <= 299 {
					l.Println("Heath check ok ", status)
					wasTimeout = true
					return true
				} else {
					l.Println("Status not healthy ", status)
				}
				return false
			})).WithDeadline(6 * time.Second),
		}
		if wasTimeout {
			l.Println("HeathCheck timeout")
		}
		container, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			t.Fatal(err)
		}
		l.Println("Terminando j0", container)
		*c <- true
	})

	st1.AddJob(func(c *chan bool) {

		req := testcontainers.ContainerRequest{
			Image:        "a863582a7457",
			ExposedPorts: []string{"8080/tcp"},
			WaitingFor: wait.ForAll(wait.ForHTTP("/q/health/live").WithPort("8080/tcp").WithStatusCodeMatcher(func(status int) bool {
				if status <= 299 {
					l.Println("Heath check ok ", status)
					wasTimeout = true
					return true
				} else {
					l.Println("Status not healthy ", status)
				}
				return false
			})).WithDeadline(6 * time.Second),
		}
		// req.
		l.Println("st1 j1", req)
		*c <- true

	})
	l.Println("Adding stage")
	// stages.AddStages([]*stage.Stage{st1})
	stages.AddStage(st1)
	l.Println("Starting job 1")
	stages.Run()
	stopTimeout := time.Second * 2
	l.Println("Stoping container")
	container.Stop(ctx, &stopTimeout)

}
