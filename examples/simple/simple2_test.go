package simple

import (
	"context"
	"testing"
	"time"

	"github.com/Pedro-Magalhaes/async-microservice-test/pkg/kafkatest"
	"github.com/Pedro-Magalhaes/async-microservice-test/pkg/stage"
	"github.com/Pedro-Magalhaes/async-microservice-test/pkg/topic"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func SampleTestEmptyMsgOdj(t *testing.T) {
	topicMessages := make(map[string][]string)
	stages := stage.CreateStages()
	st1 := stage.CreateStage("Inicializa serviço sob teste")
	st2 := stage.CreateStage("Envia mensagem mal formatada ao kafka")
	st3 := stage.CreateStage("Verifica se o tópico demanda-falhada recebeu uma mensagem")

	// inicializa helper do kafka
	k, _ := kafkatest.NewKafka(kafka.ConfigMap{"bootstrap.servers": "localhost:9092",
		"acks": "all"})
	// Cria os tópicos vazios
	k.CreateTopics(&topic.TopicConfig{Topics: []topic.Topics{
		{Name: "demanda-submetida", NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: "resposta-complementacao-judicial", NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: "demanda-falhada", NumPartitions: 1, Messages: []topic.Messages{}},
	}})
	// Cria o consumidor e subscreve aos tópiocs de interesse
	consumer, _ := k.NewConsumer("empty-msg-test-group", kafkatest.OffsetEarliest)
	_ = consumer.SubscribeTopics([]string{"demanda-submetida", "demanda-falhada"}, nil)

	// Inicia o serviço de complementação e aguarda resposta do health check para continuar, falha em caso de timeout
	st1.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		_, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:      "repo.tecgraf.puc-rio.br:18089/odrtj/odr-complementacao-tj:master",
				WaitingFor: wait.ForHTTP("/q/health/live").WithPort("8080/tcp").WithStartupTimeout(time.Second * 10),
				Env:        map[string]string{"ODR_KAFKA_HOST": "localhost:9092"},
			},
			Started: true,
		})
		if err != nil {
			t.Fatal(err)
		}
	}, nil)

	// Job multi estágio que checa periodicamente os tópicos subscritos
	st1.AddJobMultiStage(func(dcag stage.DoneCancelArgGet) {
		ticker := time.NewTicker(time.Millisecond * 100)
		defer ticker.Stop()
		for {
			select {
			case <-dcag.Canceled(): // cancelado ao final do estagio st3
				return
			case <-ticker.C:
				// faz poll no nos tópicos e caso receba mensagem adiciona ao mapa 'topicMessages'
				checkMessages(consumer, topicMessages, t)
			}
		}
	}, st3, nil)

	// produz uma mensagem mal formatada para o tópico de entrada do serviço
	st2.AddJob(func(dcag stage.DoneCancelArgGet) {
		produceChan := make(chan kafka.Event)
		_ = k.Produce("demanda-submetida", "{}", produceChan)
		// aguarda confirmação da entrega da mensagem, falha se der timeout
		select {
		case <-time.After(5 * time.Second):
			t.Fatal("Não recebeu confirmação de mensagem enviada")
		case <-produceChan:
			dcag.Done()
		}

	}, nil)

	// Job para verificar a cada 500ms se que o tópico demanda-falhada recebeu uma nova mensagem
	// código omitido para simplificar
	st3.AddJob(func(dcag stage.DoneCancelArgGet) {
		// Verifica se recebeu mensagem a cada 500ms com timeout de 5 segundos
		// Em caso de timeout falha
	}, nil)

	stages.AddStages([]*stage.Stage{st1, st2, st3})
	stages.Run()
}
