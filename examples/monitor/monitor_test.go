package monitor

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/Pedro-Magalhaes/async-microservice-test/pkg/dockertest"
	"github.com/Pedro-Magalhaes/async-microservice-test/pkg/kafkatest"
	"github.com/Pedro-Magalhaes/async-microservice-test/pkg/kafkatest/topic"
	"github.com/Pedro-Magalhaes/async-microservice-test/pkg/stage"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	targetFolder     = "/go/src/app/test_files"
	oneLineFileName  = "file_with_one_line.txt"
	monitorReadyLogMsg = "Iniciando espera por mensagens"
	monitorRecoveredStateLogMsg = "TIME SPENT WAITING FOR STATE RECOVERY"
)

var (
	oneLineFileTopic = "test_files__" + oneLineFileName
	tConfig = topic.TopicConfig{Topics: []topic.Topics{
		{Name: "monitor_interesse", NumPartitions: 2, Messages: []topic.Messages{}},
		{Name: oneLineFileTopic, NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: "job_info", NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: "monitor_estado", NumPartitions: 2, Messages: []topic.Messages{
			{Partition: 0, Message: "[]", Key: ""}, // estado "vazio"
			{Partition: 1, Message: "[]", Key: ""}, // estado "vazio"
		}},
	}}
	monitorMounts = testcontainers.Mounts(
		testcontainers.ContainerMount{
			Source: testcontainers.GenericBindMountSource{HostPath: "/local/ProgramasLocais/Documents/pessoal/Puc/mestrado/async-microservice-test/examples/monitor/files"},
			Target: testcontainers.ContainerMountTarget(targetFolder),
		},
		testcontainers.ContainerMount{
			Source: testcontainers.GenericBindMountSource{HostPath: "/local/ProgramasLocais/Documents/pessoal/Puc/mestrado/async-microservice-test/examples/monitor/config-monitor.json"},
			Target: "/go/src/app/config.json",
		},
	)
	monitorCRequest = testcontainers.ContainerRequest{
		Image:      "monitor:0.0.1-snapshot",
		WaitingFor: wait.ForLog(monitorRecoveredStateLogMsg).WithOccurrence(1).WithStartupTimeout(time.Second * 10),
		Mounts:     monitorMounts,
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.NetworkMode = "host"
		},
	}
)

// Reproduz bug do monitor que não envia msg até que tenha ocorrido mudança no arquivo
func TestFileWithInitialContent(t *testing.T) {
	var containerMonitor testcontainers.Container
	stages := stage.CreateStages()
	st := stage.CreateStage("inicia_container")
	st2 := stage.CreateStage("envia msg e aguarda o monitor receber")
	st3 := stage.CreateStage("aguarda msg no topico do arquivo")

	// inicializa helper do kafka
	k, err := kafkatest.NewKafka(kafka.ConfigMap{"bootstrap.servers": "localhost:9092",
		"acks": "all"})
	if err != nil {
		t.Fatal(err)
	}
	// Cria os tópicos
	k.CreateTopics(&tConfig)
	// Cria o consumidor e subscreve ao tópico de interesse
	consumer, err := k.NewConsumer("test-group", kafkatest.OffsetEarliest)
	if err != nil {
		t.Fatal(err)
	}
	err = consumer.SubscribeTopics([]string{oneLineFileTopic}, nil)
	if err != nil {
		t.Fatal(err)
	}

	st.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		var err error
		containerMonitor, err = testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
			ContainerRequest: monitorCRequest,
			Started:          true,
		})
		if err != nil {
			t.Fatal(err)
		}
	}, nil)

	st2.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		ch := make(chan kafka.Event)
		k.Produce("monitor_interesse", fmt.Sprintf(`{ "path": "%s", "project": "p1", "watch": true}`, oneLineFileName), ch)
		<-ch // Aguarda confirmação de entrega
	}, nil)

	st2.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		log.Println("Waiting for the monitor to recieve the msg")
		err := dockertest.WaitForLogMessage(oneLineFileName, 1, time.Second*3, containerMonitor)
		if err != nil {
			t.Fatal(err) // timeout
		}
		log.Println("Monitor got the msg")
	}, nil)

	st3.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		const timeout = time.Second * 5
		m, err := consumer.ReadMessage(timeout)
		if err != nil {
			t.Fatal(err) // timeout
		}
		assert.NotEmpty(t, m)
	}, nil)

	stages.AddStages([]*stage.Stage{st, st2, st3})
	stages.Run()
}


// Teste com 2 réplicas do monitor
func TestReplicas(t *testing.T) {
	var containerMonitor, containerMonitor2 testcontainers.Container
	stages := stage.CreateStages()
	st := stage.CreateStage("inicia_container")
	st2 := stage.CreateStage("envia msg e aguarda o monitor receber")
	st3 := stage.CreateStage("aguarda msg no topico do arquivo")

	monitorCRequest.WaitingFor = wait.ForLog(monitorRecoveredStateLogMsg).WithOccurrence(1).WithStartupTimeout(time.Second * 10)

	// inicializa helper do kafka
	k, err := kafkatest.NewKafka(kafka.ConfigMap{"bootstrap.servers": "localhost:9092",
		"acks": "all"})
	if err != nil {
		t.Fatal(err)
	}
	// Cria os tópicos
	
	k.CreateTopics(&tConfig)
	// Cria o consumidor e subscreve ao tópico de interesse
	consumer, err := k.NewConsumer("test-group", kafkatest.OffsetEarliest)
	if err != nil {
		t.Fatal(err)
	}
	err = consumer.SubscribeTopics([]string{oneLineFileTopic}, nil)
	if err != nil {
		t.Fatal(err)
	}

	st.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		var err error
		containerMonitor, err = testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
			ContainerRequest: monitorCRequest,
			Started:          true,
		})
		if err != nil {
			t.Fatal(err)
		}
	}, nil)

	st.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		var err error
		containerMonitor2, err = testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
			ContainerRequest: monitorCRequest,
			Started:          true,
		})
		if err != nil {
			t.Fatal(err)
		}
	}, nil)

	st2.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		ch := make(chan kafka.Event)
		k.Produce("monitor_interesse", fmt.Sprintf(`{ "path": "%s", "project": "p1", "watch": true}`, oneLineFileName), ch)
		<-ch // Aguarda confirmação de entrega
	}, nil)

	var monitorInCharge testcontainers.Container

	st2.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		log.Println("Waiting for any monitor to recive the file monitoring msg")
		ct, err := dockertest.WaitForLogMessageForAny(
			oneLineFileName,
			1,
			time.Second*20,
			[]testcontainers.Container{containerMonitor, containerMonitor2},
		)
		if err != nil {
			t.Fatal(err) // timeout
		}
		log.Println("Monitor got the msg", ct.GetContainerID())
		monitorInCharge = ct // o monitor que recebeu a mensagem é o que vai observar o arquivo
	}, nil)

	st3.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		// aguarda o monitor que ficou responsavel pelo arquivo iniciar o watcher
		dockertest.WaitForLogMessage("Iniciando loop do watcher", 1, time.Second * 10, monitorInCharge)
		mySt := "uma escrita no arquivo \n com três linhas \n fim!"
		e := os.WriteFile("./files/" + oneLineFileName, []byte(mySt), 0777)
		if e != nil {
			t.Fatal(e)
		}
		const timeout = time.Second * 10
		msg, err := consumer.ReadMessage(timeout) // aguarda mensagem no tópico de 
		if err != nil {
			t.Fatal(err) // timeout
		}
		assert.NotEmpty(t, msg.Value) // teste simples para ver que não é uma msg vazia
	}, nil)

	stages.AddStages([]*stage.Stage{st, st2, st3})
	stages.Run()
}