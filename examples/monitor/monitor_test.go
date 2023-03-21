package monitor

import (
	"context"
	"fmt"
	"io"
	"log"
	"testing"
	"time"

	"github.com/Pedro-Magalhaes/async-microservice-test/pkg/dockertest"
	"github.com/Pedro-Magalhaes/async-microservice-test/pkg/kafkatest"
	"github.com/Pedro-Magalhaes/async-microservice-test/pkg/stage"
	"github.com/Pedro-Magalhaes/async-microservice-test/pkg/topic"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	targetFolder     = "/go/src/app/test_files"
	oneLineFileName  = "file_with_one_line.txt"
	oneLineFile      = targetFolder + "/" + oneLineFileName
	oneLineFileTopic = "__go__src__app__test_files__file_with_one_line.txt"
)

var (
	tConfig = topic.TopicConfig{Topics: []topic.Topics{
		{Name: "monitor_interesse", NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: oneLineFileTopic, NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: "job_info", NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: "monitor_estado", NumPartitions: 1, Messages: []topic.Messages{
			{Partition: 0, Message: "[]", Key: ""}, // estado "vazio"
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
)

// Reproduz bug do monitor que não envia msg até que tenha ocorrido mudança no arquivo
func TestFileWithInitialContent(t *testing.T) {
	var containerMonitor testcontainers.Container
	stages := stage.CreateStages()
	st := stage.CreateStage("inicia_container")
	st2 := stage.CreateStage("envia msg e aguarda o monitor receber")
	st3 := stage.CreateStage("aguarda msg no topico do arquivo")

	const monitorReadyLogMsg = "TIME SPENT WAITING FOR STATE RECOVERY"
	r := testcontainers.ContainerRequest{
		Image:      "monitor:0.0.1-snapshot",
		WaitingFor: wait.ForLog(monitorReadyLogMsg).WithOccurrence(1).WithStartupTimeout(time.Second * 10),
		Mounts:     monitorMounts,
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.NetworkMode = "host"
		},
	}

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
			ContainerRequest: r,
			Started:          true,
		})
		if err != nil {
			t.Fatal(err)
		}
	}, nil)

	st2.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		ch := make(chan kafka.Event)
		k.Produce("monitor_interesse", fmt.Sprintf(`{ "path": "%s", "project": "p1", "watch": true}`, oneLineFile), ch)
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

	// INIT DEBUG
	t.Log("\n\n*******DEBUG*******\n\n") // DEBUG
	read, err := containerMonitor.Logs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	text, err := io.ReadAll(read)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(text)) // DEBUG
}
