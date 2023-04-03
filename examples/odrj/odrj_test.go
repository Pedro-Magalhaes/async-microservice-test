package odrj

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
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
	entryTopic = "demanda-submetida"
	errorTopic = "demanda-falhada"
	outTopic   = "resposta-complementacao-judicial"
	odrImage = "repo.tecgraf.puc-rio.br:18089/odrtj/odr-complementacao-tj:master"
	defaultOdrPort = 8888
	defaultGelfPort = 12201
)

var portOffset = 0

func getRandTopicName(baseName, testName string) string {
	return fmt.Sprintf("%s-%s-%d", baseName, testName, rand.Uint32())
}

func getNewContainerDefinition(mockServerUrl, entryTopic string) map[string]string {
	cDef := map[string]string{
		"ODR_KAFKA_HOST":                 "localhost:9092",
		"ODR_TJRJ_PROCESSOS_RETRY_DELAY": "100",
		"ODR_TJRJ_PROCESSOS_TIMEOUT":     "1000",
		"ODR_HTTP_PORT":                  fmt.Sprint(defaultOdrPort + portOffset),
		"ODR_GELF_PORT":                  fmt.Sprint(defaultGelfPort + portOffset),
		"ODR_TJRJ_PROCESSOS_URI":         mockServerUrl,
		"ODR_TJRJ_OIDC_URI":              mockServerUrl + "/auth/realms/homologacao",
		"ODR_TJRJ_OIDC_CLIENT_ID":        "odr",
		"ODR_TJRJ_OIDC_CLIENT_SECRET":    "66e56811-fff0-4ed6-9132-bd96981f276f",
		"ODR_TJRJ_OIDC_USERNAME":         "api-reincidencia",
		"ODR_TJRJ_OIDC_PASSWORD":         "1234",
		"ODR_KAFKA_IN_TOPIC": 			  entryTopic,
	}
	portOffset++
	return cDef
}

func checkMessages(kafkaConsumer *kafka.Consumer, messageMap map[string][]string, t *testing.T) {
	// Process messages
	ev, err := kafkaConsumer.ReadMessage(100 * time.Millisecond)
	if err != nil {
		// Errors are informational and automatically handled by the consumer
		return
	}
	
	t.Logf("Consumed event from topic %s: key = %-10s value = %s\n",
		*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
	// adiciona ao mapa de mensagens
	messageMap[*ev.TopicPartition.Topic] = append(messageMap[*ev.TopicPartition.Topic], string(ev.Value))
}

func createFakeServer(authStatus, tjStatus int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if strings.Contains(r.URL.Path, "auth") { // requisição token
			w.WriteHeader(authStatus)
			w.Write([]byte(`{"access_token": "66e56811-fff0-4ed6-9132-bd96981f276f"}`))
			return
		}
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(tjStatus)
		w.Write([]byte("[\"395566265.2019.8.19.0206\", \"343039222.2021.81.9.0208\", \"527925574.2022.81.9.0029\"]"))
	}))
}

func TestWrongMsgFormat(t *testing.T) {
	var containerOdj testcontainers.Container
	stages := stage.CreateStages()
	st := stage.CreateStage("Primeiro")
	st2 := stage.CreateStage("Segundo")
	st3 := stage.CreateStage("Terceiro")

	mockTJsvr := createFakeServer(200,200)
	defer mockTJsvr.Close()

	env := getNewContainerDefinition(mockTJsvr.URL, entryTopic)
	r := testcontainers.ContainerRequest{
		Image: odrImage,
		WaitingFor: wait.ForLog("Profile prod activated").WithPollInterval(time.Second / 2).WithStartupTimeout(time.Second * 10),
		Env: env,
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.NetworkMode = "host"
		},
	}

	k, err := kafkatest.NewKafka(kafka.ConfigMap{"bootstrap.servers": "localhost:9092",
		"acks": "all"})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	k.CreateTopics(&topic.TopicConfig{Topics: []topic.Topics{
		{Name: entryTopic, NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: outTopic, NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: errorTopic, NumPartitions: 1, Messages: []topic.Messages{}},
	}})
	defer k.DeleteTopics([]string{entryTopic, outTopic, errorTopic})

	// Inicia o serviço de complementação e aguarda resposta do health check para terminar
	st.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		var err error
		log.Println("Inicio job inicia docker")
		containerOdj, err = testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
			ContainerRequest: r,
			Started:          true,
		})
		if err != nil {
			t.Fatal(err)
		}
		log.Println("Fim job inicia docker", containerOdj.IsRunning())
	}, nil)

	st2.AddJob(func(dcag stage.DoneCancelArgGet) {
		produceChan := make(chan kafka.Event)
		time.Sleep(time.Second * 10) // Health check da complementação fica 200 antes de estar pronto pra consumir

		err = k.Produce(entryTopic, `{"idDemanda": "234"}`, produceChan)

		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-time.After(5 * time.Second):
			t.Fatal("Não recebeu confirmação de mensagem")
		case <-produceChan:
			dcag.Done()
		}

	}, nil)

	// Job para verificar que o tópico demanda-falhada recebeu mensagem
	st3.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		time.Sleep(time.Second * 10) // espera a recuperação de erro

		assert.Equal(t, true, containerOdj.IsRunning())

		r, err := http.Get("http://localhost:" + env["ODR_HTTP_PORT"] + "/q/health/live")
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 200, r.StatusCode)
	}, nil)
	stages.AddStages([]*stage.Stage{st, st2, st3})
	stages.Run()
	if containerOdj != nil {
		containerOdj.Terminate(context.Background())
	}
	// DEBUG
	// t.Log(dockertest.GetAllLog(containerOdj))
}

func TestEmptyMsgFormat(t *testing.T) {
	var containerOdj testcontainers.Container
	stages := stage.CreateStages()
	st := stage.CreateStage("Primeiro")
	st2 := stage.CreateStage("Segundo")
	st3 := stage.CreateStage("Terceiro")

	mockTJsvr := createFakeServer(200,200)
	defer mockTJsvr.Close()

	env := getNewContainerDefinition(mockTJsvr.URL, entryTopic)
	r := testcontainers.ContainerRequest{
		Image: odrImage,
		WaitingFor: wait.ForLog("Profile prod activated").WithPollInterval(time.Second / 2).WithStartupTimeout(time.Second * 10),
		Env: env,
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.NetworkMode = "host"
		},
	}
	
	k, err := kafkatest.NewKafka(kafka.ConfigMap{"bootstrap.servers": "localhost:9092",
		"acks": "all"})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	k.CreateTopics(&topic.TopicConfig{Topics: []topic.Topics{
		{Name: entryTopic, NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: outTopic, NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: errorTopic, NumPartitions: 1, Messages: []topic.Messages{}},
	}})
	defer k.DeleteTopics([]string{entryTopic, outTopic, errorTopic})

	// Inicia o serviço de complementação e aguarda resposta do health check para terminar
	st.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		var err error
		log.Println("Inicio job inicia docker")
		containerOdj, err = testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
			ContainerRequest: r,
			Started:          true,
		})
		if err != nil {
			t.Fatal(err)
		}
		log.Println("Fim job inicia docker", containerOdj.IsRunning())
	}, nil)

	st2.AddJob(func(dcag stage.DoneCancelArgGet) {
		produceChan := make(chan kafka.Event)
		time.Sleep(time.Second * 10) // Health check da complementação fica 200 antes de estar pronto pra consumir

		err = k.Produce(entryTopic, `{}`, produceChan)

		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-time.After(5 * time.Second):
			t.Fatal("Não recebeu confirmação de mensagem")
		case <-produceChan:
			dcag.Done()
		}

	}, nil)

	// Job para verificar que o tópico demanda-falhada recebeu mensagem
	st3.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		time.Sleep(time.Second * 10)

		assert.Equal(t, true, containerOdj.IsRunning())

		r, err := http.Get("http://localhost:" + env["ODR_HTTP_PORT"] + "/q/health/live")
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 200, r.StatusCode)
	}, nil)
	stages.AddStages([]*stage.Stage{st, st2, st3})
	stages.Run()
	if containerOdj != nil {
		containerOdj.Terminate(context.Background())
	}
	// t.Log(dockertest.GetAllLog(containerOdj)) //DEBUG
}

func TestFailHttpOdj(t *testing.T) {
	var containerOdj testcontainers.Container
	topicMessages := make(map[string][]string)
	stages := stage.CreateStages()
	st := stage.CreateStage("Primeiro")
	st2 := stage.CreateStage("Segundo")
	st3 := stage.CreateStage("Terceiro")

	mockTJsvr := createFakeServer(200,500)
	defer mockTJsvr.Close()
	env := getNewContainerDefinition(mockTJsvr.URL, entryTopic)
	r := testcontainers.ContainerRequest{
		Image: odrImage,
		WaitingFor: wait.ForLog("Profile prod activated").WithPollInterval(time.Second / 2).WithStartupTimeout(time.Second * 10),
		Env: env,
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.NetworkMode = "host"
		},
	}
	k, err := kafkatest.NewKafka(kafka.ConfigMap{"bootstrap.servers": "localhost:9092",
		"acks": "all"})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	k.CreateTopics(&topic.TopicConfig{Topics: []topic.Topics{
		{Name: entryTopic, NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: outTopic, NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: errorTopic, NumPartitions: 1, Messages: []topic.Messages{}},
	}})
	defer k.DeleteTopics([]string{entryTopic, outTopic, errorTopic})

	consumer, err := k.NewConsumer("test-group", kafkatest.OffsetEarliest)
	if err != nil {
		t.Fatal(err)
	}

	err = consumer.SubscribeTopics([]string{entryTopic, errorTopic}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Inicia o serviço de complementação e aguarda resposta do health check para terminar
	st.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		var err error
		log.Println("Inicio job inicia docker")
		containerOdj, err = testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
			ContainerRequest: r,
			Started:          true,
		})
		if err != nil {
			t.Fatal(err)
		}
		log.Println("Fim job inicia docker", containerOdj.IsRunning())
	}, nil)

	// Job multi estágio que vai ficar periodicamente checando mensagens nos tópicos subscritos
	st.AddJobMultiStage(func(dcag stage.DoneCancelArgGet) {
		ticker := time.NewTicker(time.Millisecond * 100)
		defer ticker.Stop()
		checkMessages(consumer, topicMessages, t)
		for {
			select {
			case <-dcag.Canceled(): // cancelado ao final do estagio st3
				t.Log("Job de checagem de mensagens cancelado")
				return
			case <-ticker.C:
				// t.Log("check message")
				checkMessages(consumer, topicMessages, t)
			}
		}
	}, st3, nil)

	st2.AddJob(func(dcag stage.DoneCancelArgGet) {
		produceChan := make(chan kafka.Event)
		time.Sleep(time.Second * 8) // Health check da complementação fica 200 antes de estar pronto pra consumir

		err = k.Produce(entryTopic, `{"documentoDemandada": 22211133344, "documentoDemandante": 22211133344, "idDemanda": "234"}`, produceChan)

		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-time.After(5 * time.Second):
			t.Fatal("Não recebeu confirmação de mensagem")
		case <-produceChan:
			dcag.Done()
		}

	}, nil)

	// Job para verificar que o tópico demanda-falhada recebeu mensagem
	st3.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		to := time.NewTimer(10 * time.Second)
		defer to.Stop()
		for {
			select {
			case <-to.C: // timeout
				t.Fatal("Não recebeu mensagem no canal de falha")
				return
			case <-ticker.C:
				t.Log("Teste array demanda-falhada")
				if len(topicMessages[errorTopic]) > 0 {
					t.Log(topicMessages[errorTopic][0])
					return
				}
			}
		}
	}, nil)
	stages.AddStages([]*stage.Stage{st, st2, st3})
	stages.Run()
	if containerOdj != nil {
		containerOdj.Terminate(context.Background())
	}
}

func TestCorrectBehaviourOdj(t *testing.T) {
	var containerOdj testcontainers.Container
	topicMessages := make(map[string][]string)
	stages := stage.CreateStages()
	st := stage.CreateStage("Primeiro")
	st2 := stage.CreateStage("Segundo")
	st3 := stage.CreateStage("Terceiro")

	mockTJsvr := createFakeServer(200,200)
	defer mockTJsvr.Close()

	r := testcontainers.ContainerRequest{
		Image:      odrImage,
		WaitingFor: wait.ForLog("Profile prod activated").WithPollInterval(time.Second / 2).WithStartupTimeout(time.Second * 10),
		Env: getNewContainerDefinition(mockTJsvr.URL, entryTopic),
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.NetworkMode = "host"
		},
	}

	k, err := kafkatest.NewKafka(kafka.ConfigMap{"bootstrap.servers": "localhost:9092",
		"acks": "all"})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	k.CreateTopics(&topic.TopicConfig{Topics: []topic.Topics{
		{Name: entryTopic, NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: outTopic, NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: errorTopic, NumPartitions: 1, Messages: []topic.Messages{}},
	}})
	defer k.DeleteTopics([]string{entryTopic, outTopic, errorTopic})

	consumer, err := k.NewConsumer("test-group", kafkatest.OffsetEarliest)
	if err != nil {
		t.Fatal(err)
	}

	err = consumer.SubscribeTopics([]string{entryTopic, errorTopic, outTopic}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Inicia o serviço de complementação e aguarda resposta do health check para terminar
	st.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		var err error
		log.Println("Inicio job inicia docker")
		containerOdj, err = testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
			ContainerRequest: r,
			Started:          true,
		})
		if err != nil {
			t.Fatal(err)
		}
	}, nil)

	// Job multi estágio que vai ficar periodicamente checando mensagens nos tópicos subscritos
	st.AddJobMultiStage(func(dcag stage.DoneCancelArgGet) { // TODO: alterar interface para MultiStage, Done() não é necessário
		ticker := time.NewTicker(time.Millisecond * 100)
		defer ticker.Stop()
		checkMessages(consumer, topicMessages, t)
		for {
			select {
			case <-dcag.Canceled(): // cancelado ao final do estagio st3
				t.Log("Job de checagem de mensagens cancelado")
				return
			case <-ticker.C:
				checkMessages(consumer, topicMessages, t)
			}
		}
	}, st3, nil)

	st2.AddJob(func(dcag stage.DoneCancelArgGet) {
		produceChan := make(chan kafka.Event)
		time.Sleep(time.Second * 8)
		err = k.Produce(entryTopic, `{"documentoDemandada": 12211133344, "documentoDemandante": 12211133344, "idDemanda": "134"}`, produceChan)
		err = k.Produce(entryTopic, `{"documentoDemandada": 22222222222, "documentoDemandante": 22211133344, "idDemanda": "234"}`, produceChan)
		err = k.Produce(entryTopic, `{"documentoDemandada": 32233333333, "documentoDemandante": 32211133344, "idDemanda": "334"}`, produceChan)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			select {
			case <-time.After(5 * time.Second):
				t.Fatal("Não recebeu confirmação de mensagem")
			case <-produceChan:
				if i == 2 {
					dcag.Done()
				}
			}
		}

	}, nil)
	st2.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		err := dockertest.WaitForLogMessage("Recebida mensagem de demanda", 3, time.Second*12, containerOdj)
		if err != nil {
			t.Fatal("ODJ não recebeu mensagens", err)
		}
	}, nil)

	// Job para verificar que o tópico demanda-falhada recebeu uma nova mensagem
	st3.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		to := time.NewTimer(10 * time.Second)
		defer to.Stop()
		for {
			select {
			case <-to.C: // timeout
				t.Fatal("Timeout")
				return
			case <-ticker.C:
				if len(topicMessages[errorTopic]) > 0 { // recebeu msg no tópico de falha!
					t.Fatal(topicMessages[errorTopic][0])
					return
				} else if len(topicMessages[outTopic]) == 3 {
					t.Log("Sucess. All msgs correctly processed")
					return
				}
			}
		}
	}, nil)

	stages.AddStages([]*stage.Stage{st, st2, st3})
	stages.Run()
	if containerOdj != nil {
		containerOdj.Terminate(context.Background())
	}
}

func TestReplicaOdj(t *testing.T) {
	var containerOdj, containerOdj2 testcontainers.Container
	topicMessages := make(map[string][]string)
	var tEntry = getRandTopicName(entryTopic, t.Name())
	
	stages := stage.CreateStages()
	st := stage.CreateStage("Primeiro")
	st2 := stage.CreateStage("Segundo")
	st3 := stage.CreateStage("Terceiro")

	mockTJsvr := createFakeServer(200,200)
	defer mockTJsvr.Close()

	r1 := testcontainers.ContainerRequest{
		Image:      odrImage,
		WaitingFor: wait.ForLog("Profile prod activated").WithPollInterval(time.Second / 2).WithStartupTimeout(time.Second * 10),
		Env:getNewContainerDefinition(mockTJsvr.URL, tEntry),
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.NetworkMode = "host"
		},
	}

	r2 := testcontainers.ContainerRequest{
		Image:      odrImage,
		WaitingFor: wait.ForLog("Profile prod activated").WithPollInterval(time.Second / 2).WithStartupTimeout(time.Second * 10),
		Env: getNewContainerDefinition(mockTJsvr.URL, tEntry),
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.NetworkMode = "host"
		},
	}

	k, err := kafkatest.NewKafka(kafka.ConfigMap{"bootstrap.servers": "localhost:9092",
		"acks": "all"})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	k.CreateTopics(&topic.TopicConfig{Topics: []topic.Topics{
		{Name: tEntry, NumPartitions: 2, Messages: []topic.Messages{}},
		{Name: outTopic, NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: errorTopic, NumPartitions: 1, Messages: []topic.Messages{}},
	}})
	defer k.DeleteTopics([]string{tEntry, outTopic, errorTopic})

	consumer, err := k.NewConsumer("test-group", kafkatest.OffsetEarliest)
	if err != nil {
		t.Fatal(err)
	}

	err = consumer.SubscribeTopics([]string{errorTopic, outTopic}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Inicia o serviço de complementação e aguarda resposta do health check para terminar
	st.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		var err error
		log.Println("Inicio job inicia docker 1")
		containerOdj, err = testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
			ContainerRequest: r1,
			Started:          true,
		})
		if err != nil {
			t.Fatal(err)
		}

	}, nil)

	st.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		var err error
		log.Println("Inicio job inicia docker 2")
		containerOdj2, err = testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
			ContainerRequest: r2,
			Started:          true,
		})
		if err != nil {
			t.Fatal(err)
		}
	}, nil)

	// Job multi estágio que vai ficar periodicamente checando mensagens nos tópicos subscritos
	st.AddJobMultiStage(func(dcag stage.DoneCancelArgGet) { // TODO: alterar interface para MultiStage, Done() não é necessário
		ticker := time.NewTicker(time.Millisecond * 100)
		defer ticker.Stop()
		checkMessages(consumer, topicMessages, t)
		for {
			select {
			case <-dcag.Canceled(): // cancelado ao final do estagio st3
				t.Log("Job de checagem de mensagens cancelado")
				return
			case <-ticker.C:
				checkMessages(consumer, topicMessages, t)
			}
		}
	}, st3, nil)

	st2.AddJob(func(dcag stage.DoneCancelArgGet) {
		// Mesmo com health-check e/ou aguardando o log do container, precisamos esperar ou o container não recebe as msgs
		// Isso é necessário no teste porque o tópico inicia vazio e sem informações de offset de mensagens
		time.Sleep(time.Second * 15)
		produceChan := make(chan kafka.Event)
		err = k.ProduceToPartition(tEntry, `{"documentoDemandada": 12211133344, "documentoDemandante": 12211133344, "idDemanda": "134"}`, 0, produceChan)
		err = k.ProduceToPartition(tEntry, `{"documentoDemandada": 22222222222, "documentoDemandante": 22211133344, "idDemanda": "234"}`, 1, produceChan)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2; i++ {
			select {
			case <-time.After(5 * time.Second):
				t.Fatal("Não recebeu confirmação de mensagem")
			case <-produceChan:
				if i == 1 {
					dcag.Done()
				}
			}
		}

	}, nil)
	st3.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		err := dockertest.WaitForLogMessage("Recebida mensagem de demanda", 1, time.Second*12, containerOdj)
		if err != nil {
			t.Fatal("ODJ não recebeu mensagens", err)
		}
	}, nil)

	st3.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		err := dockertest.WaitForLogMessage("Recebida mensagem de demanda", 1, time.Second*12, containerOdj2)
		if err != nil {
			t.Fatal("ODJ 2 não recebeu mensagens", err)
		}
	}, nil)

	// Job para verificar que o tópico demanda-falhada recebeu uma nova mensagem
	st3.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		to := time.NewTimer(15 * time.Second)
		defer to.Stop()
		for {
			select {
			case <-to.C: // timeout
				t.Fatal("Timeout")
				return
			case <-ticker.C:
				if len(topicMessages[errorTopic]) > 0 { // recebeu msg no tópico de falha!
					t.Log("Fail. Got msg on the failureTopic")
					t.Fatal(topicMessages[errorTopic][0])
					return
				} else if len(topicMessages[outTopic]) == 2 {
					t.Log("Sucess. All msgs correctly processed")
					return
				}
			}
		}
	}, nil)

	stages.AddStages([]*stage.Stage{st, st2, st3})
	stages.Run()
	t.Log(dockertest.GetAllLog(containerOdj))
	if containerOdj != nil {
		containerOdj.Terminate(context.Background())
	}
	if containerOdj2 != nil {
		containerOdj2.Terminate(context.Background())
	}
}
