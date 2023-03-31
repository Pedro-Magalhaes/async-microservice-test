package odrj

import (
	"context"
	"io"
	"log"
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
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func checkMessages(kafkaConsumer *kafka.Consumer, messageMap map[string][]string, t *testing.T) {
	// log.Print("oi from check")
	// Process messages
	ev, err := kafkaConsumer.ReadMessage(100 * time.Millisecond)
	if err != nil {
		// Errors are informational and automatically handled by the consumer
		return
	}
	// *&ev.TopicPartition.Offset
	// vou ignorar as keys por enquanto
	t.Logf("Consumed event from topic %s: key = %-10s value = %s\n",
		*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
	messageMap[*ev.TopicPartition.Topic] = append(messageMap[*ev.TopicPartition.Topic], string(ev.Value))

}

func testFailHttpOdj(t *testing.T) {
	const (
		entryTopic = "demanda-submetida"
		errorTopic = "demanda-falhada"
		outTopic   = "resposta-complementacao-judicial"
	)
	var containerOdj testcontainers.Container
	topicMessages := make(map[string][]string)
	stages := stage.CreateStages()
	st := stage.CreateStage("Primeiro")
	st2 := stage.CreateStage("Segundo")
	st3 := stage.CreateStage("Terceiro")

	mockLightsvr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Log("Responding fake error")
		w.WriteHeader(400)
		w.Write([]byte("Fake error"))
	}))
	defer mockLightsvr.Close()

	r := testcontainers.ContainerRequest{
		Image: "repo.tecgraf.puc-rio.br:18089/odrtj/odr-complementacao-tj:master",
		WaitingFor: wait.ForHTTP("/q/health/live").WithPort("8888/tcp").WithStatusCodeMatcher(func(status int) bool {
			return status >= 200 && status <= 299
		}).WithStartupTimeout(time.Second * 10),
		Env: map[string]string{
			"ODR_KAFKA_HOST":                 "localhost:9092",
			"ODR_TJRJ_PROCESSOS_RETRY_DELAY": "100",
			"ODR_TJRJ_PROCESSOS_TIMEOUT":     "1000",
			"ODR_HTTP_PORT":                  "8888",
			// "ODR_LOG_LEVEL":                  "DEBUG",
			"ODR_TJRJ_PROCESSOS_URI": mockLightsvr.URL,
			"ODR_TJRJ_OIDC_URI":      "localhost:8080",
		},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.NetworkMode = "host"
		},
	}

	k, err := kafkatest.NewKafka(kafka.ConfigMap{"bootstrap.servers": "localhost:9092",
		"acks": "all"})
	if err != nil {
		t.Fatal(err)
	}
	k.CreateTopics(&topic.TopicConfig{Topics: []topic.Topics{
		{Name: entryTopic, NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: outTopic, NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: errorTopic, NumPartitions: 1, Messages: []topic.Messages{}},
	}})

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
				// t.Log("check message")
				checkMessages(consumer, topicMessages, t)
			}
		}
	}, st3, nil)

	// produz uma mensagem mal formatada para o tópico de entrada do serviço de complementação
	st2.AddJob(func(dcag stage.DoneCancelArgGet) {
		produceChan := make(chan kafka.Event)
		time.Sleep(time.Second * 4) // Health check da complementação fica 200 antes de estar pronto pra consumir
		// Envia para falha recuperavel --
		err = k.Produce(entryTopic, `{"documentoDemandada": 22211133344, "documentoDemandante": 22211133344, "idDemanda": "234"}`, produceChan)

		// causa erro ao abrir o json, não envia mensagem ao tópico de falhas
		// err = k.Produce(entryTopic, `{"documentoDemandada": 22211133344, "documentoDemandante": 22211133344, "idDemanda": "234L"}`, produceChan)
		// err = k.Produce(entryTopic, "{'not': 'valid', 'format': 'null'}", produceChan)
		// err = k.Produce(entryTopic, "{'not': 'valid', 'format': 'null'}", produceChan) // ' causa erro na aplicação sem enviar para topico de falha
		// err = k.Produce(entryTopic, "{'not': 'valid', 'format': 'null'}", produceChan)
		// err = k.Produce(entryTopic, "{'not': 'valid', 'format': 'null'}", produceChan)

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

	read, err := containerOdj.Logs(context.Background())

	text, err := io.ReadAll(read)
	t.Log(string(text)) // // DEBUG
}

func TestCorrectBehaviourOdj(t *testing.T) {
	const (
		entryTopic = "demanda-submetida"
		errorTopic = "demanda-falhada"
		outTopic   = "resposta-complementacao-judicial"
	)
	var containerOdj testcontainers.Container
	topicMessages := make(map[string][]string)
	stages := stage.CreateStages()
	st := stage.CreateStage("Primeiro")
	st2 := stage.CreateStage("Segundo")
	st3 := stage.CreateStage("Terceiro")

	mockLightsvr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		
		if(strings.Contains(r.URL.Path, "auth")) { // requisição token
			t.Log("token")
			w.Write([]byte(`{"access_token": "66e56811-fff0-4ed6-9132-bd96981f276f"}`))
			return
		}
		t.Log("Repondig 3 processes") // mock consultar por processos
		w.Header().Set("content-type", "application/json")
		w.Write([]byte("[\"395566265.2019.8.19.0206\", \"343039222.2021.81.9.0208\", \"527925574.2022.81.9.0029\"]"))
	}))
	defer mockLightsvr.Close()

	r := testcontainers.ContainerRequest{
		Image: "repo.tecgraf.puc-rio.br:18089/odrtj/odr-complementacao-tj:master",
		WaitingFor: wait.ForLog("Profile prod activated").WithPollInterval(time.Second / 2),
		Env: map[string]string{
			"ODR_KAFKA_HOST":                 "localhost:9092",
			"ODR_TJRJ_PROCESSOS_RETRY_DELAY": "100",
			"ODR_TJRJ_PROCESSOS_TIMEOUT":     "1000",
			"ODR_HTTP_PORT":                  "8888",
			// "ODR_LOG_LEVEL":                  "DEBUG",
			"ODR_TJRJ_PROCESSOS_URI": mockLightsvr.URL,
			// "ODR_TJRJ_OIDC_URI":      "localhost:8080",
			"ODR_TJRJ_OIDC_URI":           mockLightsvr.URL + "/auth/realms/homologacao",
			"ODR_TJRJ_OIDC_CLIENT_ID":     "odr",
			"ODR_TJRJ_OIDC_CLIENT_SECRET": "66e56811-fff0-4ed6-9132-bd96981f276f",
			"ODR_TJRJ_OIDC_USERNAME":      "api-reincidencia",
			"ODR_TJRJ_OIDC_PASSWORD":      "1234",
		},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.NetworkMode = "host"
		},
	}

	k, err := kafkatest.NewKafka(kafka.ConfigMap{"bootstrap.servers": "localhost:9092",
		"acks": "all"})
	if err != nil {
		t.Fatal(err)
	}
	k.CreateTopics(&topic.TopicConfig{Topics: []topic.Topics{
		{Name: entryTopic, NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: outTopic, NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: errorTopic, NumPartitions: 1, Messages: []topic.Messages{}},
	}})

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
		
		time.Sleep(time.Second * 5) // Mesmo com health-check e/ou aguardando o log do container, precisamos esperar ou
		// o container perde mensagens
	
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
				t.Log("OI", i)
				if i == 2 {
					dcag.Done()	
				}
			}
		}

	}, nil)
	st2.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		err := dockertest.WaitForLogMessage("Recebida mensagem de demanda", 3, time.Second * 12, containerOdj)
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
				t.Log("Teste array demanda-falhada")
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

	read, err := containerOdj.Logs(context.Background())

	text, err := io.ReadAll(read)
	t.Log(string(text)) // // DEBUG
}

// 	String documentoDemandada = "111222333444555";
//  String documentoDemandante = "22211133344";
//  Long idDemanda = 234L;
//  List<String> processos = Arrays.asList("1111111-22.2021.4.02.2222");
//  LocalDate dataReferencia = LocalDate.now().minusYears(horizonteAnos);
//  DemandaSubmetidaDTO demandaSubmetida = new DemandaSubmetidaDTO();
//  demandaSubmetida.setDocumentoDemandada(documentoDemandada);
//  demandaSubmetida.setDocumentoDemandante(documentoDemandante);
//  demandaSubmetida.setIdDemanda(idDemanda);
