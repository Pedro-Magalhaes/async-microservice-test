package simple

import (
	"context"
	"io/ioutil"
	"log"
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

// func TestSimple(t *testing.T) {
// 	stages := stage.CreateStages()
// 	st := stage.CreateStage("Primeiro")
// 	st2 := stage.CreateStage("Segundo")

// 	st.AddJobMultiStage(func(w stage.DoneCancelArgGet) {
// 		for {
// 			select {
// 			case <-w.Canceled():
// 				fmt.Println("job 1 encerrando via channel")
// 				return
// 			default:
// 				fmt.Println("st1 job 1 escrevendo")
// 				time.Sleep(time.Millisecond * 100)
// 			}
// 		}
// 	}, st2, nil)

// 	st.AddJob(func(w stage.DoneCancelArgGet) {
// 		for _, v := range []string{"H", "E", "L", "L", "O"} {
// 			fmt.Println("st1 Job 2, Letra: ", v)
// 			time.Sleep(75 * time.Millisecond)
// 		}
// 		w.Done()
// 	}, nil)

// 	st2.AddJob(func(w stage.DoneCancelArgGet) {
// 		for _, v := range []string{"H", "E", "L", "L", "O"} {
// 			fmt.Println("st2 Job 1, Letra: ", v)
// 			time.Sleep(75 * time.Millisecond)
// 		}
// 		w.Done()
// 	}, nil)
// 	st3 := stage.CreateStage("Último")
// 	st3.AddJob(func(w stage.DoneCancelArgGet) {
// 		argi := w.GetFuncArg()
// 		arg, ok := argi.(string) // fazendo cast pro tipo correto, que foi passado via parametro
// 		if ok == false {
// 			arg = "Default string"
// 		}
// 		for _, v := range []string{"H", "E", "L", "L", "O"} {
// 			fmt.Println(arg, v)
// 			time.Sleep(75 * time.Millisecond)
// 		}
// 		w.Done()
// 	}, "Último Job! Com Arg")
// 	stages.AddStages([]*stage.Stage{st, st2, st3})
// 	stages.Run()
// }

func checkMessages(kafkaConsumer *kafka.Consumer, messageMap map[string][]string) {
	log.Print("oi from check")
	// Process messages
	ev, err := kafkaConsumer.ReadMessage(100 * time.Millisecond)
	if err != nil {
			// Errors are informational and automatically handled by the consumer
		return
	}
	// *&ev.TopicPartition.Offset
	// vou ignorar as keys por enquanto
	log.Printf("Consumed event from topic %s: key = %-10s value = %s\n",
		*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
	messageMap[*ev.TopicPartition.Topic] = append(messageMap[*ev.TopicPartition.Topic], string(ev.Value))

}

func testSimpleDocker(t *testing.T) {
	var c testcontainers.Container
	stages := stage.CreateStages()
	st := stage.CreateStage("Primeiro")
	st2 := stage.CreateStage("Segundo")
	st3 := stage.CreateStage("Terceiro")
	r := testcontainers.ContainerRequest{
		Image: "ubuntu",
		Cmd:   []string{"echo", "oi", "&& sleep", "10"},
	}

	st.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		var err error
		c, err = testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
			ContainerRequest: r,
			Started:          true,
		})
		if err != nil {
			t.Fatal(err)
		}
	}, nil)

	st2.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		err := dockertest.WaitForLogMessage2("oi", time.Second*3, c)
		if err != nil {
			t.Fatal(err)
		}
	}, nil)

	st3.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		t.Log("Job 3")
	}, nil)
	stages.AddStages([]*stage.Stage{st, st2, st3})
	stages.Run()
}

func TestSimpleOdj(t *testing.T) {
	var c testcontainers.Container
	topicMessages := make(map[string][]string)
	stages := stage.CreateStages()
	st := stage.CreateStage("Primeiro")
	st2 := stage.CreateStage("Segundo")
	st3 := stage.CreateStage("Terceiro")

	r := testcontainers.ContainerRequest{
		Image:      "repo.tecgraf.puc-rio.br:18089/odrtj/odr-complementacao-tj:master",
		WaitingFor: wait.ForHTTP("/q/health/live").WithPort("8080/tcp").WithStartupTimeout(time.Second * 10),
		// Networks:   []string{testNetworkName},
		Env: map[string]string{"ODR_KAFKA_HOST": "localhost:9092"},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.NetworkMode = "host"
		},
		// ExposedPorts: []string{"8080"},
	}

	k, err := kafkatest.NewKafka(kafka.ConfigMap{"bootstrap.servers": "localhost:9092",
		"acks": "all"})
	if err != nil {
		t.Fatal(err)
	}
	k.CreateTopics(&topic.TopicConfig{Topics: []topic.Topics{ // demanda-falhada
		{Name: "demanda-submetida", NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: "resposta-complementacao-judicial", NumPartitions: 1, Messages: []topic.Messages{}},
		{Name: "demanda-falhada", NumPartitions: 1, Messages: []topic.Messages{}},
	}})

	consumer, err := k.NewConsumer("test-group")
	if err != nil {
		t.Fatal(err)
	}
	err = consumer.SubscribeTopics([]string{"demanda-submetida", "demanda-falhada"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// consumer.
	st.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		var err error
		c, err = testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
			ContainerRequest: r,
			Started:          true,
		})
		if err != nil {
			t.Fatal(err)
		}
		log.Println("Fim job inicia docker", c.IsRunning())
	}, nil)

	st.AddJobMultiStage(func(dcag stage.DoneCancelArgGet) {
		ticker := time.NewTicker(time.Millisecond * 100)
		defer ticker.Stop()
		for {
			select {
				case <-dcag.Canceled(): // cancelado ao final do estagio st2
					t.Log("Job de checagem de mensagens cancelado")
					return
				case <-ticker.C:
					t.Log("check message")
					checkMessages(consumer, topicMessages)
			}
		}
		// dcag.Done() // não precisa pra jobs multistage
	}, st2, nil)

	st2.AddJob(func(dcag stage.DoneCancelArgGet) {
		produceChan := make(chan kafka.Event)
		
		err = k.Produce("demanda-submetida", "{}", produceChan)
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
	
	st3.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done() 
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		to := time.NewTimer(5 * time.Second)
		defer to.Stop()
		for {
			select {
				case <-to.C:
					t.Fatal("Não recebeu mensagem no canal de falha")
					return // timeout
				case <-ticker.C:
					if len(topicMessages["demanda-falhada"]) > 0 {
						t.Log(topicMessages["demanda-falhada"][0])
						return
					}
			}
		}
	}, nil)

	stages.AddStages([]*stage.Stage{st, st2, st3})
	stages.Run()
	
	read,err := c.Logs(context.Background())
	
	text, err := ioutil.ReadAll(read)
	t.Log(string(text)) // // DEBUG
}
