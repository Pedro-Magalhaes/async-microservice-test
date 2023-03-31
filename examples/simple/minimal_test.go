package simple

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Pedro-Magalhaes/async-microservice-test/pkg/dockertest"
	"github.com/Pedro-Magalhaes/async-microservice-test/pkg/stage"
	"github.com/testcontainers/testcontainers-go"
)

func TestSimpleDocker(t *testing.T) {
	var container testcontainers.Container
	stages := stage.CreateStages()
	st := stage.CreateStage("Primeiro")
	st2 := stage.CreateStage("Segundo")
	st3 := stage.CreateStage("Terceiro")

	const comando = "while sleep 1; do echo 'oi'; done"
	st.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		arg := dcag.GetFuncArg()
		cmd, _ := arg.(string) //typecast
		container, _ = testcontainers.GenericContainer(context.Background(),
			testcontainers.GenericContainerRequest{
				ContainerRequest: testcontainers.ContainerRequest{
					Image: "ubuntu",
					Cmd:   []string{"sh", "-c", cmd},
				},
				Started: true,
			})
	}, comando)

	st2.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		err := dockertest.WaitForLogMessage("oi", 3, time.Second*5, container)
		if err != nil {
			t.Fatal(err) // timeout
		}
	}, nil)

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Log("mockServer got request")
		time.Sleep(time.Second * 3)
		w.Write([]byte("ok"))
	}))

	respChan := make(chan interface{})
	st3.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		t.Log("Job 3")
		go func() {
			r, _ := http.Get(mockServer.URL)
			body, _ := io.ReadAll(r.Body)
			t.Log("Job 3 Got response: ", string(body))
			close(respChan) // fecha e todos ouvindo recebem nil
		}()
		<-respChan
	}, nil)

	st3.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		t.Log("Job 4")
		ticker := time.NewTicker(time.Millisecond * 500)
		defer ticker.Stop()
		for {
			select {
			case <-respChan:
				t.Log("Job4 end")
				return
			case tick := <-ticker.C: // ocorre varias vezes até o caso de cima ocorrer
				t.Log("Job4 tick", tick.Second())
			}
		}
	}, nil)
	stages.AddStages([]*stage.Stage{st, st2, st3})
	stages.Run()
}
