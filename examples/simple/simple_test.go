package simple

import (
	"context"
	"testing"
	"time"

	"github.com/Pedro-Magalhaes/async-microservice-test/pkg/dockertest"
	"github.com/Pedro-Magalhaes/async-microservice-test/pkg/stage"
	"github.com/testcontainers/testcontainers-go"
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

func TestSimpleDocker(t *testing.T) {
	var c testcontainers.Container
	stages := stage.CreateStages()
	st := stage.CreateStage("Primeiro")
	st2 := stage.CreateStage("Segundo")
	st3 := stage.CreateStage("Terceiro")
	r := testcontainers.ContainerRequest{
		Image: "ubuntu",
		Cmd:   []string{"echo", "oi", "sleep", "10"},
		// Cmd:   []string{"/bin/bash sleep 5 && echo 'oi'"},
	}

	st.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		// args := dcag.GetFuncArg()
		var err error
		// // c, ok := args.(testcontainers.Container)
		// // if !ok {
		// // 	t.Fatal("Nao foi possivel converter argumento tipo container")
		// // }
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
		dcag.Done()
	}, nil)

	st3.AddJob(func(dcag stage.DoneCancelArgGet) {
		defer dcag.Done()
		t.Log("Job 3")
	}, nil)
	stages.AddStages([]*stage.Stage{st, st2, st3})
	stages.Run()
}
