package simple

import (
	"fmt"
	"testing"
	"time"

	"github.com/Pedro-Magalhaes/async-microservice-test/pkg/stage"
)

func TestSimple(t *testing.T) {
	stages := stage.CreateStages()
	st := stage.CreateStage("Primeiro")
	st2 := stage.CreateStage("Segundo")

	st.AddJobMultiStage(func(c *chan bool) {
		for {
			select {
			case <-*c:
				fmt.Println("job 1 encerrando via channel")
				return
			default:
				fmt.Println("st1 job 1 escrevendo")
				time.Sleep(time.Millisecond * 100)
			}
		}
	}, st2)

	st.AddJob(func(c *chan bool) {
		for _, v := range []string{"H", "E", "L", "L", "O"} {
			fmt.Println("st1 Job 2, Letra: ", v)
			time.Sleep(75 * time.Millisecond)
		}
		*c <- true
	})

	st2.AddJob(func(c *chan bool) {
		for _, v := range []string{"H", "E", "L", "L", "O"} {
			fmt.Println("st2 Job 1, Letra: ", v)
			time.Sleep(75 * time.Millisecond)
		}
		*c <- true
	})
	st3 := stage.CreateStage("Último")
	st3.AddJob(func(c *chan bool) {
		for _, v := range []string{"H", "E", "L", "L", "O"} {
			fmt.Println("ÚLTIMO!: ", v)
			time.Sleep(75 * time.Millisecond)
		}
		*c <- true
	})
	stages.AddStages([]*stage.Stage{st, st2, st3})
	stages.Run()
}
