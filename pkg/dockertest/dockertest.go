package dockertest

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)


type waitResponse struct {
	err       error
	container testcontainers.Container
}

type waitStrategy struct {
	container testcontainers.Container
	st        wait.Strategy
}


// Retorna todo o log de um container. 
// Não deve ser usado por logs muito grandes
func GetAllLog(c testcontainers.Container) (string, error)  {
	read, err := c.Logs(context.Background())
	if err != nil {
		return "", err
	}
	text, err := io.ReadAll(read)
	if err != nil {
		return "", err
	}
	return string(text), nil
}

// Aguarda por um container 
func WaitForLogMessage(message string, occurrence int, timeout time.Duration, container testcontainers.Container) error {
	st := wait.ForLog(message).WithOccurrence(occurrence).WithPollInterval(time.Second / 2).WithStartupTimeout(timeout)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	c := waitForStrategies(ctx, []waitStrategy{{container: container, st: st}})
	resp := <-c
	return resp.err
}

// Recebe um lista de containers e retorna quando econtrar "messsage" x vezes no log de todos os containers.
func WaitForLogMessageForAll(message string, occurrence int, timeout time.Duration, containers []testcontainers.Container) error {
	var errCount uint64 // contador thread safe
	wg := sync.WaitGroup{}
	for _, v := range containers {
		wg.Add(1)
		go func(c testcontainers.Container) {
			defer wg.Done()
			err := WaitForLogMessage(message, occurrence, timeout, c)
			if err != nil {
				atomic.AddUint64(&errCount, 1)
			}
		}(v)
	}
	wg.Wait()
	if errCount > 0 {
		return errors.New("error waiting for logs")
	}
	return nil
}

// Recebe um lista de containers e retorna o primeiro container que a "message" for encontrada x vezes.
func WaitForLogMessageForAny(message string,
							occurrence int,
							timeout time.Duration,
							containers [] testcontainers.Container) (testcontainers.Container,error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var strategies []waitStrategy
	for _, v := range containers {
		strategy := wait.ForLog(message).WithOccurrence(occurrence).WithPollInterval(time.Second/2).WithStartupTimeout(timeout)
		strategies = append(strategies, waitStrategy{ st: strategy, container: v})
	}
	c := <- waitForStrategies(ctx, strategies)
	return c.container, c.err
}

// Cria rotina e faz o WaitUntilReady. Retorna via canal quando cada terminar
// Se ocorrer um timeout retornar um erro além do container
func waitForStrategies(ctx context.Context, strategies []waitStrategy) chan waitResponse {
	c := make(chan waitResponse, len(strategies)) // canal com buffer pra não ficar preso se não for lido
	wg := sync.WaitGroup{}
	for _, strategy := range strategies {
		wg.Add(1)
		go func(s waitStrategy) {
			defer wg.Done()
			select {
			case <-ctx.Done():
				c <- waitResponse{err: errors.New("timeout"), container: s.container}
			default:
				c <- waitResponse{err: s.st.WaitUntilReady(ctx, s.container), container: s.container}
			}

		}(strategy)
	}
	return c
}

