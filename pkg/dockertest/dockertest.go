package dockertest

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/testcontainers/testcontainers-go/wait"
)

// type Docker struct {
// 	client *docker.Client
// 	config DockerConfigFile
// }

// type DockerConfigFile struct {
// 	Config           docker.Config           `json:"config,omitempty"`
// 	HostConfig       docker.HostConfig       `json:"hostConfig,omitempty"`
// 	NetworkingConfig docker.NetworkingConfig `json:"networkingConfig,omitempty"`
// }

// func NewDockerInterface() (*Docker, error) {
// 	client, err := docker.NewClientFromEnv()
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &Docker{
// 		client: client,
// 		config: DockerConfigFile{},
// 	}, nil
// }

// func (d Docker) CreateContainer(name string) (*docker.Container, error) {
// 	dockerCreatorConfig := docker.CreateContainerOptions{
// 		Name:             name,
// 		Config:           &d.config.Config,
// 		HostConfig:       &d.config.HostConfig, // Contém os binds de arquivos
// 		NetworkingConfig: &d.config.NetworkingConfig,
// 	}
// 	container, e := d.client.CreateContainer(dockerCreatorConfig)
// 	return container, e
// }

// func LoadDockerConfig(jsonFile string) (DockerConfigFile, error) {
// 	file, err := os.Open(jsonFile)
// 	conf := DockerConfigFile{}

// 	if err != nil {
// 		return conf, err
// 	}
// 	bytes, err := io.ReadAll(file)
// 	if err != nil {
// 		return conf, err
// 	}

// 	err = json.Unmarshal(bytes, &conf)

// 	if err != nil {
// 		return conf, err
// 	}
// 	return conf, nil
// }

type waitResponse struct {
	err       error
	container wait.StrategyTarget
}

type waitStrategy struct {
	container wait.StrategyTarget
	st        wait.Strategy
}

// Cria rotina e faz o WaitUntil, o que retornar? apenas erro? é necessário saber o container que causou o erro/sucesso?
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

func WaitForLogMessage(message string, occurrence int, timeout time.Duration, container wait.StrategyTarget) error {
	log.Print("a")
	st := wait.ForLog(message).WithOccurrence(occurrence).WithStartupTimeout(timeout)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	c := waitForStrategies(ctx, []waitStrategy{{container: container, st: st}})
	log.Print("b")
	resp := <-c
	log.Print("c")
	return resp.err
}

// Criar uma função que faz o wait em rotinas e retorna via canal quando cada log for encontrado

// func WaitForLogMessageForAll(message string, occurrence int, timeout time.Duration, containers []wait.StrategyTarget) error {
// 	ctx, cancel := context.WithTimeout(context.Background(), timeout)
// 	defer cancel()
// 	return wait.ForLog(message).WithOccurrence(occurrence).WithStartupTimeout(timeout).WaitUntilReady(ctx, container)
// }

// func WaitForLogMessageForAny(message string, occurrence int, timeout time.Duration, containers []wait.StrategyTarget) error {
// 	ctx, cancel := context.WithTimeout(context.Background(), timeout)
// 	defer cancel()
// 	strategies := wait.MultiStrategy{}
// 	for _, v := range containers {
// 		strategy := wait.ForLog(message).WithOccurrence(occurrence).WithStartupTimeout(timeout)
// 		strategies.Strategies = append(strategies.Strategies, strategy)
// 	}
// 	// .WaitUntilReady(ctx, v)
// 	return wait
// }
