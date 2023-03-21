package dockertest

import (
	"context"
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

func WaitForLogMessage(message string, occurrence int, timeout time.Duration, container wait.StrategyTarget) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return wait.ForLog(message).WithOccurrence(occurrence).WithStartupTimeout(timeout).WaitUntilReady(ctx, container)
}
