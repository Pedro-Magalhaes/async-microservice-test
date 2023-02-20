package dockertest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	docker "github.com/fsouza/go-dockerclient"
)

type Docker struct {
	client *docker.Client
	config DockerConfigFile
}

type DockerConfigFile struct {
	Config           docker.Config           `json:"config,omitempty"`
	HostConfig       docker.HostConfig       `json:"hostConfig,omitempty"`
	NetworkingConfig docker.NetworkingConfig `json:"networkingConfig,omitempty"`
}

func NewDockerInterface() (*Docker, error) {
	client, err := docker.NewClientFromEnv()
	if err != nil {
		return nil, err
	}
	return &Docker{
		client: client,
		config: DockerConfigFile{},
	}, nil
}

func (d Docker) CreateContainer(name string) (*docker.Container, error) {
	dockerCreatorConfig := docker.CreateContainerOptions{
		Name:             name,
		Config:           &d.config.Config,
		HostConfig:       &d.config.HostConfig, // Contém os binds de arquivos
		NetworkingConfig: &d.config.NetworkingConfig,
	}
	container, e := d.client.CreateContainer(dockerCreatorConfig)
	return container, e
}

func LoadDockerConfig(jsonFile string) (DockerConfigFile, error) {
	file, err := os.Open(jsonFile)
	conf := DockerConfigFile{}

	if err != nil {
		return conf, err
	}
	bytes, err := io.ReadAll(file)
	if err != nil {
		return conf, err
	}

	err = json.Unmarshal(bytes, &conf)

	if err != nil {
		return conf, err
	}
	return conf, nil
}

func WaitForLogMessage(message string, timeout time.Duration, dockerClient *docker.Client, containerId string) error {
	// Cria um objeto "Client" do Docker
	// dockerClient, err := docker.NewClientFromEnv()
	// if err != nil {
	// 	return err
	// }

	// Cria um contexto para a operação
	ctx := context.Background()
	var buf bytes.Buffer
	// Obtém as informações do container
	err := dockerClient.Logs(docker.LogsOptions{
		Container:    containerId,
		OutputStream: &buf,
		Follow:       true,
		Tail:         "",
		Stdout:       true,
		Stderr:       true,
		RawTerminal:  true,
		Context:      ctx,
	})
	// containerInfo, err := dockerClient.ContainerInspect(ctx, containerName)
	if err != nil {
		return err
	}
	log.Default().Println(buf.String())
	s := bufio.NewScanner(&buf)
	if s == nil {
		return errors.New("could not create the buffer scanner")
	}
	log.Default().Println("Antes do scan func wait....", s.Text())
	for {
		hasTokens := s.Scan()
		if !hasTokens {
			log.Default().Println("scan retorna false")
			break
		}
		line := s.Text()
		fmt.Println(line)
		if strings.Contains(line, message) {
			fmt.Println("found string")
			return nil
		}
	}

	// Obtém os logs do container
	// reader, err := dockerClient.ContainerLogs(ctx, containerName, logOptions)
	// if err != nil {
	// 	return err
	// }

	// Cria um canal para receber o resultado da operação
	// result := make(chan error)

	// Cria um objeto "Tail" para monitorar o log
	// tailer, err := tail.TailReader(reader, &tail.Config{
	// 	Follow: true,
	// 	ReOpen: true,
	// })
	// if err != nil {
	// 	return err
	// }

	// Inicia uma goroutine para monitorar o log
	// go func() {
	// 	for line := range tailer.Lines {
	// 		fmt.Println(line.Text)
	// 		if line.Text == message {
	// 			result <- nil
	// 			return
	// 		}
	// 	}
	// 	result <- fmt.Errorf("Timeout: string %q not found in log of container %q", message, containerName)
	// }()

	// Espera o resultado da operação ou o timeout
	// select {
	// case err := <-result:
	// 	return err
	// case <-time.After(timeout):
	// 	return fmt.Errorf("Timeout: string %q not found in log of container %q", message, containerName)
	// }
	return nil
}
