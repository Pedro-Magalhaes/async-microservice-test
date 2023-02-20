package topic

import (
	"encoding/json"
	"io"
	"os"
)

type Messages struct {
	Partition int16  `json:"partition"`
	Message   string `json:"message"`
	Key       string `json:"key,omitempty"`
}
type Topics struct {
	Name          string     `json:"name"`
	NumPartitions int        `json:"numPartitions"`
	Messages      []Messages `json:"messages"`
}
type TopicConfig struct {
	Topics []Topics `json:"topics"`
}

func LoadTopicConfig(jsonFile string) (*TopicConfig, error) {
	file, err := os.Open(jsonFile)
	if err != nil {
		return nil, err
	}
	bytes, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	conf := TopicConfig{}

	err = json.Unmarshal(bytes, &conf)

	if err != nil {
		return nil, err
	}
	return &conf, nil
}
