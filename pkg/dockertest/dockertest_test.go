package dockertest

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/wait"
)

// func Test_WaitForLogMessage(t *testing.T) {
// 	t.Parallel()
// 	var req http.Request
// 	var count int
// 	msgs := []string{"string 1", "string2 com mais palavras", "string 3", "string buscada"}
// 	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
// 		prefix := []byte{1, 0, 0, 0, 0, 0, 0, 19}
// 		w.Write(prefix)
// 		if len(msgs) <= count {
// 			t.Error("Erro, fake server enviou mais mensagens que deveria")
// 		} else {
// 			t.Log("fakeserver sending", msgs[count])
// 		}
// 		w.Write([]byte(msgs[count]))
// 		req = *r
// 		count++
// 	}))
// 	defer server.Close()
// 	testcontainers.NewDockerClient()
// 	client, _ := docker.NewClient(server.URL)
// 	client.SkipServerVersionCheck = true
// 	fakeId := "container1234"
// 	t.Log("antes de chamar wait")
// 	err := WaitForLogMessage("nao tem", 1, time.Second, client)
// 	t.Log("depois de chamar wait")

// 	if err != nil {
// 		t.Error("error calling waitForLog", err)
// 	}
// 	if strings.Contains(req.URL.Path, fakeId) == false {
// 		t.Error("consultou o container errado?", req.URL.Path)
// 	}

// }

func Test_WaitForLogMessage(t *testing.T) {
	target := wait.NopStrategyTarget{
		ReaderCloser: io.NopCloser(bytes.NewReader([]byte("kubernetes\ndocker\ndocker"))),
	}
	type args struct {
		text    string
		times   int
		timeout time.Duration
		waitSt  wait.NopStrategyTarget
	}
	tests := []struct {
		name string
		args args
		want error
	}{
		{"not found", args{text: "not found", times: 1, timeout: time.Millisecond * 100, waitSt: target}, nil},
		{"one found, want 2", args{text: "kubernetes", times: 2, timeout: time.Millisecond * 100, waitSt: target}, nil},
		{"want 1 found 2", args{text: "docker", times: 1, timeout: time.Millisecond * 100, waitSt: target}, nil},
		{"want 2 found 2", args{text: "docker", times: 2, timeout: time.Millisecond * 100, waitSt: target}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WaitForLogMessage(tt.args.text, tt.args.times, tt.args.timeout, tt.args.waitSt); got != tt.want {
				t.Errorf("Stages.GetStage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStages_GetStage(t *testing.T) {

}
