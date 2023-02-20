package dockertest

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	docker "github.com/fsouza/go-dockerclient"
	"github.com/testcontainers/testcontainers-go/wait"
)

func Test_WaitForLogMessage(t *testing.T) {
	t.Parallel()
	var req http.Request
	var count int
	msgs := []string{"string 1", "string2 com mais palavras", "string 3", "string buscada"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prefix := []byte{1, 0, 0, 0, 0, 0, 0, 19}
		w.Write(prefix)
		if len(msgs) <= count {
			t.Error("Erro, fake server enviou mais mensagens que deveria")
		} else {
			t.Log("fakeserver sending", msgs[count])
		}
		w.Write([]byte(msgs[count]))
		req = *r
		count++
	}))
	defer server.Close()
	client, _ := docker.NewClient(server.URL)
	client.SkipServerVersionCheck = true
	fakeId := "container1234"
	t.Log("antes de chamar wait")
	err := WaitForLogMessage("nao tem", time.Second, client, fakeId)
	t.Log("depois de chamar wait")

	if err != nil {
		t.Error("error calling waitForLog", err)
	}
	if strings.Contains(req.URL.Path, fakeId) == false {
		t.Error("consultou o container errado?", req.URL.Path)
	}

}

func Test_WaitForLogMessage2(t *testing.T) {
	target := wait.NopStrategyTarget{
		ReaderCloser: io.NopCloser(bytes.NewReader([]byte("kubernetes\r\ndocker\n\rdocker"))),
	}

	err := WaitForLogMessage2("not found", time.Second*100, nil, target)
	if err != nil {
		t.Fatal(err)
	}
}
