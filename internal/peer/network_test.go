package peer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestRequirePeerContainerRejectsHostStartup(t *testing.T) {
	for _, test := range []struct {
		name, goos, wantError string
		statErr               error
	}{
		{"non-Linux", "darwin", "Linux Docker containers", nil},
		{"host", "linux", "outside a Docker container", os.ErrNotExist},
		{"container", "linux", "", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := requirePeerContainer(test.goos, func(path string) (os.FileInfo, error) {
				if path != "/.dockerenv" {
					t.Fatalf("unexpected container marker: %s", path)
				}
				return nil, test.statErr
			})
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("container check = %v, want %s", err, test.wantError)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDisabledNetworkStillRequiresPeerContainer(t *testing.T) {
	config := model.PeerProcessConfig{NodeConfig: model.NodeConfig{Network: model.NetworkConfig{Delay: "0s"}}}
	err := prepareNetwork(context.Background(), config)
	containerErr := requirePeerContainer(runtime.GOOS, os.Stat)
	if containerErr != nil {
		if err == nil || err.Error() != containerErr.Error() {
			t.Fatalf("disabled network bypassed the container requirement: %v", err)
		}
	} else if err != nil {
		t.Fatalf("disabled network should not invoke tc: %v", err)
	}
}

func TestPublishRejectsRecycledContainerAddress(t *testing.T) {
	s := &Server{config: model.PeerProcessConfig{Node: model.Node{ID: "new-peer"}}}
	request := httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader(`{"runId":"run"}`))
	request.Header.Set("X-KPL-Node-ID", "old-peer")
	response := httptest.NewRecorder()
	s.handlePublish(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d; recycled IP must not accept old peer's publish", response.Code)
	}
}
