package peer

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	libcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
)

func TestAttackAuthorStatusMatchesObservedPublication(t *testing.T) {
	key, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := libcrypto.MarshalPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	author := &model.PubSubAuthorConfig{PrivateKey: base64.StdEncoding.EncodeToString(encoded)}
	otherKey, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	otherEncoded, err := libcrypto.MarshalPrivateKey(otherKey)
	if err != nil {
		t.Fatal(err)
	}
	otherAuthor := &model.PubSubAuthorConfig{PrivateKey: base64.StdEncoding.EncodeToString(otherEncoded)}
	for _, mode := range []string{"host-signed", "host-unsigned", "explicit", "publish-override", "anonymous"} {
		t.Run(mode, func(t *testing.T) {
			servers, sub, _ := attackTestChain(t, func([]host.Host) model.NodeConfig {
				var config model.NodeConfig
				switch mode {
				case "host-unsigned":
					config.GossipSub.SignaturePolicy = "strict-no-sign"
				case "explicit":
					config.GossipSub.Author = author
				case "publish-override":
					config.GossipSub.Author = otherAuthor
					config.GossipSub.Publish = &model.PubSubPublishConfig{Author: author}
				case "anonymous":
					config.GossipSub.SignaturePolicy = "strict-no-sign"
					config.GossipSub.MessageID = "sha256"
					config.GossipSub.Author = &model.PubSubAuthorConfig{NoAuthor: true}
				}
				return config
			})
			publisher := servers[1]
			options, err := gossipPublishOptions(publisher.config.NodeConfig.GossipSub)
			if err != nil {
				t.Fatal(err)
			}
			if err := publisher.topics["blocks"].Publish(t.Context(), []byte(mode), options...); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			message, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var status model.Node
			publisher.addAttackStatus(&status)
			if got, want := status.Metadata["pubsubAuthorId"], message.GetFrom().String(); got != want {
				t.Fatalf("metadata author %q differs from remote wire author %q", got, want)
			}
			wantMode := "host"
			if mode == "explicit" || mode == "publish-override" {
				wantMode = "explicit"
			} else if mode == "anonymous" {
				wantMode = "anonymous"
			}
			if status.Metadata["pubsubAuthorMode"] != wantMode {
				t.Fatalf("metadata author mode %q, want %q", status.Metadata["pubsubAuthorMode"], wantMode)
			}
		})
	}
}
