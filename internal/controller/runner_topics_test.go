package controller

import (
	"reflect"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestPublicationTopicNamesKeepLiteralSubscriptionIdentity(t *testing.T) {
	node := model.Node{Type: "full", Metadata: map[string]string{"topicsJSON": `[" topic ","topic","comma,topic"]`}}
	if !reflect.DeepEqual(nodePublishTopics(node, "*"), []string{" topic ", "comma,topic", "topic"}) {
		t.Fatal("wildcard publication changed literal topic names")
	}
	if nodeDefaultTopic(node) != " topic " {
		t.Fatal("default topic was trimmed")
	}
	node.Metadata["topicsJSON"] = `[" topic "]`
	if nodeSubscribesToTopic(node, "topic") || !nodeSubscribesToTopic(node, " topic ") {
		t.Fatal("distinct PubSub subscription topics were merged")
	}
}
