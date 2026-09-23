package scenario

import (
	"fmt"
	"strings"
	"testing"
)

func TestRejectsNonFiniteReadyRatio(t *testing.T) {
	for _, ratio := range []string{".nan", ".inf", "-.inf"} {
		_, err := Parse([]byte("name: limits\nphases: [{action: wait-ready, readyRatio: " + ratio + "}]"))
		if err == nil || !strings.Contains(err.Error(), "readyRatio") {
			t.Errorf("readyRatio %s: expected validation error, got %v", ratio, err)
		}
	}
}

func TestPublishPayloadLimitValidatedBeforeExecution(t *testing.T) {
	for _, size := range []int{32, 16 << 20, (16 << 20) + 1} {
		_, err := Parse([]byte(fmt.Sprintf("name: limits\nphases: [{action: publish, group: peers, count: 1, payloadSize: %d}]", size)))
		if size <= 16<<20 && err != nil {
			t.Errorf("valid payload size %d: %v", size, err)
		}
		if size > 16<<20 && (err == nil || !strings.Contains(err.Error(), "payloadSize")) {
			t.Errorf("oversized payload %d: expected validation error, got %v", size, err)
		}
	}
}
