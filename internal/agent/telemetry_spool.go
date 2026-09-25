package agent

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type telemetrySegment struct {
	name string
	keys []string
	ack  int
}
type telemetryRef struct {
	segment *telemetrySegment
	index   int
}
type telemetrySpool struct {
	directory string
	refs      map[string][]telemetryRef
}

func telemetryKey(event model.TraceEvent) string {
	data, _ := json.Marshal(event)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func syncTelemetryDirectory(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	return errors.Join(f.Sync(), f.Close())
}
func writeTelemetryFile(path string, data []byte) error {
	f, e := os.CreateTemp(filepath.Dir(path), ".spool-*")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if _, e = f.Write(data); e == nil {
		e = f.Sync()
	}
	e = errors.Join(e, f.Close())
	if e != nil {
		return e
	}
	if e = os.Rename(f.Name(), path); e != nil {
		return e
	}
	return syncTelemetryDirectory(filepath.Dir(path))
}
func openTelemetrySpool(dataDir string) (*telemetrySpool, []model.TraceEvent, error) {
	directory := filepath.Join(dataDir, "telemetry-spool")
	if e := os.MkdirAll(directory, 0700); e != nil {
		return nil, nil, e
	}
	if e := syncTelemetryDirectory(dataDir); e != nil {
		return nil, nil, e
	}
	spool := &telemetrySpool{directory: directory, refs: map[string][]telemetryRef{}}
	entries, e := os.ReadDir(directory)
	if e != nil {
		return nil, nil, e
	}
	var events []model.TraceEvent
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		info, e := entry.Info()
		if e != nil {
			return nil, nil, e
		}
		if !info.Mode().IsRegular() || info.Size() > 64<<20 {
			return nil, nil, errors.New("invalid telemetry spool segment")
		}
		data, e := os.ReadFile(filepath.Join(directory, entry.Name()))
		if e != nil {
			return nil, nil, e
		}
		var batch []model.TraceEvent
		if e = json.Unmarshal(data, &batch); e != nil {
			return nil, nil, fmt.Errorf("read telemetry spool %s: %w", entry.Name(), e)
		}
		ack := 0
		data, e = os.ReadFile(filepath.Join(directory, entry.Name()+".ack"))
		if e == nil {
			ack, e = strconv.Atoi(string(data))
		}
		if e != nil && !errors.Is(e, os.ErrNotExist) {
			return nil, nil, e
		}
		if ack < 0 || ack > len(batch) {
			return nil, nil, errors.New("invalid telemetry acknowledgement")
		}
		segment := &telemetrySegment{name: entry.Name(), ack: ack}
		for i, event := range batch {
			key := telemetryKey(event)
			segment.keys = append(segment.keys, key)
			if i >= ack {
				spool.refs[key] = append(spool.refs[key], telemetryRef{segment, i})
				events = append(events, event)
			}
		}
	}
	return spool, events, nil
}

// Called with eventsMu held. One durable segment per received batch avoids
// rewriting the whole queue and bounds the work even during a Controller outage.
func (spool *telemetrySpool) append(events []model.TraceEvent) error {
	if spool == nil || len(events) == 0 {
		return nil
	}
	data, e := json.Marshal(events)
	if e != nil {
		return e
	}
	name := fmt.Sprintf("%020d-%s.json", time.Now().UnixNano(), rand.Text())
	if e = writeTelemetryFile(filepath.Join(spool.directory, name), data); e != nil {
		return e
	}
	segment := &telemetrySegment{name: name}
	for i, event := range events {
		key := telemetryKey(event)
		segment.keys = append(segment.keys, key)
		spool.refs[key] = append(spool.refs[key], telemetryRef{segment, i})
	}
	return nil
}
func (spool *telemetrySpool) acknowledge(events []model.TraceEvent) error {
	if spool == nil {
		return nil
	}
	used := map[string]int{}
	selected := map[*telemetrySegment][]int{}
	for _, event := range events {
		key := telemetryKey(event)
		refs := spool.refs[key]
		n := used[key]
		if n < len(refs) {
			ref := refs[n]
			used[key]++
			selected[ref.segment] = append(selected[ref.segment], ref.index)
		}
	}
	for segment, indices := range selected {
		sort.Ints(indices)
		next := segment.ack
		for _, index := range indices {
			if index != next {
				return errors.New("out-of-order telemetry acknowledgement")
			}
			next++
		}
		path := filepath.Join(spool.directory, segment.name)
		if e := writeTelemetryFile(path+".ack", []byte(strconv.Itoa(next))); e != nil {
			return e
		}
		for i := segment.ack; i < next; i++ {
			key := segment.keys[i]
			refs := spool.refs[key]
			for j, ref := range refs {
				if ref.segment == segment && ref.index == i {
					refs = append(refs[:j], refs[j+1:]...)
					break
				}
			}
			if len(refs) == 0 {
				delete(spool.refs, key)
			} else {
				spool.refs[key] = refs
			}
		}
		segment.ack = next
		if next == len(segment.keys) {
			if e := os.Remove(path); e != nil && !errors.Is(e, os.ErrNotExist) {
				return e
			}
			if e := syncTelemetryDirectory(spool.directory); e != nil {
				return e
			}
			_ = os.Remove(path + ".ack")
		}
	}
	return nil
}
