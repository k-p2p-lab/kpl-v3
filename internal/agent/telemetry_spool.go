package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const (
	telemetryWindowBytes   = 1 << 20
	telemetryWindowEvents  = model.MaxEventBatchEvents
	telemetryBacklogBytes  = 256 << 20
	telemetryBacklogEvents = 50000
)

// Only offsets and run identities stay in memory. Payloads are read in a bounded
// window; even recovery never decodes the entire backlog into TraceEvent maps.
type telemetrySegment struct {
	name           string
	count, ack     int
	size, ackBytes int64
}
type telemetryRecord struct {
	segment *telemetrySegment
	offset  int64
	size    int
	runID   string
}
type telemetrySpool struct {
	directory   string
	records     []telemetryRecord
	bytes       int64
	lastStamp   int64
	cleanup     *telemetrySegment
	active      *telemetrySegment
	formatReady bool
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
func readTelemetrySpool(dataDir string) (*telemetrySpool, error) {
	directory := filepath.Join(dataDir, "telemetry-spool")
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	if err := syncTelemetryDirectory(dataDir); err != nil {
		return nil, err
	}
	spool := &telemetrySpool{directory: directory}
	// Retain names only, not FileInfo or event payloads. Existing segments are
	// compatible with previous versions, including partially acknowledged arrays.
	dir, err := os.Open(directory)
	if err != nil {
		return nil, err
	}
	var names []string
	for {
		entries, readErr := dir.ReadDir(128)
		for _, entry := range entries {
			if !entry.IsDir() && (strings.HasSuffix(entry.Name(), ".json") || strings.HasSuffix(entry.Name(), ".jsonl")) {
				names = append(names, entry.Name())
			}
		}
		if readErr != nil {
			_ = dir.Close()
			if readErr != io.EOF {
				return nil, readErr
			}
			break
		}
	}
	sort.Strings(names)
	for _, name := range names {
		if name == telemetryFormatMarker {
			if err := spool.ensureLogFormat(); err != nil {
				return nil, err
			}
			continue
		}
		if err := spool.recoverSegment(name); err != nil {
			return nil, fmt.Errorf("read telemetry spool %s: %w", name, err)
		}
	}
	if err := spool.releaseIdleLog(); err != nil {
		return nil, err
	}
	return spool, nil
}

func (spool *telemetrySpool) recoverSegment(name string) error {
	if stamp, err := strconv.ParseInt(strings.SplitN(name, "-", 2)[0], 10, 64); err == nil {
		spool.lastStamp = max(stamp, spool.lastStamp)
	}
	path := filepath.Join(spool.directory, name)
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > 64<<20 {
		return errors.New("invalid telemetry spool segment")
	}
	segment := &telemetrySegment{name: name}
	if strings.HasSuffix(name, ".jsonl") {
		return spool.recoverLog(segment)
	}
	data, err := os.ReadFile(path + ".ack")
	if err == nil {
		segment.ack, err = strconv.Atoi(string(data))
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('[') {
		return errors.New("invalid telemetry array")
	}
	for decoder.More() {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return err
		}
		if len(raw) > model.MaxEventBatchBytes {
			return errors.New("telemetry event exceeds size limit")
		}
		if segment.count >= segment.ack {
			var identity struct {
				RunID string `json:"runId"`
			}
			if err := json.Unmarshal(raw, &identity); err != nil {
				return err
			}
			spool.records = append(spool.records, telemetryRecord{segment: segment, offset: decoder.InputOffset() - int64(len(raw)), size: len(raw), runID: identity.RunID})
			spool.bytes += int64(len(raw))
		}
		segment.count++
	}
	if _, err = decoder.Token(); err != nil {
		return err
	}
	var trailing any
	if err = decoder.Decode(&trailing); err != io.EOF {
		return errors.New("trailing telemetry spool data")
	}
	if segment.ack < 0 || segment.ack > segment.count {
		return errors.New("invalid telemetry acknowledgement")
	}
	if segment.ack == segment.count {
		return spool.removeSegment(segment)
	}
	return nil
}

// Called with eventsMu held. A successful append means every event is durable;
// the HTTP handler can discard all decoded payloads as soon as it acknowledges.
func (spool *telemetrySpool) append(events []model.TraceEvent) error {
	if spool == nil || len(events) == 0 {
		return nil
	}
	return spool.appendLog(events)
}

func (spool *telemetrySpool) loadPrefix() ([]model.TraceEvent, error) {
	if spool.cleanup != nil {
		if err := spool.removeSegment(spool.cleanup); err != nil {
			return nil, err
		}
		spool.cleanup = nil
	}
	var events []model.TraceEvent
	var size int
	var f *os.File
	var segment *telemetrySegment
	defer func() {
		if f != nil {
			_ = f.Close()
		}
	}()
	for _, record := range spool.records {
		// Allow one individually valid large event, never an oversized multi-event window.
		if len(events) > 0 && (len(events) >= telemetryWindowEvents || size+record.size > telemetryWindowBytes) {
			break
		}
		if segment != record.segment {
			if f != nil {
				_ = f.Close()
			}
			var err error
			f, err = os.Open(filepath.Join(spool.directory, record.segment.name))
			if err != nil {
				return nil, err
			}
			segment = record.segment
		}
		var event model.TraceEvent
		if err := json.NewDecoder(io.NewSectionReader(f, record.offset, int64(record.size))).Decode(&event); err != nil {
			return nil, err
		}
		events = append(events, event)
		size += record.size
	}
	return events, nil
}

// Acknowledge only a delivered prefix. Return the committed count even if a
// later segment fails: retrying must never acknowledge unsent subsequent events.
func (spool *telemetrySpool) acknowledge(count int) (int, error) {
	if count < 0 || count > len(spool.records) {
		return 0, errors.New("invalid telemetry acknowledgement count")
	}
	committed := 0
	for committed < count {
		segment := spool.records[0].segment
		n := min(count-committed, segment.count-segment.ack)
		next := segment.ack + n
		var err error
		if strings.HasSuffix(segment.name, ".jsonl") {
			err = spool.acknowledgeLog(segment, next)
		} else {
			err = writeTelemetryFile(filepath.Join(spool.directory, segment.name)+".ack", []byte(strconv.Itoa(next)))
		}
		if err != nil {
			return committed, err
		}
		for _, record := range spool.records[:n] {
			spool.bytes -= int64(record.size)
		}
		clear(spool.records[:n])
		spool.records = spool.records[n:]
		if len(spool.records) == 0 {
			spool.records = nil
		}
		segment.ack = next
		committed += n
		if next == segment.count && segment != spool.active {
			if err := spool.removeSegment(segment); err != nil {
				spool.cleanup = segment
				return committed, err
			}
		}
	}
	return committed, nil
}

func (spool *telemetrySpool) removeSegment(segment *telemetrySegment) error {
	path := filepath.Join(spool.directory, segment.name)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := syncTelemetryDirectory(spool.directory); err != nil {
		return err
	}
	if err := os.Remove(path + ".ack"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
