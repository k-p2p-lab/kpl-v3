package agent

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const telemetrySegmentBytes = 4 << 20
const telemetryFormatMarker = "format-v2.json"
const telemetryFormatData = `{"format":"kpl-telemetry-jsonl-v1"}`
const telemetryAckFrameBytes = 16
const telemetryAckMagic = 0x4b504c31

// Reuse an append-only segment instead of creating a payload file and replacing
// a cursor inode for every small Peer batch. Only one bounded segment stays open
// logically; file descriptors are closed after each write.
func (spool *telemetrySpool) appendLog(events []model.TraceEvent) error {
	if err := spool.ensureLogFormat(); err != nil {
		return err
	}
	data := make([]byte, 0)
	records := make([]telemetryRecord, 0, len(events))
	var bytes int64
	for _, event := range events {
		encoded, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if len(encoded) > model.MaxEventBatchBytes {
			return errors.New("telemetry event exceeds size limit")
		}
		records = append(records, telemetryRecord{offset: int64(len(data)), size: len(encoded), runID: event.RunID})
		data = append(data, encoded...)
		data = append(data, '\n')
		bytes += int64(len(encoded))
	}
	if len(data) > 64<<20 {
		return errors.New("telemetry segment exceeds size limit")
	}
	segment := spool.active
	if segment != nil && segment.size+int64(len(data)) > telemetrySegmentBytes {
		spool.active = nil
		if segment.ack == segment.count {
			if err := spool.removeSegment(segment); err != nil {
				spool.cleanup = segment
				return err
			}
		}
		segment = nil
	}
	if segment == nil {
		stamp := max(time.Now().UnixNano(), spool.lastStamp+1)
		segment = &telemetrySegment{name: fmt.Sprintf("%020d-%s.jsonl", stamp, rand.Text())}
		if err := writeTelemetryFile(filepath.Join(spool.directory, segment.name), data); err != nil {
			return err
		}
		spool.lastStamp = stamp
		spool.active = segment
	} else {
		if err := appendTelemetryFile(filepath.Join(spool.directory, segment.name), data, segment.size, false); err != nil {
			return err
		}
	}
	for i := range records {
		records[i].segment = segment
		records[i].offset += segment.size
	}
	segment.count += len(events)
	segment.size += int64(len(data))
	spool.records = append(spool.records, records...)
	spool.bytes += bytes
	return nil
}

// offset describes the last committed in-memory prefix. A failed write may
// leave an unacknowledged tail; truncate that tail before retrying, never bytes
// belonging to a batch for which the Agent returned success.
func appendTelemetryFile(path string, data []byte, offset int64, create bool) error {
	flags := os.O_WRONLY
	if create {
		flags |= os.O_CREATE
	}
	f, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() < offset {
		return errors.New("invalid telemetry append offset")
	}
	if info.Size() > offset {
		if err := f.Truncate(offset); err != nil {
			return err
		}
	}
	if n, err := f.WriteAt(data, offset); err != nil {
		return err
	} else if n != len(data) {
		return io.ErrShortWrite
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if create && offset == 0 {
		return syncTelemetryDirectory(filepath.Dir(path))
	}
	return nil
}

func (spool *telemetrySpool) acknowledgeLog(segment *telemetrySegment, next int) error {
	var frame [telemetryAckFrameBytes]byte
	binary.BigEndian.PutUint32(frame[:4], telemetryAckMagic)
	binary.BigEndian.PutUint64(frame[4:12], uint64(next))
	binary.BigEndian.PutUint32(frame[12:], crc32.ChecksumIEEE(frame[:12]))
	if err := appendTelemetryFile(filepath.Join(spool.directory, segment.name)+".ack", frame[:], segment.ackBytes, true); err != nil {
		return err
	}
	segment.ackBytes += int64(len(frame))
	return nil
}

func recoverTelemetryAck(path string, count int) (ack int, validBytes int64, err error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	if !info.Mode().IsRegular() || info.Size() > int64(count+1)*telemetryAckFrameBytes {
		return 0, 0, errors.New("invalid telemetry acknowledgement journal")
	}
	var frame [telemetryAckFrameBytes]byte
	for {
		n, readErr := io.ReadFull(f, frame[:])
		if readErr == io.EOF {
			return ack, validBytes, nil
		}
		valid := readErr == nil && binary.BigEndian.Uint32(frame[:4]) == telemetryAckMagic && binary.BigEndian.Uint32(frame[12:]) == crc32.ChecksumIEEE(frame[:12])
		if !valid {
			// Only the last, interrupted append can be ignored. Earlier corruption is
			// surfaced instead of silently skipping evidence. Replay remains at least once.
			if readErr != nil && readErr != io.ErrUnexpectedEOF {
				return 0, 0, readErr
			}
			if validBytes+int64(n) != info.Size() {
				return 0, 0, errors.New("corrupt telemetry acknowledgement journal")
			}
			if err := f.Truncate(validBytes); err != nil {
				return 0, 0, err
			}
			return ack, validBytes, f.Sync()
		}
		next := binary.BigEndian.Uint64(frame[4:12])
		if next <= uint64(ack) || next > uint64(count) {
			return 0, 0, errors.New("invalid telemetry acknowledgement sequence")
		}
		ack = int(next)
		validBytes += telemetryAckFrameBytes
	}
}

func (spool *telemetrySpool) recoverLog(segment *telemetrySegment) error {
	path := filepath.Join(spool.directory, segment.name)
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	var records []telemetryRecord
	var offset int64
	var tornTail bool
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				return err
			}
			// Verify the durable ACK before truncating: if it references this tail,
			// surface corruption and preserve the file for recovery.
			tornTail = len(line) > 0
			break
		}
		raw := line[:len(line)-1]
		if len(raw) > model.MaxEventBatchBytes {
			return errors.New("telemetry event exceeds size limit")
		}
		var identity struct {
			RunID string `json:"runId"`
		}
		if err := json.Unmarshal(raw, &identity); err != nil {
			return err
		}
		records = append(records, telemetryRecord{segment: segment, offset: offset, size: len(raw), runID: identity.RunID})
		offset += int64(len(line))
	}
	segment.count = len(records)
	segment.size = offset
	segment.ack, segment.ackBytes, err = recoverTelemetryAck(path+".ack", segment.count)
	if err != nil {
		return err
	}
	if tornTail {
		if err := f.Truncate(offset); err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			return err
		}
	}
	if segment.ack == segment.count {
		return spool.removeSegment(segment)
	}
	for _, record := range records[segment.ack:] {
		spool.bytes += int64(record.size)
		spool.records = append(spool.records, record)
	}
	return nil
}

// Older Agents only discover .json arrays. A durable format marker makes them
// refuse startup rather than silently ignoring accepted .jsonl evidence during
// a rollback. It can be removed after all new-format evidence has drained.
func (spool *telemetrySpool) ensureLogFormat() error {
	if spool.formatReady {
		return nil
	}
	path := filepath.Join(spool.directory, telemetryFormatMarker)
	info, err := os.Lstat(path)
	if err == nil {
		if !info.Mode().IsRegular() || info.Size() > 256 {
			return errors.New("invalid telemetry format marker")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !bytes.Equal(data, []byte(telemetryFormatData)) {
			return errors.New("unsupported telemetry spool format")
		}
	} else if errors.Is(err, os.ErrNotExist) {
		if err := writeTelemetryFile(path, []byte(telemetryFormatData)); err != nil {
			return err
		}
	} else {
		return err
	}
	spool.formatReady = true
	return nil
}

func (spool *telemetrySpool) releaseIdleLog() error {
	if spool == nil {
		return nil
	}
	for _, record := range spool.records {
		if strings.HasSuffix(record.segment.name, ".jsonl") {
			return nil
		}
	}
	if spool.cleanup != nil {
		if err := spool.removeSegment(spool.cleanup); err != nil {
			return err
		}
		spool.cleanup = nil
	}
	if spool.active != nil {
		if spool.active.ack != spool.active.count {
			return errors.New("telemetry segment still has pending events")
		}
		if err := spool.removeSegment(spool.active); err != nil {
			return err
		}
		spool.active = nil
	}
	if err := os.Remove(filepath.Join(spool.directory, telemetryFormatMarker)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			spool.formatReady = false
			return nil
		}
		return err
	}
	if err := syncTelemetryDirectory(spool.directory); err != nil {
		return err
	}
	spool.formatReady = false
	return nil
}
