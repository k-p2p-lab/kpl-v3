//go:build linux

package runtimehealth

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const smallFileLimit = 64 << 10

var processFields = []string{
	"rssBytes", "rssAnonBytes", "rssFileBytes", "rssShmemBytes",
	"openFDs", "maxOpenFDs", "hardMaxOpenFDs", "cpuUserTicks", "cpuSystemTicks",
	"clockTicksPerSecond", "processStartedAt", "processUptimeSeconds",
}

type processReader struct {
	readFile func(string, int64) ([]byte, error)
	countFDs func() (uint64, error)
	limits   func() (uint64, uint64, error)
	bootTime func() (float64, error)
}

func processObservation(now time.Time) observation {
	return observeProcess(now, processReader{
		readFile: readSmallFile,
		countFDs: func() (uint64, error) { return countFDs("/proc/self/fd") },
		limits: func() (uint64, uint64, error) {
			var limit unix.Rlimit
			err := unix.Getrlimit(unix.RLIMIT_NOFILE, &limit)
			return limit.Cur, limit.Max, err
		},
		bootTime: func() (float64, error) {
			var at unix.Timespec
			err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &at)
			return float64(at.Sec) + float64(at.Nsec)/1e9, err
		},
	})
}

func observeProcess(now time.Time, r processReader) observation {
	o := observation{}
	// Read the boot clock near the supplied wall-clock sample, before an FD
	// directory scan can delay this observation.
	boot, bootErr := r.bootTime()
	status, err := r.readFile("/proc/self/status", smallFileLimit)
	if err != nil {
		o.missing("rssBytes", "rssAnonBytes", "rssFileBytes", "rssShmemBytes")
	} else {
		appendRSS(&o, status)
	}
	if n, err := r.countFDs(); err == nil {
		o.attrs = append(o.attrs, slog.Uint64("openFDs", n))
	} else {
		o.missing("openFDs")
	}
	if soft, hard, err := r.limits(); err == nil {
		o.attrs = append(o.attrs, slog.Uint64("maxOpenFDs", soft), slog.Uint64("hardMaxOpenFDs", hard))
	} else {
		o.missing("maxOpenFDs", "hardMaxOpenFDs")
	}
	stat, err := r.readFile("/proc/self/stat", smallFileLimit)
	user, system, start, statErr := parseProcessStat(stat)
	if err != nil || statErr != nil {
		o.missing("cpuUserTicks", "cpuSystemTicks")
	} else {
		o.attrs = append(o.attrs, slog.Uint64("cpuUserTicks", user), slog.Uint64("cpuSystemTicks", system))
	}
	auxv, auxErr := r.readFile("/proc/self/auxv", 4<<10)
	hz, hzErr := clockTicks(auxv, strconv.IntSize/8, binary.NativeEndian)
	if auxErr != nil || hzErr != nil {
		o.missing("clockTicksPerSecond")
	} else {
		o.attrs = append(o.attrs, slog.Uint64("clockTicksPerSecond", hz))
	}
	uptime := boot - float64(start)/float64(hz)
	if err != nil || statErr != nil || auxErr != nil || hzErr != nil || bootErr != nil || math.IsNaN(uptime) || math.IsInf(uptime, 0) || uptime < 0 || uptime >= float64(math.MaxInt64)/float64(time.Second) {
		o.missing("processStartedAt", "processUptimeSeconds")
	} else {
		o.attrs = append(o.attrs, slog.Time("processStartedAt", now.Add(-time.Duration(uptime*float64(time.Second))).UTC()), slog.Float64("processUptimeSeconds", uptime))
	}
	return o
}

func readSmallFile(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(f, limit+1))
	closeErr := f.Close()
	if int64(len(data)) > limit {
		return nil, errors.New("process observation exceeds size limit")
	}
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	return data, nil
}

func countFDs(path string) (uint64, error) {
	dir, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	self := strconv.FormatUint(uint64(dir.Fd()), 10)
	var count uint64
	for {
		names, readErr := dir.Readdirnames(128)
		for _, name := range names {
			if name != self {
				if _, err := strconv.ParseUint(name, 10, 64); err == nil {
					count++
				}
			}
		}
		if readErr != nil {
			closeErr := dir.Close()
			if !errors.Is(readErr, io.EOF) {
				return 0, errors.Join(readErr, closeErr)
			}
			return count, closeErr
		}
	}
}

func appendRSS(o *observation, data []byte) {
	fields := map[string]string{"VmRSS:": "rssBytes", "RssAnon:": "rssAnonBytes", "RssFile:": "rssFileBytes", "RssShmem:": "rssShmemBytes"}
	known := make(map[string]bool, len(fields))
	for line := range strings.SplitSeq(string(data), "\n") {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		field, wanted := fields[parts[0]]
		if !wanted || known[field] || len(parts) != 3 || parts[2] != "kB" {
			continue
		}
		n, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil || n > math.MaxUint64/1024 {
			continue
		}
		known[field] = true
		o.attrs = append(o.attrs, slog.Uint64(field, n*1024))
	}
	for _, field := range fields {
		if !known[field] {
			o.missing(field)
		}
	}
}

func parseProcessStat(data []byte) (user, system, start uint64, err error) {
	// comm can contain whitespace and parentheses. Numeric fields begin after
	// the final closing parenthesis, with process state at field three.
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return 0, 0, 0, errors.New("invalid process stat")
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) < 20 {
		return 0, 0, 0, errors.New("short process stat")
	}
	values := []*uint64{&user, &system, &start}
	for i, index := range []int{11, 12, 19} {
		*values[i], err = strconv.ParseUint(fields[index], 10, 64)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("invalid process stat counter: %w", err)
		}
	}
	return user, system, start, nil
}

func clockTicks(data []byte, wordSize int, order binary.ByteOrder) (uint64, error) {
	if wordSize != 4 && wordSize != 8 || len(data)%(wordSize*2) != 0 {
		return 0, errors.New("invalid process auxiliary vector")
	}
	word := func(b []byte) uint64 {
		if wordSize == 4 {
			return uint64(order.Uint32(b))
		}
		return order.Uint64(b)
	}
	for offset := 0; offset < len(data); offset += wordSize * 2 {
		key, value := word(data[offset:]), word(data[offset+wordSize:])
		if key == 0 {
			break
		}
		if key == 17 && value != 0 { // Linux AT_CLKTCK.
			return value, nil
		}
	}
	return 0, errors.New("process clock tick rate unavailable")
}
