//go:build linux

package runtimehealth

import (
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func fixtureStat(user, system, start string) []byte {
	fields := make([]string, 20)
	for i := range fields {
		fields[i] = "0"
	}
	fields[0], fields[11], fields[12], fields[19] = "S", user, system, start
	return []byte("123 (name with ) spaces (and parentheses)) " + strings.Join(fields, " "))
}

func fixtureAuxv(wordSize int, order binary.ByteOrder, ticks uint64) []byte {
	data := make([]byte, wordSize*4)
	if wordSize == 4 {
		order.PutUint32(data, 17)
		order.PutUint32(data[wordSize:], uint32(ticks))
	} else {
		order.PutUint64(data, 17)
		order.PutUint64(data[wordSize:], ticks)
	}
	return data
}

func fixtureProcessReader() processReader {
	return processReader{
		readFile: func(path string, _ int64) ([]byte, error) {
			switch path {
			case "/proc/self/status":
				return []byte("Name:\tignored\nVmRSS:\t15 kB\nRssAnon:\t10 kB\nRssFile:\t5 kB\nRssShmem:\t0 kB\n"), nil
			case "/proc/self/stat":
				return fixtureStat("12", "34", "40000"), nil
			case "/proc/self/auxv":
				return fixtureAuxv(strconv.IntSize/8, binary.NativeEndian, 250), nil
			}
			return nil, errors.New("unexpected file")
		},
		countFDs: func() (uint64, error) { return 0, nil },
		limits:   func() (uint64, uint64, error) { return 1024, 4096, nil },
		bootTime: func() (float64, error) { return 200.5, nil },
	}
}

func TestProcessObservationUsesExactUnitsAndNativeClockRate(t *testing.T) {
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.FixedZone("test", 9*60*60))
	o := observeProcess(now, fixtureProcessReader())
	attrs := attrMap(o)
	for field, want := range map[string]uint64{
		"rssBytes": 15 * 1024, "rssAnonBytes": 10 * 1024, "rssFileBytes": 5 * 1024, "rssShmemBytes": 0,
		"openFDs": 0, "maxOpenFDs": 1024, "hardMaxOpenFDs": 4096,
		"cpuUserTicks": 12, "cpuSystemTicks": 34, "clockTicksPerSecond": 250,
	} {
		if attrs[field] != want {
			t.Errorf("%s=%v want %d", field, attrs[field], want)
		}
	}
	wantStart := now.Add(-40500 * time.Millisecond).UTC()
	if len(o.unavailable) != 0 || attrs["processUptimeSeconds"] != 40.5 || attrs["processStartedAt"] != wantStart {
		t.Fatalf("non-100Hz process clock conversion failed: %+v attrs=%v", o, attrs)
	}
}

func TestProcessReadFailuresOmitMeasurements(t *testing.T) {
	failure := errors.New("sensitive details must not be logged")
	o := observeProcess(time.Now(), processReader{
		readFile: func(string, int64) ([]byte, error) { return nil, failure },
		countFDs: func() (uint64, error) { return 0, failure },
		limits:   func() (uint64, uint64, error) { return 0, 0, failure },
		bootTime: func() (float64, error) { return 0, failure },
	})
	if len(o.attrs) != 0 {
		t.Fatalf("failed measurements were fabricated: %v", attrMap(o))
	}
	slices.Sort(o.unavailable)
	want := slices.Clone(processFields)
	slices.Sort(want)
	if !reflect.DeepEqual(o.unavailable, want) || strings.Contains(strings.Join(o.unavailable, " "), "sensitive") {
		t.Fatalf("missing fields or leaked errors: %v", o.unavailable)
	}
}

func TestPartialRSSAndInvalidClockRemainUnknown(t *testing.T) {
	r := fixtureProcessReader()
	read := r.readFile
	r.readFile = func(path string, limit int64) ([]byte, error) {
		if path == "/proc/self/status" {
			return []byte("VmRSS: 0 kB\nRssAnon: -1 kB\nRssFile: 18014398509481984 kB\nRssShmem: 9 MB\n"), nil
		}
		if path == "/proc/self/auxv" {
			return fixtureAuxv(strconv.IntSize/8, binary.NativeEndian, 0), nil
		}
		return read(path, limit)
	}
	o := observeProcess(time.Now(), r)
	attrs := attrMap(o)
	if attrs["rssBytes"] != uint64(0) || attrs["cpuUserTicks"] != uint64(12) {
		t.Fatalf("valid zero or independently known CPU counter lost: %v", attrs)
	}
	for _, name := range []string{"rssAnonBytes", "rssFileBytes", "rssShmemBytes", "clockTicksPerSecond", "processStartedAt", "processUptimeSeconds"} {
		if _, exists := attrs[name]; exists || !slices.Contains(o.unavailable, name) {
			t.Errorf("%s should be unknown: attrs=%v missing=%v", name, attrs, o.unavailable)
		}
	}
}

func TestProcessStatParserPreservesCountersAndRejectsMalformedInput(t *testing.T) {
	user, system, start, err := parseProcessStat(fixtureStat("18446744073709551615", "0", "42"))
	if err != nil || user != math.MaxUint64 || system != 0 || start != 42 {
		t.Fatalf("process comm/counter parsing failed: %d %d %d %v", user, system, start, err)
	}
	for _, data := range [][]byte{nil, []byte("123 no parentheses"), []byte("123 (short) S 0"), fixtureStat("-1", "0", "42"), fixtureStat("0", "18446744073709551616", "42")} {
		if _, _, _, err := parseProcessStat(data); err == nil {
			t.Errorf("accepted malformed stat: %q", data)
		}
	}
}

func TestClockTickParserSupportsWordSizesAndByteOrders(t *testing.T) {
	for _, size := range []int{4, 8} {
		for _, order := range []binary.ByteOrder{binary.LittleEndian, binary.BigEndian} {
			if ticks, err := clockTicks(fixtureAuxv(size, order, 1000), size, order); err != nil || ticks != 1000 {
				t.Fatalf("size=%d order=%T: ticks=%d err=%v", size, order, ticks, err)
			}
		}
	}
	for _, data := range [][]byte{nil, {1}, make([]byte, 16), fixtureAuxv(8, binary.LittleEndian, 0)} {
		if _, err := clockTicks(data, 8, binary.LittleEndian); err == nil {
			t.Fatalf("accepted absent/zero clock rate: %v", data)
		}
	}
}

func TestSmallFileReadIsBoundedAndFDCountStreams(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sample")
	if err := os.WriteFile(path, []byte("12345"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readSmallFile(path, 4); err == nil {
		t.Fatal("oversized process file was not rejected")
	}
	if data, err := readSmallFile(path, 5); err != nil || string(data) != "12345" {
		t.Fatalf("exact file boundary failed: %q %v", data, err)
	}
	fdDir := filepath.Join(dir, "fds")
	if err := os.Mkdir(fdDir, 0700); err != nil {
		t.Fatal(err)
	}
	// More than one directory-read page, without colliding with this test
	// process's low-numbered directory descriptor.
	for i := 10000; i < 10257; i++ {
		if err := os.WriteFile(filepath.Join(fdDir, strconv.Itoa(i)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(fdDir, "not-an-fd"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if n, err := countFDs(fdDir); err != nil || n != 257 {
		t.Fatalf("streamed FD count = %d err=%v", n, err)
	}
}

func TestSelfProcessObservationDoesNotAccumulateFDs(t *testing.T) {
	before, err := countFDs("/proc/self/fd")
	if err != nil {
		t.Skipf("self FD inventory unavailable: %v", err)
	}
	for i := 0; i < 50; i++ {
		o := processObservation(time.Now())
		if _, exists := attrMap(o)["cpuUserTicks"]; !exists {
			t.Fatalf("self stat unavailable: %v", o.unavailable)
		}
	}
	after, err := countFDs("/proc/self/fd")
	if err != nil || after != before {
		t.Fatalf("observations changed FD baseline: before=%d after=%d err=%v", before, after, err)
	}
}
