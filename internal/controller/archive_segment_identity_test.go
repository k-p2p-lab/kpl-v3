package controller

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestRunArchiveSegmentIdentityValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		valid  bool
		change func(*runArchiveManifestData)
	}{
		{"distinct sealed segments", true, func(*runArchiveManifestData) {}},
		{"legacy empty segments", true, func(m *runArchiveManifestData) {
			m.Logs["events.jsonl"][0].Segment, m.Logs["events.jsonl"][1].Segment = "", ""
		}},
		{"two streams share a suffix", true, func(m *runArchiveManifestData) {
			m.Logs["observations.jsonl"] = []storedRunFile{{Object: "observations.jsonl", Segment: "observations-segment-one.jsonl", Size: 4}}
		}},
		{"object shared across streams", true, func(m *runArchiveManifestData) {
			m.Logs["observations.jsonl"] = []storedRunFile{{Object: "first.jsonl", Segment: "observations-segment-one.jsonl", Size: 4}}
		}},
		{"duplicate segment with different objects", false, func(m *runArchiveManifestData) {
			m.Logs["events.jsonl"][1].Segment = m.Logs["events.jsonl"][0].Segment
		}},
		{"duplicate object with different segments", false, func(m *runArchiveManifestData) {
			m.Logs["events.jsonl"][1].Object = m.Logs["events.jsonl"][0].Object
		}},
		{"segment belongs to another stream", false, func(m *runArchiveManifestData) {
			m.Logs["events.jsonl"][1].Segment = "observations-segment-two.jsonl"
		}},
		{"unsafe object", false, func(m *runArchiveManifestData) { m.Logs["events.jsonl"][1].Object = "../outside" }},
		{"missing files map", false, func(m *runArchiveManifestData) { m.Files = nil }},
		{"missing logs map", false, func(m *runArchiveManifestData) { m.Logs = nil }},
		{"wrong Run identity", false, func(m *runArchiveManifestData) { m.RunID = "another-run" }},
		{"wrong manifest version", false, func(m *runArchiveManifestData) { m.Version++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			root, err := os.OpenRoot(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			manifest := runArchiveManifestData{Version: 1, RunID: "segment-identity", Files: map[string]storedRunFile{}, Logs: map[string][]storedRunFile{
				"events.jsonl": {{Object: "first.jsonl", Segment: "events-segment-one.jsonl", Size: 4}, {Object: "second.jsonl", Segment: "events-segment-two.jsonl", Size: 4}},
			}}
			test.change(&manifest)
			if err := writeRunArchive(root, manifest); err != nil {
				t.Fatal(err)
			}
			parsed, err := readRunArchive(root, "segment-identity")
			if test.valid {
				if err != nil {
					t.Fatalf("compatible archive manifest was rejected: %v", err)
				}
				return
			}
			if err != nil {
				return
			}
			if test.name == "duplicate segment with different objects" {
				payload := []byte("one\n")
				if err := root.WriteFile("events-segment-one.jsonl", payload, 0600); err != nil {
					t.Fatal(err)
				}
				captured, err := captureRunSource(root, parsed, "events.jsonl")
				if err != nil {
					t.Fatal(err)
				}
				defer captured.close()
				data, err := io.ReadAll(captured.reader())
				if err != nil {
					t.Fatal(err)
				}
				t.Fatalf("one sealed segment was accepted twice: logicalBytes=%d physicalBytes=%d duplicated=%t", len(data), len(payload), bytes.Equal(data, []byte("one\none\n")))
			}
			t.Fatal("invalid archive identity was accepted")
		})
	}
}

func TestRunArchiveRejectsPartialOrTrailingManifest(t *testing.T) {
	for _, payload := range []string{`{"version":1`, `{"version":1,"runId":"partial","files":{},"logs":{}} {}`} {
		root, err := os.OpenRoot(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		if err := root.WriteFile(runArchiveManifest, []byte(payload), 0600); err != nil {
			root.Close()
			t.Fatal(err)
		}
		_, err = readRunArchive(root, "partial")
		root.Close()
		if err == nil {
			t.Fatalf("partial or trailing manifest was accepted: %s", payload)
		}
	}
}
