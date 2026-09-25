package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
)

func sourceRevisionOf(files []resultFile) string {
	h := sha256.New()
	for _, name := range []string{"scenario.yaml", "experiment.json", "events.jsonl", "observations.jsonl"} {
		var size, modified int64
		var visit func(resultFile)
		visit = func(f resultFile) {
			if f.size == 0 {
				return
			}
			if len(f.parts) > 0 {
				for _, p := range f.parts {
					visit(p)
				}
				return
			}
			size += f.size
			if f.remote != nil {
				modified = max(modified, f.remote.ModifiedAt)
			} else if f.info != nil {
				modified = max(modified, f.info.ModTime().UnixNano())
			}
		}
		for _, file := range files {
			if file.name == name {
				visit(file)
			}
		}
		fmt.Fprintf(h, "%s:%d:%d;", name, size, modified)
	}
	return hex.EncodeToString(h.Sum(nil))
}
func sourceRevisionAt(root *os.Root, id string) (string, error) {
	manifest, err := readRunArchive(root, id)
	if err != nil {
		return "", err
	}
	var files []resultFile
	defer func() {
		for _, f := range files {
			f.close()
		}
	}()
	for _, name := range []string{"scenario.yaml", "experiment.json", "events.jsonl", "observations.jsonl"} {
		f, e := captureRunSource(root, manifest, name)
		if e != nil {
			if os.IsNotExist(e) && isRunLog(name) {
				continue
			}
			return "", e
		}
		files = append(files, f)
	}
	return sourceRevisionOf(files), nil
}
func (s *Server) runSourceRevision(id string) (string, error) {
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	root, err := s.analysisDirectory(id)
	if err != nil {
		return "", err
	}
	defer root.Close()
	return sourceRevisionAt(root, id)
}
