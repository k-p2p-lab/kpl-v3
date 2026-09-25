package controller

import (
	"context"
	"errors"
	"os"
)

const resultGroupsDirectory = "result-groups"

// Group metadata stays local and independent of individual attempts and analysis
// output. Creation, note changes and group deletion are serialized by cancelMu.
func (s *Server) resultGroupDirectory(id string, create bool) (*os.Root, error) {
	if !validResultID(id) {
		return nil, errResultNotFound
	}
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return nil, err
	}
	defer data.Close()
	if create {
		if err := data.Mkdir(resultGroupsDirectory, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	parent, err := openResultDirectory(data, resultGroupsDirectory)
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	if create {
		if err := parent.Mkdir(id, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	return openResultDirectory(parent, id)
}

func (s *Server) resultGroupNote(ctx context.Context, id string, text *string, revision string) (resultNote, error) {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	root, err := s.resultGroupDirectory(id, false)
	if errors.Is(err, os.ErrNotExist) {
		if _, err := s.allBatchMembers(ctx, id); err != nil {
			return resultNote{}, err
		}
		if text == nil {
			return emptyResultNote(id, true), nil
		}
		root, err = s.resultGroupDirectory(id, true)
	}
	if err != nil {
		return resultNote{}, err
	}
	defer root.Close()
	if err := ctx.Err(); err != nil {
		return resultNote{}, err
	}
	return saveScopedNote(root, id, true, text, revision)
}

func (s *Server) resultGroupNoteSummary(id string) (*resultNoteSummary, error) {
	root, err := s.resultGroupDirectory(id, false)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer root.Close()
	note, err := readScopedNote(root, id, true)
	return note.summary(), err
}
