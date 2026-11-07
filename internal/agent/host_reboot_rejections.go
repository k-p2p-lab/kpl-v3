package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const hostRebootRejectionsDirectory = "host-reboot-rejections"

type hostRebootRejection struct {
	Version int                     `json:"version"`
	Request model.HostRebootRequest `json:"request"`
	Result  model.HostRebootResult  `json:"result"`
	Status  int                     `json:"status"`
}

func (s *Server) rebootRejectionsRoot(create bool) (*os.Root, error) {
	root, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	if create {
		if err := root.Mkdir(hostRebootRejectionsDirectory, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	info, err := root.Lstat(hostRebootRejectionsDirectory)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("invalid reboot rejection directory")
	}
	if create {
		directory, err := root.Open(".")
		if err != nil {
			return nil, err
		}
		err = errors.Join(directory.Sync(), directory.Close())
		if err != nil {
			return nil, err
		}
	}
	return root.OpenRoot(hostRebootRejectionsDirectory)
}

func hostRebootRejectionName(operation string) string {
	hash := sha256.Sum256([]byte(operation))
	return hex.EncodeToString(hash[:]) + ".json"
}

func (s *Server) readHostRebootRejection(request model.HostRebootRequest) (*hostRebootRejection, error) {
	root, err := s.rebootRejectionsRoot(false)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer root.Close()
	name := hostRebootRejectionName(request.OperationID)
	info, err := root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > 16<<10 {
		return nil, errors.New("invalid reboot rejection receipt")
	}
	file, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var receipt hostRebootRejection
	decoder := json.NewDecoder(io.LimitReader(file, 16<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return nil, err
	}
	if decoder.Decode(new(any)) != io.EOF || receipt.Version != 1 || !validHostRebootRequest(receipt.Request) || receipt.Result.Signature != "" || receipt.Result.State != "rejected" || receipt.Status < 400 || receipt.Status >= 600 || receipt.Result.OperationID != receipt.Request.OperationID || receipt.Result.NodeID != receipt.Request.NodeID || receipt.Result.BootID != receipt.Request.BootID {
		return nil, errors.New("invalid reboot rejection receipt")
	}
	if receipt.Request.OperationID != request.OperationID {
		return nil, errors.New("reboot rejection receipt belongs to a different operation")
	}
	if receipt.Request.NodeID != request.NodeID || receipt.Request.BootID != request.BootID || !receipt.Request.AgentStartedAt.Equal(request.AgentStartedAt) {
		// A previous boot/Agent instance cannot authorize the current request.
		// Old request signatures are rejected by the actor fence before this read.
		return nil, nil
	}
	return &receipt, nil
}

func (s *Server) persistHostRebootRejection(request model.HostRebootRequest, result model.HostRebootResult, status int) error {
	root, err := s.rebootRejectionsRoot(true)
	if err != nil {
		return err
	}
	defer root.Close()
	file, err := os.CreateTemp(filepath.Join(s.config.DataDir, hostRebootRejectionsDirectory), ".rejection-*.tmp")
	if err != nil {
		return err
	}
	name := filepath.Base(file.Name())
	defer root.Remove(name)
	err = json.NewEncoder(file).Encode(hostRebootRejection{Version: 1, Request: request, Result: result, Status: status})
	if err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return err
	}
	if err = root.Rename(name, hostRebootRejectionName(request.OperationID)); err != nil {
		return err
	}
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync reboot rejection receipt: %w", err)
	}
	return nil
}
