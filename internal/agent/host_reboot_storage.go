package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const hostRebootReceiptFile = "host-reboot.json"

type hostRebootReceipt struct {
	Version   int                     `json:"version"`
	Request   model.HostRebootRequest `json:"request"`
	Result    model.HostRebootResult  `json:"result"`
	UpdatedAt time.Time               `json:"updatedAt"`
}

func (s *Server) readHostRebootReceipt() (*hostRebootReceipt, error) {
	root, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	info, err := root.Lstat(hostRebootReceiptFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > 16<<10 {
		return nil, errors.New("invalid host reboot receipt file")
	}
	f, err := root.Open(hostRebootReceiptFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var receipt hostRebootReceipt
	decoder := json.NewDecoder(io.LimitReader(f, 16<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return nil, fmt.Errorf("read host reboot receipt: %w", err)
	}
	if decoder.Decode(new(any)) != io.EOF || receipt.Version != 1 || !validHostRebootRequest(receipt.Request) ||
		receipt.Request.OperationID != receipt.Result.OperationID || receipt.Request.NodeID != receipt.Result.NodeID || receipt.Request.BootID != receipt.Result.BootID ||
		(receipt.Result.State != "requested" && receipt.Result.State != "unknown") {
		return nil, errors.New("invalid host reboot receipt")
	}
	return &receipt, nil
}

// The receipt is committed before starting a helper. A crash can therefore
// leave an unknown operation, but can never silently authorize its replay.
func (s *Server) persistHostRebootReceipt(receipt *hostRebootReceipt) error {
	root, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return err
	}
	defer root.Close()
	file, err := os.CreateTemp(s.config.DataDir, ".host-reboot-*.tmp")
	if err != nil {
		return err
	}
	name := filepath.Base(file.Name())
	defer root.Remove(name)
	err = json.NewEncoder(file).Encode(receipt)
	if err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return err
	}
	if err := root.Rename(name, hostRebootReceiptFile); err != nil {
		return err
	}
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
