//go:build !linux

package agent

import "os"

// Production Agent execution requires Linux. Retain construction for tooling
// on other platforms without claiming an unsupported ownership mechanism.
func lockAgentDataDirectory(string) (*os.File, error) { return nil, nil }
