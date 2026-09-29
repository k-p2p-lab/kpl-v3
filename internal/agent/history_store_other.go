//go:build !linux

package agent

import "errors"

func exchangeHistoryPaths(left, right string) error {
	return errors.New("legacy Agent history migration requires a Linux filesystem supporting atomic rename exchange")
}
