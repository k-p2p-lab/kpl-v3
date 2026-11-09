//go:build !linux

package runtimehealth

import "time"

func processObservation(time.Time) observation {
	return observation{unavailable: []string{
		"rssBytes", "rssAnonBytes", "rssFileBytes", "rssShmemBytes",
		"openFDs", "maxOpenFDs", "hardMaxOpenFDs", "cpuUserTicks", "cpuSystemTicks",
		"clockTicksPerSecond", "processStartedAt", "processUptimeSeconds",
	}}
}
