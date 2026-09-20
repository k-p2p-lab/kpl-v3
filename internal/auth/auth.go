package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"strings"
)

// Validate rejects incomplete credentials instead of starting a public service.
func Validate(user, password string) error {
	if user == "" || password == "" {
		return fmt.Errorf("KPL_USER and KPL_PASSWORD are required")
	}
	if len(user) > 256 || strings.TrimSpace(user) != user || strings.ContainsAny(user, "\r\n") {
		return fmt.Errorf("KPL_USER must be a nonblank single-line username of at most 256 bytes")
	}
	if len(password) > 4096 || strings.ContainsAny(password, "\r\n") {
		return fmt.Errorf("KPL_PASSWORD must be a single-line password of at most 4096 bytes")
	}
	return nil
}

// InternalToken is domain-separated from browser credentials and never sent to the UI.
func InternalToken(user, password string) string {
	mac := hmac.New(sha256.New, []byte(password))
	_, _ = mac.Write([]byte("kpl/internal-service-auth/v1\x00" + user))
	return hex.EncodeToString(mac.Sum(nil))
}

func Equal(a, b string) bool {
	left, right := sha256.Sum256([]byte(a)), sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(left[:], right[:]) == 1
}
