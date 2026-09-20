package auth

import "testing"

func TestCredentialValidationAndInternalKeySeparation(t *testing.T) {
	for _, credentials := range [][2]string{{"", ""}, {"admin", ""}, {"", "secret"}, {" admin", "secret"}, {"admin", "line\nbreak"}} {
		if Validate(credentials[0], credentials[1]) == nil {
			t.Fatal("accepted incomplete or invalid credentials")
		}
	}
	if err := Validate("admin", " spaces remain literal "); err != nil {
		t.Fatal(err)
	}
	key := InternalToken("admin", "secret")
	if len(key) != 64 || key == "secret" || key != InternalToken("admin", "secret") {
		t.Fatal("invalid deterministic internal key")
	}
	if Equal(key, InternalToken("other", "secret")) || Equal(key, InternalToken("admin", "changed")) {
		t.Fatal("credential changes did not rotate internal key")
	}
	if !Equal("same", "same") || Equal("same", "different") {
		t.Fatal("credential comparison failed")
	}
}
