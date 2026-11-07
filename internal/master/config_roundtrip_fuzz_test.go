package master

import (
	"reflect"
	"testing"
)

// Configuration is a literal format. Any accepted value must survive a write
// and read without shell expansion, quote loss, or a different effective key.
func FuzzConfigLiteralRoundTrip(f *testing.F) {
	for i, key := range ConfigKeys {
		seeds := map[string][]string{
			"KPL_PASSWORD":                  {"$(touch literal-only)", "'quoted'", "\"quoted\"", "a=b#c", "literal \\n value", "x\xffy"},
			"KPL_IMAGE":                     {"registry.example.test/kpl:latest", "registry.example.test:5000/team/kpl:v3"},
			"KPL_PEER_SUBNET":               {"", "10.4.0.0/16"},
			"KPL_AGENT_HOST_REBOOT_ENABLED": {"false", "true"},
			"KPL_HTTP_PORT":                 {"8080", "65535"},
		}
		for _, value := range seeds[key] {
			f.Add(uint8(i), value)
		}
	}
	f.Fuzz(func(t *testing.T, index uint8, value string) {
		key := ConfigKeys[int(index)%len(ConfigKeys)]
		if err := ValidateSetting(key, value); err != nil {
			return
		}
		original := Config{key: value}
		decoded, legacy, err := parseConfig(encodeConfig(original))
		if err != nil || legacy || !reflect.DeepEqual(decoded, original) {
			t.Fatalf("literal config did not round trip for %s: %v (legacy=%t)", key, err, legacy)
		}
	})
}

func FuzzConfigParseCanonicalRoundTrip(f *testing.F) {
	for _, data := range []string{
		"# config\nKPL_IMAGE=registry.example.test/kpl:latest\n",
		"KPL_PASSWORD='quoted fake value'\nKPL_PASSWORD=ignored duplicate\n",
		"KPL_API_TOKEN=legacy-fake-token\nKPL_USER=admin\n",
		"KPL_PEER_SUBNET=\nKPL_ARCHIVE_MIB_PER_SECOND=0\n",
	} {
		f.Add([]byte(data))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			return
		}
		parsed, _, err := parseConfig(data)
		if err != nil {
			return
		}
		if err := validateConfig(ConfigDefaults(parsed)); err != nil {
			return
		}
		decoded, legacy, err := parseConfig(encodeConfig(parsed))
		if err != nil || legacy || !reflect.DeepEqual(decoded, parsed) {
			t.Fatalf("canonical config did not round trip: %v (legacy=%t)", err, legacy)
		}
	})
}
