package webui

import (
	"bytes"
	"image/jpeg"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
	"golang.org/x/net/html"
)

func TestBrandLogoIsEmbeddedAndReferenced(t *testing.T) {
	asset, err := fs.ReadFile(FS(), "kpl-logo.jpg")
	if err != nil {
		t.Fatal(err)
	}
	config, err := jpeg.DecodeConfig(bytes.NewReader(asset))
	if err != nil || config.Width < 128 || config.Height < 128 {
		t.Fatalf("embedded logo is not a usable JPEG: %dx%d, %v", config.Width, config.Height, err)
	}
	content, err := fs.ReadFile(FS(), "index.html")
	if err != nil {
		t.Fatal(err)
	}
	document, err := html.Parse(strings.NewReader(string(content)))
	if err != nil {
		t.Fatal(err)
	}
	var logos []*html.Node
	var visit func(*html.Node)
	visit = func(node *html.Node) {
		if node.Type == html.ElementNode && node.Data == "img" {
			for _, attribute := range node.Attr {
				if attribute.Key == "class" && attribute.Val == "brand-mark" {
					logos = append(logos, node)
				}
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			visit(child)
		}
	}
	visit(document)
	if len(logos) != 1 {
		t.Fatalf("expected exactly one brand logo, got %d", len(logos))
	}
	attributes := make(map[string]string)
	present := make(map[string]bool)
	for _, attribute := range logos[0].Attr {
		attributes[attribute.Key], present[attribute.Key] = attribute.Val, true
	}
	if attributes["src"] != "/kpl-logo.jpg" || !present["alt"] || attributes["alt"] != "" || attributes["width"] != "42" || attributes["height"] != "42" {
		t.Fatalf("brand logo markup is incomplete: %#v", attributes)
	}
}

func TestDefaultScenarioMatchesSmokeExample(t *testing.T) {
	script, err := fs.ReadFile(FS(), "app.js")
	if err != nil {
		t.Fatal(err)
	}
	const prefix = "const defaultScenario = `"
	start := strings.Index(string(script), prefix)
	if start < 0 {
		t.Fatal("dashboard default scenario is missing")
	}
	remainder := string(script)[start+len(prefix):]
	end := strings.Index(remainder, "`;")
	if end < 0 {
		t.Fatal("dashboard default scenario has no closing delimiter")
	}
	got, err := scenario.Parse([]byte(remainder[:end]))
	if err != nil {
		t.Fatalf("parse dashboard default scenario: %v", err)
	}
	example, err := os.ReadFile(filepath.Join("..", "..", "examples", "smoke.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	want, err := scenario.Parse(example)
	if err != nil {
		t.Fatalf("parse smoke example: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("dashboard default scenario differs from examples/smoke.yaml")
	}
	if len(got.Phases) == 0 || got.Phases[len(got.Phases)-1].Action != "stop-all" {
		t.Fatal("dashboard smoke scenario must clean up its peer processes")
	}
}

func TestTopologyInspectorAndControlsStayInsideTopologyPanel(t *testing.T) {
	content, err := fs.ReadFile(FS(), "index.html")
	if err != nil {
		t.Fatal(err)
	}
	document, err := html.Parse(strings.NewReader(string(content)))
	if err != nil {
		t.Fatal(err)
	}
	attribute := func(node *html.Node, name string) (string, bool) {
		for _, item := range node.Attr {
			if item.Key == name {
				return item.Val, true
			}
		}
		return "", false
	}
	hasClass := func(node *html.Node, name string) bool {
		value, _ := attribute(node, "class")
		for _, class := range strings.Fields(value) {
			if class == name {
				return true
			}
		}
		return false
	}
	byID := make(map[string]*html.Node)
	var inspector *html.Node
	var visit func(*html.Node)
	visit = func(node *html.Node) {
		if id, found := attribute(node, "id"); found {
			if byID[id] != nil {
				t.Fatalf("duplicate UI id %q", id)
			}
			byID[id] = node
		}
		if hasClass(node, "topology-inspector") {
			inspector = node
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			visit(child)
		}
	}
	visit(document)
	if inspector == nil || !hasClass(inspector.Parent, "panel-body") || !hasClass(inspector.Parent.Parent, "topology-panel") {
		t.Fatal("Peer inspector must be inside the network tab body")
	}
	previous := inspector.PrevSibling
	for previous != nil && previous.Type != html.ElementNode {
		previous = previous.PrevSibling
	}
	if previous != byID["topologyStage"] {
		t.Fatal("Peer inspector must immediately follow the topology viewport")
	}
	for ancestor := inspector; ancestor != nil; ancestor = ancestor.Parent {
		if hasClass(ancestor, "results-panel") {
			t.Fatal("Peer inspector must not appear inside saved results")
		}
	}
	for id, checked := range map[string]bool{"showkademlia": false, "showgossipsub": false, "showtransport": false} {
		node := byID[id]
		if node == nil || node.Data != "input" {
			t.Fatalf("missing layer input %q", id)
		}
		if _, got := attribute(node, "checked"); got != checked {
			t.Errorf("layer %q checked=%t, want %t", id, got, checked)
		}
	}
}

func TestTopologyLayoutScriptIsEmbeddedAndLoadsBeforeApplication(t *testing.T) {
	for _, name := range []string{"topology-layout.js", "app.js"} {
		content, err := fs.ReadFile(FS(), name)
		if err != nil || len(content) == 0 {
			t.Fatalf("embedded script %s is missing or empty: %v", name, err)
		}
	}
	content, err := fs.ReadFile(FS(), "index.html")
	if err != nil {
		t.Fatal(err)
	}
	document, err := html.Parse(strings.NewReader(string(content)))
	if err != nil {
		t.Fatal(err)
	}
	var scripts []string
	var visit func(*html.Node)
	visit = func(node *html.Node) {
		if node.Type == html.ElementNode && node.Data == "script" {
			var source string
			deferred, async := false, false
			for _, attr := range node.Attr {
				switch attr.Key {
				case "src":
					source = attr.Val
				case "defer":
					deferred = true
				case "async":
					async = true
				}
			}
			if source == "/topology-layout.js" || source == "/app.js" {
				if !deferred || async {
					t.Errorf("%s must use ordered deferred loading", source)
				}
				scripts = append(scripts, source)
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			visit(child)
		}
	}
	visit(document)
	if !reflect.DeepEqual(scripts, []string{"/topology-layout.js", "/app.js"}) {
		t.Fatalf("layout engine must load exactly once before app.js; got %v", scripts)
	}
}
