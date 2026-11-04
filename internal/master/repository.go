package master

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const runtimeModule = "github.com/k-p2p-lab/kpl-v3"

func hasModule(directory, module string) bool {
	data, err := os.ReadFile(filepath.Join(directory, "go.mod"))
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "module" {
			return strings.Trim(fields[1], "\"") == module
		}
	}
	return false
}

func findModule(explicit, module, cwd, executable string) (string, error) {
	if explicit != "" {
		path, err := filepath.Abs(explicit)
		if err != nil {
			return "", err
		}
		if !hasModule(path, module) {
			return "", fmt.Errorf("%s is not the %s source checkout", path, module)
		}
		return filepath.Clean(path), nil
	}
	seen := map[string]bool{}
	for _, start := range []string{cwd, filepath.Dir(executable)} {
		for dir := start; dir != ""; dir = filepath.Dir(dir) {
			for _, candidate := range []string{dir, filepath.Join(dir, "v3"), filepath.Join(dir, "kpl-v3")} {
				if seen[candidate] {
					continue
				}
				seen[candidate] = true
				if hasModule(candidate, module) {
					return candidate, nil
				}
			}
			if filepath.Dir(dir) == dir {
				break
			}
		}
	}
	return "", fmt.Errorf("cannot locate %s source checkout; specify --repo DIR", module)
}

func (a *App) resolveRepository(repo string) error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	executable, _ := os.Executable()
	if resolved, e := filepath.EvalSymlinks(executable); e == nil {
		executable = resolved
	}
	if repo == "" {
		repo = a.Env("KPL_REPO")
	}
	a.Repo, err = findModule(repo, runtimeModule, cwd, executable)
	return err
}
