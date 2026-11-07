package master

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

var sourceRevisionPattern = regexp.MustCompile(`^([a-f0-9]{40}|[a-f0-9]{64})$`)

// sourceBuildArguments stamps the source checkout, rather than the potentially
// older operator executable. Source archives without Git retain unknown source
// metadata; the executable content hash remains available in every build.
func (s *swarm) sourceBuildArguments(ctx context.Context) ([]string, error) {
	if _, err := os.Lstat(filepath.Join(s.a.Repo, ".git")); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("inspect build source metadata: %w", err)
	}
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	// An operator may be invoked from a Git hook or another worktree. Git's
	// repository-location environment overrides its working directory; discard
	// it so the image describes the checkout being built, not the caller's.
	queryApp := s.a.WithEnv(map[string]*string{
		"GIT_DIR": nil, "GIT_WORK_TREE": nil, "GIT_COMMON_DIR": nil,
		"GIT_INDEX_FILE": nil, "GIT_OBJECT_DIRECTORY": nil, "GIT_ALTERNATE_OBJECT_DIRECTORIES": nil,
	})
	revision, err := queryApp.Output(queryCtx, "git", "rev-parse", "--verify", "HEAD")
	if err != nil || !sourceRevisionPattern.MatchString(revision) {
		return nil, errors.New("cannot identify the build source revision")
	}
	status, err := queryApp.Output(queryCtx, "git", "status", "--porcelain", "--untracked-files=normal")
	if err != nil {
		return nil, errors.New("cannot identify whether the build source is modified")
	}
	return []string{
		"--build-arg", "KPL_SOURCE_REVISION=" + revision,
		"--build-arg", "KPL_SOURCE_MODIFIED=" + strconv.FormatBool(status != ""),
		"--build-arg", "KPL_BUILD_TIME=" + time.Now().UTC().Format(time.RFC3339),
	}, nil
}
