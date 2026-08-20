package directory

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Service struct{ Base string }
type Prepared struct {
	Path    string
	Created bool
}

func (s Service) Normalize(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", errors.New("directory path is empty")
	}
	if path == "~" || strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		path = filepath.Join(home, strings.TrimPrefix(path, "~/"))
	}
	if !filepath.IsAbs(path) {
		base := s.Base
		if base == "" {
			var err error
			base, err = os.Getwd()
			if err != nil {
				return "", err
			}
		}
		path = filepath.Join(base, path)
	}
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	return abs, nil
}
func (s Service) Prepare(path string, create bool) (Prepared, error) {
	normalized, err := s.Normalize(path)
	if err != nil {
		return Prepared{}, err
	}
	info, err := os.Stat(normalized)
	if err == nil {
		if !info.IsDir() {
			return Prepared{}, fmt.Errorf("%s is not a directory", normalized)
		}
		return Prepared{Path: normalized}, nil
	}
	if !os.IsNotExist(err) {
		return Prepared{}, err
	}
	if !create {
		return Prepared{}, fmt.Errorf("directory does not exist: %s", normalized)
	}
	if err := os.MkdirAll(normalized, 0o755); err != nil {
		return Prepared{}, fmt.Errorf("create directory %s: %w", normalized, err)
	}
	return Prepared{Path: normalized, Created: true}, nil
}
