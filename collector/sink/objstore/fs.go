// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package objstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// FS writes objects to a local directory.
//
// It is the default for local development (UC-5) and the substrate for tests,
// and it is a legitimate production sink for a deployment that ships files
// onward by other means.
type FS struct {
	root string
}

// NewFS creates a filesystem store rooted at dir.
func NewFS(dir string) (*FS, error) {
	if dir == "" {
		return nil, fmt.Errorf("objstore: dir is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("objstore: create %s: %w", dir, err)
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	return &FS{root: abs}, nil
}

// Put writes via a temporary file and renames, so a reader never observes a
// half-written object (F-9.4). Rename within a directory is atomic on POSIX.
func (f *FS) Put(_ context.Context, key string, body []byte) error {
	path, err := f.path(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op once the rename succeeds

	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		return err
	}
	// fsync before rename: a rename that reaches disk before the contents
	// would expose an empty file after a crash.
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func (f *FS) Exists(_ context.Context, key string) (bool, error) {
	path, err := f.path(key)
	if err != nil {
		return false, err
	}
	_, err = os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (f *FS) Get(_ context.Context, key string) ([]byte, error) {
	path, err := f.path(key)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, ErrNotFound
	}
	return b, err
}

func (f *FS) Describe() string { return "file://" + f.root }
func (f *FS) Close() error     { return nil }

// Root exposes the base directory, for tooling that reads files directly.
func (f *FS) Root() string { return f.root }

// path resolves a key and refuses anything that would escape the root.
//
// Keys are built from producer-supplied values such as task_type (§7.1, "free
// form"), so a traversal here would let a producer write outside the configured
// prefix. The sink sanitises partition values too; this is the backstop.
func (f *FS) path(key string) (string, error) {
	clean := filepath.Clean("/" + strings.TrimPrefix(key, "/"))
	full := filepath.Join(f.root, clean)

	rel, err := filepath.Rel(f.root, full)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("objstore: key %q escapes the store root", key)
	}
	return full, nil
}
