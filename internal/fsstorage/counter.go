package fsstorage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// Counter owns the monotonic resourceVersion for a storage tree.
//
// Multiple Store instances rooted at the same directory MUST share a single
// Counter — otherwise each instance keeps its own in-memory counter and
// races on the on-disk <root>/.rv file.
type Counter struct {
	root string
	mu   sync.Mutex
	rv   uint64
}

// NewCounter loads the persisted RV from <root>/.rv (or initializes to 1).
func NewCounter(root string) (*Counter, error) {
	c := &Counter{root: root}
	rv, err := c.load()
	if err != nil {
		return nil, err
	}
	c.rv = rv
	return c, nil
}

func (c *Counter) file() string { return filepath.Join(c.root, ".rv") }

func (c *Counter) load() (uint64, error) {
	b, err := os.ReadFile(c.file())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 1, nil
		}
		return 0, err
	}
	str := strings.TrimSpace(string(b))
	if str == "" {
		return 1, nil
	}
	v, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("fsstorage: parse .rv: %w", err)
	}
	if v < 1 {
		v = 1
	}
	return v, nil
}

func (c *Counter) persist(rv uint64) error {
	tmp := c.file() + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(strconv.FormatUint(rv, 10)); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, c.file())
}

// Next bumps and persists the counter, returning the new value.
func (c *Counter) Next() (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rv++
	if err := c.persist(c.rv); err != nil {
		c.rv--
		return 0, err
	}
	return c.rv, nil
}

// Current returns the latest issued value without bumping.
func (c *Counter) Current() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rv
}
