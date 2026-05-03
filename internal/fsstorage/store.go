package fsstorage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
)

// Config configures a filesystem storage instance.
type Config struct {
	// Root is the base directory under which objects are stored.
	Root string
	// Codec is used to encode/decode objects on disk.
	Codec runtime.Codec
	// Newer constructs an empty object pointer (e.g. &corev1.Pod{}) used as
	// a decode target.
	Newer func() runtime.Object
	// NewerList constructs an empty list pointer (e.g. &corev1.PodList{}).
	NewerList func() runtime.Object
}

// New constructs a new filesystem-backed storage.Interface.
func New(cfg Config) (storage.Interface, error) {
	if cfg.Root == "" {
		return nil, fmt.Errorf("fsstorage: Root is required")
	}
	if cfg.Codec == nil {
		return nil, fmt.Errorf("fsstorage: Codec is required")
	}
	if cfg.Newer == nil {
		return nil, fmt.Errorf("fsstorage: Newer is required")
	}
	if err := os.MkdirAll(cfg.Root, 0o755); err != nil {
		return nil, fmt.Errorf("fsstorage: create root: %w", err)
	}
	s := &store{
		cfg:       cfg,
		versioner: storage.APIObjectVersioner{},
		broadcast: newBroadcaster(1000),
	}
	rv, err := s.loadRV()
	if err != nil {
		return nil, err
	}
	s.rv = rv
	return s, nil
}

type store struct {
	cfg       Config
	versioner storage.APIObjectVersioner
	broadcast *broadcaster

	rvMu sync.Mutex
	rv   uint64

	keyMuMap sync.Map // map[string]*sync.Mutex
}

func (s *store) Versioner() storage.Versioner { return s.versioner }

func (s *store) ReadinessCheck() error                            { return nil }
func (s *store) RequestWatchProgress(ctx context.Context) error   { return nil }
func (s *store) CompactRevision() int64                           { return 0 }
func (s *store) EnableResourceSizeEstimation(storage.KeysFunc) error {
	return nil
}

// --- RV management ---

func (s *store) rvFile() string { return filepath.Join(s.cfg.Root, ".rv") }

func (s *store) loadRV() (uint64, error) {
	b, err := os.ReadFile(s.rvFile())
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

func (s *store) persistRV(rv uint64) error {
	tmp := s.rvFile() + ".tmp"
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
	return os.Rename(tmp, s.rvFile())
}

// nextRV bumps and persists the global RV counter.
func (s *store) nextRV() (uint64, error) {
	s.rvMu.Lock()
	defer s.rvMu.Unlock()
	s.rv++
	if err := s.persistRV(s.rv); err != nil {
		s.rv--
		return 0, err
	}
	return s.rv, nil
}

func (s *store) currentRV() uint64 {
	s.rvMu.Lock()
	defer s.rvMu.Unlock()
	return s.rv
}

func (s *store) GetCurrentResourceVersion(ctx context.Context) (uint64, error) {
	return s.currentRV(), nil
}

// --- Key parsing & file paths ---

// parseKey splits a storage key like "/pods/default/nginx" or "/namespaces/foo"
// into (resource, namespace, name). For cluster-scoped resources, namespace == "".
// trailing slashes (recursive prefixes) are tolerated.
func parseKey(key string) (resource, namespace, name string, err error) {
	k := strings.Trim(key, "/")
	if k == "" {
		return "", "", "", fmt.Errorf("invalid key: %q", key)
	}
	parts := strings.Split(k, "/")
	switch len(parts) {
	case 1:
		return parts[0], "", "", nil
	case 2:
		// Could be /resource/name (cluster-scoped) OR /resource/namespace (prefix).
		// Caller distinguishes via Recursive. We default to cluster-scoped name
		// and let prefix-paths use parsePrefix.
		return parts[0], "", parts[1], nil
	case 3:
		return parts[0], parts[1], parts[2], nil
	default:
		return "", "", "", fmt.Errorf("invalid key %q: too many segments", key)
	}
}

// parsePrefix returns (resource, namespace, restrictsToName) where the bool is true
// only if the key targets a single object. Used by GetList/Watch when Recursive=true.
func parsePrefix(key string) (resource, namespace string, single bool, name string) {
	k := strings.Trim(key, "/")
	if k == "" {
		return "", "", false, ""
	}
	parts := strings.Split(k, "/")
	switch len(parts) {
	case 1:
		return parts[0], "", false, ""
	case 2:
		return parts[0], parts[1], false, ""
	default:
		return parts[0], parts[1], true, parts[2]
	}
}

func (s *store) objectPath(resource, namespace, name string) string {
	if namespace == "" {
		return filepath.Join(s.cfg.Root, resource, name+".json")
	}
	return filepath.Join(s.cfg.Root, resource, namespace, name+".json")
}

func (s *store) resourceDir(resource, namespace string) string {
	if namespace == "" {
		return filepath.Join(s.cfg.Root, resource)
	}
	return filepath.Join(s.cfg.Root, resource, namespace)
}

func (s *store) keyMu(path string) *sync.Mutex {
	mu, _ := s.keyMuMap.LoadOrStore(path, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

// --- Encode/decode helpers ---

func (s *store) encode(obj runtime.Object) ([]byte, error) {
	return runtime.Encode(s.cfg.Codec, obj)
}

func (s *store) decodeInto(data []byte, into runtime.Object) error {
	_, _, err := s.cfg.Codec.Decode(data, nil, into)
	return err
}

func (s *store) decodeFresh(data []byte) (runtime.Object, error) {
	obj := s.cfg.Newer()
	if err := s.decodeInto(data, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

// atomicWrite writes data to path via tmp+rename.
func atomicWrite(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
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
	return os.Rename(tmp, path)
}

// --- storage.Interface methods ---

func (s *store) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	resource, namespace, name, err := parseKey(key)
	if err != nil {
		return err
	}
	if name == "" {
		return fmt.Errorf("fsstorage: Create requires a name in key %q", key)
	}
	path := s.objectPath(resource, namespace, name)
	mu := s.keyMu(path)
	mu.Lock()
	defer mu.Unlock()

	if _, err := os.Stat(path); err == nil {
		return storage.NewKeyExistsError(key, 0)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	objCopy := obj.DeepCopyObject()
	accessor, err := meta.Accessor(objCopy)
	if err != nil {
		return err
	}
	if accessor.GetUID() == "" {
		accessor.SetUID(types.UID(generateUID()))
	}

	rv, err := s.nextRV()
	if err != nil {
		return err
	}
	if err := s.versioner.UpdateObject(objCopy, rv); err != nil {
		return err
	}

	data, err := s.encode(objCopy)
	if err != nil {
		return err
	}
	if err := atomicWrite(path, data); err != nil {
		return err
	}

	if out != nil {
		if err := s.decodeInto(data, out); err != nil {
			return err
		}
	}

	s.broadcast.publish(event{rv: rv, etype: watch.Added, obj: objCopy})
	return nil
}

func (s *store) Get(ctx context.Context, key string, opts storage.GetOptions, objPtr runtime.Object) error {
	resource, namespace, name, err := parseKey(key)
	if err != nil {
		return err
	}
	if name == "" {
		return fmt.Errorf("fsstorage: Get requires a name in key %q", key)
	}
	path := s.objectPath(resource, namespace, name)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if opts.IgnoreNotFound {
				// Leave objPtr as zero value.
				return nil
			}
			return storage.NewKeyNotFoundError(key, 0)
		}
		return err
	}
	return s.decodeInto(data, objPtr)
}

func (s *store) Delete(
	ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions,
	validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object,
	opts storage.DeleteOptions,
) error {
	resource, namespace, name, err := parseKey(key)
	if err != nil {
		return err
	}
	if name == "" {
		return fmt.Errorf("fsstorage: Delete requires a name in key %q", key)
	}
	path := s.objectPath(resource, namespace, name)
	mu := s.keyMu(path)
	mu.Lock()
	defer mu.Unlock()

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return storage.NewKeyNotFoundError(key, 0)
		}
		return err
	}
	cur, err := s.decodeFresh(data)
	if err != nil {
		if !opts.IgnoreStoreReadError {
			return err
		}
	}
	if cur != nil {
		if err := preconditions.Check(key, cur); err != nil {
			return err
		}
		if validateDeletion != nil {
			if err := validateDeletion(ctx, cur); err != nil {
				return err
			}
		}
	}

	if err := os.Remove(path); err != nil {
		return err
	}

	rv, err := s.nextRV()
	if err != nil {
		return err
	}

	if cur != nil {
		// Stamp the deletion RV onto the deleted object snapshot.
		_ = s.versioner.UpdateObject(cur, rv)
		if out != nil {
			data2, err := s.encode(cur)
			if err == nil {
				_ = s.decodeInto(data2, out)
			}
		}
		s.broadcast.publish(event{rv: rv, etype: watch.Deleted, obj: cur})
	}
	return nil
}

func (s *store) GuaranteedUpdate(
	ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool,
	preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object,
) error {
	resource, namespace, name, err := parseKey(key)
	if err != nil {
		return err
	}
	if name == "" {
		return fmt.Errorf("fsstorage: GuaranteedUpdate requires a name in key %q", key)
	}
	path := s.objectPath(resource, namespace, name)
	mu := s.keyMu(path)
	mu.Lock()
	defer mu.Unlock()

	var (
		cur     runtime.Object
		curData []byte
		curRV   uint64
		exists  bool
	)
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		exists = true
		cur, err = s.decodeFresh(data)
		if err != nil {
			return err
		}
		curData = data
		curRV, _ = s.versioner.ObjectResourceVersion(cur)
	case errors.Is(err, os.ErrNotExist):
		if !ignoreNotFound {
			return storage.NewKeyNotFoundError(key, 0)
		}
		cur = s.cfg.Newer()
	default:
		return err
	}

	if exists {
		if err := preconditions.Check(key, cur); err != nil {
			return err
		}
	}

	updated, _, err := tryUpdate(cur, storage.ResponseMeta{ResourceVersion: curRV})
	if err != nil {
		return err
	}

	newData, err := s.encode(updated)
	if err != nil {
		return err
	}
	// No-op short circuit: identical encoded contents.
	if exists && bytesEqual(newData, curData) {
		if destination != nil {
			return s.decodeInto(curData, destination)
		}
		return nil
	}

	rv, err := s.nextRV()
	if err != nil {
		return err
	}
	updatedCopy := updated.DeepCopyObject()
	if err := s.versioner.UpdateObject(updatedCopy, rv); err != nil {
		return err
	}
	// Preserve UID if missing (e.g. created via update).
	if exists {
		curAcc, _ := meta.Accessor(cur)
		newAcc, _ := meta.Accessor(updatedCopy)
		if curAcc != nil && newAcc != nil && newAcc.GetUID() == "" {
			newAcc.SetUID(curAcc.GetUID())
		}
	} else {
		newAcc, _ := meta.Accessor(updatedCopy)
		if newAcc != nil && newAcc.GetUID() == "" {
			newAcc.SetUID(types.UID(generateUID()))
		}
	}

	encoded, err := s.encode(updatedCopy)
	if err != nil {
		return err
	}
	if err := atomicWrite(path, encoded); err != nil {
		return err
	}
	if destination != nil {
		if err := s.decodeInto(encoded, destination); err != nil {
			return err
		}
	}

	etype := watch.Modified
	if !exists {
		etype = watch.Added
	}
	s.broadcast.publish(event{rv: rv, etype: etype, obj: updatedCopy})
	return nil
}

func (s *store) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	listPtr, err := meta.GetItemsPtr(listObj)
	if err != nil {
		return err
	}
	v, err := getListItemsValue(listPtr)
	if err != nil {
		return err
	}

	listRV := s.currentRV()

	if !opts.Recursive {
		// Single-object list (key is /resource/[ns/]name).
		resource, namespace, name, err := parseKey(key)
		if err != nil {
			return err
		}
		if name != "" {
			path := s.objectPath(resource, namespace, name)
			data, err := os.ReadFile(path)
			if err == nil {
				obj, derr := s.decodeFresh(data)
				if derr != nil {
					return derr
				}
				ok, merr := opts.Predicate.Matches(obj)
				if merr != nil {
					return merr
				}
				if ok {
					if err := appendItem(v, obj); err != nil {
						return err
					}
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		return s.versioner.UpdateList(listObj, listRV, "", nil)
	}

	resource, namespace, single, name := parsePrefix(key)
	if single {
		// /resource/ns/name treated as recursive: just that object.
		path := s.objectPath(resource, namespace, name)
		data, err := os.ReadFile(path)
		if err == nil {
			obj, derr := s.decodeFresh(data)
			if derr != nil {
				return derr
			}
			ok, merr := opts.Predicate.Matches(obj)
			if merr != nil {
				return merr
			}
			if ok {
				if err := appendItem(v, obj); err != nil {
					return err
				}
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return s.versioner.UpdateList(listObj, listRV, "", nil)
	}

	dir := s.resourceDir(resource, namespace)
	if err := walkObjects(dir, func(data []byte) error {
		obj, err := s.decodeFresh(data)
		if err != nil {
			return err
		}
		ok, err := opts.Predicate.Matches(obj)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		return appendItem(v, obj)
	}); err != nil {
		return err
	}
	return s.versioner.UpdateList(listObj, listRV, "", nil)
}

func (s *store) Count(key string) (int64, error) {
	resource, namespace, single, name := parsePrefix(key)
	if single {
		path := s.objectPath(resource, namespace, name)
		if _, err := os.Stat(path); err == nil {
			return 1, nil
		}
		return 0, nil
	}
	dir := s.resourceDir(resource, namespace)
	var n int64
	err := walkObjects(dir, func([]byte) error {
		n++
		return nil
	})
	return n, err
}

func (s *store) Stats(ctx context.Context) (storage.Stats, error) {
	var (
		count int64
		total int64
	)
	err := walkObjects(s.cfg.Root, func(data []byte) error {
		count++
		total += int64(len(data))
		return nil
	})
	if err != nil {
		return storage.Stats{}, err
	}
	avg := int64(0)
	if count > 0 {
		avg = total / count
	}
	return storage.Stats{ObjectCount: count, EstimatedAverageObjectSizeBytes: avg}, nil
}

func (s *store) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	var startRV uint64
	if opts.ResourceVersion != "" {
		v, err := s.versioner.ParseResourceVersion(opts.ResourceVersion)
		if err != nil {
			return nil, apierrors.NewBadRequest(fmt.Sprintf("invalid resource version: %v", err))
		}
		startRV = v
	}

	resource, namespace, single, name := parsePrefix(key)
	matchesKey := func(o runtime.Object) bool {
		acc, err := meta.Accessor(o)
		if err != nil {
			return false
		}
		gvk := o.GetObjectKind().GroupVersionKind()
		_ = gvk
		objNS := acc.GetNamespace()
		objName := acc.GetName()
		// Resource matching: compare to the resource segment from the key. Since
		// we only ever serve one resource per store, we don't reject on resource
		// here, but we filter by namespace/name.
		_ = resource
		if !opts.Recursive {
			// non-recursive: exact match
			if single {
				return objNS == namespace && objName == name
			}
			// /resource/ns/name with recursive=false is also exact match
			return false
		}
		if single {
			return objNS == namespace && objName == name
		}
		if namespace != "" && objNS != namespace {
			return false
		}
		return true
	}

	w := newWatcher(ctx, opts.Predicate, matchesKey, 100)

	// Subscribe BEFORE listing, so we don't miss events. We hold the broadcast
	// lock while determining the start point.
	s.broadcast.mu.Lock()
	currentRV := s.rv

	// Initial state delivery.
	if startRV == 0 {
		// Snapshot current state.
		var snap []runtime.Object
		dir := s.resourceDir(resource, namespace)
		if single {
			path := s.objectPath(resource, namespace, name)
			if data, err := os.ReadFile(path); err == nil {
				if obj, derr := s.decodeFresh(data); derr == nil {
					snap = append(snap, obj)
				}
			}
		} else {
			_ = walkObjects(dir, func(data []byte) error {
				if obj, err := s.decodeFresh(data); err == nil {
					snap = append(snap, obj)
				}
				return nil
			})
		}
		for _, o := range snap {
			w.deliver(watch.Event{Type: watch.Added, Object: o})
		}
	} else if startRV < currentRV {
		// Need to replay from buffer.
		replay, ok := s.broadcast.replayLocked(startRV)
		if !ok {
			s.broadcast.mu.Unlock()
			w.stopWithError(apierrors.NewResourceExpired(
				fmt.Sprintf("too old resource version: %d (current: %d)", startRV, currentRV)))
			return w, nil
		}
		for _, ev := range replay {
			if !matchesKey(ev.obj) {
				continue
			}
			ok, err := opts.Predicate.Matches(ev.obj)
			if err != nil || !ok {
				continue
			}
			w.deliver(watch.Event{Type: ev.etype, Object: ev.obj.DeepCopyObject()})
		}
	}
	// Subscribe to live events.
	s.broadcast.subscribeLocked(w)
	s.broadcast.mu.Unlock()

	return w, nil
}

// --- helpers ---

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// walkObjects walks dir non-recursively at namespace level but recursively
// across namespace subdirs. It calls fn with the file contents of every .json
// file (excluding .tmp and .rv).
func walkObjects(dir string, fn func([]byte) error) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		path := filepath.Join(dir, e.Name())
		if e.IsDir() {
			if err := walkObjects(path, fn); err != nil {
				return err
			}
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".json") || strings.HasPrefix(name, ".") {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}
		if err := fn(data); err != nil {
			return err
		}
	}
	return nil
}

// generateUID produces a simple non-cryptographic uid for tests/dev.
var uidCounter struct {
	sync.Mutex
	n uint64
}

func generateUID() string {
	uidCounter.Lock()
	defer uidCounter.Unlock()
	uidCounter.n++
	return fmt.Sprintf("fsstorage-uid-%d", uidCounter.n)
}
