// Package fsstorage implements k8s.io/apiserver/pkg/storage.Interface
// against a local filesystem tree, instead of etcd.
//
// Layout: <root>/<resource-plural>/<namespace>/<name>.json
// Cluster-scoped resources omit the <namespace> path segment.
//
// A monotonic uint64 resourceVersion is persisted to <root>/.rv and
// stamped into each written object.
package fsstorage
