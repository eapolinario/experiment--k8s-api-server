// Package fsstorage implements k8s.io/apiserver/pkg/storage.Interface
// against a local filesystem tree, instead of etcd.
//
// Layout: <root>/<resource-plural>/<namespace>/<name>.json
// Cluster-scoped resources omit the <namespace> path segment.
//
// TODO(fsstorage): implement Create, Get, GetList, Delete, GuaranteedUpdate,
// Watch, Count. Maintain a monotonic resourceVersion in <root>/.rv.
package fsstorage
