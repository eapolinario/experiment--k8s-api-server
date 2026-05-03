package kubelet

import "k8s.io/apimachinery/pkg/labels"

// podSelectorAll returns a labels.Selector that matches every pod. We pull
// it out into its own helper so reconciler.go reads cleanly.
func podSelectorAll() labels.Selector { return labels.Everything() }
