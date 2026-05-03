package fsstorage

import (
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/runtime"
)

// getListItemsValue returns the reflect.Value of the items slice in a list object.
func getListItemsValue(itemsPtr interface{}) (reflect.Value, error) {
	v, err := conversionEnforcePtr(itemsPtr)
	if err != nil {
		return reflect.Value{}, err
	}
	if v.Kind() != reflect.Slice {
		return reflect.Value{}, fmt.Errorf("fsstorage: expected items slice, got %v", v.Kind())
	}
	return v, nil
}

// conversionEnforcePtr is a small re-implementation of meta.EnforcePtr scoped
// to slice pointers used by list types.
func conversionEnforcePtr(obj interface{}) (reflect.Value, error) {
	v := reflect.ValueOf(obj)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return reflect.Value{}, fmt.Errorf("fsstorage: nil pointer to slice")
		}
		return v.Elem(), nil
	}
	return v, nil
}

// appendItem appends obj to a slice reflect.Value. The slice element type is
// either the concrete struct (e.g. corev1.Pod) or runtime.Object.
func appendItem(items reflect.Value, obj runtime.Object) error {
	elemType := items.Type().Elem()
	if elemType.Kind() == reflect.Interface {
		items.Set(reflect.Append(items, reflect.ValueOf(obj)))
		return nil
	}
	// Slice of concrete struct: dereference pointer.
	v := reflect.ValueOf(obj)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if !v.Type().AssignableTo(elemType) {
		return fmt.Errorf("fsstorage: object of type %v not assignable to list element type %v", v.Type(), elemType)
	}
	items.Set(reflect.Append(items, v))
	return nil
}
