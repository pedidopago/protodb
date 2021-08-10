package protodb

import (
	"errors"
	"reflect"
)

// TransformFunc is a function that transforms a value.
type TransformFunc func(interface{}) interface{}

// Transform uses rules of funcMap to transform values by checking the
// transform tag.
func Transform(v interface{}, funcMap map[string]TransformFunc) error {
	var vval reflect.Value
	if v == nil {
		return errors.New("v is nil")
	}
	if vi, vok := v.(reflect.Value); vok {
		vval = vi
	} else {
		vval = reflect.ValueOf(v)
	}
	return transformStep(vval, funcMap)
}

func transformStep(v reflect.Value, funcMap map[string]TransformFunc) error {
	// if v is slice, then remap each element
	if v.Kind() == reflect.Slice {
		for i := 0; i < v.Len(); i++ {
			if err := transformStep(v.Index(i), funcMap); err != nil {
				return err
			}
		}
		return nil
	}
	// if v is a pointer, then transform the value pointed to
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil
		}
		return transformStep(v.Elem(), funcMap)
	}
	if v.Kind() != reflect.Struct {
		return nil
	}
	// if v is a struct, then transform each field
	for i := 0; i < v.NumField(); i++ {
		f := v.Type().Field(i)
		xname := ""
		if v := f.Tag.Get("transform"); v != "" {
			xname = v
		} else {
			continue
		}
		xval := v.Field(i)
		if !xval.CanSet() {
			continue
		}
		fn := funcMap[xname]
		if fn == nil {
			continue
		}
		xval.Set(reflect.ValueOf(fn(xval.Interface())))
	}
	return nil
}
