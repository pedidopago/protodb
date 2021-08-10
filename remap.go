package protodb

import (
	"errors"
	"reflect"
)

// RemapperDestination is for a struct that can be filled by src
type RemapperDestination interface {
	RemapFrom(src interface{}) error
}

// RemapperSource is for a struct that can be used to fill a destination
type RemapperSource interface {
	Remap() (interface{}, error)
}

// remap
func remap(v interface{}) error {
	var vval reflect.Value
	if v == nil {
		return errors.New("v is nil")
	}
	if vi, vok := v.(reflect.Value); vok {
		vval = vi
	} else {
		vval = reflect.ValueOf(v)
	}
	return remapStep(vval)
}

func remapStep(v reflect.Value) error {
	// if v is slice, then remap each element
	if v.Kind() == reflect.Slice {
		for i := 0; i < v.Len(); i++ {
			if err := remapStep(v.Index(i)); err != nil {
				return err
			}
		}
		return nil
	}
	// if v is a pointer, then remap the value pointed to
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil
		}
		return remapStep(v.Elem())
	}
	if v.Kind() != reflect.Struct {
		return nil
	}
	// if v is a struct, then remap each field
	srcindex := make(map[string]int)
	dstindex := make(map[string]int)
	for i := 0; i < v.NumField(); i++ {
		f := v.Type().Field(i)
		if v := f.Tag.Get("remapsrc"); v != "" {
			srcindex[v] = i
		} else if v := f.Tag.Get("remapdst"); v != "" {
			dstindex[v] = i
		}
	}
	for tname, dsti := range dstindex {
		if srci, ok := srcindex[tname]; ok {
			dstval := v.Field(dsti)
			srcval := v.Field(srci)
			if dstval.Kind() == reflect.Ptr {
				if dstval.IsNil() {
					dstval.Set(reflect.New(dstval.Type().Elem()))
				}
				if vint := dstval.Interface(); vint != nil {
					if vintm, ok := vint.(RemapperDestination); ok {
						if err := vintm.RemapFrom(srcval.Interface()); err != nil {
							return err
						}
					} else {
						if srci := srcval.Interface(); srci != nil {
							if srvn, ok := srci.(RemapperSource); ok {
								if dstvn, err := srvn.Remap(); err != nil {
									return err
								} else {
									dstval.Set(reflect.ValueOf(dstvn))
								}
							}
						}
					}
				}
			} else if dstval.CanSet() {
				if srci := srcval.Interface(); srci != nil {
					if srvn, ok := srci.(RemapperSource); ok {
						if dstvn, err := srvn.Remap(); err != nil {
							return err
						} else {
							dstval.Set(reflect.ValueOf(dstvn))
						}
					}
				}
			}
		} else {
			//TODO: log error (src not found)

		}
	}
	return nil
}

// RemapDestination
type RemapDestination interface {
	Remap(src interface{}) error
}
