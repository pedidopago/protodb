package protodb

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

func extract(v interface{}, tags ...string) ([]TagData, error) {
	var vval reflect.Value
	if v == nil {
		return nil, errors.New("v is nil")
	}
	if vi, vok := v.(reflect.Value); vok {
		vval = vi
	} else {
		vval = reflect.ValueOf(v)
	}
	x := make([]TagData, 0)
	err := extractStep(vval, tags, &x)
	return x, err
}

func extractStep(v reflect.Value, tags []string, x *[]TagData) error {
	kind := v.Kind()
	switch kind {
	case reflect.Ptr:
		return extractStep(v.Elem(), tags, x)
	case reflect.Struct: //, reflect.Map:
		// okay
	default:
		return fmt.Errorf("invalid source kind %v", kind.String())
	}
	srcn := v.NumField()
	srcType := v.Type()
	for i := 0; i < srcn; i++ {
		srcfield := srcType.Field(i)
		for _, tag := range tags {
			if tt, ok := srcfield.Tag.Lookup(tag); ok {
				tms := strings.Split(tt, ",")
				item := TagData{
					Value: tms[0],
					Meta:  make(map[string]string),
				}
				if len(tms) > 1 {
					for _, v := range tms[1:] {
						keyval := strings.SplitN(v, "=", 2)
						if len(keyval) == 2 {
							item.Meta[keyval[0]] = keyval[1]
						}
					}
				}
				*x = append(*x, item)
				// parts := strings.Split(tt, ",")
				// if strings.TrimSpace(parts[0]) != "-" {}
				break
			}
		}
		switch srcfield.Type.Kind() {
		case reflect.Struct, reflect.Ptr:
			if err := extractStep(v.Field(i), tags, x); err != nil {
				//TODO: return recursive fields error without breaking higher levels
				_ = err
			}
		}
	}
	return nil
}
