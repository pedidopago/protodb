package protodb

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var TagSeparator = ";"

func extract(v interface{}, tagSeparators map[string]string, tags ...string) ([]TagData, error) {
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
	err := extractStep(vval, tagSeparators, tags, &x, nil)
	return x, err
}

func extractStep(v reflect.Value, tagSeparators map[string]string, tags []string, x *[]TagData, valrecursiveIf *ConditionalContextKey) error {
	kind := v.Kind()
	switch kind {
	case reflect.Ptr:
		return extractStep(v.Elem(), tagSeparators, tags, x, valrecursiveIf)
	case reflect.Struct: //, reflect.Map:
		// okay
	default:
		return fmt.Errorf("invalid source kind %v", kind.String())
	}
	srcn := v.NumField()
	srcType := v.Type()
	for i := 0; i < srcn; i++ {
		srcfield := srcType.Field(i)
		skipRecursive := false
		var recursiveIf *ConditionalContextKey
		for _, tag := range tags {
			ts := TagSeparator
			if tagSeparators != nil && tagSeparators[tag] != "" {
				ts = tagSeparators[tag]
			}
			// replace ''' with `
			tag = strings.Replace(tag, "'''", "`", -1)
			if tt, ok := srcfield.Tag.Lookup(tag); ok {
				tms := strings.Split(tt, ts)
				item := TagData{
					Name:        tms[0],
					Meta:        make(map[string]string),
					FieldName:   srcfield.Name,
					FieldValue:  v.Field(i),
					RecursiveIf: valrecursiveIf,
				}
				if len(tms) > 1 {
					for _, vf := range tms[1:] {
						if strings.TrimSpace(vf) == "" {
							continue
						}
						keyval := strings.SplitN(vf, "=", 2)
						if len(keyval) == 2 {
							switch keyval[0] {
							case "recursiveif":
								rif := IfKey(keyval[1])
								recursiveIf = &rif
							default:
								item.Meta[keyval[0]] = keyval[1]
							}
						} else {
							switch keyval[0] {
							case "norecursive", "skiprecursive":
								skipRecursive = true
							}
						}
					}
				}
				*x = append(*x, item)
				// parts := strings.Split(tt, ",")
				// if strings.TrimSpace(parts[0]) != "-" {}
				break
			}
		}
		if !skipRecursive {
			switch srcfield.Type.Kind() {
			case reflect.Struct, reflect.Ptr:
				vif := recursiveIf
				if vif == nil {
					vif = valrecursiveIf
				}
				if err := extractStep(v.Field(i), tagSeparators, tags, x, vif); err != nil {
					//TODO: return recursive fields error without breaking higher levels
					_ = err
				}
			}
		}
	}
	return nil
}
