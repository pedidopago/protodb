package protodb

import (
	"errors"
	"fmt"
	"github.com/pedidopago/protodb/valer"
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
	err := extractStep(vval, tagSeparators, tags, &x, nil, nil)
	return x, err
}

func extractStep(v reflect.Value, tagSeparators map[string]string, tags []string, x *[]TagData, valrecursiveIf *ConditionalContextKey, parent *TagData) error {
	kind := v.Kind()
	switch kind {
	case reflect.Ptr:
		a1 := fmt.Sprintln(kind, v.Type())
		_ = a1
		return extractStep(v.Elem(), tagSeparators, tags, x, valrecursiveIf, parent)
	case reflect.Slice:
		// get zero value of v slice
		a1 := fmt.Sprintln(kind, v.Type(), v.Type().Elem())
		_ = a1
		var slcval reflect.Value
		if v.Type().Elem().Kind() == reflect.Ptr {
			slcval = reflect.New(v.Type().Elem().Elem())
		} else {
			slcval = reflect.New(v.Type().Elem())
		}
		// slcval := reflect.New(v.Type().Elem())
		return extractStep(slcval, tagSeparators, tags, x, valrecursiveIf, parent)
	case reflect.Struct: //, reflect.Map:
		// okay
	default:
		return fmt.Errorf("invalid source kind %v", kind.String())
	}
	srcn := v.NumField()
	srcType := v.Type()
	var table string
	for i := 0; i < srcn; i++ {
		srcfield := srcType.Field(i)
		srcFieldValue := v.Field(i)
		// replace ''' with `
		srcfield.Tag = reflect.StructTag(strings.Replace(string(srcfield.Tag), "'''", "`", -1))
		skipRecursive := false
		var recursiveIf *ConditionalContextKey
		var foundItem *TagData
		for _, tag := range tags {
			ts := TagSeparator
			if tagSeparators != nil && tagSeparators[tag] != "" {
				ts = tagSeparators[tag]
			}
			if tt, ok := srcfield.Tag.Lookup(tag); ok {
				tms := strings.Split(tt, ts)
				item := TagData{
					Name:        tms[0],
					Table:       table,
					Meta:        make(map[string]string),
					FieldName:   srcfield.Name,
					FieldValue:  v.Field(i),
					RecursiveIf: valrecursiveIf,
				}

				if v, ok := srcfield.Tag.Lookup("db"); ok {
					item.DbTag = v
				}
				if vjs, _ := srcfield.Tag.Lookup("json"); vjs != "" {
					if x := strings.IndexAny(vjs, ","); x != -1 {
						item.JSON.Name = vjs[:x]
					} else {
						item.JSON.Name = vjs
					}
				} else {
					item.JSON.Name = srcfield.Name
				}
				if parent != nil {
					item.JSON.Parent = parent.JSON.Name
					item.JSON.FullPath = parent.JSON.FullPath + "/" + item.JSON.Name
				} else {
					item.JSON.FullPath = "/" + item.JSON.Name
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
							case "table", "tablename":
								item.Table = keyval[1]
								table = keyval[1]
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
				if srcfield.Type.Kind() == reflect.Slice {
					item.IsSlice = true
				} else if srcfield.Type.Kind() == reflect.Ptr {
					if srcfield.Type.Elem().Kind() == reflect.Slice {
						item.IsSlice = true
					}
				}
				*x = append(*x, item)
				foundItem = &((*x)[len(*x)-1])
				// parts := strings.Split(tt, ",")
				// if strings.TrimSpace(parts[0]) != "-" {}
				break
			}
		}
		if !skipRecursive {
			akind := srcfield.Type.Kind()
			switch akind {
			case reflect.Struct, reflect.Ptr, reflect.Slice:
				aname := srcfield.Name
				atypename := srcfield.Type.String()
				switch atypename {
				case "impl.MessageState":
					// skip
				default:
				}
				if isTypeOK(atypename) && isNameOK(aname) {
					vif := recursiveIf
					if vif == nil {
						vif = valrecursiveIf
					}
					var fieldx = srcFieldValue
					if akind == reflect.Ptr && srcfield.Type.Elem().Kind() == reflect.Struct {
						if fieldx.IsNil() {
							fieldx = reflect.New(srcfield.Type.Elem())
						}
						if foundItem != nil {
							// Converting custom interfaces to driver.Valuer
							vi := srcFieldValue.Interface()
							if valS, ok := vi.(valer.StringValer); ok {
								foundItem.FieldValue = reflect.ValueOf(valer.WrapStringValuer(valS))
							} else if valB, ok := vi.(valer.BoolValer); ok {
								foundItem.FieldValue = reflect.ValueOf(valer.WrapBoolValuer(valB))
							} else if valI32, ok := vi.(valer.Int32Valer); ok {
								foundItem.FieldValue = reflect.ValueOf(valer.WrapInt32Valuer(valI32))
							} else if valI64, ok := vi.(valer.Int64Valer); ok {
								foundItem.FieldValue = reflect.ValueOf(valer.WrapInt64Valuer(valI64))
							} else if valT, ok := vi.(valer.TimeValer); ok {
								foundItem.FieldValue = reflect.ValueOf(valer.WrapTimeValuer(valT))
							} else {
								// This will probably return an error
							}
						}
					}
					if err := extractStep(fieldx, tagSeparators, tags, x, vif, foundItem); err != nil {
						//TODO: return recursive fields error without breaking higher levels
						_ = err
					}
				}
			}
		}
	}
	return nil
}

func isTypeOK(typename string) bool {
	switch typename {
	case "impl.MessageState":
		return false
	}
	return true
}

func isNameOK(name string) bool {
	nameUpper := strings.ToUpper(name)
	// switch name {
	// case "unknownFields":
	// 	return false
	// }
	return name[0] == nameUpper[0]
}
