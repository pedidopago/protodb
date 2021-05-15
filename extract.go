package protodb

import (
	"fmt"
	"reflect"
)

func extract(v interface{}, tags ...string) ([]string, error) {
	x := make([]string, 0)
	err := extractStep(reflect.ValueOf(v), tags, &x)
	return x, err
}

func extractStep(v reflect.Value, tags []string, x *[]string) error {
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
				*x = append(*x, tt)
				// parts := strings.Split(tt, ",")
				// if strings.TrimSpace(parts[0]) != "-" {}
				break
			}
		}
		switch srcfield.Type.Kind() {
		case reflect.Struct, reflect.Ptr:
			if err := extractStep(v.Field(i), tags, x); err != nil {
				return err
			}
		}
	}
	return nil
}
