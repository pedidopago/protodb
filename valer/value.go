package valer

import "reflect"

func WrapValue(v reflect.Value) reflect.Value {
	// Converting custom interfaces to driver.Valuer
	vi := v.Interface()
	if valS, ok := vi.(StringValer); ok {
		return reflect.ValueOf(WrapStringValuer(valS))
	} else if valB, ok := vi.(BoolValer); ok {
		return reflect.ValueOf(WrapBoolValuer(valB))
	} else if valI32, ok := vi.(Int32Valer); ok {
		return reflect.ValueOf(WrapInt32Valuer(valI32))
	} else if valI64, ok := vi.(Int64Valer); ok {
		return reflect.ValueOf(WrapInt64Valuer(valI64))
	} else if valT, ok := vi.(TimeValer); ok {
		return reflect.ValueOf(WrapTimeValuer(valT))
	}
	return v
}
