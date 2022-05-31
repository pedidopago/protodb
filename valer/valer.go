package valer

import (
	"database/sql/driver"
	"reflect"
	"time"
)

type StringValer interface {
	Val() *string
}

type StringValerWrapper struct {
	Valer StringValer
}

func (w StringValerWrapper) Value() (v driver.Value, err error) {
	if w.Valer == nil {
		return
	}
	if p := w.Valer.Val(); p != nil {
		v = *p
	}
	return
}

func WrapStringValuer(v StringValer) driver.Valuer {
	return StringValerWrapper{Valer: v}
}

type BoolValer interface {
	Val() *bool
}

type BoolValerWrapper struct {
	Valer BoolValer
}

func (w BoolValerWrapper) Value() (v driver.Value, err error) {
	if w.Valer == nil {
		return
	}
	if p := w.Valer.Val(); p != nil {
		v = *p
	}
	return
}

func WrapBoolValuer(v BoolValer) driver.Valuer {
	return BoolValerWrapper{Valer: v}
}

type Int32Valer interface {
	Val() *int32
}

type Int32ValerWrapper struct {
	Valer Int32Valer
}

func (w Int32ValerWrapper) Value() (v driver.Value, err error) {
	if w.Valer == nil {
		return
	}
	if p := w.Valer.Val(); p != nil {
		v = *p
	}
	return
}

func WrapInt32Valuer(v Int32Valer) driver.Valuer {
	return Int32ValerWrapper{Valer: v}
}

type Int64Valer interface {
	Val() *int64
}

type Int64ValerWrapper struct {
	Valer Int64Valer
}

func (w Int64ValerWrapper) Value() (v driver.Value, err error) {
	if w.Valer == nil {
		return
	}
	if p := w.Valer.Val(); p != nil {
		v = *p
	}
	return
}

func WrapInt64Valuer(v Int64Valer) driver.Valuer {
	return Int64ValerWrapper{Valer: v}
}

type TimeValer interface {
	Val() *time.Time
}

type TimeValerWrapper struct {
	Valer TimeValer
}

func (w TimeValerWrapper) Value() (v driver.Value, err error) {
	if w.Valer == nil {
		return
	}
	if p := w.Valer.Val(); p != nil {
		v = *p
	}
	return
}

func WrapTimeValuer(v TimeValer) driver.Valuer {
	return TimeValerWrapper{Valer: v}
}

func IsZeroValer(v reflect.Value) (isZero bool) {
	tt := v.Type()
	switch tt.Kind() {
	case reflect.Struct:
		switch tt.Name() {
		case "StringValerWrapper", "BoolValerWrapper", "Int32ValerWrapper", "Int64ValerWrapper", "TimeValerWrapper":
			isZero = reflect.ValueOf(v.FieldByName("Valer").Interface()).IsZero()
		}
	}
	return
}
