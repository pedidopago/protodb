package valer

import (
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
	"time"
)

type optS struct {
	Value string
}
func (s *optS) Val() *string {
	if s == nil {
		return nil
	}
	v := s.Value
	return &v
}

type optB struct {
	Value bool
}

func (b *optB) Val() *bool {
	if b == nil {
		return nil
	}
	v := b.Value
	return &v
}

type optI32 struct {
	Value int32
}

func (i *optI32) Val() *int32 {
	if i == nil {
		return nil
	}
	v := i.Value
	return &v
}

type optI64 struct {
	Value int64
}

func (i *optI64) Val() *int64 {
	if i == nil {
		return nil
	}
	v := i.Value
	return &v
}

type optT struct {
	Value time.Time
}

func (t *optT) Val() *time.Time {
	if t == nil {
		return nil
	}
	v := t.Value
	return &v
}

func TestIsZeroValer(t *testing.T) {
	var s *optS = nil
	require.True(t, IsZeroValer(reflect.ValueOf(StringValerWrapper{Valer: s})))
	s = &optS{Value: ""}
	require.False(t, IsZeroValer(reflect.ValueOf(StringValerWrapper{Valer: s})))

	var b *optB = nil
	require.True(t, IsZeroValer(reflect.ValueOf(BoolValerWrapper{Valer: b})))
	b = &optB{Value: false}
	require.False(t, IsZeroValer(reflect.ValueOf(BoolValerWrapper{Valer: b})))

	var i32 *optI32 = nil
	require.True(t, IsZeroValer(reflect.ValueOf(Int32ValerWrapper{Valer: i32})))
	i32 = &optI32{Value: 0}
	require.False(t, IsZeroValer(reflect.ValueOf(Int32ValerWrapper{Valer: i32})))

	var i64 *optI64 = nil
	require.True(t, IsZeroValer(reflect.ValueOf(Int64ValerWrapper{Valer: i64})))
	i64 = &optI64{Value: 0}
	require.False(t, IsZeroValer(reflect.ValueOf(Int64ValerWrapper{Valer: i64})))

	var ti *optT = nil
	require.True(t, IsZeroValer(reflect.ValueOf(TimeValerWrapper{Valer: ti})))
	ti = &optT{Value: time.Now()}
	require.False(t, IsZeroValer(reflect.ValueOf(TimeValerWrapper{Valer: ti})))
}
