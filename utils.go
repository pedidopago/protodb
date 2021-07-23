package protodb

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx/reflectx"
)

// source: github.com/jmoiron/sqlx
func baseType(t reflect.Type, expected reflect.Kind) (reflect.Type, error) {
	t = reflectx.Deref(t)
	if t.Kind() != expected {
		return nil, fmt.Errorf("expected %s but got %s", expected, t.Kind())
	}
	return t, nil
}

// isTypeSliceOrSlicePointer returns true if t if reflect.Slice or points to a reflect.Slice
func isTypeSliceOrSlicePointer(t reflect.Type) bool {
	t = reflectx.Deref(t)
	return t.Kind() == reflect.Slice
}

type contextVar string

const (
	joinReplace contextVar = "join_replace"
)

func extractJoinReplace(ctx context.Context) map[string]string {
	v := ctx.Value(joinReplace)
	if v == nil {
		return make(map[string]string)
	}
	vx := v.(map[string]string)
	return vx
}

func mapReplace(haystack string, needlem map[string]string) string {
	for k, v := range needlem {
		haystack = strings.Replace(haystack, k, v, -1)
	}
	return haystack
}

func WithJoinReplace(ctx context.Context, from, to string) context.Context {
	jr := extractJoinReplace(ctx)
	jr[from] = to
	return context.WithValue(ctx, joinReplace, jr)
}
