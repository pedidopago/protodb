package protodb

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type TestRemapped struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func (m *TestRemapped) RemapFrom(src interface{}) error {
	if src == nil {
		return errors.New("nil src")
	}
	switch v := src.(type) {
	case string:
		return json.Unmarshal([]byte(src.(string)), m)
	case []byte:
		return json.Unmarshal(v, m)
	}
	return errors.New("unsupported src type")
}

func TestRemap(t *testing.T) {
	a := struct {
		A string        `json:"a"`
		B string        `remapsrc:"x" json:"-"`
		C *TestRemapped `remapdst:"x" json:"c"`
	}{}
	a.B = `{"name":"Jon Snow","age":3}`
	require.NoError(t, remap(&a))
	require.NotNil(t, a.C)
	require.Equal(t, int(3), a.C.Age)
	require.Equal(t, "Jon Snow", a.C.Name)
}

func TestRemapSlice(t *testing.T) {
	type z struct {
		A string        `json:"a"`
		B string        `remapsrc:"x" json:"-"`
		C *TestRemapped `remapdst:"x" json:"c"`
	}
	a := []z{
		{B: `{"name":"Bob","age":34}`},
		{B: `{"name":"Mark","age":55}`},
	}
	require.NoError(t, remap(&a))
	require.Equal(t, 2, len(a))
	require.NotNil(t, a[0].C)
	require.NotNil(t, a[1].C)
	require.Equal(t, int(34), a[0].C.Age)
	require.Equal(t, "Bob", a[0].C.Name)
	require.Equal(t, int(55), a[1].C.Age)
	require.Equal(t, "Mark", a[1].C.Name)
}

func TestRemapSliceOfPointers(t *testing.T) {
	type z struct {
		A string        `json:"a"`
		B string        `remapsrc:"x" json:"-"`
		C *TestRemapped `remapdst:"x" json:"c"`
	}
	a := []*z{
		{B: `{"name":"Bob","age":34}`},
		{B: `{"name":"Mark","age":55}`},
	}
	require.NoError(t, remap(&a))
	require.Equal(t, 2, len(a))
	require.NotNil(t, a[0].C)
	require.NotNil(t, a[1].C)
	require.Equal(t, int(34), a[0].C.Age)
	require.Equal(t, "Bob", a[0].C.Name)
	require.Equal(t, int(55), a[1].C.Age)
	require.Equal(t, "Mark", a[1].C.Name)
}

//TODO: TEST RemapperSource
