package protodb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInsertColumnScan(t *testing.T) {
	rows := []struct {
		Name string `dbinsert:"name;table=users"`
		Age  int    `dbinsert:"age"`
	}{
		struct {
			Name string "dbinsert:\"name;table=users\""
			Age  int    "dbinsert:\"age\""
		}{
			"Tom",
			20,
		},
		struct {
			Name string "dbinsert:\"name;table=users\""
			Age  int    "dbinsert:\"age\""
		}{
			"John",
			30,
		},
	}
	cres := InsertColumnScan(rows[0])
	require.NoError(t, cres.Err)
	require.Equal(t, "name", cres.Columns[0].Name)
	require.Equal(t, "age", cres.Columns[1].Name)
	require.Equal(t, "Tom", cres.Columns[0].FieldValue.Interface())
	cres = InsertColumnScan(rows[1])
	require.NoError(t, cres.Err)
	require.Equal(t, "John", cres.Columns[0].FieldValue.Interface())
}
