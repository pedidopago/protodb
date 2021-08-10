package protodb

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtract(t *testing.T) {

	type A1 struct {
		Name    string `db:"name" dbselect:"name1"`
		Score   int64  `db:"score"`
		Age     int64  `db:"age" dbselect:"estimate_age AS age"`
		Complex string `db:"complex" dbselect:"COALESCE(a,b,c,'');joina=2"`
	}

	expected := []TagData{
		{
			Name:       "name1",
			Meta:       make(map[string]string),
			FieldName:  "Name",
			FieldValue: reflect.ValueOf(""),
		},
		{
			Name:       "score",
			Meta:       make(map[string]string),
			FieldName:  "Score",
			FieldValue: reflect.ValueOf(int64(0)),
		},
		{
			Name:       "estimate_age AS age",
			Meta:       make(map[string]string),
			FieldName:  "Age",
			FieldValue: reflect.ValueOf(int64(0)),
		},
		{
			Name: "COALESCE(a,b,c,'')",
			Meta: map[string]string{
				"joina": "2",
			},
			FieldName:  "Complex",
			FieldValue: reflect.ValueOf(""),
		},
	}
	tagd, err := extract(A1{}, map[string]string{"db": ","}, "dbselect", "db")
	require.NoError(t, err)
	assert.Len(t, tagd, len(expected))
	for i, v := range tagd {
		assert.Equal(t, expected[i].FieldName, v.FieldName)
		assert.Equal(t, expected[i].FieldValue.Interface(), v.FieldValue.Interface())
		assert.Equal(t, expected[i].Meta, v.Meta)
		assert.Equal(t, expected[i].Name, v.Name)
	}
}
