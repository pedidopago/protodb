package protodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func mustStringSlice(t *testing.T) func([]string, error) []string {
	return func(s []string, e error) []string {
		assert.NoError(t, e)
		return s
	}
}

func mustTagDataSlice(t *testing.T) func([]TagData, error) []TagData {
	return func(s []TagData, e error) []TagData {
		assert.NoError(t, e)
		return s
	}
}

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
			FieldValue: "",
		},
		{
			Name:       "score",
			Meta:       make(map[string]string),
			FieldName:  "Score",
			FieldValue: int64(0),
		},
		{
			Name:       "estimate_age AS age",
			Meta:       make(map[string]string),
			FieldName:  "Age",
			FieldValue: int64(0),
		},
		{
			Name: "COALESCE(a,b,c,'')",
			Meta: map[string]string{
				"joina": "2",
			},
			FieldName:  "Complex",
			FieldValue: "",
		},
	}

	assert.Equal(t, expected, mustTagDataSlice(t)(extract(A1{}, "dbselect", "db")))
}
