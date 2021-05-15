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

func TestExtract(t *testing.T) {

	type A1 struct {
		Name  string `db:"name" dbselect:"name1"`
		Score int64  `db:"score"`
		Age   int64  `db:"age" dbselect:"estimate_age AS age"`
	}

	assert.Equal(t, []string{"name1", "score", "estimate_age AS age"}, mustStringSlice(t)(extract(A1{}, "dbselect", "db")))
}
