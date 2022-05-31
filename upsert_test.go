package protodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildUpsert(t *testing.T) {
	rows := []struct {
		Name string `dbinsert:"name;table=users" dbupdate:"name"`
		Age  int    `dbinsert:"age" dbupdate:"age"`
	}{
		{
			"Tom",
			20,
		},
		{
			"John",
			30,
		},
	}

	rq, err := BuildUpsert(context.Background(), &rows, nil)
	require.NoError(t, err)
	q, _, err := rq.ToSql()
	require.NoError(t, err)
	require.Equal(t, "INSERT INTO users (name,age) VALUES (?,?),(?,?) ON DUPLICATE KEY UPDATE name = VALUES(name), age = VALUES(age)", q)
}
