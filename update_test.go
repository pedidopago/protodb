package protodb_test

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Masterminds/squirrel"
	"github.com/pedidopago/protodb"
	ptesting "github.com/pedidopago/protodb/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateContext(t *testing.T) {
	db, mock := ptesting.MockDBMySQL(t)
	defer db.Close()
	defer assert.NoError(t, mock.ExpectationsWereMet())
	item := struct {
		ID    int    `db:"id" dbupdate:"id;table=agents"`
		Name  string `db:"name" dbupdate:"name;skipzerovalue=true"`
		Age   int    `db:"age" dbupdate:"age"`
		Score *int   `db:"-" dbupdate:"score;skipnil=true"`
	}{
		ID:    1,
		Name:  "Mole Person",
		Age:   40,
		Score: nil,
	}
	mock.ExpectExec("UPDATE agents").WithArgs("Mole Person", 40, 1).WillReturnResult(sqlmock.NewResult(1, 1))
	result, err := protodb.UpdateContext(context.Background(), db, item, func(rq squirrel.UpdateBuilder) squirrel.UpdateBuilder {
		return rq.Where("id=?", item.ID)
	}, "id")
	require.NoError(t, err)
	require.NotNil(t, result)
	ra, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), ra)
	//
	//
	newScore := 10
	item.Score = &newScore
	//
	mock.ExpectExec("UPDATE agents").WithArgs("Mole Person", 40, 10, 1).WillReturnResult(sqlmock.NewResult(1, 1))
	result, err = protodb.UpdateContext(context.Background(), db, item, func(rq squirrel.UpdateBuilder) squirrel.UpdateBuilder {
		return rq.Where("id=?", item.ID)
	}, "id")
	require.NoError(t, err)
	require.NotNil(t, result)
	ra, err = result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), ra)
}
