package protodb

import (
	"context"
	"strings"
	"testing"

	"github.com/Masterminds/squirrel"

	sqlm "github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func getdb(t *testing.T) (db *sqlx.DB, mock sqlm.Sqlmock) {
	rawdb, mock, err := sqlm.New()
	require.NoError(t, err)
	db = sqlx.NewDb(rawdb, "mysql")
	return db, mock
}

type privateB struct {
}

type SelStructA struct {
	state         *privateB
	sizeCache     *privateB
	unknownFields *privateB

	// @inject_tag: db:"id"
	StoreId string `protobuf:"bytes,1,opt,name=store_id,json=storeId,proto3" json:"store_id,omitempty" db:"id"`
	// @inject_tag: db:"domain"
	Domain string `protobuf:"bytes,2,opt,name=domain,proto3" json:"domain,omitempty" db:"domain"`
	// @inject_tag: db:"name"
	Name string `protobuf:"bytes,3,opt,name=name,proto3" json:"name,omitempty" db:"name,table=stores"`
}

func TestSelectColumns(t *testing.T) {
	result := SelectColumns(SelStructA{})
	require.NoError(t, result.Err)
	expected := []TagData{
		{
			Value: "id",
			Meta:  make(map[string]string),
		},
		{
			Value: "domain",
			Meta:  make(map[string]string),
		},
		{
			Value: "name",
			Meta: map[string]string{
				"table": "stores",
			},
		},
	}
	require.Equal(t, expected, result.Columns)
}

func TestSelectContext(t *testing.T) {
	db, mock := getdb(t)
	defer db.Close()

	x := strings.SplitN("a=1b=2", "=", 2)
	require.Equal(t, 2, len(x))

	mock.ExpectQuery("SELECT .*").WillReturnRows(mock.NewRows([]string{"id", "domain", "name"}).AddRow("1", "domain1", "Domain 1"))

	items := make([]*SelStructA, 0)

	require.NoError(t, SelectContext(context.Background(), db, &items, nil))

	require.NoError(t, mock.ExpectationsWereMet())

	require.Equal(t, 1, len(items))
	require.Equal(t, "1", items[0].StoreId)
	require.Equal(t, "domain1", items[0].Domain)
	require.Equal(t, "Domain 1", items[0].Name)
}

func TestSelectContextWithParams(t *testing.T) {

	type stitem struct {
		ID    int    `db:"id,table=accounts act,select=act.id"`
		Name  string `db:"name,select=act.full_name AS name"`
		Score int    `db:"score,select=ascore.score,join=LEFT JOIN accounts_score ascore ON ascore.account_id=act.id"`
	}

	db, mock := getdb(t)
	defer db.Close()

	mock.ExpectQuery("SELECT .*").WithArgs("A%").WillReturnRows(mock.NewRows([]string{"id", "name", "score"}).AddRow(1, "Alice", 1000).AddRow(2, "Anne", 500))

	items := make([]*stitem, 0)

	ctx := context.Background()

	// // to replace a string inside a join tag, use:
	// ctx = WithJoinReplace(ctx, "needle", "replacement")

	require.NoError(t, SelectContext(ctx, db, &items, func(rq squirrel.SelectBuilder) squirrel.SelectBuilder {
		rq = rq.Where("full_name LIKE ?", "A%")
		rq = rq.OrderBy("id ASC", "name ASC")
		return rq
	}))

	require.NoError(t, mock.ExpectationsWereMet())

	require.Equal(t, 2, len(items))
	require.Equal(t, 1, items[0].ID)
	require.Equal(t, 2, items[1].ID)
	require.Equal(t, "Alice", items[0].Name)
	require.Equal(t, "Anne", items[1].Name)
	require.Equal(t, 1000, items[0].Score)
	require.Equal(t, 500, items[1].Score)
}
