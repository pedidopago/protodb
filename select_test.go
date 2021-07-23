package protodb_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/Masterminds/squirrel"
	"github.com/pedidopago/protodb"
	ptesting "github.com/pedidopago/protodb/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type SelStructA struct {
	// @inject_tag: db:"id"
	StoreId string `protobuf:"bytes,1,opt,name=store_id,json=storeId,proto3" json:"store_id,omitempty" db:"id"`
	// @inject_tag: db:"domain"
	Domain string `protobuf:"bytes,2,opt,name=domain,proto3" json:"domain,omitempty" db:"domain"`
	// @inject_tag: db:"name"
	Name string `protobuf:"bytes,3,opt,name=name,proto3" json:"name,omitempty" db:"name,table=stores"`
}

func TestSelectColumns(t *testing.T) {
	result := protodb.SelectColumnScan(SelStructA{})
	require.NoError(t, result.Err)
	expected := []protodb.TagData{
		{
			Name:       "id",
			Meta:       make(map[string]string),
			FieldName:  "StoreId",
			FieldValue: reflect.ValueOf(""),
		},
		{
			Name:       "domain",
			Meta:       make(map[string]string),
			FieldName:  "Domain",
			FieldValue: reflect.ValueOf(""),
		},
		{
			Name: "name",
			Meta: map[string]string{
				"table": "stores",
			},
			FieldName:  "Name",
			FieldValue: reflect.ValueOf(""),
		},
	}
	assert.Len(t, result.Columns, len(expected))
	for i, v := range result.Columns {
		assert.Equal(t, expected[i].FieldName, v.FieldName)
		assert.Equal(t, expected[i].FieldValue.Interface(), v.FieldValue.Interface())
		assert.Equal(t, expected[i].Meta, v.Meta)
		assert.Equal(t, expected[i].Name, v.Name)
	}
}

func TestSelectContext(t *testing.T) {
	db, mock := ptesting.MockDBMySQL(t)
	defer db.Close()

	x := strings.SplitN("a=1b=2", "=", 2)
	require.Equal(t, 2, len(x))

	mock.ExpectQuery("SELECT .*").WillReturnRows(mock.NewRows([]string{"id", "domain", "name"}).AddRow("1", "domain1", "Domain 1"))

	items := make([]*SelStructA, 0)

	require.NoError(t, protodb.SelectContext(context.Background(), db, &items, nil))

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

	db, mock := ptesting.MockDBMySQL(t)
	defer db.Close()

	mock.ExpectQuery("SELECT .*").WithArgs("A%").WillReturnRows(mock.NewRows([]string{"id", "name", "score"}).AddRow(1, "Alice", 1000).AddRow(2, "Anne", 500))

	items := make([]*stitem, 0)

	ctx := context.Background()

	// // to replace a string inside a join tag, use:
	// ctx = WithJoinReplace(ctx, "needle", "replacement")

	require.NoError(t, protodb.SelectContext(ctx, db, &items, func(rq squirrel.SelectBuilder) squirrel.SelectBuilder {
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

func TestGetContext(t *testing.T) {
	db, mock := ptesting.MockDBMySQL(t)
	defer db.Close()
	defer assert.NoError(t, mock.ExpectationsWereMet())

	item := struct {
		ID    int    `db:"id" dbselect:"id;table=agents"`
		Name  string `db:"name" dbselect:"name"`
		Score int    `db:"score" dbselect:"-"`
	}{}

	mock.ExpectQuery("SELECT").WithArgs(1).WillReturnRows(mock.NewRows([]string{"id", "name"}).AddRow(1, "Alice"))
	require.NoError(t, protodb.GetContext(context.Background(), db, &item, func(rq squirrel.SelectBuilder) squirrel.SelectBuilder {
		return rq.Where("id=?", 1)
	}))
	require.Equal(t, int(1), item.ID)
	require.Equal(t, "Alice", item.Name)
}
