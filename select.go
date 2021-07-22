package protodb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
)

// ColumnsResult is the metadata obtained by SelectColumns, InsertColumns or UpdateColumns
type ColumnsResult struct {
	Err     error
	Columns []TagData
}

// SelectColumns extract the column names to be selected by  the SQL.
// Example:
//      // the example below extracts: ["fielda", "b.fieldb"]
//      type Example struct {
// 		   FieldA string `dbselect:"fielda;table=agents"`
//         FieldB int `dbselect:"select=b.fieldb;join=LEFT JOIN tableb b"`
//      }
func (r ColumnsResult) SelectColumns() []string {
	cols := make([]string, 0)
	for _, v := range r.Columns {
		if v.Meta != nil && v.Meta["select"] != "" {
			cols = append(cols, v.Meta["select"])
		} else {
			//TODO: workaround if v.Value == ""
			if v.Value == "-" || v.Value == "" {
				continue
			}
			cols = append(cols, v.Value)
		}
	}
	return cols
}

// SelectTable extract the table name to be selected by the SQL.
// Valid subtags: "select_table", "table"
// Example:
//      // the example below extracts: "agents"
//      type Example struct {
// 		   FieldA string `dbselect:"fielda;table=agents"`
//         FieldB int `dbselect:"select=b.fieldb;join=LEFT JOIN tableb b"`
//      }
func (r ColumnsResult) SelectTable() string {
	for _, v := range r.Columns {
		if v.Meta == nil {
			continue
		}
		if x := v.Meta["select_table"]; x != "" {
			return x
		}
	}
	for _, v := range r.Columns {
		if v.Meta == nil {
			continue
		}
		if x := v.Meta["table"]; x != "" {
			return x
		}
	}
	return ""
}

// SelectJoins extracts the tables to be joined by the SQL.
// Valid subtags: "select_join", "join"
// Example:
//      // the example below extracts: ["LEFT JOIN tableb b ON b.agentid=a.id"]
//      type Example struct {
// 		   FieldA string `dbselect:"a.fielda;table=agents a"`
//         FieldB int `dbselect:"select=b.fieldb;join=LEFT JOIN tableb b ON b.agentid=a.id"`
//      }
func (r ColumnsResult) SelectJoins() []string {
	joins := make([]string, 0)
	for _, v := range r.Columns {
		if v.Meta == nil {
			continue
		}
		if x := v.Meta["select_join"]; x != "" {
			joins = append(joins, x)
		} else if x := v.Meta["join"]; x != "" {
			joins = append(joins, x)
		}
	}
	return joins
}

// TagData is a collection of metadata and value, retrieved by parsing the tags of a field
type TagData struct {
	Value string
	Meta  map[string]string
}

// SelectColumns uses db_select, dbselect, db (in this order) to map columns to be selected
func SelectColumns(v interface{}, tags ...string) ColumnsResult {
	tags = append(tags, "db_select", "dbselect", "db")
	result, err := extract(v, tags...)
	return ColumnsResult{
		Err:     err,
		Columns: result,
	}
}

// SelectContext executes a SelectColumns on dest (with reflection) to determine which table, columns and joins are used
// to retrieve data. Use qfn to apply where filters (and other query modifiers).
func SelectContext(ctx context.Context, dbtx sqlx.QueryerContext, dest interface{}, qfn func(rq squirrel.SelectBuilder) squirrel.SelectBuilder) error {
	// 1 - extract ther underlying type
	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Ptr {
		return errors.New("dest is not a pointer")
	}
	if value.IsNil() {
		return errors.New("dest is nil")
	}
	// direct := reflect.Indirect(value)
	slice, err := baseType(value.Type(), reflect.Slice)
	if err != nil {
		return err
	}
	// isPtr := slice.Elem().Kind() == reflect.Ptr
	base := reflectx.Deref(slice.Elem())

	vp := reflect.New(base)
	// v := reflect.Indirect(vp)

	columnsResult := SelectColumns(vp)
	if columnsResult.Err != nil {
		return columnsResult.Err
	}
	// 2 - build query
	rq := squirrel.Select(columnsResult.SelectColumns()...)
	seltable := columnsResult.SelectTable()
	if seltable == "" {
		return errors.New("select table not found")
	}
	rq = rq.From(seltable)
	if joins := columnsResult.SelectJoins(); len(joins) > 0 {
		jr := extractJoinReplace(ctx)
		for _, v := range joins {
			v = mapReplace(v, jr)
			if strings.Contains(strings.ToUpper(v), "JOIN ") {
				rq = rq.JoinClause(v)
			} else {
				rq = rq.Join(v)
			}
		}
	}
	if qfn != nil {
		rq = qfn(rq)
	}
	q, args, err := rq.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build query: %w", err)
	}

	return sqlx.SelectContext(ctx, dbtx, dest, q, args...)
}
