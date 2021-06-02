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

type ColumnsResult struct {
	Err     error
	Columns []TagData
}

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
