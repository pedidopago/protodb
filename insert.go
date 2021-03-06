package protodb

import (
	"context"
	"database/sql"
	"errors"
	"reflect"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
)

// InsertColumnScan uses db_insert, dbinsert, insert, db (in this order) to map columns and values to be inserted
func InsertColumnScan(v interface{}, tags ...string) ColumnsResult {
	tags = append(tags, "db_insert", "dbinsert", "insert", "db")
	result, err := extract(v, map[string]string{"db": ","}, tags...)
	return ColumnsResult{
		Err:     err,
		Columns: result,
	}
}

// InsertContext executes a InsertColumnScan on dest (with reflection) to determine which tableand rows are used
// to insert data. Use qfn to apply where filters (and other query modifiers).
func InsertContext(ctx context.Context, dbtx sqlx.ExecerContext, items interface{}, qfn func(rq squirrel.InsertBuilder) squirrel.InsertBuilder) (sql.Result, error) {
	// 1 - extract ther underlying type
	value := reflect.ValueOf(items)
	if err := errIfNotAPointerOrNil(value); err != nil {
		return nil, err
	}
	var rq squirrel.InsertBuilder
	if !isTypeSliceOrSlicePointer(value.Type()) {
		// Insert a single row
		columns := InsertColumnScan(value)
		if err := columns.Err; err != nil {
			return nil, err
		}
		tname := columns.GetTableNameMeta(ctx)
		if tname == "" {
			return nil, errors.New("(insert) subtag 'table' not found")
		}
		rq = squirrel.Insert(tname)
		colNames := []string{}
		vals := []interface{}{}
		for _, v := range columns.Columns {
			if v.Name != "-" && v.Name != "" {
				if !skipInsertSingleRow(v) {
					colNames = append(colNames, v.Name)
					vals = append(vals, resolveValue(v))
				}
			}
		}
		rq = rq.Columns(colNames...).Values(vals...)
		if qfn != nil {
			rq = qfn(rq)
		}
		rawq, args, err := rq.ToSql()
		if err != nil {
			return nil, err
		}
		return dbtx.ExecContext(ctx, rawq, args...)
	}
	sliceIter := reflect.Indirect(value)
	if sliceIter.Len() < 1 {
		return nil, errors.New("needs at least one row to insert")
	}
	for i := 0; i < sliceIter.Len(); i++ {
		columns := InsertColumnScan(value)
		if err := columns.Err; err != nil {
			return nil, err
		}
		if i == 0 {
			// start query and insert columns
			tname := columns.GetTableNameMeta(ctx)
			if tname == "" {
				return nil, errors.New("(insert) subtag 'table' not found")
			}
			rq = squirrel.Insert(tname)
			colNames := []string{}
			for _, v := range columns.Columns {
				if v.Name == "-" || v.Name == "" {
					colNames = append(colNames, v.Name)
				}
			}
			rq = rq.Columns(colNames...)
		}
		vals := []interface{}{}
		for _, v := range columns.Columns {
			if v.Name != "-" && v.Name != "" {
				vals = append(vals, resolveValue(v))
			}
		}
		rq = rq.Values(vals...)
	}
	if qfn != nil {
		rq = qfn(rq)
	}
	rawq, args, err := rq.ToSql()
	if err != nil {
		return nil, err
	}
	return dbtx.ExecContext(ctx, rawq, args...)
}

func skipInsertSingleRow(v TagData) bool {
	if !v.FieldValue.IsValid() {
		return true
	}
	if isNilSafe(v.FieldValue) {
		return v.MetaBool("skipnil", false)
	}
	if v.FieldValue.IsZero() && !isNilSafe(v.FieldValue) {
		return (v.MetaBool("skipzero", false) || v.MetaBool("skipzerovalue", false) || v.MetaBool("skipzeroval", false))
	}
	if sk, ok := v.FieldValue.Interface().(Skippable); ok && sk.Skip() {
		return true
	}
	return false
}
