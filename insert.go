package protodb

import (
	"context"
	"database/sql"
	"errors"
	"reflect"

	"github.com/pedidopago/protodb/valer"

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

func BuildInsert(ctx context.Context, items interface{}, qfn func(rq squirrel.InsertBuilder) squirrel.InsertBuilder) (rq squirrel.InsertBuilder, rerr error) {
	// 1 - extract ther underlying type
	value := reflect.ValueOf(items)
	if rerr = errIfNotAPointerOrNil(value); rerr != nil {
		return
	}
	if !isTypeSliceOrSlicePointer(value.Type()) {
		if value, rerr = ensureStruct(value); rerr != nil {
			return
		}
		// Insert a single row
		columns := InsertColumnScan(value)
		if err := columns.Err; err != nil {
			return
		}
		tname := columns.GetTableNameMeta(ctx)
		if tname == "" {
			rerr = errors.New("(insert) subtag 'table' not found")
			return
		}
		rq = squirrel.Insert(tname)
		colNames := []string{}
		vals := []interface{}{}
		for _, v := range columns.Columns {
			if v.Name != "-" && v.Name != "" {
				if !skipInsertSingleRow(v) {
					colNames = append(colNames, v.Name)
					vals = append(vals, resolveValue(value, v))
				}
			}
		}
		rq = rq.Columns(colNames...).Values(vals...)
		if qfn != nil {
			rq = qfn(rq)
		}
		return
	}
	sliceIter := reflect.Indirect(value)
	if sliceIter.Len() < 1 {
		rerr = errors.New("needs at least one row to insert")
		return
	}

	// start query and insert columns
	var columns ColumnsResult
	if vElem, err := ensureStruct(sliceIter.Elem()); err != nil {
		rerr = err
		return
	} else if columns = InsertColumnScan(vElem); columns.Err != nil {
		rerr = columns.Err
		return
	}
	tname := columns.GetTableNameMeta(ctx)
	if tname == "" {
		rerr = errors.New("(insert) subtag 'table' not found")
		return
	}

	var valueAtIndex func(i int) reflect.Value
	switch sliceIter.Type().Elem().Kind() {
	case reflect.Ptr:
		valueAtIndex = func(i int) reflect.Value {
			return sliceIter.Index(i).Elem()
		}
	case reflect.Struct:
		valueAtIndex = func(i int) reflect.Value {
			return sliceIter.Index(i)
		}
	}

	rq = squirrel.Insert(tname)
	colNames := []string{}
	for _, v := range columns.Columns {
		if v.Name != "-" && v.Name != "" {
			colNames = append(colNames, v.Name)
		}
	}
	rq = rq.Columns(colNames...)

	for i := 0; i < sliceIter.Len(); i++ {
		vi := valueAtIndex(i)
		vals := []interface{}{}
		for _, v := range columns.Columns {
			if v.Name != "-" && v.Name != "" {
				td := v
				td.FieldValue = valer.WrapValue(vi.FieldByName(v.FieldName))
				vals = append(vals, resolveValueMultiRowInsert(vi, td))
			}
		}
		rq = rq.Values(vals...)
	}
	if qfn != nil {
		rq = qfn(rq)
	}
	return
}

// InsertContext executes a InsertColumnScan on dest (with reflection) to determine which tableand rows are used
// to insert data. Use qfn to apply where filters (and other query modifiers).
func InsertContext(ctx context.Context, dbtx sqlx.ExecerContext, items interface{}, qfn func(rq squirrel.InsertBuilder) squirrel.InsertBuilder) (sql.Result, error) {
	rq, err := BuildInsert(ctx, items, qfn)
	if err != nil {
		return nil, err
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
	if (v.FieldValue.IsZero() || valer.IsZeroValer(v.FieldValue)) && !isNilSafe(v.FieldValue) {
		return (v.MetaBool("skipzero", false) || v.MetaBool("skipzerovalue", false) || v.MetaBool("skipzeroval", false))
	}
	if sk, ok := v.FieldValue.Interface().(Skippable); ok && sk.Skip() {
		return true
	}
	return false
}
