package protodb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/pedidopago/protodb/valer"
)

func buildOnDuplicate(cols []string) squirrel.Sqlizer {
	if len(cols) == 0 {
		return nil
	}
	var q = "ON DUPLICATE KEY UPDATE "
	updStmt := func(col string) string {
		return fmt.Sprintf("%s = VALUES(%s)", col, col)
	}
	for _, col := range cols[:len(cols)-1] {
		q += updStmt(col) + ", "
	}
	q += updStmt(cols[len(cols)-1])
	return squirrel.Expr(q)
}

func BuildUpsert(ctx context.Context, items interface{}, qfn func(rq squirrel.InsertBuilder) squirrel.InsertBuilder, columns ...string) (rq squirrel.InsertBuilder, rerr error) {
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
		insColumns := InsertColumnScan(value)
		if err := insColumns.Err; err != nil {
			return
		}
		tname := insColumns.GetTableNameMeta(ctx)
		if tname == "" {
			rerr = errors.New("(insert) subtag 'table' not found")
			return
		}
		rq = squirrel.Insert(tname)
		colNames := []string{}
		vals := []interface{}{}
		for _, v := range insColumns.Columns {
			if v.Name != "-" && v.Name != "" {
				if !skipInsertSingleRow(v) {
					colNames = append(colNames, v.Name)
					vals = append(vals, resolveValue(value, v))
				}
			}
		}
		var updColNames = columns
		if len(updColNames) == 0 {
			updColumns := UpdateColumnScan(value)
			if rerr = updColumns.Err; rerr != nil {
				return
			}
			for _, v := range updColumns.Columns {
				if v.Name != "-" && v.Name != "" {
					updColNames = append(updColNames, v.Name)
				}
			}
		}
		rq = rq.Columns(colNames...).Values(vals...)
		if qfn != nil {
			rq = qfn(rq)
		}
		rq = rq.SuffixExpr(buildOnDuplicate(updColNames))
		return
	}
	sliceIter := reflect.Indirect(value)
	if sliceIter.Len() < 1 {
		rerr = errors.New("needs at least one row to insert")
		return
	}

	// start query and insert columns
	// start query and insert columns
	var insColumns ColumnsResult
	if vElem, err := ensureStruct(sliceIter.Index(0)); err != nil {
		rerr = err
		return
	} else if insColumns = InsertColumnScan(vElem); insColumns.Err != nil {
		rerr = insColumns.Err
		return
	}
	tname := insColumns.GetTableNameMeta(ctx)
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
	insColNames := []string{}
	for _, v := range insColumns.Columns {
		if v.Name != "-" && v.Name != "" {
			insColNames = append(insColNames, v.Name)
		}
	}
	var updColNames = columns
	if len(updColNames) == 0 {
		updColumns := UpdateColumnScan(value)
		if rerr = updColumns.Err; rerr != nil {
			return
		}
		for _, v := range updColumns.Columns {
			if v.Name != "-" && v.Name != "" {
				updColNames = append(updColNames, v.Name)
			}
		}
	}
	rq = rq.Columns(insColNames...)

	for i := 0; i < sliceIter.Len(); i++ {
		vi := valueAtIndex(i)
		vals := []interface{}{}
		for _, v := range insColumns.Columns {
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
	rq = rq.SuffixExpr(buildOnDuplicate(updColNames))
	return
}

func UpsertContext(ctx context.Context, dbtx sqlx.ExecerContext, items interface{}, qfn func(rq squirrel.InsertBuilder) squirrel.InsertBuilder, columns ...string) (sql.Result, error) {
	rq, err := BuildUpsert(ctx, items, qfn, columns...)
	if err != nil {
		return nil, err
	}
	rawq, args, err := rq.ToSql()
	if err != nil {
		return nil, err
	}
	return dbtx.ExecContext(ctx, rawq, args...)
}