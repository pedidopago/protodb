package protodb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
)

// UpdateColumnScan uses db_update, dbupdate, update, db (in this order) to map columns and values to be updateed
func UpdateColumnScan(v interface{}, tags ...string) ColumnsResult {
	tags = append(tags, "db_update", "dbupdate", "update", "db")
	result, err := extract(v, map[string]string{"db": ","}, tags...)
	return ColumnsResult{
		Err:     err,
		Columns: result,
	}
}

// UpdateContext executes a UpdateColumnScan on dest (with reflection) to determine which table and rows are used
// to insert data. Use qfn to apply where filters (and other query modifiers).
func UpdateContext(ctx context.Context, dbtx sqlx.ExecerContext, item interface{}, qfn func(rq squirrel.UpdateBuilder) squirrel.UpdateBuilder, skipColumns ...string) (sql.Result, error) {
	skipColumnMap := make(map[string]struct{})
	for _, v := range skipColumns {
		skipColumnMap[v] = struct{}{}
	}
	// 1 - extract ther underlying type
	value := reflect.ValueOf(item)
	if value.Kind() != reflect.Struct && value.IsNil() {
		return nil, errors.New("item is nil")
	}
	var rq squirrel.UpdateBuilder
	if isTypeSliceOrSlicePointer(value.Type()) {
		return nil, errors.New("UpdateContext: cannot update a slice or a slice pointer")
	}
	// Update a single row
	columns := UpdateColumnScan(value)
	if err := columns.Err; err != nil {
		return nil, err
	}
	tname := columns.GetTableNameMeta(ctx)
	if tname == "" {
		return nil, errors.New("(update) subtag 'table' not found")
	}
	rq = squirrel.Update(tname)
	for _, v := range columns.Columns {
		if v.Name != "-" && v.Name != "" {
			if _, ok := skipColumnMap[v.Name]; !ok {
				if !skipUpdate(v) {
					rq = rq.Set(v.Name, resolveValue(v))
				}
			}
		}
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

type Skippable interface {
	Skip() bool
}

type UpdateValidator interface {
	ValidForUpdate() bool
}

func skipUpdate(v TagData) bool {
	if !v.FieldValue.IsValid() {
		return true
	}
	if isNilSafe(v.FieldValue) {
		return v.MetaBool("skipnil", false)
	}
	if v.FieldValue.IsZero() && !isNilSafe(v.FieldValue) {
		if v.MetaBool("skipzero", false) || v.MetaBool("skipzerovalue", false) || v.MetaBool("skipzeroval", false) {
			return true
		}
	}
	if sk, ok := v.FieldValue.Interface().(UpdateValidator); ok && !sk.ValidForUpdate() {
		return true
	}
	if sk, ok := v.FieldValue.Interface().(Skippable); ok && sk.Skip() {
		return true
	}
	return false
}

func resolveValueMultiRowInsert(v TagData) interface{} {
	if isNilSafe(v.FieldValue) {
		if vs, ok := v.MetaStringCheck("nilval"); ok {
			return vs
		}
		return nil
	}
	if v.FieldValue.IsZero() {
		if v.MetaBool("zeronil", false) {
			return nil
		}
	}
	if skipInsertSingleRow(v) {
		return squirrel.Expr(fmt.Sprintf("DEFAULT(%s)", v.Name))
	}
	return v.FieldValue.Interface()
}

func resolveValue(v TagData) interface{} {
	if isNilSafe(v.FieldValue) {
		if vs, ok := v.MetaStringCheck("nilval"); ok {
			return vs
		}
		return nil
	}
	if v.FieldValue.IsZero() {
		if v.MetaBool("zeronil", false) {
			return nil
		}
	}
	return v.FieldValue.Interface()
}
