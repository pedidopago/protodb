package protodb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
)

// ColumnsResult is the metadata obtained by SelectColumnScan, InsertColumnScan or UpdateColumnScan
type ColumnsResult struct {
	Err     error
	Columns []TagData
}

type ConditionalContextKey string

func IfKey(v string) ConditionalContextKey {
	return ConditionalContextKey(v)
}

func contextIfIsTrue(ctx context.Context, name ConditionalContextKey, defaultv bool) bool {
	if v := ctx.Value(name); v != nil {
		if vb, ok := v.(bool); ok {
			return vb
		}
	}
	return defaultv
}

// SelectColumns extract the column names to be selected by  the SQL.
// Example:
//      // the example below extracts: ["fielda", "b.fieldb"]
//      type Example struct {
// 		   FieldA string `dbselect:"fielda;table=agents"`
//         FieldB int `dbselect:"select=b.fieldb;join=LEFT JOIN tableb b"`
//      }
// Options:
//   - "joinif": the value will be interpreted as a ConditionalContextKey and will be
//               evaluated with the context.Value(ConditionalContextKey(joinifKey))
func (r ColumnsResult) SelectColumns(ctx context.Context) []string {
	columnFn := func(c string) string {
		return c
	}
	seltable := r.GetTableNameMeta(ctx)
	if seltable != "" {
		columnFn = func(c string) string {
			if strings.Contains(c, ".") {
				return c
			}
			return fmt.Sprintf("%s.%s", seltable, c)
		}
	}
	cols := make([]string, 0)
	for _, v := range r.Columns {
		isok := true
		if v.RecursiveIf != nil && !contextIfIsTrue(ctx, *v.RecursiveIf, true) {
			continue
		}
		if ifctxv := v.Meta["if"]; ifctxv != "" {
			if vi := ctx.Value(IfKey(ifctxv)); vi != nil {
				if vb, ok := vi.(bool); ok {
					isok = vb
				}
			}
		}
		if isok {
			if v.Meta != nil && v.Meta["select"] != "" {
				cols = append(cols, columnFn(v.Meta["select"]))
			} else {
				//TODO: workaround if v.Value == ""
				if v.Name == "-" || v.Name == "" {
					continue
				}
				cols = append(cols, columnFn(v.Name))
			}
		}
	}
	return cols
}

// GetTableNameMeta extract the table name to be selected/inserted/updated by the SQL.
// Valid subtags: "table", "select_table"
// Example:
//      // the example below extracts: "agents"
//      type Example struct {
// 		   FieldA string `dbselect:"fielda;table=agents"`
//         FieldB int `dbselect:"select=b.fieldb;join=LEFT JOIN tableb b"`
//      }
func (r ColumnsResult) GetTableNameMeta(ctx context.Context) string {
	for _, v := range r.Columns {
		if v.Meta == nil {
			continue
		}
		if v.RecursiveIf != nil && !contextIfIsTrue(ctx, *v.RecursiveIf, true) {
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
		if v.RecursiveIf != nil && !contextIfIsTrue(ctx, *v.RecursiveIf, true) {
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
// Options:
//   - "joinif": the value will be interpreted as a ConditionalContextKey and will be
//               evaluated with the context.Value(ConditionalContextKey(joinifKey))
func (r ColumnsResult) SelectJoins(ctx context.Context) []string {
	joins := make([]string, 0)
	for _, v := range r.Columns {
		if v.Meta == nil {
			continue
		}
		if v.RecursiveIf != nil && !contextIfIsTrue(ctx, *v.RecursiveIf, true) {
			continue
		}
		isok := true
		if ifctxv := v.Meta["joinif"]; ifctxv != "" {
			if vi := ctx.Value(IfKey(ifctxv)); vi != nil {
				if vb, ok := vi.(bool); ok {
					isok = vb
				}
			}
		}
		if isok {
			if x := v.Meta["select_join"]; x != "" {
				joins = append(joins, x)
			} else if x := v.Meta["join"]; x != "" {
				joins = append(joins, x)
			}
		}
	}
	return joins
}

// TagData is a collection of metadata and value, retrieved by parsing the tags of a field
type TagData struct {
	Name        string
	Meta        map[string]string
	FieldName   string
	FieldValue  reflect.Value
	RecursiveIf *ConditionalContextKey
}

func (d *TagData) MetaBool(name string, defaultv bool) bool {
	if d.Meta == nil {
		return defaultv
	}
	if v, ok := d.Meta[name]; ok {
		if vb, err := strconv.ParseBool(v); err == nil {
			return vb
		}
	}
	return defaultv
}

func (d *TagData) MetaStringCheck(name string) (string, bool) {
	if d.Meta == nil {
		return "", false
	}
	v, ok := d.Meta[name]
	return v, ok
}

func (d *TagData) MetaString(name string, defaultv string) string {
	if d.Meta == nil {
		return defaultv
	}
	if v, ok := d.Meta[name]; ok {
		return v
	}
	return defaultv
}

// SelectColumnScan uses db_select, dbselect, db (in this order) to map columns to be selected
func SelectColumnScan(v interface{}, tags ...string) ColumnsResult {
	tags = append(tags, "db_select", "dbselect", "db")
	result, err := extract(v, map[string]string{"db": ","}, tags...)
	return ColumnsResult{
		Err:     err,
		Columns: result,
	}
}

// errIfNotAPointerOrNil returns an error if value is not a pointer or is nil
func errIfNotAPointerOrNil(value reflect.Value) error {
	if value.Kind() != reflect.Ptr {
		return errors.New("dest is not a pointer")
	}
	if value.IsNil() {
		return errors.New("dest is nil")
	}
	return nil
}

// BuildSelect executes a SelectColumnScan on dest (with reflection) to determine which table, columns and joins are used
// to build the query. Use qfn to apply where filters (and other query modifiers).
func BuildSelect(ctx context.Context, dest interface{}, qfn func(rq squirrel.SelectBuilder) squirrel.SelectBuilder) (q string, args []interface{}, err error) {
	// 1 - extract ther underlying type
	value := reflect.ValueOf(dest)
	if isNilSafe(value) {
		err = errors.New("item is nil")
		return
	}
	var rq squirrel.SelectBuilder
	if isTypeSliceOrSlicePointer(value.Type()) {
		err = errors.New("GetContext: cannot use a slice or a slice pointer")
		return
	}
	// Select a single row
	columnsResult := SelectColumnScan(value)
	if columnsResult.Err != nil {
		err = columnsResult.Err
		return
	}
	seltable := columnsResult.GetTableNameMeta(ctx)
	if seltable == "" {
		err = errors.New("select table not found")
		return
	}
	rq = squirrel.Select(columnsResult.SelectColumns(ctx)...)

	rq = rq.From(seltable)
	if joins := columnsResult.SelectJoins(ctx); len(joins) > 0 {
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
	q, args, err = rq.ToSql()
	if err != nil {
		 err = fmt.Errorf("failed to build query: %w", err)
	}
	return
}

// GetContext executes a SelectColumnScan on dest (with reflection) to determine which table, columns and joins are used
// to retrieve data. Use qfn to apply where filters (and other query modifiers).
func GetContext(ctx context.Context, dbtx sqlx.QueryerContext, dest interface{}, qfn func(rq squirrel.SelectBuilder) squirrel.SelectBuilder) error {
	// 1 - extract ther underlying type
	value := reflect.ValueOf(dest)
	if isNilSafe(value) {
		return errors.New("item is nil")
	}
	var rq squirrel.SelectBuilder
	if isTypeSliceOrSlicePointer(value.Type()) {
		return errors.New("GetContext: cannot use a slice or a slice pointer")
	}
	// Select a single row
	columnsResult := SelectColumnScan(value)
	if columnsResult.Err != nil {
		return columnsResult.Err
	}
	rq = squirrel.Select(columnsResult.SelectColumns(ctx)...)
	seltable := columnsResult.GetTableNameMeta(ctx)
	if seltable == "" {
		return errors.New("select table not found")
	}
	rq = rq.From(seltable)
	if joins := columnsResult.SelectJoins(ctx); len(joins) > 0 {
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
	if err := sqlx.GetContext(ctx, dbtx, dest, q, args...); err != nil {
		return err
	}
	if err := remap(dest); err != nil {
		return fmt.Errorf("failed to remap: %w", err)
	}
	return nil
}

// SelectContext executes a SelectColumnScan on dest (with reflection) to determine which table, columns and joins are used
// to retrieve data. Use qfn to apply where filters (and other query modifiers).
func SelectContext(ctx context.Context, dbtx sqlx.QueryerContext, dest interface{}, qfn func(rq squirrel.SelectBuilder) squirrel.SelectBuilder) error {
	// 1 - extract ther underlying type
	value := reflect.ValueOf(dest)
	if err := errIfNotAPointerOrNil(value); err != nil {
		return err
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

	columnsResult := SelectColumnScan(vp)
	if columnsResult.Err != nil {
		return columnsResult.Err
	}
	// 2 - build query
	rq := squirrel.Select(columnsResult.SelectColumns(ctx)...)
	seltable := columnsResult.GetTableNameMeta(ctx)
	if seltable == "" {
		return errors.New("select table not found")
	}
	rq = rq.From(seltable)
	if joins := columnsResult.SelectJoins(ctx); len(joins) > 0 {
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

	if err := sqlx.SelectContext(ctx, dbtx, dest, q, args...); err != nil {
		return err
	}
	if err := remap(dest); err != nil {
		return fmt.Errorf("failed to remap: %w", err)
	}
	return nil
}

func QueryxContext(ctx context.Context, dbtx sqlx.QueryerContext, dest interface{}, qfn func(rq squirrel.SelectBuilder) squirrel.SelectBuilder) (*sqlx.Rows, error) {
	q, args, err := BuildSelect(ctx, dest, qfn)
	if err != nil {
		return nil, err
	}
	return dbtx.QueryxContext(ctx, q, args...)
}

func RowStructScan(r *sqlx.Row, dest interface{}) error {
	err := r.StructScan(dest)
	if err != nil {
		return err
	}
	if err := remap(dest); err != nil {
		return fmt.Errorf("failed to remap: %w", err)
	}
	return nil
}

func RowsStructScan(r *sqlx.Rows, dest interface{}) error {
	err := r.StructScan(dest)
	if err != nil {
		return err
	}
	if err := remap(dest); err != nil {
		return fmt.Errorf("failed to remap: %w", err)
	}
	return nil
}
