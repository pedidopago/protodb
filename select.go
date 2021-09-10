package protodb

import (
	"context"
	"encoding/json"
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
	cols := make([]string, 0)
	for _, v := range r.Columns {
		isok := true
		if v.RecursiveIf != nil && !contextIfIsTrue(ctx, *v.RecursiveIf, true) {
			continue
		}
		if ifctxv := v.Meta["joinif"]; ifctxv != "" {
			if vi := ctx.Value(IfKey(ifctxv)); vi != nil {
				if vb, ok := vi.(bool); ok {
					isok = vb
				}
			}
		}
		if isok {
			if v.Meta != nil && v.Meta["select"] != "" {
				cols = append(cols, v.Meta["select"])
			} else {
				//TODO: workaround if v.Value == ""
				if v.Name == "-" || v.Name == "" {
					continue
				}
				cols = append(cols, v.Name)
			}
		}
	}
	return cols
}

// SelectJSON extract the column names to be selected by the SQL using a json key/pair
// Example:
//      // the example below extracts: "JSON_OBJECT('flda', a.id, 'fldb', JSON_ARRAYAGG(JSON_OBJECT('store_id', b.store_id))) output"
//      type Example struct {
// 		   FieldA int `dbselect:"a.fielda;table=agents a" json:"flda"`
//         FieldB struct{
//			  StoreID string `json:"store_id" dbselect:"b.store_id"`
//		   } `dbselect:"-;join=LEFT JOIN tableb b ON b.aid=a.id"`
//      }
// Options:
//   - "selectif": the value will be interpreted as a ConditionalContextKey and will be
//               evaluated with the context.Value(ConditionalContextKey(joinifKey))
func (r ColumnsResult) SelectJSON(ctx context.Context) string {
	root := &jgroup{
		Values: make([]*nameValuePair, 0),
	}
	vmap := make(map[string]*nameValuePair)

	addval := func(jsonFullPath, name, value string) {
		if strings.Count(jsonFullPath, "/") < 2 {
			v := &nameValuePair{
				Name:  name,
				Value: value,
			}
			root.Values = append(root.Values, v)
			vmap[jsonFullPath] = v
		} else {
			lvlabove := jsonFullPath[:strings.LastIndex(jsonFullPath, "/")]
			parent := vmap[lvlabove]
			if parent == nil {
				println("SelectJSON: parent should not be nil")
			} else {
				x := &nameValuePair{
					Name:  name,
					Value: value,
				}
				parent.ValueArray = append(parent.ValueArray, x)
				vmap[jsonFullPath] = x
			}
		}
	}

	//cols := make([]string, 0)
	for _, v := range r.Columns {
		isok := true
		if v.RecursiveIf != nil && !contextIfIsTrue(ctx, *v.RecursiveIf, true) {
			continue
		}
		if ifctxv := v.Meta["selectif"]; ifctxv != "" {
			if vi := ctx.Value(IfKey(ifctxv)); vi != nil {
				if vb, ok := vi.(bool); ok {
					isok = vb
				}
			}
		}
		if isok {
			vv := ""
			if v.Meta != nil && v.Meta["select"] != "" {
				vv = v.Meta["select"]
			} else {
				vv = v.Name
			}
			if v.JSON.Name != "" && v.JSON.Name != "-" {
				addval(v.JSON.FullPath, v.JSON.Name, vv)
			}
		}
	}

	selectq := new(strings.Builder)

	selectq.WriteString("JSON_OBJECT(")
	recursiveJsonWrite(selectq, root.Values)
	selectq.WriteString(") json_output")

	return selectq.String()
}

type nameValuePair struct {
	Name       string
	Value      string
	ValueArray []*nameValuePair
	IsSlice    bool
}

type jgroup struct {
	Values []*nameValuePair
}

func recursiveJsonWrite(b *strings.Builder, vals []*nameValuePair) {
	for i, v := range vals {
		if i != 0 {
			b.WriteString(", ")
		}

		if v.ValueArray != nil {
			if v.IsSlice {
				b.WriteString("'" + v.Name + "', ")
				b.WriteString("JSON_ARRAYAGG(JSON_OBJECT(")
				recursiveJsonWrite(b, v.ValueArray)
				b.WriteString("))")
			} else if len(v.ValueArray) > 0 {
				b.WriteString("'" + v.Name + "', ")
				b.WriteString("JSON_OBJECT(")
				recursiveJsonWrite(b, v.ValueArray)
				b.WriteString(")")
			}
		} else if v.Value != "-" && v.Value != "" {
			// get children!
			b.WriteString("'" + v.Name + "', ")
			b.WriteString(v.Value)
		}
	}
}

// GetGroupBy extract the GROUP BY to be inserted to the query.
// Example:
//      // the example below extracts: ["fielda", "b.fieldb"]
//      type Example struct {
// 		   FieldA string `dbselect:"fielda;groupby=a.id"`
//      }
// Options:
//   - "groupif": the value will be interpreted as a ConditionalContextKey and will be
//               evaluated with the context.Value(ConditionalContextKey(joinifKey))
func (r ColumnsResult) GetGroupBy(ctx context.Context) string {
	for _, v := range r.Columns {
		isok := true
		if v.RecursiveIf != nil && !contextIfIsTrue(ctx, *v.RecursiveIf, true) {
			continue
		}
		if ifctxv := v.Meta["groupif"]; ifctxv != "" {
			if vi := ctx.Value(IfKey(ifctxv)); vi != nil {
				if vb, ok := vi.(bool); ok {
					isok = vb
				}
			}
		}
		if isok {
			if v.Meta != nil && v.Meta["groupby"] != "" {
				return v.Meta["groupby"]
			}
		}
	}
	return ""
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
	JSON        struct {
		Name     string
		FullPath string
		Parent   string
	}
	IsSlice bool
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
	if groupby := columnsResult.GetGroupBy(ctx); groupby != "" {
		rq = rq.GroupBy(groupby)
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
	if groupby := columnsResult.GetGroupBy(ctx); groupby != "" {
		rq = rq.GroupBy(groupby)
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

// JSONGetContext executes a SelectColumnScan on dest (with reflection) to determine which table, columns and joins are used
// to retrieve data. Use qfn to apply where filters (and other query modifiers).
// The data is queried using JSON_OBJECT amd JSON_ARRAYAGG.
// Mariadb 10.5+
func JSONGetContext(ctx context.Context, dbtx sqlx.QueryerContext, dest interface{}, qfn func(rq squirrel.SelectBuilder) squirrel.SelectBuilder) error {
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
	// 2 - build query
	jselect := columnsResult.SelectJSON(ctx)
	rq = squirrel.Select(jselect)
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
	if groupby := columnsResult.GetGroupBy(ctx); groupby != "" {
		rq = rq.GroupBy(groupby)
	}
	if qfn != nil {
		rq = qfn(rq)
	}
	q, args, err := rq.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build query: %w", err)
	}
	if Trace {
		fmt.Printf("JSONGetContext: %s %v\n", q, args)
	}
	rawjson := ""
	if err := sqlx.GetContext(ctx, dbtx, &rawjson, q, args...); err != nil {
		return err
	}
	if err := json.Unmarshal([]byte(rawjson), dest); err != nil {
		return err
	}
	if err := remap(dest); err != nil {
		return fmt.Errorf("failed to remap: %w", err)
	}
	return nil
}

// JSONSelectContext executes a SelectColumnScan on dest (with reflection) to determine which table, columns and joins are used
// to retrieve data. Use qfn to apply where filters (and other query modifiers).
// The data is queried using JSON_OBJECT amd JSON_ARRAYAGG.
// Mariadb 10.5+
func JSONSelectContext(ctx context.Context, dbtx sqlx.QueryerContext, dest interface{}, qfn func(rq squirrel.SelectBuilder) squirrel.SelectBuilder) error {
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
	jselect := columnsResult.SelectJSON(ctx)
	rq := squirrel.Select(jselect)
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
	if groupby := columnsResult.GetGroupBy(ctx); groupby != "" {
		rq = rq.GroupBy(groupby)
	}
	if qfn != nil {
		rq = qfn(rq)
	}
	q, args, err := rq.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build query: %w", err)
	}
	if Trace {
		fmt.Printf("JSONSelectContext: %s %v\n", q, args)
	}
	txdest := []string{}
	if err := sqlx.SelectContext(ctx, dbtx, &txdest, q, args...); err != nil {
		return err
	}
	var lasterr error
	var errc int
	destval := reflect.ValueOf(dest).Elem()
	for _, v := range txdest {
		slcelm := slice.Elem()
		if slcelm.Kind() == reflect.Ptr {
			slcelm = slcelm.Elem()
		}
		vp := reflect.New(slcelm)
		vx := vp.Interface()
		if err := json.Unmarshal([]byte(v), vx); err != nil {
			errc += 1
			lasterr = err
			continue
		}
		if slice.Elem().Kind() != reflect.Ptr {
			vx = reflect.Indirect(reflect.ValueOf(vx)).Interface()
		}
		vp = reflect.ValueOf(vx)
		// append to dest via reflection
		destval.Set(reflect.Append(destval, vp))
	}
	if errc > 0 && errc == len(txdest) {
		return lasterr
	}
	if err := remap(dest); err != nil {
		return fmt.Errorf("failed to remap: %w", err)
	}
	return nil
}
