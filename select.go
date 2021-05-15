package protodb

type ColumnsResult struct {
	Err     error
	Columns []string
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
