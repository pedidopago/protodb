package protodb

type QueryError struct {
	Query string
	Err   error
}

func (e *QueryError) Error() string { return e.Query + ": " + e.Err.Error() }
func (e *QueryError) Unwrap() error { return e.Err }

func QueryErr(query string, err error) error {
	return &QueryError{
		Query: query,
		Err:   err,
	}
}

type NotFoundError struct {
	Name string
}

func (e *NotFoundError) Error() string { return e.Name + ": not found" }

// IsNotFound tests if an error is a *NotFoundError
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(*NotFoundError); ok {
		return true
	}
	return false
}

func NotFound(name string) error {
	return &NotFoundError{
		Name: name,
	}
}
