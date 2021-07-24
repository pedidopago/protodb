package protodb

type QueryError struct {
	Message string // public message
	Query   string // query identifier
	Err     error  // private underlying error
}

func (e *QueryError) Error() string { return e.Query + ": " + e.Err.Error() }
func (e *QueryError) Unwrap() error { return e.Err }

func QueryErr(pubmsg, queryid string, err error) error {
	return &QueryError{
		Message: pubmsg,
		Query:   queryid,
		Err:     err,
	}
}

// IsQueryError tests if an error is a *QueryError
func IsQueryError(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(*QueryError); ok {
		return true
	}
	return false
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
