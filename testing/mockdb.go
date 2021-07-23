package testing

import (
	"testing"

	sqlm "github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// MockDBMySQL returns a "mysql/mariadb" mock database
func MockDBMySQL(t *testing.T) (db *sqlx.DB, mock sqlm.Sqlmock) {
	rawdb, mock, err := sqlm.New()
	require.NoError(t, err)
	db = sqlx.NewDb(rawdb, "mysql")
	return db, mock
}
