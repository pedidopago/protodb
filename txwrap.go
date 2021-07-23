package protodb

import "github.com/jmoiron/sqlx"

// Wrap creates a new DB transaction that automatically commits or performs a rollback
// when the function returns.
func Wrap(db *sqlx.DB, fn func(*sqlx.Tx) error) error {
	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}
