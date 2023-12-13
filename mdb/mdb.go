package mdb

import (
	"database/sql"
	"log"
	"time"

	"github.com/mattn/go-sqlite3"
)

// We'll use this to read and write from the database
type EmailEntry struct {
	Id          int64
	Email       string
	ConfirmedAt *time.Time
	OptOut      bool
}

func TryCreate(db *sql.DB) {
	_, err := db.Exec(`
		CREATE TABLE emails (
			id INTEGER PRIMARY KEY,
			email TEXT UNIQUE,
			confirmed_at INTEGER,
			opt_out INTEGER
		);
	`)
	if err != nil {
		if sqlError, ok := err.(sqlite3.Error); ok {
			// Error code 1 is "table alread exists"
			if sqlError.Code != 1 {
				log.Fatal(sqlError)
			}
		}
	} else {
		log.Fatal(err)
	}
}

func emailEntryFromRow(row *sql.Rows) (*EmailEntry, error) {
	var id int64
	var email string
	var confirmedAt int64
	var optOut bool

	// scans row in database in column order
	err := row.Scan(&id, &email, &confirmedAt, &optOut)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	t := time.Unix(confirmedAt, 0)
	return &EmailEntry{id, email, &t, optOut}, nil
}

func CreateEmail(db *sql.DB, email string) error {
	// We are using 0 to indicate email has not been confirmed
	_, err := db.Exec(`
		INSERT INTO
			emails(email, confirmed_at, opt_out)
			VALUES(?, 0, false)
	`, email)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func GetEmail(db *sql.DB, email string) (*EmailEntry, error) {
	rows, err := db.Query(`
		SELECT id, email, confirmed_at, opt_out
		FROM emails
		WHERE email = ?
	`, email)

	if err != nil {
		log.Println(err)
		return nil, err
	}
	// Unlike db.Exec, db.Query keeps connection open to read more rows
	// so we need to close it when we are finished with it
	defer rows.Close()

	// email rows are unique so this should only iterate once
	for rows.Next() {
		return emailEntryFromRow(rows)
	}

	return nil, nil
}

func UpdateEmail(db *sql.DB, entry EmailEntry) error {
	t := entry.ConfirmedAt.Unix() // converts time to int for db

	// Insert when email doesn't exist else update
	_, err := db.Exec(`
		INSERT INTO
			emails(email, confirmed_at, opt_out)
			VALUES(?, ?, ?)
			ON CONFLICT(email) DO UPDATE SET
				confirmed_at=?
				opt_out=?
	`, entry.Email, t, entry.OptOut, t, entry.OptOut)

	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func DeleteEmail(db *sql.DB, email string) error {
	// Rather than deleting the data we're opting out to
	// guarantee the user's email can't be readded
	_, err := db.Exec(`
		UPDATE emails
		SET opt_out=true
		WHERE email=?
	`, email)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

type GetEmailBatchQueryParams struct {
	Page  int // for pagination
	Count int // number emails returned
}

func GetEmailBatch(db *sql.DB, params GetEmailBatchQueryParams) ([]EmailEntry, error) {
	var empty []EmailEntry

	rows, err := db.Query(`
		SELECT id, email, confirmed_at, opt_out
		FROM emails
		WHERE opt_out = false
		ORDER BY id ASC
		LIMIT ? OFFSET ?
	`, params.Count, (params.Page-1)*params.Count)
	if err != nil {
		log.Println(err)
		return empty, err
	}

	defer rows.Close()

	emails := make([]EmailEntry, 0, params.Count)

	for rows.Next() {
		email, err := emailEntryFromRow(rows)
		if err != nil {
			return nil, err
		}
		emails = append(emails, *email)
	}
	return emails, nil
}
