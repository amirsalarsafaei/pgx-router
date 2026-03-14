package classify

import (
	"testing"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		comments []string
		want     QueryMode
	}{
		// Basic reads
		{
			name: "simple select",
			sql:  "SELECT * FROM authors WHERE id = $1",
			want: ModeRead,
		},
		{
			name: "select with join",
			sql:  "SELECT a.*, b.title FROM authors a JOIN books b ON a.id = b.author_id",
			want: ModeRead,
		},
		{
			name: "select with subquery",
			sql:  "SELECT * FROM authors WHERE id IN (SELECT author_id FROM books)",
			want: ModeRead,
		},
		{
			name: "select with aggregate",
			sql:  "SELECT author_id, COUNT(*) FROM books GROUP BY author_id",
			want: ModeRead,
		},
		{
			name: "select with CTE read only",
			sql:  "WITH top_authors AS (SELECT * FROM authors LIMIT 10) SELECT * FROM top_authors",
			want: ModeRead,
		},
		{
			name: "select union",
			sql:  "SELECT id, name FROM authors UNION ALL SELECT id, title FROM books",
			want: ModeRead,
		},

		// Locking reads → write
		{
			name: "select for update",
			sql:  "SELECT * FROM authors WHERE id = $1 FOR UPDATE",
			want: ModeWrite,
		},
		{
			name: "select for share",
			sql:  "SELECT * FROM authors WHERE id = $1 FOR SHARE",
			want: ModeWrite,
		},
		{
			name: "select for no key update",
			sql:  "SELECT * FROM authors WHERE id = $1 FOR NO KEY UPDATE",
			want: ModeWrite,
		},
		{
			name: "select for key share",
			sql:  "SELECT * FROM authors WHERE id = $1 FOR KEY SHARE",
			want: ModeWrite,
		},

		// Basic writes
		{
			name: "insert",
			sql:  "INSERT INTO authors (name, bio) VALUES ($1, $2)",
			want: ModeWrite,
		},
		{
			name: "insert returning",
			sql:  "INSERT INTO authors (name, bio) VALUES ($1, $2) RETURNING *",
			want: ModeWrite,
		},
		{
			name: "update",
			sql:  "UPDATE authors SET name = $2 WHERE id = $1",
			want: ModeWrite,
		},
		{
			name: "update returning",
			sql:  "UPDATE authors SET name = $2 WHERE id = $1 RETURNING *",
			want: ModeWrite,
		},
		{
			name: "delete",
			sql:  "DELETE FROM authors WHERE id = $1",
			want: ModeWrite,
		},

		// CTEs with mutations
		{
			name: "CTE with insert",
			sql:  "WITH new_author AS (INSERT INTO authors (name) VALUES ($1) RETURNING *) SELECT * FROM new_author",
			want: ModeWrite,
		},
		{
			name: "CTE with update",
			sql:  "WITH updated AS (UPDATE authors SET name = $2 WHERE id = $1 RETURNING *) SELECT * FROM updated",
			want: ModeWrite,
		},
		{
			name: "CTE with delete",
			sql:  "WITH deleted AS (DELETE FROM authors WHERE id = $1 RETURNING *) SELECT * FROM deleted",
			want: ModeWrite,
		},

		// Comment overrides
		{
			name:     "override select to write",
			sql:      "SELECT * FROM authors WHERE id = $1",
			comments: []string{" rw_mode:write"},
			want:     ModeWrite,
		},
		{
			name:     "override insert to read",
			sql:      "INSERT INTO authors (name) VALUES ($1) RETURNING *",
			comments: []string{" rw_mode:read"},
			want:     ModeRead,
		},
		{
			name:     "override case insensitive",
			sql:      "SELECT * FROM authors",
			comments: []string{" RW_MODE:WRITE"},
			want:     ModeWrite,
		},
		{
			name:     "override select to write with rw shorthand",
			sql:      "SELECT * FROM authors WHERE id = $1",
			comments: []string{"rw:write"},
			want:     ModeWrite,
		},
		{
			name:     "override insert to read with rw shorthand",
			sql:      "INSERT INTO authors (name) VALUES ($1) RETURNING *",
			comments: []string{"rw:read"},
			want:     ModeRead,
		},
		{
			name:     "non-matching comment ignored",
			sql:      "SELECT * FROM authors",
			comments: []string{" this is a regular comment"},
			want:     ModeRead,
		},

		// Edge cases
		{
			name: "explain is write (safe fallback)",
			sql:  "EXPLAIN SELECT * FROM authors",
			want: ModeWrite,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Classify(tt.sql, tt.comments)
			if got != tt.want {
				t.Errorf("Classify(%q) = %v, want %v", tt.sql, got, tt.want)
			}
		})
	}
}
