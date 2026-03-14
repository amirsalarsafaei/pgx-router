package pgxrouter

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/amirsalarsafaei/pgx-router/classify"
)

// Option configures the Pool behaviour.
type Option func(*poolConfig)

type poolConfig struct {
	retryOnError func(error) bool
}

func WithRetryOnError(fn func(error) bool) Option {
	return func(c *poolConfig) {
		c.retryOnError = fn
	}
}

type Pool struct {
	*pgxpool.Pool
	read *pgxpool.Pool
	cfg  poolConfig
}

func New(main, read *pgxpool.Pool, opts ...Option) *Pool {
	if read == nil {
		read = main
	}
	var cfg poolConfig
	for _, o := range opts {
		o(&cfg)
	}
	return &Pool{Pool: main, read: read, cfg: cfg}
}

func (p *Pool) ReadPool() *pgxpool.Pool { return p.read }

func (p *Pool) MainPool() *pgxpool.Pool { return p.Pool }

func (p *Pool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	pool := p.route(sql)
	tag, err := pool.Exec(ctx, sql, args...)
	if pool == p.read && p.read != p.Pool && p.shouldRetryOnMain(err) {
		return p.Pool.Exec(ctx, sql, args...)
	}
	return tag, err
}

func (p *Pool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	pool := p.route(sql)
	rows, err := pool.Query(ctx, sql, args...)
	if pool == p.read && p.read != p.Pool {
		if p.shouldRetryOnMain(err) {
			return p.Pool.Query(ctx, sql, args...)
		}
		if err == nil {
			return &retryRows{ctx: ctx, sql: sql, args: args, pool: p, rows: rows, routed: pool}, nil
		}
	}
	return rows, err
}

func (p *Pool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	pool := p.route(sql)
	return &retryRow{ctx: ctx, sql: sql, args: args, pool: p, row: pool.QueryRow(ctx, sql, args...), routed: pool}
}

func (p *Pool) Close() {
	if p.read != p.Pool {
		p.read.Close()
	}
	p.Pool.Close()
}

func (p *Pool) Reset() {
	if p.read != p.Pool {
		p.read.Reset()
	}
	p.Pool.Reset()
}

func (p *Pool) route(sql string) *pgxpool.Pool {
	comments := extractLeadingComments(sql)
	if classify.Classify(sql, comments) == classify.ModeRead {
		return p.read
	}
	return p.Pool
}

// shouldRetryOnMain returns true when an error from the read replica
// warrants retrying on the main pool.
func (p *Pool) shouldRetryOnMain(err error) bool {
	if err == nil {
		return false
	}

	// Always consult custom retry logic (if provided), then also apply
	// built-in read-only detection so fallback-to-main remains robust.
	customRetry := false
	if p.cfg.retryOnError != nil {
		customRetry = p.cfg.retryOnError(err)
	}

	return customRetry || isReadOnlyError(err)
}

// retryRow wraps a pgx.Row and retries on the main pool if the read replica
// returns an error that should be retried.
type retryRow struct {
	ctx    context.Context
	sql    string
	args   []any
	pool   *Pool
	row    pgx.Row
	routed *pgxpool.Pool
}

func (r *retryRow) Scan(dest ...any) error {
	err := r.row.Scan(dest...)
	if r.routed == r.pool.read && r.pool.read != r.pool.Pool && r.pool.shouldRetryOnMain(err) {
		return r.pool.Pool.QueryRow(r.ctx, r.sql, r.args...).Scan(dest...)
	}
	return err
}

// retryRows wraps pgx.Rows so deferred read-only errors from the read replica
// can still fallback to main when first observed during iteration.
type retryRows struct {
	ctx       context.Context
	sql       string
	args      []any
	pool      *Pool
	rows      pgx.Rows
	routed    *pgxpool.Pool
	retried   bool
	overrideE error
}

func (r *retryRows) Close() {
	r.rows.Close()
}

func (r *retryRows) Err() error {
	if r.overrideE != nil {
		return r.overrideE
	}
	return r.rows.Err()
}

func (r *retryRows) CommandTag() pgconn.CommandTag {
	return r.rows.CommandTag()
}

func (r *retryRows) FieldDescriptions() []pgconn.FieldDescription {
	return r.rows.FieldDescriptions()
}

func (r *retryRows) Next() bool {
	if r.rows.Next() {
		return true
	}
	if r.retried || r.routed != r.pool.read || r.pool.read == r.pool.Pool {
		return false
	}
	if !r.pool.shouldRetryOnMain(r.rows.Err()) {
		return false
	}

	r.rows.Close()
	mainRows, err := r.pool.Pool.Query(r.ctx, r.sql, r.args...)
	r.retried = true
	if err != nil {
		r.overrideE = err
		return false
	}

	r.rows = mainRows
	return r.rows.Next()
}

func (r *retryRows) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

func (r *retryRows) Values() ([]any, error) {
	return r.rows.Values()
}

func (r *retryRows) RawValues() [][]byte {
	return r.rows.RawValues()
}

func (r *retryRows) Conn() *pgx.Conn {
	return r.rows.Conn()
}

func isReadOnlyError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgerrcode.ReadOnlySQLTransaction
	}
	return false
}

func extractLeadingComments(sql string) []string {
	var comments []string
	s := strings.TrimSpace(sql)
	for {
		if strings.HasPrefix(s, "--") {
			end := strings.IndexByte(s, '\n')
			if end == -1 {
				comments = append(comments, s)
				break
			}
			comments = append(comments, s[:end])
			s = strings.TrimSpace(s[end+1:])
		} else if strings.HasPrefix(s, "/*") {
			end := strings.Index(s, "*/")
			if end == -1 {
				comments = append(comments, s)
				break
			}
			comments = append(comments, s[:end+2])
			s = strings.TrimSpace(s[end+2:])
		} else {
			break
		}
	}
	return comments
}
