package classify

import (
	"regexp"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

type QueryMode int

const (
	ModeRead QueryMode = iota
	ModeWrite
)

func (m QueryMode) String() string {
	if m == ModeRead {
		return "read"
	}
	return "write"
}

var rwOverrideRe = regexp.MustCompile(`(?i)^\s*rw(?:_mode)?:\s*(read|write)\s*$`)

func Classify(sql string, comments []string) QueryMode {
	if mode, ok := checkOverride(comments); ok {
		return mode
	}

	result, err := pg_query.Parse(sql)
	if err != nil {
		return ModeWrite
	}

	for _, stmt := range result.Stmts {
		if classifyNode(stmt.Stmt) == ModeWrite {
			return ModeWrite
		}
	}

	return ModeRead
}

func checkOverride(comments []string) (QueryMode, bool) {
	for _, c := range comments {
		c = strings.TrimSpace(c)
		c = strings.TrimPrefix(c, "--")
		c = strings.TrimPrefix(c, "/*")
		c = strings.TrimSuffix(c, "*/")
		c = strings.TrimSpace(c)

		matches := rwOverrideRe.FindStringSubmatch(c)
		if len(matches) == 2 {
			switch strings.ToLower(matches[1]) {
			case "write":
				return ModeWrite, true
			case "read":
				return ModeRead, true
			}
		}
	}
	return ModeRead, false
}

func classifyNode(node *pg_query.Node) QueryMode {
	if node == nil {
		return ModeWrite
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_SelectStmt:
		return classifySelect(n.SelectStmt)
	case *pg_query.Node_InsertStmt:
		return ModeWrite
	case *pg_query.Node_UpdateStmt:
		return ModeWrite
	case *pg_query.Node_DeleteStmt:
		return ModeWrite
	case *pg_query.Node_ExplainStmt:
		return ModeWrite
	default:
		return ModeWrite
	}
}

func classifySelect(stmt *pg_query.SelectStmt) QueryMode {
	if stmt == nil {
		return ModeWrite
	}

	// Check for set operations (UNION, INTERSECT, EXCEPT)
	if stmt.Larg != nil || stmt.Rarg != nil {
		if stmt.Larg != nil && classifySelect(stmt.Larg) == ModeWrite {
			return ModeWrite
		}
		if stmt.Rarg != nil && classifySelect(stmt.Rarg) == ModeWrite {
			return ModeWrite
		}
	}

	// Check for WITH clause (CTEs)
	if stmt.WithClause != nil && hasWriteCTE(stmt.WithClause) {
		return ModeWrite
	}

	// Check for locking clauses (FOR UPDATE, FOR SHARE, etc.)
	if len(stmt.LockingClause) > 0 {
		return ModeWrite
	}

	return ModeRead
}

func hasWriteCTE(withClause *pg_query.WithClause) bool {
	if withClause == nil {
		return false
	}

	for _, cte := range withClause.Ctes {
		cteNode, ok := cte.Node.(*pg_query.Node_CommonTableExpr)
		if !ok {
			continue
		}
		if classifyNode(cteNode.CommonTableExpr.Ctequery) == ModeWrite {
			return true
		}
	}
	return false
}
