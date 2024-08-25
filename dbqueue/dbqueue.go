package dbqueue

import (
	"context"
	"github.com/yunussandikci/dbqueue-go/dbqueue/engines"
	"github.com/yunussandikci/dbqueue-go/dbqueue/types"
	"strings"
)

func Connect(ctx context.Context, connString string) (types.Engine, error) {
	switch {
	case strings.HasPrefix(connString, "postgres://") ||
		strings.HasPrefix(connString, "postgresql://"):
		return engines.NewPostgreSQLEngine(ctx, connString)
	case strings.HasSuffix(connString, ".db") ||
		strings.HasPrefix(connString, "sqlite:") ||
		strings.HasPrefix(connString, "file:"):
		return engines.NewSQLiteEngine(ctx, connString)
	default:
		return nil, types.ErrDatabaseNotSupported
	}
}
