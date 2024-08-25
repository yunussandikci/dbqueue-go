package dbqueue

import (
	"context"
	"github.com/yunussandikci/dbqueue-go/dbqueue/engines"
	"github.com/yunussandikci/dbqueue-go/dbqueue/types"
	"strings"
)

func Connect(ctx context.Context, connString string) (types.Engine, error) {
	switch {
	case strings.Contains(connString, "postgres"):
		return engines.NewPostgreSQLEngine(ctx, connString)
	case strings.Contains(connString, "sqlite"):
		return engines.NewSQLiteEngine(ctx, connString)
	default:
		return nil, types.ErrDatabaseNotSupported
	}
}
