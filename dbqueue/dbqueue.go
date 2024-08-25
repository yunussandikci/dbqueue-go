package dbqueue

import (
	"context"
	"github.com/yunussandikci/dbqueue-go/dbqueue/engines"
	"github.com/yunussandikci/dbqueue-go/dbqueue/types"
)

func OpenPostgreSQL(ctx context.Context, connString string) (types.Engine, error) {
	return engines.NewPostgreSQLEngine(ctx, connString)
}

func OpenMySQL(ctx context.Context, connString string) (types.Engine, error) {
	return engines.NewMySQLEngine(ctx, connString)
}

func OpenSQLite(ctx context.Context, connString string) (types.Engine, error) {
	return engines.NewSQLiteEngine(ctx, connString)
}
