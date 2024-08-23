package dbqueue

import (
	"context"
	"github.com/yunussandikci/dbqueue-go/dbqueue/engines"
	"github.com/yunussandikci/dbqueue-go/dbqueue/types"
	"strings"
)

func Connect(ctx context.Context, connString string) (types.Engine, error) {
	if strings.Contains(connString, "postgres") {
		return engines.NewPostgreSQLEngine(ctx, connString)
	}

	return nil, types.ErrEngineNotSupported
}
