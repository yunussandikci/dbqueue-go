package engines

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yunussandikci/dbqueue-go/dbqueue/types"
	"strconv"
	"time"
)

type postgreSQLEngine struct {
	db *pgxpool.Pool
}
type postgreSQLQueue struct {
	db    *pgxpool.Pool
	table string
}

func NewPostgreSQLEngine(ctx context.Context, conn string) (types.Engine, error) {
	db, newErr := pgxpool.New(ctx, conn)
	if newErr != nil {
		return nil, newErr
	}
	return &postgreSQLEngine{
		db: db,
	}, nil
}

func (p *postgreSQLEngine) OpenQueue(ctx context.Context, name string) (types.Queue, error) {
	var (
		exists = false
		query  = `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1);`
	)
	if queryErr := p.db.QueryRow(ctx, query, name).Scan(&exists); queryErr != nil {
		return nil, queryErr
	}

	if !exists {
		return nil, types.ErrQueueNotFound
	}

	return &postgreSQLQueue{
		db:    p.db,
		table: name,
	}, nil
}

func (p *postgreSQLEngine) CreateQueue(ctx context.Context, name string) (types.Queue, error) {
	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
				id SERIAL PRIMARY KEY,
				deduplication_id TEXT UNIQUE,
				payload BYTEA,
				priority INTEGER DEFAULT 0,
				retrieval INTEGER DEFAULT 0,
				visible_after BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()),
				created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()));`, name)
	_, execErr := p.db.Exec(ctx, query)
	return &postgreSQLQueue{
		db:    p.db,
		table: name,
	}, execErr
}

func (p *postgreSQLEngine) DeleteQueue(ctx context.Context, name string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s;", name)
	_, execErr := p.db.Exec(ctx, query)
	return execErr
}

func (p *postgreSQLEngine) PurgeQueue(ctx context.Context, name string) error {
	query := fmt.Sprintf("DELETE FROM %s;", name)
	_, execErr := p.db.Exec(ctx, query, name)
	return execErr
}

func (p *postgreSQLQueue) ReceiveMessage(ctx context.Context,
	fun func(message types.ReceivedMessage), options types.ReceiveMessageOptions) error {
	opts := options.Defaults()
	limit := strconv.Itoa(*opts.MaxNumberOfMessages)
	if *opts.MaxNumberOfMessages == 0 {
		limit = "ALL"
	}

	for {
		query := fmt.Sprintf(`UPDATE %s 
		SET retrieval = retrieval + 1, visible_after = %d
		WHERE id IN (
			SELECT id FROM %s 
			WHERE visible_after < %d
			ORDER BY priority DESC, id ASC 
			FOR UPDATE SKIP LOCKED
			LIMIT %s
		)
		RETURNING id, deduplication_id, payload, priority, visible_after, retrieval, created_at;`,
			p.table, time.Now().Add(*opts.VisibilityTimeout).Unix(), p.table, time.Now().Unix(), limit)

		rows, queryErr := p.db.Query(ctx, query)
		if queryErr != nil {
			return queryErr
		}

		rowCount := 0
		for rows.Next() {
			rowCount += 1
			var msg types.ReceivedMessage
			if scanErr := rows.Scan(&msg.ID, &msg.DeduplicationID, &msg.Payload, &msg.Priority, &msg.VisibleAfter,
				&msg.Retrieval, &msg.CreatedAt); scanErr != nil {
				return scanErr
			}
			fun(msg)
		}

		if rowsErr := rows.Err(); rowsErr != nil {
			return rowsErr
		}

		rows.Close()
		if rowCount == 0 {
			time.Sleep(*opts.WaitTime)
		}
	}
}

func (p *postgreSQLQueue) SendMessage(ctx context.Context, message *types.Message) error {
	return p.SendMessageBatch(ctx, []*types.Message{message})
}

func (p *postgreSQLQueue) SendMessageBatch(ctx context.Context, messages []*types.Message) error {
	batch := &pgx.Batch{}
	for _, message := range messages {
		var deduplicationID string
		if message.DeduplicationID != nil {
			deduplicationID = *message.DeduplicationID
		} else {
			deduplicationID = uuid.NewString()
		}

		query := fmt.Sprintf(`INSERT INTO %s 
			(deduplication_id, payload, priority, visible_after) 
			VALUES ('%s', '%s', %d, %d)
			ON CONFLICT (deduplication_id) DO NOTHING;`,
			p.table, deduplicationID, message.Payload, message.Priority, message.VisibleAfter)

		batch.Queue(query)
	}

	batchResult := p.db.SendBatch(ctx, batch)
	if batchCloseErr := batchResult.Close(); batchCloseErr != nil {
		return batchCloseErr
	}

	return nil
}

func (p *postgreSQLQueue) DeleteMessage(ctx context.Context, id uint) error {
	return p.DeleteMessageBatch(ctx, []uint{id})
}

func (p *postgreSQLQueue) DeleteMessageBatch(ctx context.Context, ids []uint) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE id = ANY($1);`, p.table)
	_, execErr := p.db.Exec(ctx, query, ids)
	return execErr
}

func (p *postgreSQLQueue) ChangeMessageVisibility(ctx context.Context, id uint, visibilityTimeout time.Duration) error {
	return p.ChangeMessageVisibilityBatch(ctx, []uint{id}, visibilityTimeout)
}

func (p *postgreSQLQueue) ChangeMessageVisibilityBatch(ctx context.Context, ids []uint,
	visibilityTimeout time.Duration) error {
	query := fmt.Sprintf(`UPDATE %s SET visible_after = $1 WHERE id = ANY($2);`, p.table)
	_, execErr := p.db.Exec(ctx, query, time.Now().Add(visibilityTimeout).Unix(), ids)
	return execErr
}
