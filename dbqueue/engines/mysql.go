package engines

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/yunussandikci/dbqueue-go/dbqueue/types"
	"strconv"
	"strings"
	"time"
)

type mysqlEngine struct {
	db *sql.DB
}

type mysqlQueue struct {
	db    *sql.DB
	table string
}

func NewMySQLEngine(_ context.Context, conn string) (types.Engine, error) {
	db, newErr := sql.Open("mysql", conn)
	if newErr != nil {
		return nil, newErr
	}
	return &mysqlEngine{
		db: db,
	}, nil
}

func (p *mysqlEngine) OpenQueue(ctx context.Context, name string) (types.Queue, error) {
	var exists int
	query := `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?;`
	if queryErr := p.db.QueryRowContext(ctx, query, name).Scan(&exists); queryErr != nil {
		return nil, queryErr
	}

	if exists == 0 {
		return nil, types.ErrQueueNotFound
	}

	return &mysqlQueue{
		db:    p.db,
		table: name,
	}, nil
}

func (p *mysqlEngine) CreateQueue(ctx context.Context, name string) (types.Queue, error) {
	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
				id BIGINT AUTO_INCREMENT PRIMARY KEY,
				deduplication_id VARCHAR(255) UNIQUE,
				payload BLOB,
				priority INT DEFAULT 0,
				retrieval INT DEFAULT 0,
				visible_after INT(11) NOT NULL,
				created_at INT(11) NOT NULL);`, name)
	_, execErr := p.db.ExecContext(ctx, query)
	return &mysqlQueue{
		db:    p.db,
		table: name,
	}, execErr
}

func (p *mysqlEngine) DeleteQueue(ctx context.Context, name string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s;", name)
	_, execErr := p.db.ExecContext(ctx, query)
	return execErr
}

func (p *mysqlEngine) PurgeQueue(ctx context.Context, name string) error {
	query := fmt.Sprintf("DELETE FROM %s;", name)
	_, execErr := p.db.ExecContext(ctx, query)
	return execErr
}

func (p *mysqlQueue) ReceiveMessage(ctx context.Context, fun func(message types.ReceivedMessage),
	options types.ReceiveMessageOptions) error {
	opts := options.Defaults()
	limit := ""

	if *opts.MaxNumberOfMessages != 0 {
		limit = fmt.Sprintf("LIMIT %d", *opts.MaxNumberOfMessages)
	}

	for {
		transaction, beginErr := p.db.BeginTx(ctx, nil)
		if beginErr != nil {
			return beginErr
		}

		query := fmt.Sprintf(`SELECT id, deduplication_id, payload, priority, retrieval, created_at 
		FROM %s WHERE visible_after < ? ORDER BY priority DESC, id ASC %s FOR UPDATE SKIP LOCKED;`, p.table, limit)

		rows, queryErr := transaction.QueryContext(ctx, query, time.Now().Unix())
		if queryErr != nil {
			return errors.Join(queryErr, transaction.Rollback())
		}

		var ids []string
		var messages []types.ReceivedMessage
		var visibleAfter = time.Now().Add(*opts.VisibilityTimeout).Unix()

		for rows.Next() {
			var message types.ReceivedMessage
			if scanErr := rows.Scan(&message.ID, &message.DeduplicationID, &message.Payload,
				&message.Priority, &message.Retrieval, &message.CreatedAt); scanErr != nil {
				return errors.Join(scanErr, transaction.Rollback())
			}

			message.VisibleAfter = &visibleAfter
			message.Retrieval++
			messages = append(messages, message)
			ids = append(ids, strconv.Itoa(int(message.ID)))
		}

		if rowsErr := rows.Err(); rowsErr != nil {
			return errors.Join(rowsErr, transaction.Rollback())
		}

		if closeErr := rows.Close(); closeErr != nil {
			return errors.Join(closeErr, transaction.Rollback())
		}

		if len(messages) != 0 {
			updateQuery := fmt.Sprintf(`UPDATE %s SET visible_after = ?, retrieval = retrieval + 1 
		WHERE id IN (%s);`, p.table, strings.Join(ids, ", "))

			if _, execErr := transaction.ExecContext(ctx, updateQuery, visibleAfter); execErr != nil {
				return errors.Join(execErr, transaction.Rollback())
			}
		}

		if commitErr := transaction.Commit(); commitErr != nil {
			return commitErr
		}

		for _, message := range messages {
			fun(message)
		}

		if len(messages) == 0 {
			time.Sleep(*opts.WaitTime)
		}
	}
}

func (p *mysqlQueue) SendMessage(ctx context.Context, message *types.Message) error {
	return p.SendMessageBatch(ctx, []*types.Message{message})
}

func (p *mysqlQueue) SendMessageBatch(ctx context.Context, messages []*types.Message) error {
	now := time.Now()
	transaction, beginTransactionErr := p.db.BeginTx(ctx, nil)
	if beginTransactionErr != nil {
		return beginTransactionErr
	}

	query := fmt.Sprintf(`INSERT IGNORE INTO %s 
		(deduplication_id, payload, priority, visible_after, created_at) 
		VALUES (?, ?, ?, ?, ?);`, p.table)

	statement, prepareErr := transaction.Prepare(query)
	if prepareErr != nil {
		return errors.Join(prepareErr, transaction.Rollback())
	}

	for _, message := range messages {
		var (
			deduplicationID string
			visibleAfter    int64
		)
		if message.DeduplicationID == nil {
			deduplicationID = uuid.NewString()
		} else {
			deduplicationID = *message.DeduplicationID
		}

		if message.VisibleAfter != nil {
			visibleAfter = *message.VisibleAfter
		} else {
			visibleAfter = now.Unix()
		}

		_, execErr := statement.Exec(deduplicationID, message.Payload,
			message.Priority, visibleAfter, now.Unix())
		if execErr != nil {
			return errors.Join(execErr, transaction.Rollback())
		}
	}

	return errors.Join(transaction.Commit(), statement.Close())
}

func (p *mysqlQueue) DeleteMessage(ctx context.Context, id uint) error {
	return p.DeleteMessageBatch(ctx, []uint{id})
}

func (p *mysqlQueue) DeleteMessageBatch(ctx context.Context, ids []uint) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE id = ?;`, p.table)
	statement, prepareErr := p.db.PrepareContext(ctx, query)
	if prepareErr != nil {
		return prepareErr
	}
	defer statement.Close()

	for _, id := range ids {
		if _, execErr := statement.Exec(id); execErr != nil {
			return execErr
		}
	}

	return nil
}

func (p *mysqlQueue) ChangeMessageVisibility(ctx context.Context, id uint, visibilityTimeout time.Duration) error {
	return p.ChangeMessageVisibilityBatch(ctx, []uint{id}, visibilityTimeout)
}

func (p *mysqlQueue) ChangeMessageVisibilityBatch(ctx context.Context, ids []uint,
	visibilityTimeout time.Duration) error {
	query := fmt.Sprintf(`UPDATE %s SET visible_after = ? WHERE id = ?;`, p.table)
	statement, prepareErr := p.db.PrepareContext(ctx, query)
	if prepareErr != nil {
		return prepareErr
	}
	defer statement.Close()

	visibleAfter := time.Now().Add(visibilityTimeout).Unix()
	for _, id := range ids {
		if _, execErr := statement.Exec(visibleAfter, id); execErr != nil {
			return execErr
		}
	}

	return nil
}
