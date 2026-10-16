package vector

import (
	"context"
	"database/sql"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strings"
)

// ChunkCheckpoint is one completed provider input awaiting message publication.
type ChunkCheckpoint struct {
	MessageID  int64
	ChunkIndex int
	InputHash  string
	Vector     []float32
}

// ChunkCheckpointStore shares the generation database's lifetime and dialect.
type ChunkCheckpointStore struct {
	DB     *sql.DB
	Rebind func(string) string
}

func (s ChunkCheckpointStore) Load(ctx context.Context, gen GenerationID, messageID int64) (map[int]ChunkCheckpoint, error) {
	rows, err := s.DB.QueryContext(ctx, s.Rebind(`SELECT chunk_index, input_hash, vector_json
		FROM embedding_chunk_checkpoints WHERE generation_id = ? AND document_key = '' AND message_id = ?`), gen, messageID)
	if err != nil {
		return nil, fmt.Errorf("load embedding checkpoints: %w", err)
	}
	defer func() { _ = rows.Close() }()
	result := make(map[int]ChunkCheckpoint)
	for rows.Next() {
		c := ChunkCheckpoint{MessageID: messageID}
		var encoded string
		if err := rows.Scan(&c.ChunkIndex, &c.InputHash, &encoded); err != nil {
			return nil, fmt.Errorf("read embedding checkpoint: %w", err)
		}
		if err := json.Unmarshal([]byte(encoded), &c.Vector); err != nil {
			return nil, fmt.Errorf("decode embedding checkpoint: %w", err)
		}
		result[c.ChunkIndex] = c
	}
	return result, rows.Err()
}

func (s ChunkCheckpointStore) Save(ctx context.Context, gen GenerationID, chunks []ChunkCheckpoint) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin embedding checkpoint: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, c := range chunks {
		encoded, err := json.Marshal(c.Vector)
		if err != nil {
			return fmt.Errorf("encode embedding checkpoint: %w", err)
		}
		_, err = tx.ExecContext(ctx, s.Rebind(`INSERT INTO embedding_chunk_checkpoints
			(generation_id, message_id, chunk_index, input_hash, vector_json) VALUES (?, ?, ?, ?, ?)
			ON CONFLICT (generation_id, document_key, message_id, chunk_index) DO UPDATE
			SET input_hash = excluded.input_hash, vector_json = excluded.vector_json`),
			gen, c.MessageID, c.ChunkIndex, c.InputHash, string(encoded))
		if err != nil {
			return fmt.Errorf("save embedding checkpoint: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit embedding checkpoint: %w", err)
	}
	return nil
}

func (s ChunkCheckpointStore) Delete(ctx context.Context, gen GenerationID, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	for start := 0; start < len(ids); start += 500 {
		batch := ids[start:min(start+500, len(ids))]
		args := []any{gen}
		for _, id := range batch {
			args = append(args, id)
		}
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(batch)), ",")
		if _, err := s.DB.ExecContext(ctx, s.Rebind(`DELETE FROM embedding_chunk_checkpoints
			WHERE generation_id = ? AND document_key = '' AND message_id IN (`+placeholders+`)`), args...); err != nil {
			return fmt.Errorf("delete embedding checkpoints: %w", err)
		}
	}
	return nil
}

// DocumentCheckpoint stages a whole contextual document without changing scope membership.
type DocumentCheckpoint struct {
	Key            string      `json:"-"`
	ScopeKey       string      `json:"-"`
	InputHash      string      `json:"-"`
	Vectors        [][]float32 `json:"vectors"`
	EmbeddedChunks []string    `json:"embedded_chunks,omitempty"`
}

func (s ChunkCheckpointStore) LoadDocument(ctx context.Context, gen GenerationID, key, hash string, chunks int) (DocumentCheckpoint, bool, error) {
	var c DocumentCheckpoint
	var savedHash, encoded string
	var dimension int
	err := s.DB.QueryRowContext(ctx, s.Rebind(`SELECT c.input_hash, c.scope_key, c.vector_json, g.dimension
		FROM embedding_chunk_checkpoints c JOIN index_generations g ON g.id = c.generation_id
		WHERE c.generation_id = ? AND c.document_key = ? AND c.message_id = 0 AND c.chunk_index = 0`), gen, key).Scan(&savedHash, &c.ScopeKey, &encoded, &dimension)
	if errors.Is(err, sql.ErrNoRows) {
		return c, false, nil
	}
	if err != nil {
		return c, false, fmt.Errorf("load document checkpoint: %w", err)
	}
	if savedHash != hash {
		return c, false, nil
	}
	if err := json.Unmarshal([]byte(encoded), &c); err != nil {
		return c, false, fmt.Errorf("decode document checkpoint: %w", err)
	}
	c.Key, c.InputHash = key, hash
	if len(c.Vectors) != chunks || (c.EmbeddedChunks != nil && len(c.EmbeddedChunks) != chunks) {
		return c, false, nil
	}
	for _, v := range c.Vectors {
		if len(v) != dimension {
			return c, false, nil
		}
	}
	return c, true, nil
}

func (s ChunkCheckpointStore) SaveDocuments(ctx context.Context, gen GenerationID, documents []DocumentCheckpoint) error {
	if len(documents) == 0 {
		return nil
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin document checkpoint: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, c := range documents {
		encoded, err := json.Marshal(c)
		if err != nil {
			return fmt.Errorf("encode document checkpoint: %w", err)
		}
		_, err = tx.ExecContext(ctx, s.Rebind(`INSERT INTO embedding_chunk_checkpoints
			(generation_id, document_key, scope_key, message_id, chunk_index, input_hash, vector_json) VALUES (?, ?, ?, 0, 0, ?, ?)
			ON CONFLICT (generation_id, document_key, message_id, chunk_index) DO UPDATE
			SET scope_key = excluded.scope_key, input_hash = excluded.input_hash, vector_json = excluded.vector_json`), gen, c.Key, c.ScopeKey, c.InputHash, string(encoded))
		if err != nil {
			return fmt.Errorf("save document checkpoint: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit document checkpoint: %w", err)
	}
	return nil
}

func (s ChunkCheckpointStore) DeleteDocumentScope(ctx context.Context, gen GenerationID, scope string) error {
	_, err := s.DB.ExecContext(ctx, s.Rebind(`DELETE FROM embedding_chunk_checkpoints WHERE generation_id = ? AND scope_key = ?`), gen, scope)
	if err != nil {
		return fmt.Errorf("delete document checkpoints: %w", err)
	}
	return nil
}
