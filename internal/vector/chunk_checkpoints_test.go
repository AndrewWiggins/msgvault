//go:build cgo

package vector

import (
	"database/sql"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckpointCleanupBoundsDeleteParameters(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(err)
	t.Cleanup(func() { require.NoError(db.Close()) })
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TABLE index_generations (id BIGINT PRIMARY KEY); INSERT INTO index_generations VALUES (1)`)
	require.NoError(err)
	_, err = db.Exec(ChunkCheckpointSchema)
	require.NoError(err)
	checkpoints := ChunkCheckpointStore{DB: db, Rebind: func(query string) string { return query }}
	chunks := make([]ChunkCheckpoint, 600)
	ids := make([]int64, len(chunks))
	for i := range chunks {
		ids[i] = int64(i + 1)
		chunks[i] = ChunkCheckpoint{MessageID: ids[i], InputHash: "synthetic", Vector: []float32{1, 2, 3, 4}}
	}
	require.NoError(checkpoints.Save(t.Context(), GenerationID(1), chunks))
	conn, err := db.Conn(t.Context())
	require.NoError(err)
	require.NoError(conn.Raw(func(driverConn any) error {
		sqliteConn, ok := driverConn.(*sqlite3.SQLiteConn)
		require.True(ok)
		sqliteConn.SetLimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 512)
		return nil
	}))
	require.NoError(conn.Close())
	require.NoError(checkpoints.Delete(t.Context(), GenerationID(1), ids))
	var remaining int
	require.NoError(db.QueryRow(`SELECT COUNT(*) FROM embedding_chunk_checkpoints`).Scan(&remaining))
	assert.Zero(remaining)
}
