//go:build sqlite_vec

package vector_test

import (
	"database/sql"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/vector"
	"go.kenn.io/msgvault/internal/vector/sqlitevec"
)

func TestCheckpointCleanupBoundsDeleteParameters(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	require.NoError(sqlitevec.RegisterExtension())
	db, err := sql.Open(sqlitevec.DriverName(), ":memory:")
	require.NoError(err)
	t.Cleanup(func() { require.NoError(db.Close()) })
	db.SetMaxOpenConns(1)
	require.NoError(sqlitevec.Migrate(t.Context(), db, 4))
	_, err = db.Exec(`INSERT INTO index_generations (id, model, dimension, fingerprint, started_at, state) VALUES (1, 'synthetic', 4, 'synthetic:4', 1, 'building')`)
	require.NoError(err)
	checkpoints := vector.ChunkCheckpointStore{DB: db, Rebind: func(query string) string { return query }}
	chunks := make([]vector.ChunkCheckpoint, 600)
	ids := make([]int64, len(chunks))
	for i := range chunks {
		ids[i] = int64(i + 1)
		chunks[i] = vector.ChunkCheckpoint{MessageID: ids[i], InputHash: "synthetic", Vector: []float32{1, 2, 3, 4}}
	}
	require.NoError(checkpoints.Save(t.Context(), vector.GenerationID(1), chunks))
	conn, err := db.Conn(t.Context())
	require.NoError(err)
	require.NoError(conn.Raw(func(driverConn any) error {
		sqliteConn, ok := driverConn.(*sqlite3.SQLiteConn)
		require.True(ok)
		sqliteConn.SetLimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 512)
		return nil
	}))
	require.NoError(conn.Close())
	require.NoError(checkpoints.Delete(t.Context(), vector.GenerationID(1), ids))
	var remaining int
	require.NoError(db.QueryRow(`SELECT COUNT(*) FROM embedding_chunk_checkpoints`).Scan(&remaining))
	assert.Zero(remaining)
}
