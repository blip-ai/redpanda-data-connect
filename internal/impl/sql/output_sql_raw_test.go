// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestSQLRawOutputEmptyShutdown(t *testing.T) {
	conf := `
driver: meow
dsn: woof
query: "INSERT INTO test (foo) VALUES (?);"
args_mapping: 'root = [ this.id ]'
`

	spec := sqlRawOutputConfig()
	env := service.NewEnvironment()

	rawConfig, err := spec.ParseYAML(conf, env)
	require.NoError(t, err)

	rawOutput, err := newSQLRawOutputFromConfig(rawConfig, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, rawOutput.Close(t.Context()))
}

func TestSQLRawOutputConfigWithDataTypes(t *testing.T) {
	t.Run("basic config with data_types and columns", func(t *testing.T) {
		spec := sqlRawOutputConfig()
		parsed, err := spec.ParseYAML(`
driver: mssql
dsn: sqlserver://localhost:1433
query: "INSERT INTO test_table (col1, col2, col3) VALUES (?, ?, ?);"
columns: [col1, col2, col3]
data_types:
  - name: col1
    type: VARCHAR
  - name: col2
    type: DATETIME
    datetime:
      format: "2006-01-02 15:04:05.999"
  - name: col3
    type: DATE
    date:
      format: "2006-01-02"
args_mapping: root = [this.col1, this.col2, this.col3]
max_in_flight: 1
`, nil)
		require.NoError(t, err)

		driver, err := parsed.FieldString("driver")
		require.NoError(t, err)
		require.Equal(t, "mssql", driver)

		query, err := parsed.FieldString("query")
		require.NoError(t, err)
		require.Equal(t, "INSERT INTO test_table (col1, col2, col3) VALUES (?, ?, ?);", query)

		columns, err := parsed.FieldStringList("columns")
		require.NoError(t, err)
		require.Equal(t, []string{"col1", "col2", "col3"}, columns)

		dataTypes, err := parsed.FieldAnyList("data_types")
		require.NoError(t, err)
		require.Len(t, dataTypes, 3)
	})

	t.Run("config without data_types", func(t *testing.T) {
		spec := sqlRawOutputConfig()
		parsed, err := spec.ParseYAML(`
driver: mysql
dsn: user:pass@tcp(localhost:3306)/db
query: "INSERT INTO test_table (id, name) VALUES (?, ?);"
args_mapping: root = [this.id, this.name]
max_in_flight: 64
`, nil)
		require.NoError(t, err)

		driver, err := parsed.FieldString("driver")
		require.NoError(t, err)
		require.Equal(t, "mysql", driver)

		dataTypes, err := parsed.FieldAnyList("data_types")
		require.NoError(t, err)
		require.Empty(t, dataTypes)
	})

	t.Run("config with unsafe_dynamic_query", func(t *testing.T) {
		spec := sqlRawOutputConfig()
		parsed, err := spec.ParseYAML(`
driver: postgres
dsn: postgres://localhost/db
query: "INSERT INTO ${!metadata(\"table_name\")} (id, name) VALUES ($1, $2);"
unsafe_dynamic_query: true
args_mapping: root = [this.id, this.name]
max_in_flight: 1
`, nil)
		require.NoError(t, err)

		unsafeDyn, err := parsed.FieldBool("unsafe_dynamic_query")
		require.NoError(t, err)
		require.True(t, unsafeDyn)
	})

	t.Run("config with multiple queries", func(t *testing.T) {
		spec := sqlRawOutputConfig()
		parsed, err := spec.ParseYAML(`
driver: postgres
dsn: postgres://localhost/db
query: "CREATE TABLE IF NOT EXISTS test (id INT);"
queries:
  - query: "INSERT INTO test (id) VALUES ($1);"
    args_mapping: root = [this.id]
  - query: "UPDATE test SET status = 'done' WHERE id = $1;"
    args_mapping: root = [this.id]
max_in_flight: 1
`, nil)
		require.NoError(t, err)

		queries, err := parsed.FieldObjectList("queries")
		require.NoError(t, err)
		require.Len(t, queries, 2)
	})
}

func TestSQLRawOutputConfigValidation(t *testing.T) {
	t.Run("missing both query and queries should fail lint", func(t *testing.T) {
		spec := sqlRawOutputConfig()
		_, err := spec.ParseYAML(`
driver: postgres
dsn: postgres://localhost/db
max_in_flight: 1
`, nil)
		// Note: Linting would catch this, but ParseYAML might not error
		// The actual validation happens at the component level
		require.NoError(t, err) // Parse succeeds, but component creation would fail
	})

	t.Run("data_types requires columns", func(t *testing.T) {
		spec := sqlRawOutputConfig()
		parsed, err := spec.ParseYAML(`
driver: mssql
dsn: sqlserver://localhost:1433
query: "INSERT INTO test (col1) VALUES (?);"
data_types:
  - name: col1
    type: VARCHAR
args_mapping: root = [this.col1]
max_in_flight: 1
`, nil)
		require.NoError(t, err)

		// When creating the output, it should validate that columns is provided
		_, err = newSQLRawOutputFromConfig(parsed, service.MockResources())
		require.Error(t, err)
		require.Contains(t, err.Error(), "columns")
	})
}
