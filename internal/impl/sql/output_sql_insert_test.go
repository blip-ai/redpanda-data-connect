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

func TestSQLInsertOutputEmptyShutdown(t *testing.T) {
	conf := `
driver: meow
dsn: woof
table: quack
columns: [ foo ]
args_mapping: 'root = [ this.id ]'
`

	spec := sqlInsertOutputConfig()
	env := service.NewEnvironment()

	insertConfig, err := spec.ParseYAML(conf, env)
	require.NoError(t, err)

	insertOutput, err := newSQLInsertOutputFromConfig(insertConfig, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, insertOutput.Close(t.Context()))
}

func TestSQLInsertOutputConfigWithDataTypes(t *testing.T) {
	t.Run("basic config with data_types", func(t *testing.T) {
		spec := sqlInsertOutputConfig()
		parsed, err := spec.ParseYAML(`
driver: mssql
dsn: sqlserver://localhost:1433
table: test_table
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

		table, err := parsed.FieldString("table")
		require.NoError(t, err)
		require.Equal(t, "test_table", table)

		columns, err := parsed.FieldStringList("columns")
		require.NoError(t, err)
		require.Equal(t, []string{"col1", "col2", "col3"}, columns)

		dataTypes, err := parsed.FieldAnyList("data_types")
		require.NoError(t, err)
		require.Len(t, dataTypes, 3)
	})

	t.Run("config without data_types", func(t *testing.T) {
		spec := sqlInsertOutputConfig()
		parsed, err := spec.ParseYAML(`
driver: mysql
dsn: user:pass@tcp(localhost:3306)/db
table: test_table
columns: [id, name]
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

	t.Run("config with prefix and suffix", func(t *testing.T) {
		spec := sqlInsertOutputConfig()
		parsed, err := spec.ParseYAML(`
driver: postgres
dsn: postgres://localhost/db
table: test_table
columns: [id, name]
args_mapping: root = [this.id, this.name]
prefix: "WITH cte AS (SELECT 1)"
suffix: "ON CONFLICT (id) DO NOTHING"
max_in_flight: 1
`, nil)
		require.NoError(t, err)

		prefix, err := parsed.FieldString("prefix")
		require.NoError(t, err)
		require.Equal(t, "WITH cte AS (SELECT 1)", prefix)

		suffix, err := parsed.FieldString("suffix")
		require.NoError(t, err)
		require.Equal(t, "ON CONFLICT (id) DO NOTHING", suffix)
	})

	t.Run("config with options", func(t *testing.T) {
		spec := sqlInsertOutputConfig()
		parsed, err := spec.ParseYAML(`
driver: mysql
dsn: user:pass@tcp(localhost:3306)/db
table: test_table
columns: [id, name]
args_mapping: root = [this.id, this.name]
options: [DELAYED, IGNORE]
max_in_flight: 1
`, nil)
		require.NoError(t, err)

		options, err := parsed.FieldStringList("options")
		require.NoError(t, err)
		require.Equal(t, []string{"DELAYED", "IGNORE"}, options)
	})
}

func TestDataTypesMapping(t *testing.T) {
	t.Run("data types for mssql", func(t *testing.T) {
		applyFunc, exists := applyDataTypeMap["mssql"]
		require.True(t, exists, "mssql should have data type mapping")
		require.NotNil(t, applyFunc)
	})

	t.Run("no data types for unsupported driver", func(t *testing.T) {
		_, exists := applyDataTypeMap["mysql"]
		require.False(t, exists, "mysql should not have special data type mapping")
	})
}
