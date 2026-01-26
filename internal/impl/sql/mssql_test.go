package sql

import (
	"testing"
	"time"

	"github.com/golang-sql/civil"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/assert"
)

func TestApplyMSSQLDataType(t *testing.T) {
	tests := []struct {
		name     string
		arg      any
		column   string
		DataType map[string]any
		expected any
		wantErr  bool
	}{
		{
			name:     "No DataType",
			arg:      "test",
			column:   "col1",
			DataType: map[string]any{},
			expected: "test",
			wantErr:  false,
		},
		{
			name:   "VARCHAR type",
			arg:    "test",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": "VARCHAR"},
			},
			expected: mssql.VarChar("test"),
			wantErr:  false,
		},
		{
			name:   "NVARCHAR type",
			arg:    "test unicode",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": "NVARCHAR"},
			},
			expected: "test unicode",
			wantErr:  false,
		},
		{
			name:   "VARCHAR with number",
			arg:    123,
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": "VARCHAR"},
			},
			expected: mssql.VarChar("123"),
			wantErr:  false,
		},
		{
			name:   "DATETIME type",
			arg:    "2023-10-01T12:00:00Z",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": "DATETIME", "datetime": map[string]any{"format": time.RFC3339}},
			},
			expected: mssql.DateTime1(time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)),
			wantErr:  false,
		},
		{
			name:   "DATETIME with custom format",
			arg:    "2023-10-01 12:00:00.999",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": "DATETIME", "datetime": map[string]any{"format": "2006-01-02 15:04:05.999"}},
			},
			expected: mssql.DateTime1(time.Date(2023, 10, 1, 12, 0, 0, 999000000, time.UTC)),
			wantErr:  false,
		},
		{
			name:   "DATETIME_OFFSET type",
			arg:    "2023-10-01T12:00:00Z",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": "DATETIME_OFFSET", "datetime_offset": map[string]any{"format": time.RFC3339}},
			},
			expected: mssql.DateTimeOffset(time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)),
			wantErr:  false,
		},
		{
			name:   "DATE type",
			arg:    "2023-10-01",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": "DATE", "date": map[string]any{"format": "2006-01-02"}},
			},
			expected: civil.Date{Year: 2023, Month: 10, Day: 1},
			wantErr:  false,
		},
		{
			name:   "DATE with time.Time input",
			arg:    "2023-12-25",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": "DATE", "date": map[string]any{"format": "2006-01-02"}},
			},
			expected: civil.Date{Year: 2023, Month: 12, Day: 25},
			wantErr:  false,
		},
		{
			name:   "Invalid DATETIME format",
			arg:    "invalid-date",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": "DATETIME", "datetime": map[string]any{"format": time.RFC3339}},
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name:   "Invalid DATE format",
			arg:    "not-a-date",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": "DATE", "date": map[string]any{"format": "2006-01-02"}},
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name:   "Invalid DATETIME_OFFSET format",
			arg:    "bad-offset",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": "DATETIME_OFFSET", "datetime_offset": map[string]any{"format": time.RFC3339}},
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name:   "Missing type field",
			arg:    "test",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{},
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name:   "Invalid data type not a map",
			arg:    "test",
			column: "col1",
			DataType: map[string]any{
				"col1": "not-a-map",
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name:   "Type field not a string",
			arg:    "test",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": 123},
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name:   "Missing datetime config for DATETIME",
			arg:    "2023-10-01T12:00:00Z",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": "DATETIME"},
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name:   "Invalid datetime config type",
			arg:    "2023-10-01T12:00:00Z",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": "DATETIME", "datetime": "not-a-map"},
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name:   "Missing format in datetime config",
			arg:    "2023-10-01T12:00:00Z",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": "DATETIME", "datetime": map[string]any{}},
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name:   "Format not a string in datetime config",
			arg:    "2023-10-01T12:00:00Z",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": "DATETIME", "datetime": map[string]any{"format": 123}},
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name:   "Unknown type returns unchanged",
			arg:    "test",
			column: "col1",
			DataType: map[string]any{
				"col1": map[string]any{"type": "UNKNOWN_TYPE"},
			},
			expected: "test",
			wantErr:  false,
		},
		{
			name:   "Column not in DataType map",
			arg:    "test",
			column: "col2",
			DataType: map[string]any{
				"col1": map[string]any{"type": "VARCHAR"},
			},
			expected: "test",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := applyMSSQLDataType(tt.arg, tt.column, tt.DataType)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestToString(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{"string", "test", "test"},
		{"int", 123, "123"},
		{"float", 45.67, "45.67"},
		{"bool", true, "true"},
		{"nil", nil, "<nil>"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toString(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
