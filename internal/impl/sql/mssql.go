package sql

import (
	"fmt"
	"time"

	"github.com/golang-sql/civil"
	mssql "github.com/microsoft/go-mssqldb"
)

// MSSQL data type constants
const (
	mssqlTypeNVarchar       = "NVARCHAR"
	mssqlTypeVarchar        = "VARCHAR"
	mssqlTypeDatetime       = "DATETIME"
	mssqlTypeDatetimeOffset = "DATETIME_OFFSET"
	mssqlTypeDate           = "DATE"
)

// applyMSSQLDataType converts the given argument to the appropriate MSSQL data type
// based on the column's data type configuration. Returns the converted value or the
// original value if no conversion is needed.
func applyMSSQLDataType(arg any, column string, dataTypes map[string]any) (any, error) {
	// Preserve NULL values - let SQL driver handle them
	if arg == nil {
		return nil, nil
	}

	// Check if this column has a data type configuration
	columnDataType, hasDataType := dataTypes[column]
	if !hasDataType {
		return arg, nil
	}

	// Extract the data type configuration map
	fieldDataType, ok := columnDataType.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid data type configuration for column %q: expected map, got %T", column, columnDataType)
	}

	// Get the type name from the configuration
	typeName, err := getStringField(fieldDataType, "type", column)
	if err != nil {
		return nil, err
	}

	// Convert to the appropriate MSSQL type
	switch typeName {
	case mssqlTypeNVarchar:
		return fmt.Sprint(arg), nil

	case mssqlTypeVarchar:
		return mssql.VarChar(fmt.Sprint(arg)), nil

	case mssqlTypeDatetime:
		return parseTime(arg, column, fieldDataType, "datetime", func(t time.Time) any {
			return mssql.DateTime1(t)
		})

	case mssqlTypeDatetimeOffset:
		return parseTime(arg, column, fieldDataType, "datetime_offset", func(t time.Time) any {
			return mssql.DateTimeOffset(t)
		})

	case mssqlTypeDate:
		return parseTime(arg, column, fieldDataType, "date", func(t time.Time) any {
			return civil.DateOf(t)
		})

	default:
		return arg, nil
	}
}

// getStringField extracts a string field from a map with proper type checking.
func getStringField(config map[string]any, field, column string) (string, error) {
	value, exists := config[field]
	if !exists {
		return "", fmt.Errorf("missing '%s' field in configuration for column %q", field, column)
	}

	str, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("invalid '%s' field for column %q: expected string, got %T", field, column, value)
	}

	return str, nil
}

// getTimeFormat extracts the time format string from the column's data type configuration.
func getTimeFormat(column string, config map[string]any, configKey string) (string, error) {
	timeConfigValue, exists := config[configKey]
	if !exists {
		return "", fmt.Errorf("missing '%s' configuration for column %q", configKey, column)
	}

	timeConfig, ok := timeConfigValue.(map[string]any)
	if !ok {
		return "", fmt.Errorf("invalid '%s' configuration for column %q: expected map, got %T", configKey, column, timeConfigValue)
	}

	return getStringField(timeConfig, "format", column)
}

// parseTime parses a time string and converts it using the provided converter function.
// The format is extracted from the column's configuration using the provided configKey.
func parseTime(arg any, column string, fieldDataType map[string]any, configKey string, converter func(time.Time) any) (any, error) {
	format, err := getTimeFormat(column, fieldDataType, configKey)
	if err != nil {
		return nil, err
	}

	parsedTime, err := time.Parse(format, fmt.Sprint(arg))
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s for column %q with format %q: %w", configKey, column, format, err)
	}

	return converter(parsedTime), nil
}
