package sql

import (
	"fmt"
	"time"

	"github.com/golang-sql/civil"
	mssql "github.com/microsoft/go-mssqldb"
)

func applyMSSQLDataType(arg any, column string, dataTypes map[string]any) (any, error) {
	fdt, found := dataTypes[column]
	if !found {
		return arg, nil
	}
	fieldDataType, ok := fdt.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid data type configuration for column %q: expected map, got %T", column, fdt)
	}

	typeVal, ok := fieldDataType["type"]
	if !ok {
		return nil, fmt.Errorf("missing 'type' field in data type configuration for column %q", column)
	}
	typeStr, ok := typeVal.(string)
	if !ok {
		return nil, fmt.Errorf("invalid 'type' field for column %q: expected string, got %T", column, typeVal)
	}

	switch typeStr {
	case "NVARCHAR":
		return toString(arg), nil
	case "VARCHAR":
		return mssql.VarChar(toString(arg)), nil
	case "DATETIME":
		return parseMSSQLDateTime(arg, column, fieldDataType, "datetime")
	case "DATETIME_OFFSET":
		return parseMSSQLDateTimeOffset(arg, column, fieldDataType)
	case "DATE":
		return parseMSSQLDate(arg, column, fieldDataType)
	}
	return arg, nil
}

func getTimeFormat(column string, config map[string]any, configKey string) (string, error) {
	timeConfig, ok := config[configKey].(map[string]any)
	if !ok {
		return "", fmt.Errorf("invalid '%s' configuration for column %q: expected map, got %T", configKey, column, config[configKey])
	}
	formatVal, ok := timeConfig["format"]
	if !ok {
		return "", fmt.Errorf("missing 'format' field in '%s' configuration for column %q", configKey, column)
	}
	format, ok := formatVal.(string)
	if !ok {
		return "", fmt.Errorf("invalid 'format' field in '%s' configuration for column %q: expected string, got %T", configKey, column, formatVal)
	}
	return format, nil
}

func parseMSSQLDateTime(arg any, column string, fieldDataType map[string]any, configKey string) (any, error) {
	format, err := getTimeFormat(column, fieldDataType, configKey)
	if err != nil {
		return nil, err
	}
	t, err := time.Parse(format, toString(arg))
	if err != nil {
		return nil, fmt.Errorf("failed to parse datetime for column %q: %w", column, err)
	}
	return mssql.DateTime1(t), nil
}

func parseMSSQLDateTimeOffset(arg any, column string, fieldDataType map[string]any) (any, error) {
	format, err := getTimeFormat(column, fieldDataType, "datetime_offset")
	if err != nil {
		return nil, err
	}
	t, err := time.Parse(format, toString(arg))
	if err != nil {
		return nil, fmt.Errorf("failed to parse datetime_offset for column %q: %w", column, err)
	}
	return mssql.DateTimeOffset(t), nil
}

func parseMSSQLDate(arg any, column string, fieldDataType map[string]any) (any, error) {
	format, err := getTimeFormat(column, fieldDataType, "date")
	if err != nil {
		return nil, err
	}
	t, err := time.Parse(format, toString(arg))
	if err != nil {
		return nil, fmt.Errorf("failed to parse date for column %q: %w", column, err)
	}
	return civil.DateOf(t), nil
}

func toString(arg any) string {
	if arg == nil {
		return ""
	}
	return fmt.Sprintf("%v", arg)
}
