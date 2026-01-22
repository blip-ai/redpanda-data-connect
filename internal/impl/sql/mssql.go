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
	fieldDataType := fdt.(map[string]any)

	switch fieldDataType["type"].(string) {
	case "NVARCHAR":
		arg = toString(arg)
	case "VARCHAR":
		arg = mssql.VarChar(toString(arg))
	case "DATETIME":
		datetime := fieldDataType["datetime"].(map[string]any)
		t, err := time.Parse(datetime["format"].(string), toString(arg))
		if err != nil {
			return arg, err
		}
		arg = mssql.DateTime1(t)
	case "DATETIME_OFFSET":
		datetimeOffset := fieldDataType["datetime_offset"].(map[string]any)
		t, err := time.Parse(datetimeOffset["format"].(string), toString(arg))
		if err != nil {
			return arg, err
		}
		arg = mssql.DateTimeOffset(t)
	case "DATE":
		date := fieldDataType["date"].(map[string]any)
		t, err := time.Parse(date["format"].(string), toString(arg))
		if err != nil {
			return arg, err
		}
		arg = civil.DateOf(t)
	}
	return arg, nil
}

func toString(arg any) string {
	return fmt.Sprintf("%v", arg)
}
