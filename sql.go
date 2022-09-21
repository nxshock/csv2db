package main

import (
	"database/sql"
	"fmt"
)

func createTable(tx *sql.Tx, header []string, fieldTypes string) error {
	if opts.OverwriteTable {
		_, err := tx.Exec(fmt.Sprintf("IF object_id('%s', 'U') IS NOT NULL DROP TABLE %s", opts.TableName, opts.TableName))
		if err != nil {
			return fmt.Errorf("drop table: %v", err)
		}
	}

	sql := fmt.Sprintf("CREATE TABLE %s (", opts.TableName)

	for i, v := range header {
		var fieldType FieldType
		err := fieldType.UnmarshalText([]byte(fieldTypes[i : i+1]))
		if err != nil {
			return fmt.Errorf("detect field type: %v", err)
		}

		if fieldType == Skip {
			continue
		}

		sql += fmt.Sprintf(`"%s" %s`, v, fieldType.SqlFieldType())

		if i+1 < len(header) {
			sql += ", "
		} else {
			sql += ") WITH (DATA_COMPRESSION = PAGE)"
		}
	}

	_, err := tx.Exec(sql)
	if err != nil {
		return fmt.Errorf("execute table creation: %v", err)
	}

	return nil
}
