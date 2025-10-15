package dbx

import (
	"testing"
)

func TestValidateWhereClause(t *testing.T) {
	tests := []struct {
		name      string
		where     string
		wantError bool
	}{
		{
			name:      "empty where clause",
			where:     "",
			wantError: false,
		},
		{
			name:      "valid simple condition",
			where:     "id > 100",
			wantError: false,
		},
		{
			name:      "valid complex condition",
			where:     "created_at >= '2024-01-01' AND status = 'active'",
			wantError: false,
		},
		{
			name:      "valid modulo condition",
			where:     "id % 100 = 0",
			wantError: false,
		},
		{
			name:      "valid with backticks",
			where:     "`customer_id` IS NOT NULL",
			wantError: false,
		},
		{
			name:      "contains DROP keyword",
			where:     "id > 1 OR DROP TABLE users",
			wantError: true,
		},
		{
			name:      "contains DELETE keyword",
			where:     "id > 1 OR DELETE FROM users",
			wantError: true,
		},
		{
			name:      "contains UPDATE keyword",
			where:     "id > 1; UPDATE users SET password=''",
			wantError: true,
		},
		{
			name:      "contains INSERT keyword",
			where:     "id > 1; INSERT INTO users VALUES()",
			wantError: true,
		},
		{
			name:      "contains TRUNCATE keyword",
			where:     "id > 1 OR TRUNCATE TABLE users",
			wantError: true,
		},
		{
			name:      "contains ALTER keyword",
			where:     "id > 1; ALTER TABLE users DROP COLUMN password",
			wantError: true,
		},
		{
			name:      "contains CREATE keyword",
			where:     "CREATE TABLE hack(id INT)",
			wantError: true,
		},
		{
			name:      "contains LOAD_FILE",
			where:     "id = LOAD_FILE('/etc/passwd')",
			wantError: true,
		},
		{
			name:      "contains INTO OUTFILE",
			where:     "id > 1 INTO OUTFILE '/tmp/hack'",
			wantError: true,
		},
		{
			name:      "contains SQL comment --",
			where:     "id > 1 -- comment",
			wantError: true,
		},
		{
			name:      "contains SQL comment /* */",
			where:     "id > 1 /* comment */",
			wantError: true,
		},
		{
			name:      "contains semicolon",
			where:     "id > 1; DROP TABLE users",
			wantError: true,
		},
		{
			name:      "suspicious backtick with semicolon",
			where:     "id > 1`; DROP TABLE users",
			wantError: true,
		},
		{
			name:      "case insensitive DROP",
			where:     "id > 1 OR drop table users",
			wantError: true,
		},
		{
			name:      "case insensitive DELETE",
			where:     "id > 1 OR delete from users",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateWhereClause(tt.where)
			if tt.wantError && err == nil {
				t.Errorf("ValidateWhereClause() expected error for input: %q", tt.where)
			}
			if !tt.wantError && err != nil {
				t.Errorf("ValidateWhereClause() unexpected error: %v for input: %q", err, tt.where)
			}
		})
	}
}

func TestBuildSelectByRange(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		columns   []string
		pkColumn  string
		where     string
		expected  string
	}{
		{
			name:      "simple query without where",
			tableName: "log",
			columns:   []string{"id", "name", "value"},
			pkColumn:  "id",
			where:     "",
			expected:  "SELECT `id`,`name`,`value` FROM `log` WHERE `id` > ? AND `id` <= ? ORDER BY `id`",
		},
		{
			name:      "query with where clause",
			tableName: "log",
			columns:   []string{"id", "name"},
			pkColumn:  "id",
			where:     "status = 'active'",
			expected:  "SELECT `id`,`name` FROM `log` WHERE `id` > ? AND `id` <= ? AND (status = 'active') ORDER BY `id`",
		},
		{
			name:      "query with custom pk column",
			tableName: "users",
			columns:   []string{"user_id", "email"},
			pkColumn:  "user_id",
			where:     "",
			expected:  "SELECT `user_id`,`email` FROM `users` WHERE `user_id` > ? AND `user_id` <= ? ORDER BY `user_id`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildSelectByRange(tt.tableName, tt.columns, tt.pkColumn, tt.where)
			if result != tt.expected {
				t.Errorf("BuildSelectByRange() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestBuildLoadDataSQL(t *testing.T) {
	tests := []struct {
		name       string
		stagedPath string
		dstTable   string
		uuidCol    string
		columns    []string
		useLocal   bool
		wantPrefix string
	}{
		{
			name:       "server LOAD DATA INFILE",
			stagedPath: "/tmp/stage_log_1-1000.csv",
			dstTable:   "log",
			uuidCol:    "id",
			columns:    []string{"id", "nid", "ins_ts", "user_id"},
			useLocal:   false,
			wantPrefix: "LOAD DATA INFILE",
		},
		{
			name:       "client LOAD DATA LOCAL INFILE",
			stagedPath: "/tmp/stage_log_1-1000.csv",
			dstTable:   "log",
			uuidCol:    "id",
			columns:    []string{"id", "nid", "ins_ts", "user_id"},
			useLocal:   true,
			wantPrefix: "LOAD DATA LOCAL INFILE",
		},
		{
			name:       "empty columns",
			stagedPath: "/tmp/stage_log_1-1000.csv",
			dstTable:   "log",
			uuidCol:    "id",
			columns:    []string{},
			useLocal:   false,
			wantPrefix: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildLoadDataSQL(tt.stagedPath, tt.dstTable, tt.uuidCol, tt.columns, tt.useLocal)

			if tt.wantPrefix == "" {
				if result != "" {
					t.Errorf("BuildLoadDataSQL() expected empty string for empty columns, got %q", result)
				}
				return
			}

			if len(result) < len(tt.wantPrefix) {
				t.Errorf("BuildLoadDataSQL() result too short")
				return
			}

			if result[:len(tt.wantPrefix)] != tt.wantPrefix {
				t.Errorf("BuildLoadDataSQL() prefix = %q, want %q", result[:len(tt.wantPrefix)], tt.wantPrefix)
			}
		})
	}
}
