use crate::schema::{Column, ForeignKey, Index, Schema, Table};
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::process::Command;

pub fn is_write_operation(sql: &str) -> bool {
    let sql_upper = sql.trim().to_uppercase();
    let first_word = sql_upper.split_whitespace().next().unwrap_or("");

    matches!(
        first_word,
        "INSERT" | "UPDATE" | "DELETE" | "DROP" | "ALTER" | "TRUNCATE" | "CREATE"
    )
}

#[derive(Debug, Clone)]
pub struct PsqlConnection {
    pub host: String,
    pub port: String,
    pub user: String,
    pub database: String,
    pub password: Option<String>,
}

impl PsqlConnection {
    pub fn new(
        host: String,
        port: String,
        user: String,
        database: String,
        password: Option<String>,
    ) -> Self {
        Self {
            host,
            port,
            user,
            database,
            password,
        }
    }

    fn base_command(&self) -> Command {
        let mut cmd = Command::new("psql");
        cmd.args(["-h", &self.host])
            .args(["-p", &self.port])
            .args(["-U", &self.user])
            .args(["-d", &self.database]);

        if let Some(pw) = &self.password {
            cmd.env("PGPASSWORD", pw);
        }

        cmd
    }

    pub fn query(&self, sql: &str) -> Result<String> {
        let output = self
            .base_command()
            .args(["-t", "-A"])
            .args(["-c", sql])
            .output()
            .context("Failed to execute psql")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("psql query failed: {}", stderr);
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    pub fn execute_capture(&self, sql: &str) -> Result<(bool, String, String)> {
        let output = self
            .base_command()
            .args(["-c", sql])
            .output()
            .context("Failed to execute psql")?;

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        Ok((output.status.success(), stdout, stderr))
    }

    pub fn execute_write_with_confirmation(
        &self,
        sql: &str,
        commit: bool,
    ) -> Result<(bool, String, String)> {
        let transaction_end = if commit { "COMMIT" } else { "ROLLBACK" };

        let output = self
            .base_command()
            .args(["-c", "BEGIN"])
            .args(["-c", sql])
            .args(["-c", transaction_end])
            .output()
            .context("Failed to execute psql")?;

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        Ok((output.status.success(), stdout, stderr))
    }

    pub fn preview_write_with_returning(&self, sql: &str) -> Result<(bool, String, String)> {
        let sql_with_returning = if sql.to_uppercase().contains("RETURNING") {
            sql.to_string()
        } else {
            let trimmed = sql.trim().trim_end_matches(';');
            format!("{} RETURNING *;", trimmed)
        };

        let output = self
            .base_command()
            .args(["-c", "BEGIN"])
            .args(["-c", &sql_with_returning])
            .args(["-c", "ROLLBACK"])
            .output()
            .context("Failed to execute psql")?;

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        Ok((output.status.success(), stdout, stderr))
    }

    pub fn introspect_schema(&self) -> Result<Schema> {
        let mut tables: HashMap<String, Table> = HashMap::new();

        let columns_sql = r#"
            SELECT
                table_schema || '.' || table_name,
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            ORDER BY table_schema, table_name, ordinal_position
        "#;

        let output = self.query(columns_sql)?;
        for line in output.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split('|').collect();
            if parts.len() >= 4 {
                let table_name = parts[0].trim().to_string();
                let column = Column {
                    name: parts[1].trim().to_string(),
                    data_type: parts[2].trim().to_string(),
                    is_nullable: parts[3].trim() == "YES",
                    default: parts.get(4).and_then(|s| {
                        let s = s.trim();
                        if s.is_empty() {
                            None
                        } else {
                            Some(s.to_string())
                        }
                    }),
                };

                tables
                    .entry(table_name.clone())
                    .or_insert_with(|| Table {
                        name: table_name,
                        columns: Vec::new(),
                        primary_key: None,
                        foreign_keys: Vec::new(),
                        indexes: Vec::new(),
                    })
                    .columns
                    .push(column);
            }
        }

        let pk_sql = r#"
            SELECT
                tc.table_schema || '.' || tc.table_name,
                kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            ORDER BY tc.table_schema, tc.table_name, kcu.ordinal_position
        "#;

        let output = self.query(pk_sql)?;
        let mut pk_map: HashMap<String, Vec<String>> = HashMap::new();
        for line in output.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split('|').collect();
            if parts.len() >= 2 {
                let table_name = parts[0].trim().to_string();
                let column_name = parts[1].trim().to_string();
                pk_map.entry(table_name).or_default().push(column_name);
            }
        }
        for (table_name, columns) in pk_map {
            if let Some(table) = tables.get_mut(&table_name) {
                table.primary_key = Some(columns);
            }
        }

        let fk_sql = r#"
            SELECT
                tc.table_schema || '.' || tc.table_name,
                kcu.column_name,
                ccu.table_schema || '.' || ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        "#;

        let output = self.query(fk_sql)?;
        for line in output.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split('|').collect();
            if parts.len() >= 4 {
                let table_name = parts[0].trim();
                let column_name = parts[1].trim().to_string();
                let foreign_table = parts[2].trim().to_string();
                let foreign_column = parts[3].trim().to_string();

                if let Some(table) = tables.get_mut(table_name) {
                    table.foreign_keys.push(ForeignKey {
                        columns: vec![column_name],
                        references_table: foreign_table,
                        references_columns: vec![foreign_column],
                    });
                }
            }
        }

        let idx_sql = r#"
            SELECT
                schemaname || '.' || tablename,
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        "#;

        let output = self.query(idx_sql)?;
        for line in output.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split('|').collect();
            if parts.len() >= 3 {
                let table_name = parts[0].trim();
                let index_name = parts[1].trim().to_string();
                let index_def = parts[2].trim();

                let is_unique = index_def.contains("UNIQUE");

                let columns = if let Some(start) = index_def.rfind('(') {
                    if let Some(end) = index_def.rfind(')') {
                        index_def[start + 1..end]
                            .split(',')
                            .map(|s| s.trim().to_string())
                            .collect()
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                };

                if let Some(table) = tables.get_mut(table_name) {
                    table.indexes.push(Index {
                        name: index_name,
                        columns,
                        is_unique,
                    });
                }
            }
        }

        Ok(Schema {
            tables: tables.into_values().collect(),
        })
    }
}
