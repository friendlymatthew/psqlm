use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub tables: Vec<Table>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Option<Vec<String>>,
    pub foreign_keys: Vec<ForeignKey>,
    pub indexes: Vec<Index>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub default: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKey {
    pub columns: Vec<String>,
    pub references_table: String,
    pub references_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    pub name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
}

impl Schema {
    pub fn to_prompt_string(&self) -> String {
        let mut output = String::new();

        for table in &self.tables {
            output.push_str(&format!("Table: {}\n", table.name));

            output.push_str("  Columns:\n");
            for col in &table.columns {
                let nullable = if col.is_nullable { "NULL" } else { "NOT NULL" };
                let default = col
                    .default
                    .as_ref()
                    .map(|d| format!(" DEFAULT {}", d))
                    .unwrap_or_default();
                output.push_str(&format!(
                    "    - {} {} {}{}\n",
                    col.name, col.data_type, nullable, default
                ));
            }

            if let Some(pk) = &table.primary_key {
                output.push_str(&format!("  Primary Key: ({})\n", pk.join(", ")));
            }

            if !table.foreign_keys.is_empty() {
                output.push_str("  Foreign Keys:\n");
                for fk in &table.foreign_keys {
                    output.push_str(&format!(
                        "    - ({}) -> {}.{})\n",
                        fk.columns.join(", "),
                        fk.references_table,
                        fk.references_columns.join(", ")
                    ));
                }
            }

            if !table.indexes.is_empty() {
                output.push_str("  Indexes:\n");
                for idx in &table.indexes {
                    let unique = if idx.is_unique { "UNIQUE " } else { "" };
                    output.push_str(&format!(
                        "    - {}{} ({})\n",
                        unique,
                        idx.name,
                        idx.columns.join(", ")
                    ));
                }
            }

            output.push('\n');
        }

        output
    }
}
