use crate::schema::Schema;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::io::{self, Write};

const API_URL: &str = "https://api.anthropic.com/v1/messages";
const MODEL: &str = "claude-sonnet-4-20250514";

const GREEN: &str = "\x1b[32m";
const RESET: &str = "\x1b[0m";

#[derive(Debug, Clone)]
pub struct ConversationTurn {
    pub question: String,
    pub sql: String,
    pub result: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Client {
    api_key: String,
    http: reqwest::Client,
    pub history: Vec<ConversationTurn>,
}

#[derive(Debug, Serialize)]
struct ApiRequest {
    model: &'static str,
    max_tokens: u32,
    system: String,
    messages: Vec<Message>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    role: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct StreamEvent {
    #[serde(rename = "type")]
    event_type: String,
    delta: Option<Delta>,
}

#[derive(Debug, Deserialize)]
struct Delta {
    text: Option<String>,
}

impl Client {
    pub fn new(api_key: &str) -> Self {
        Self {
            api_key: api_key.to_string(),
            http: reqwest::Client::new(),
            history: Vec::new(),
        }
    }

    pub fn add_to_history(&mut self, question: String, sql: String, result: Option<String>) {
        self.history.push(ConversationTurn { question, sql, result });
        if self.history.len() > 10 {
            self.history.remove(0);
        }
    }

    fn system_prompt(schema: &Schema) -> String {
        format!(
            r#"You are a PostgreSQL expert assistant. Your job is to convert natural language questions into SQL queries.

Given the database schema below, generate a PostgreSQL query that answers the user's question.

IMPORTANT:
- Return ONLY the SQL query, nothing else
- Do not include explanations, markdown formatting, or code blocks
- The query should be ready to execute directly
- Use proper PostgreSQL syntax

Database Schema:
{}
"#,
            schema.to_prompt_string()
        )
    }

    async fn stream_response(&self, request: ApiRequest) -> Result<String> {
        let response = self
            .http
            .post(API_URL)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&request)
            .send()
            .await
            .context("Failed to send request to Claude API")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Claude API error ({}): {}", status, body);
        }

        let mut full_text = String::new();
        let mut stream = response.bytes_stream();

        print!("{}", GREEN);
        io::stdout().flush().ok();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("Failed to read stream chunk")?;
            let text = String::from_utf8_lossy(&chunk);

            for line in text.lines() {
                if let Some(data) = line.strip_prefix("data: ") {
                    if data == "[DONE]" {
                        continue;
                    }
                    if let Ok(event) = serde_json::from_str::<StreamEvent>(data) {
                        if event.event_type == "content_block_delta" {
                            if let Some(delta) = event.delta {
                                if let Some(text) = delta.text {
                                    print!("{}", text);
                                    io::stdout().flush().ok();
                                    full_text.push_str(&text);
                                }
                            }
                        }
                    }
                }
            }
        }

        print!("{}", RESET);
        println!();

        let sql = full_text
            .trim_start_matches("```sql")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim()
            .to_string();

        Ok(sql)
    }

    pub async fn text_to_sql(&self, schema: &Schema, question: &str) -> Result<String> {
        let mut messages = Vec::new();

        for turn in &self.history {
            messages.push(Message {
                role: "user".to_string(),
                content: turn.question.clone(),
            });

            let assistant_content = if let Some(result) = &turn.result {
                format!("{}\n\n-- Result:\n{}", turn.sql, result)
            } else {
                turn.sql.clone()
            };
            messages.push(Message {
                role: "assistant".to_string(),
                content: assistant_content,
            });
        }

        messages.push(Message {
            role: "user".to_string(),
            content: question.to_string(),
        });

        let request = ApiRequest {
            model: MODEL,
            max_tokens: 1024,
            system: Self::system_prompt(schema),
            messages,
            stream: Some(true),
        };

        self.stream_response(request).await
    }

    pub async fn fix_sql(
        &self,
        schema: &Schema,
        original_question: &str,
        original_sql: &str,
        error: &str,
    ) -> Result<String> {
        let request = ApiRequest {
            model: MODEL,
            max_tokens: 1024,
            system: Self::system_prompt(schema),
            messages: vec![
                Message {
                    role: "user".to_string(),
                    content: original_question.to_string(),
                },
                Message {
                    role: "assistant".to_string(),
                    content: original_sql.to_string(),
                },
                Message {
                    role: "user".to_string(),
                    content: format!(
                        "The query failed with this error:\n{}\n\nPlease fix the SQL query. Return ONLY the corrected SQL, nothing else.",
                        error
                    ),
                },
            ],
            stream: Some(true),
        };

        self.stream_response(request).await
    }
}
