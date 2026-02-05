use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::io::{self, Write};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ExecutionMode {
    Auto,
    #[default]
    Confirm,
    Show,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    #[serde(skip)]
    pub api_key: String,

    #[serde(default)]
    pub execution_mode: ExecutionMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ConfigFile {
    api_key: Option<String>,

    #[serde(default)]
    execution_mode: ExecutionMode,
}

fn config_dir() -> Result<PathBuf> {
    let dir = dirs::config_dir()
        .context("Could not determine config directory")?
        .join("psqlm");
    Ok(dir)
}

fn config_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("config.toml"))
}

pub async fn load_or_create() -> Result<Config> {
    if let Ok(api_key) = std::env::var("ANTHROPIC_API_KEY") {
        let api_key: String = api_key.chars().filter(|c| !c.is_whitespace()).collect();
        let config = load_config_file().unwrap_or_default();
        return Ok(Config {
            api_key,
            execution_mode: config.execution_mode,
        });
    }

    if let Ok(config_file) = load_config_file() {
        if let Some(api_key) = config_file.api_key {
            return Ok(Config {
                api_key,
                execution_mode: config_file.execution_mode,
            });
        }
    }

    let api_key = prompt_for_api_key()?;

    print!("Save API key to config file? [y/n]: ");
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    if input.trim().to_lowercase() == "y" {
        save_api_key(&api_key)?;
        println!("Saved to {:?}\n", config_path()?);
    }

    Ok(Config {
        api_key,
        execution_mode: ExecutionMode::default(),
    })
}

fn load_config_file() -> Result<ConfigFile> {
    let path = config_path()?;
    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read config file: {:?}", path))?;
    let config: ConfigFile = toml::from_str(&contents)?;
    Ok(config)
}

fn prompt_for_api_key() -> Result<String> {
    print!("Enter your Anthropic API key: ");
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    let key = input.trim().to_string();
    if key.is_empty() {
        anyhow::bail!("API key cannot be empty");
    }

    Ok(key)
}

fn save_api_key(api_key: &str) -> Result<()> {
    let dir = config_dir()?;
    std::fs::create_dir_all(&dir)?;

    let config = ConfigFile {
        api_key: Some(api_key.to_string()),
        execution_mode: ExecutionMode::default(),
    };

    let contents = toml::to_string_pretty(&config)?;
    std::fs::write(config_path()?, contents)?;

    Ok(())
}
