mod claude;
mod config;
mod psql;
mod repl;
mod schema;

use anyhow::Result;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "psqlm", version, about = "A natural language interface to PostgreSQL", disable_help_flag = true)]
pub struct Args {
    #[arg(long, action = clap::ArgAction::Help)]
    help: Option<bool>,

    #[arg(short = 'h', long, default_value = "localhost")]
    pub host: String,

    #[arg(short, long, default_value = "5432")]
    pub port: String,

    #[arg(short = 'U', long = "username")]
    pub user: String,

    #[arg(short, long = "dbname")]
    pub database: String,

    #[arg(short = 'W', long)]
    pub password: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = config::load_or_create().await?;

    let psql = psql::PsqlConnection::new(
        args.host,
        args.port,
        args.user,
        args.database,
        args.password,
    );

    println!("Connecting to {}...", psql.database);
    let schema = psql.introspect_schema()?;
    println!("Schema loaded ({} tables)\n", schema.tables.len());

    let claude = claude::Client::new(&config.api_key);

    repl::run(psql, claude, schema, config).await
}
