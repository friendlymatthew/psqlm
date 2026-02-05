use crate::claude::Client as ClaudeClient;
use crate::config::{Config, ExecutionMode};
use crate::psql::{is_write_operation, PsqlConnection};
use crate::schema::Schema;
use anyhow::Result;
use crossterm::cursor;
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Style};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Terminal;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::io::{self, Write};
use tui_textarea::TextArea;

pub async fn run(
    psql: PsqlConnection,
    mut claude: ClaudeClient,
    mut schema: Schema,
    mut config: Config,
) -> Result<()> {
    let mut rl = DefaultEditor::new()?;

    let history_path = dirs::data_dir()
        .map(|p| p.join("psqlm").join("history.txt"))
        .unwrap_or_default();
    let _ = rl.load_history(&history_path);

    println!("Type your question in natural language, or use commands:");
    println!("  \\q          - quit");
    println!("  \\schema     - show/refresh schema");
    println!("  \\mode [m]   - show/set execution mode (auto/confirm/show)");
    println!();

    loop {
        let readline = rl.readline("psqlm> ");

        match readline {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                let _ = rl.add_history_entry(line);

                if line.starts_with('\\') {
                    match handle_command(line, &psql, &mut schema, &mut config) {
                        Ok(should_quit) => {
                            if should_quit {
                                break;
                            }
                        }
                        Err(e) => eprintln!("Error: {}", e),
                    }
                    continue;
                }

                if let Err(e) = handle_query(line, &psql, &mut claude, &schema, &mut config).await {
                    eprintln!("Error: {}", e);
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }

    if let Some(parent) = history_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = rl.save_history(&history_path);

    Ok(())
}

fn handle_command(
    line: &str,
    psql: &PsqlConnection,
    schema: &mut Schema,
    config: &mut Config,
) -> Result<bool> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    let cmd = parts.first().unwrap_or(&"");

    match *cmd {
        "\\q" | "\\quit" => return Ok(true),

        "\\schema" => {
            println!("Refreshing schema...");
            *schema = psql.introspect_schema()?;
            println!("Schema loaded ({} tables):\n", schema.tables.len());
            print!("{}", schema.to_prompt_string());
        }

        "\\mode" => {
            if let Some(mode) = parts.get(1) {
                match *mode {
                    "auto" => {
                        config.execution_mode = ExecutionMode::Auto;
                        println!("Execution mode: auto (run immediately)");
                    }
                    "confirm" => {
                        config.execution_mode = ExecutionMode::Confirm;
                        println!("Execution mode: confirm (ask before running)");
                    }
                    "show" => {
                        config.execution_mode = ExecutionMode::Show;
                        println!("Execution mode: show (display SQL only)");
                    }
                    _ => println!("Unknown mode. Use: auto, confirm, or show"),
                }
            } else {
                let mode_str = match config.execution_mode {
                    ExecutionMode::Auto => "auto",
                    ExecutionMode::Confirm => "confirm",
                    ExecutionMode::Show => "show",
                };
                println!("Current mode: {}", mode_str);
            }
        }

        _ => println!("Unknown command: {}", cmd),
    }

    Ok(false)
}

fn is_valid_sql(input: &str) -> bool {
    let trimmed = input.trim().to_uppercase();

    let sql_starters = [
        "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TRUNCATE", "WITH",
        "EXPLAIN", "ANALYZE", "BEGIN", "COMMIT", "ROLLBACK", "SET", "GRANT", "REVOKE", "COPY",
        "VACUUM", "REINDEX",
    ];

    let starts_with_sql = sql_starters.iter().any(|&kw| {
        trimmed.starts_with(kw)
            && trimmed
                .chars()
                .nth(kw.len())
                .is_some_and(|c| c.is_whitespace() || c == '(' || c == ';')
    });

    if !starts_with_sql {
        return false;
    }

    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, input).is_ok()
}

async fn handle_query(
    question: &str,
    psql: &PsqlConnection,
    claude: &mut ClaudeClient,
    schema: &Schema,
    config: &mut Config,
) -> Result<()> {
    let mut current_question = question.to_string();
    let mut current_sql: Option<String> = None;
    let mut is_raw_sql = false;

    if is_valid_sql(question) {
        current_sql = Some(question.to_string());
        is_raw_sql = true;
    }

    loop {
        if current_sql.is_none() {
            println!("");
            let sql = claude.text_to_sql(schema, &current_question).await?;
            println!();
            current_sql = Some(sql);
            is_raw_sql = false;
        }

        let sql = current_sql.as_ref().unwrap();

        if is_raw_sql {
            execute_with_recovery(psql, claude, schema, &current_question, sql, config).await?;
            return Ok(());
        }

        match config.execution_mode {
            ExecutionMode::Show => {
                return Ok(());
            }
            ExecutionMode::Confirm => match confirm_execution(config)? {
                RunChoice::Run | RunChoice::AutoRun => {}
                RunChoice::EditSql => {
                    current_sql = Some(prompt_edit_sql(sql)?);
                    is_raw_sql = false;
                    continue;
                }
                RunChoice::EditPrompt => {
                    print!("Enter new prompt: ");
                    io::stdout().flush()?;
                    let mut new_prompt = String::new();
                    io::stdin().read_line(&mut new_prompt)?;
                    let new_prompt = new_prompt.trim();
                    if new_prompt.is_empty() {
                        println!("Cancelled.\n");
                        return Ok(());
                    }
                    current_question = new_prompt.to_string();
                    current_sql = None;
                    continue;
                }
                RunChoice::Cancel => {
                    println!("Cancelled.\n");
                    return Ok(());
                }
            },
            ExecutionMode::Auto => {}
        }

        execute_with_recovery(psql, claude, schema, &current_question, sql, config).await?;
        return Ok(());
    }
}

enum RunChoice {
    Run,
    AutoRun,
    EditPrompt,
    EditSql,
    Cancel,
}

fn pick_option(options: &[&str]) -> Result<Option<usize>> {
    let mut selected: usize = 0;
    let mut stdout = io::stdout();

    terminal::enable_raw_mode()?;

    let draw = |stdout: &mut io::Stdout, sel: usize| -> io::Result<()> {
        for (i, option) in options.iter().enumerate() {
            if i == sel {
                write!(stdout, "\r  \x1b[32m> {option}\x1b[0m\x1b[K\n")?;
            } else {
                write!(stdout, "\r    {option}\x1b[K\n")?;
            }
        }
        Ok(())
    };

    draw(&mut stdout, selected)?;
    crossterm::execute!(stdout, cursor::MoveUp(options.len() as u16))?;
    stdout.flush()?;

    let result = loop {
        if let Event::Key(key) = event::read()? {
            match key.code {
                KeyCode::Up | KeyCode::Char('k') => {
                    if selected > 0 {
                        selected -= 1;
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if selected < options.len() - 1 {
                        selected += 1;
                    }
                }
                KeyCode::Enter => break Some(selected),
                KeyCode::Esc => break None,
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    break None
                }
                _ => continue,
            }

            draw(&mut stdout, selected)?;
            crossterm::execute!(stdout, cursor::MoveUp(options.len() as u16))?;
            stdout.flush()?;
        }
    };

    terminal::disable_raw_mode()?;
    crossterm::execute!(stdout, cursor::MoveDown(options.len() as u16))?;
    write!(stdout, "\r")?;
    stdout.flush()?;

    Ok(result)
}

fn confirm_execution(config: &mut Config) -> Result<RunChoice> {
    let options = &["Run", "Edit SQL", "Edit prompt", "Always run (auto-mode)"];
    match pick_option(options)? {
        Some(0) => Ok(RunChoice::Run),
        Some(1) => Ok(RunChoice::EditSql),
        Some(2) => Ok(RunChoice::EditPrompt),
        Some(3) => {
            config.execution_mode = ExecutionMode::Auto;
            println!("Auto-run enabled. Use \\mode confirm to disable.\n");
            Ok(RunChoice::AutoRun)
        }
        _ => Ok(RunChoice::Cancel),
    }
}

async fn execute_with_recovery(
    psql: &PsqlConnection,
    claude: &mut ClaudeClient,
    schema: &Schema,
    original_question: &str,
    sql: &str,
    config: &mut Config,
) -> Result<()> {
    let mut current_sql = sql.to_string();

    loop {
        let is_write = is_write_operation(&current_sql);

        if is_write {
            execute_write_with_transaction(
                psql,
                claude,
                schema,
                original_question,
                &mut current_sql,
                config,
            )
            .await?;
            return Ok(());
        }

        println!();
        let (success, stdout, stderr) = psql.execute_capture(&current_sql)?;

        if !stdout.is_empty() {
            print!("{}", stdout);
        }

        if success {
            claude.add_to_history(
                original_question.to_string(),
                current_sql.clone(),
                Some(stdout.clone()),
            );
            println!();
            return Ok(());
        }

        eprintln!("{}", stderr);
        println!();

        match prompt_error_action()? {
            ErrorAction::Fix => {
                current_sql = ask_claude_to_fix(
                    claude,
                    schema,
                    original_question,
                    &current_sql,
                    &stderr,
                    config,
                )
                .await?;
                if current_sql.is_empty() {
                    return Ok(());
                }
            }
            ErrorAction::Edit => {
                current_sql = prompt_edit_sql(&current_sql)?;
                println!();
            }
            ErrorAction::Retry => match prompt_new_question(claude, schema, config).await? {
                Some(sql) => current_sql = sql,
                None => return Ok(()),
            },
            ErrorAction::Cancel => {
                println!("Cancelled.\n");
                return Ok(());
            }
        }
    }
}

async fn execute_write_with_transaction(
    psql: &PsqlConnection,
    claude: &mut ClaudeClient,
    schema: &Schema,
    original_question: &str,
    current_sql: &mut String,
    config: &mut Config,
) -> Result<()> {
    loop {
        println!();
        println!("⚠️  This is a WRITE operation. Previewing in a transaction (will rollback)...\n");

        let (success, stdout, stderr) = psql.preview_write_with_returning(current_sql)?;

        if !success {
            eprintln!("{}", stderr);
            println!();

            match prompt_error_action()? {
                ErrorAction::Fix => {
                    *current_sql = ask_claude_to_fix(
                        claude,
                        schema,
                        original_question,
                        current_sql,
                        &stderr,
                        config,
                    )
                    .await?;
                    if current_sql.is_empty() {
                        return Ok(());
                    }
                    continue;
                }
                ErrorAction::Edit => {
                    *current_sql = prompt_edit_sql(current_sql)?;
                    println!();
                    continue;
                }
                ErrorAction::Retry => {
                    match prompt_new_question(claude, schema, config).await? {
                        Some(sql) => *current_sql = sql,
                        None => return Ok(()),
                    }
                    continue;
                }
                ErrorAction::Cancel => {
                    println!("Cancelled.\n");
                    return Ok(());
                }
            }
        }

        if !stdout.is_empty() {
            println!("Rows that will be affected:");
            print!("{}", stdout);
        }

        println!("\n(Preview complete - changes were rolled back)");
        match prompt_commit_action()? {
            CommitAction::Commit => {
                let (success, stdout, stderr) =
                    psql.execute_write_with_confirmation(current_sql, true)?;
                if success {
                    println!("✓ Transaction committed.\n");
                    if !stdout.is_empty() {
                        print!("{}", stdout);
                    }
                    claude.add_to_history(
                        original_question.to_string(),
                        current_sql.clone(),
                        Some(stdout),
                    );
                } else {
                    eprintln!("Commit failed: {}", stderr);
                }
                return Ok(());
            }
            CommitAction::Rollback => {
                println!("Transaction rolled back.\n");
                return Ok(());
            }
            CommitAction::Edit => {
                *current_sql = prompt_edit_sql(current_sql)?;
                println!();
                continue;
            }
        }
    }
}

async fn ask_claude_to_fix(
    claude: &ClaudeClient,
    schema: &Schema,
    original_question: &str,
    current_sql: &str,
    error: &str,
    config: &mut Config,
) -> Result<String> {
    println!("-- Fixed SQL:");
    let mut fixed_sql = claude
        .fix_sql(schema, original_question, current_sql, error)
        .await?;

    loop {
        match confirm_execution(config)? {
            RunChoice::Run | RunChoice::AutoRun => return Ok(fixed_sql),
            RunChoice::EditSql => {
                fixed_sql = prompt_edit_sql(&fixed_sql)?;
                continue;
            }
            RunChoice::EditPrompt | RunChoice::Cancel => {
                println!("Cancelled.\n");
                return Ok(String::new());
            }
        }
    }
}

async fn prompt_new_question(
    claude: &ClaudeClient,
    schema: &Schema,
    config: &mut Config,
) -> Result<Option<String>> {
    print!("Enter new prompt: ");
    io::stdout().flush()?;
    let mut new_question = String::new();
    io::stdin().read_line(&mut new_question)?;
    let new_question = new_question.trim();

    if new_question.is_empty() {
        println!("Cancelled.\n");
        return Ok(None);
    }

    println!("\n");

    let mut new_sql = claude.text_to_sql(schema, new_question).await?;

    loop {
        match confirm_execution(config)? {
            RunChoice::Run | RunChoice::AutoRun => return Ok(Some(new_sql)),
            RunChoice::EditSql => {
                new_sql = prompt_edit_sql(&new_sql)?;
                continue;
            }
            RunChoice::EditPrompt | RunChoice::Cancel => {
                println!("Cancelled.\n");
                return Ok(None);
            }
        }
    }
}

enum CommitAction {
    Commit,
    Rollback,
    Edit,
}

fn prompt_commit_action() -> Result<CommitAction> {
    let options = &[
        "Commit transaction",
        "Rollback (discard changes)",
        "Edit SQL and retry",
    ];
    match pick_option(options)? {
        Some(0) => Ok(CommitAction::Commit),
        Some(2) => Ok(CommitAction::Edit),
        _ => Ok(CommitAction::Rollback),
    }
}

enum ErrorAction {
    Fix,
    Edit,
    Retry,
    Cancel,
}

fn prompt_error_action() -> Result<ErrorAction> {
    let options = &[
        "Ask Claude to fix",
        "Edit SQL manually",
        "Retry with different prompt",
        "Cancel",
    ];
    match pick_option(options)? {
        Some(0) => Ok(ErrorAction::Fix),
        Some(1) => Ok(ErrorAction::Edit),
        Some(2) => Ok(ErrorAction::Retry),
        _ => Ok(ErrorAction::Cancel),
    }
}

fn prompt_edit_sql(current_sql: &str) -> Result<String> {
    terminal::enable_raw_mode()?;
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen)?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let lines: Vec<String> = current_sql.lines().map(|s| s.to_string()).collect();
    let mut textarea = TextArea::new(lines);
    textarea.set_cursor_line_style(Style::default());
    textarea.set_block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Edit SQL (Ctrl+S to save, Esc to cancel) "),
    );
    textarea.set_style(Style::default().fg(Color::Green));

    let result = loop {
        terminal.draw(|f| {
            let chunks = Layout::default()
                .constraints([Constraint::Min(3), Constraint::Length(1)])
                .split(f.area());

            f.render_widget(&textarea, chunks[0]);
            f.render_widget(
                Paragraph::new("Ctrl+S: Save | Esc: Cancel | Arrow keys: Move | Enter: New line"),
                chunks[1],
            );
        })?;

        if let Event::Key(key) = event::read()? {
            match (key.code, key.modifiers) {
                (KeyCode::Char('s'), KeyModifiers::CONTROL) => {
                    break Some(textarea.lines().join("\n"));
                }
                (KeyCode::Esc, _) => {
                    break None;
                }
                _ => {
                    textarea.input(key);
                }
            }
        }
    };

    terminal::disable_raw_mode()?;
    crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen)?;

    Ok(result.unwrap_or_else(|| current_sql.to_string()))
}
