# psqlm

A `psql` wrapper that converts natural language to SQL using an LLM.

## Usage

On startup, psqlm connects to your database, introspects the schema, and drops you into a REPL. Type questions in plain English and get SQL back.

All write operations are previewed inside a transaction that gets rolled back, so you can see the affected rows before choosing to commit.
