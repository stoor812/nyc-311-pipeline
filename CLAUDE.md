# Claude Code Instructions

## Git
When asked to push changes:
1. Run `git add .`
2. Run `git commit -m "[descriptive message based on what changed]"`
3. Run `git push origin main`

Always write descriptive commit messages that explain what changed and why.
Never push if there are uncommitted changes to .env (it contains secrets).

## Project Context
This is a Data Engineering pipeline for NYC 311 Open Data.
Stack: Python, pandas, pyarrow, PostgreSQL, Docker, SQLAlchemy.
Architecture: Bronze (raw parquet) → Silver (clean parquet) → PostgreSQL.