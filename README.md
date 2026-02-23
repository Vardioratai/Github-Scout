# 🔭 GitHub Scout

**Production-grade GitHub repository intelligence spider with analytical scoring.**

GitHub Scout crawls GitHub's GraphQL & REST APIs, persists structured repository data into DuckDB, and computes a composite *potential score* to surface emerging high-impact Python and Jupyter projects.

---

## ✨ Features

- **Paginated GraphQL v4 search** — configurable queries with automatic cursor-based pagination
- **REST enrichment** — README quality analysis, contributor counts, release metadata
- **DuckDB persistence** — embedded columnar database with historical snapshots
- **Polars scoring pipeline** — multi-factor composite score (0–100) combining star velocity, recency, activity, README quality, and 7-day momentum
- **Typer CLI** — 6 commands: `crawl`, `score`, `top`, `stats`, `export`, `clean`
- **Database maintenance** — flexible `clean` command to purge stale, low-score, archived, or forked repos with dry-run preview
- **Incremental updates** — delta scraping with upsert logic, preserving original scrape timestamps
- **Rate-limit aware** — automatic sleep on low remaining quota + tenacity exponential backoff

---

## 📦 Installation

```bash
# Clone the repo
git clone https://github.com/your-user/github-scout.git
cd github-scout

# Install in editable mode with dev dependencies
pip install -e ".[dev]"
```

### Requirements

- Python 3.12+
- A [GitHub Personal Access Token](https://github.com/settings/tokens) with `public_repo` scope

---

## ⚙️ Configuration

Copy the example env file and add your token:

```bash
cp .env.example .env
```

Edit `.env`:

```env
GITHUB_TOKEN=ghp_your_token_here
```

All settings can be overridden via environment variables:

| Variable | Default | Description |
|---|---|---|
| `GITHUB_TOKEN` | *(required)* | GitHub PAT |
| `DB_PATH` | `./github_scout.duckdb` | DuckDB database path |
| `LOG_LEVEL` | `INFO` | Logging level |
| `DEFAULT_QUERY` | `language:python language:"Jupyter Notebook" stars:>100 created:>2026-01-01` | Search query |
| `MAX_PAGES` | `None` *(unlimited)* | Max pages to paginate (set to cap) |
| `MAX_CONCURRENT_ENRICHMENTS` | `10` | Concurrent REST enrichment calls |

---

## 🚀 Usage

### 1. Crawl repositories

```bash
# Use default query
github-scout crawl

# Custom query with limited pages
github-scout crawl --query "machine learning stars:>500" --max-pages 5
```

### 2. Compute scores

```bash
github-scout score
```

### 3. View top repositories

```bash
github-scout top
github-scout top --limit 50
```

### 4. View analytics

```bash
github-scout stats
```

Displays:
- Language distribution
- Topic heatmap
- Star velocity percentiles (p50 / p90 / p99)
- Score distribution histogram
- 7-day trending repos

### 5. Export data

```bash
# CSV
github-scout export -o results.csv

# Parquet
github-scout export -o results.parquet
```

### 6. Clean / purge data

Remove stale or unwanted repositories from the database. **Dry-run is ON by default** — no data is modified until you pass `--execute`.

```bash
# Preview what would be deleted (dry-run)
github-scout clean --archived --forks

# Delete repos scraped before a date
github-scout clean --before 2026-01-01 --execute

# Delete low-score repos (score < 10 or unscored)
github-scout clean --score-below 10 --execute

# Delete repos by language
github-scout clean --language Ruby --execute

# Remove orphan snapshots (no matching repo)
github-scout clean --orphan-snapshots --execute

# Full purge: truncate repositories, snapshots, and crawl runs
github-scout clean --all --execute

# Skip confirmation prompt (for CI/automation)
github-scout clean --archived --execute --yes
```

Filters can be combined (AND logic):

```bash
# Archived forks with score below 20
github-scout clean --archived --forks --score-below 20 --execute
```

| Flag | Filter |
|---|---|
| `--all` | Truncate all three tables |
| `--before YYYY-MM-DD` | `scraped_at < date` |
| `--score-below N` | `potential_score < N` or unscored |
| `--archived` | `is_archived = true` |
| `--forks` | `is_fork = true` |
| `--language X` | `primary_language` (case-insensitive) |
| `--orphan-snapshots` | Snapshots with no matching repo |
| `--dry-run / --execute` | Preview vs. apply (default: dry-run) |
| `--yes, -y` | Skip confirmation prompt |

---

## 🧮 Scoring Algorithm

The **potential score** (0–100) is a weighted composite of:

| Factor | Weight | Description |
|---|---|---|
| Star velocity | 35% | Stars per day since creation |
| Recency decay | 20% | Exponential decay for repos older than 90 days |
| Activity | 20% | `log1p(forks + open_issues + contributors)` |
| 7-day momentum | 15% | Star growth delta from historical snapshots |
| README quality | 10% | Badges, sections, install instructions, demos |

---

## 🏗️ Architecture

```
github_scout/
├── config/          # Settings + GraphQL queries
├── models/          # Pydantic data models
├── client/          # httpx async client, rate limiter, paginator
├── database/        # DuckDB connection, schema DDL, DAO + clean/purge
├── crawler/         # REST enricher + spider orchestrator
├── scoring/         # Polars feature engineering + scorer
├── analytics/       # SQL query constants
└── cli/             # Typer CLI entry point
```

### Database Tables

- **`repositories`** — main table with all repo metadata, enrichment, and scores
- **`repo_snapshots`** — point-in-time star/fork/issue counts for momentum tracking
- **`crawl_runs`** — metadata for each crawl execution

---

## 🧪 Testing

```bash
pytest tests/ -v
```

Tests cover:
- **Pagination** — stops on `hasNextPage=false`
- **Retry** — 429 responses trigger tenacity exponential backoff
- **DAO** — upsert idempotency (1 row in repos, 2 in snapshots)
- **Scoring** — scores always in `[0, 100]`, no nulls in output

---

## 📄 License

[MIT](LICENSE)
