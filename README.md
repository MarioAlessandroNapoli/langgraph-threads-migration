# LangGraph Threads Migration Tool

A Python tool to migrate threads (conversations) between LangGraph Cloud deployments while preserving thread IDs, metadata, and conversation history.

## Features

- **Export threads to JSON** - Download all threads as a backup file
- **Import threads from JSON** - Restore threads from a backup file
- **Migrate between deployments** - Transfer threads from one LangGraph Cloud deployment to another
- **Multi-tenancy support** - Preserves `metadata.owner` so users keep access to their own threads
- **Test mode** - Migrate a single thread first to verify everything works
- **Dry-run mode** - Preview changes without making any modifications
- **Progress tracking** - Real-time progress bars and detailed summaries

## Use Cases

- **Cost optimization**: Migrate from an expensive production deployment to a cheaper one
- **Environment migration**: Move threads from staging to production or vice versa
- **Backup & restore**: Create JSON backups of all conversations
- **Disaster recovery**: Restore threads after accidental deletion

## Installation

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/langgraph-threads-migration.git
cd langgraph-threads-migration

# Install dependencies
pip install -r requirements.txt

# Or install manually
pip install langgraph-sdk rich python-dotenv
```

## Configuration

Create a `.env` file with your LangSmith API key:

```bash
cp .env.example .env
# Edit .env and add your LANGSMITH_API_KEY
```

Your API key can be found at [smith.langchain.com](https://smith.langchain.com/) → Settings → API Keys.

> **Note**: Use a Service Key (`lsv2_sk_...`) for deployment access, not a Personal Token.

## Usage

### Export threads to JSON file

Download all threads from a deployment as a backup:

```bash
python migrate_threads.py \
  --source-url https://my-deployment.langgraph.app \
  --export-json threads_backup.json
```

### Import threads from JSON file

Restore threads to a deployment from a backup file:

```bash
python migrate_threads.py \
  --target-url https://my-deployment.langgraph.app \
  --import-json threads_backup.json
```

### Full migration between deployments

Export from source, import to target, and validate:

```bash
python migrate_threads.py \
  --source-url https://my-prod.langgraph.app \
  --target-url https://my-dev.langgraph.app \
  --full
```

### Test with a single thread first

Always recommended before a full migration:

```bash
python migrate_threads.py \
  --source-url https://my-prod.langgraph.app \
  --target-url https://my-dev.langgraph.app \
  --full \
  --test-single
```

### Dry-run mode

Preview what would happen without making changes:

```bash
python migrate_threads.py \
  --source-url https://my-prod.langgraph.app \
  --target-url https://my-dev.langgraph.app \
  --full \
  --dry-run
```

### Validate migration

Compare thread counts between source and target:

```bash
python migrate_threads.py \
  --source-url https://my-prod.langgraph.app \
  --target-url https://my-dev.langgraph.app \
  --validate
```

## Command Reference

| Argument | Description |
|----------|-------------|
| `--source-url` | Source LangGraph Cloud deployment URL |
| `--target-url` | Target LangGraph Cloud deployment URL |
| `--api-key` | LangSmith API key (or set in `.env`) |
| `--export-json FILE` | Export threads to JSON file only |
| `--import-json FILE` | Import threads from JSON file |
| `--migrate` | Migrate threads (export + import) |
| `--full` | Full migration (export + import + validate) |
| `--validate` | Compare source vs target thread counts |
| `--dry-run` | Simulation mode (no changes made) |
| `--test-single` | Process only one thread (for testing) |
| `--backup-file` | Backup file path (default: `threads_backup.json`) |

## Important Notes

### Authentication

If your LangGraph deployment uses custom authentication (e.g., Auth0), you may need to temporarily disable it during migration:

```json
// langgraph.json - temporarily set auth to null
{
  "auth": null
}
```

Remember to re-enable authentication after migration!

### Thread Status

Imported threads may initially show as "Interrupted" in LangGraph Studio. This is normal - the status will change to "Idle" after the first interaction.

### Multi-tenancy

The tool preserves `metadata.owner`, so each user will only see their own threads after migration. Make sure your target deployment has the same authentication setup.

### Rate Limiting

The tool includes built-in rate limiting (0.2-0.3s delays) to avoid overwhelming the API. For large migrations (1000+ threads), consider running during off-peak hours.

## Example Output

```
╭────────────────────────────────────────╮
│ 🔄 LangGraph Threads Migration Tool    │
╰────────────────────────────────────────╯

╭─────────────────────────────────────────╮
│ Phase 1: Export threads from source     │
╰─────────────────────────────────────────╯
✓ 66 threads found
✓ Backup saved: threads_backup.json
✓ Size: 29.30 MB

╭────────────────────────────────────────╮
│ Phase 2: Import 66 threads to target   │
╰────────────────────────────────────────╯
         Import Summary
┏━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┓
┃ Status                   ┃ Count  ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━┩
│ Created successfully     │     66 │
│ Already exists (skipped) │      0 │
│ Errors                   │      0 │
│ Total                    │     66 │
└──────────────────────────┴────────┘

╭─────────────────────────────────────╮
│ Phase 3: Validate migration         │
╰─────────────────────────────────────╯
✓ Migration validated successfully!
```

## Troubleshooting

### PermissionDeniedError

- Verify your API key is correct and has access to both deployments
- Use a Service Key (`lsv2_sk_...`), not a Personal Token
- Check if custom authentication needs to be disabled temporarily

### ConflictError (409)

Thread already exists in target deployment - this is handled automatically (skipped).

### Thread history not migrated

Currently, thread history (checkpoints) is exported but the full checkpoint chain may not be restored. Messages and metadata are preserved.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Acknowledgments

Built for the [LangGraph](https://github.com/langchain-ai/langgraph) community.
