# Reddit Post for r/LangChain

## Title:
**[Open Source] LangGraph Threads Export Tool - Backup, migrate, and own your conversation data**

---

## Body:

Hey everyone! 👋

I built a tool to solve a problem I had with LangGraph Cloud and wanted to share it with the community.

### The Problem

I had two LangGraph Cloud deployments - a production one (expensive) and a dev one (cheaper). I wanted to:
- Migrate all user conversations from prod to dev
- Keep the same thread IDs so users don't lose their chat history
- Preserve multi-tenancy (each user only sees their own threads)

There's no built-in way to do this in LangGraph Cloud, so I built one.

### What This Tool Does

**Export your LangGraph threads to:**
- 📄 **JSON file** - Simple backup you can store anywhere
- 🐘 **PostgreSQL database** - Own your data with proper schema and indexes
- 🔄 **Another deployment** - Migrate between environments

**What gets exported:**
- Thread IDs (preserved exactly)
- Metadata (including `owner` for multi-tenancy)
- Full checkpoint history
- Conversation values/messages

### Quick Example

```bash
# Export all threads to JSON
python migrate_threads.py \
  --source-url https://my-deployment.langgraph.app \
  --export-json backup.json

# Export to PostgreSQL
python migrate_threads.py \
  --source-url https://my-deployment.langgraph.app \
  --export-postgres

# Migrate between deployments
python migrate_threads.py \
  --source-url https://prod.langgraph.app \
  --target-url https://dev.langgraph.app \
  --full
```

### Why You Might Need This

- **Cost optimization** - Move from expensive prod to cheaper deployment
- **Backup before deletion** - Export everything before removing a deployment
- **Compliance** - Store conversation data in your own database
- **Analytics** - Query your threads with SQL
- **Disaster recovery** - Restore from JSON backup

### GitHub

🔗 **[github.com/YOUR_USERNAME/langgraph-threads-migration](https://github.com/YOUR_USERNAME/langgraph-threads-migration)**

MIT licensed, PRs welcome!

---

### Note for deployments with custom auth

If you use Auth0 or custom authentication, you'll need to temporarily disable it during export (the tool uses the LangSmith API key, not user tokens). Just set `"auth": null` in your `langgraph.json`, export, then re-enable.

---

Hope this helps someone! Let me know if you have questions or feature requests. 🙂

---

## Suggested subreddits:
- r/LangChain
- r/MachineLearning (if relevant)
- r/Python (for the tool aspect)

## Tags/Flair:
- Open Source
- Tool
- LangGraph
