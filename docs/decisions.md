# Decision log

The main design decisions, why they were made and what each one costs.

| # | Decision | Why | Trade-off |
|---|---|---|---|
| 1 | `mandi_setup` is a separate job from `mandi_daily` | Creating tables and loading data are different actions. One job for both would need if/else logic and extra parameters; two small jobs are simpler to maintain | A new environment needs two runs |
| 2 | Jobs are started manually, not on a schedule | The source file is downloaded by hand and uploaded to the volume, so there is no fixed time when new data arrives. It is also a personal project, so a schedule would only add compute cost | Someone must start the run after uploading a file |
| 3 | Prod is released by a `v*` tag, after UAT, with approval | Prod only gets reviewed code that already passed UAT. Every release names a commit, so rolling back means re-running the pipeline for an older tag | Two extra steps between a merge and prod |
| 4 | Bad rows go to a rejected table, with the rule and the reason | Nothing is lost silently, and one bad row does not block the day | Rejected rows are reviewed, not reprocessed; fix the source file and load it again |
| 5 | Tables use `CREATE TABLE IF NOT EXISTS` | Running setup again can never destroy data | A column change needs a deliberate drop and rebuild |
| 6 | Logic lives in the `lib_mandi` library, not in the notebooks | Code is cleaner and easier to test. The functions are unit tested on a local Spark session in CI, which is faster than a Databricks cluster and costs nothing | Notebooks only wire widgets to functions |
| 7 | Loads are idempotent (replace the run's rows in bronze, `MERGE` in silver and gold) | A retry or a file delivered twice gives the same result | A `MERGE` is slower than a plain append and needs a natural key. A later load of the same key overwrites the earlier values |
| 8 | Loaded files are moved from `landing_zone` to `archive/<run_id>/` | The next run sees only new files, and every input is kept. If a task fails, the file stays in `landing_zone`, so you re-run without uploading it again | The archive grows, so old files must be cleaned up manually from time to time |
| 9 | Gold is weekly, per district and commodity, with `is_complete_week` | Simple, fast dashboard queries; a partly loaded week can be ignored | Combining districts is an average of averages: a trend, not an exact state price |
| 10 | The star schema (dimensions and fact) lives in Silver, and Gold holds only marts | Rows with an unknown market, commodity or date are rejected before they reach Silver, so the fact table never has "unknown" dimension rows. Data flows one way (Bronze, Silver, Gold), and Silver never reads from Gold | A new market or commodity must be added to the seed files first, or its rows are rejected |
| 11 | One job, `mandi_daily`, handles both the backfill file and the daily file | A file can hold one day or many, and loads are idempotent. A backfill and a daily load follow the same path, so there is no second job to build and maintain | A large backfill file runs through every task in one go, and gold recomputes every week it touches |
