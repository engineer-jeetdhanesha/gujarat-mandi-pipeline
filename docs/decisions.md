# Decision log (ADRs)

Architecture Decision Records: one short entry per design decision, with the reason and what was given up. Status of all entries: accepted.

## 1. Setup is a separate job from the daily load

- **Context:** the pipeline needs tables, volumes and dimensions to exist before it can load data. Creating them is a one-time action; loading data happens many times.
- **Decision:** two jobs. `mandi_setup` creates the schemas, tables and volumes and loads the dimensions. `mandi_daily` only loads data.
- **Consequences:** the daily job stays safe to repeat and cannot change a table by accident. A new environment needs two runs (setup, then daily).
- **Alternatives:** one job that creates what is missing before every load. Rejected because every load would carry the risk and time of schema work.

## 2. The CI/CD pipeline only deploys; jobs are run by hand

- **Context:** a deploy and a data load are different actions with different risk.
- **Decision:** the pipeline checks the code and deploys the bundle to UAT and prod. It never runs a job.
- **Consequences:** a green pipeline can never load data by surprise or fail on a missing file. Someone runs `mandi_setup` and `mandi_daily` after a deploy (see the [runbook](runbook.md)).
- **Alternatives:** run setup and the daily job from the pipeline. Rejected: the first run of a new environment fails because setup has not run, and a release would load data without anyone asking.

## 3. Production is released by a tag, after UAT, with approval

- **Context:** prod must only get code that passed review, the checks and UAT.
- **Decision:** a `v*` tag starts the prod stage. It runs only if the UAT deploy for the same commit succeeded, only if the tagged commit is on `main`, and only after a manual approval on the `prod` environment.
- **Consequences:** every prod deploy names an exact commit, rolling back means re-running an older tag, and two clicks (tag and approve) separate a merge from production.
- **Alternatives:** deploy prod on every merge to `main`. Rejected: prod would change with every small fix.

## 4. Bad rows go to a rejected table

- **Context:** the source has misspelt districts, impossible prices and unknown markets.
- **Decision:** a row that fails a rule is written to `silver.mandi_prices_rejected` with its raw values, the rule ids and readable reasons, and left out of `mandi_prices_clean`. See [data quality rules](data-quality-rules.md).
- **Consequences:** nothing is silently lost, and a reviewer can see why. Rejected rows are not reprocessed; a fix is made in the source file and the file is loaded again.
- **Alternatives:** drop bad rows (loses data silently), or fail the whole load (one bad row blocks the day). Both rejected.

## 5. Tables are created with `CREATE TABLE IF NOT EXISTS`

- **Context:** setup may be run again, for example after a seed file changes.
- **Decision:** setup never replaces or alters a table.
- **Consequences:** running setup can never destroy data. The cost: a column change in the code does not reach an existing table, so it needs a deliberate drop and rebuild (runbook, section 5).
- **Alternatives:** `CREATE OR REPLACE TABLE`. Rejected: every setup run would empty the tables.

## 6. Logic lives in a library, not in the notebooks

- **Context:** notebooks are hard to test and review.
- **Decision:** the code is in `src/lib_mandi` as small `extract__`, `transform__`, `load__`, `dq_check__` and `ddl` functions. A notebook only reads its widgets and calls them.
- **Consequences:** the functions are tested with a local Spark session and Delta in CI, with no Databricks needed. Notebook tests on Databricks cover only the wiring.
- **Alternatives:** logic inside the notebooks. Rejected: untestable without a cluster.

## 7. Loads are idempotent

- **Context:** a job can be retried, and a file can be delivered twice.
- **Decision:** bronze deletes the rows of the same run and file before it appends. Silver and gold are `MERGE`s on their natural keys. Dimensions are overwritten from the seed files.
- **Consequences:** a retry or a second load gives the same tables. Duplicates inside one run are caught by the `duplicate` rule.
- **Alternatives:** append only. Rejected: a retry would double the rows.

## 8. Processed files are archived

- **Context:** the daily load reads every file that matches a pattern, so a file that stayed would be read again.
- **Decision:** the last task moves the files that the run loaded from `bronze.landing_zone` to `bronze.archive/<run_id>/`. If a task fails, the files stay.
- **Consequences:** the next run only sees new files, and the archive keeps a copy of every input, grouped by run.
- **Alternatives:** delete the files after loading (no audit trail), or keep them in place and track what was loaded (more state to manage).

## 9. Gold is weekly, per district and commodity

- **Context:** the dashboard shows price trends and a comparison of districts. Daily prices are noisy, and markets report on different days.
- **Decision:** `gold.mart_price_weekly` has one row per district, commodity and week, with the minimum, maximum and average modal price and an `is_complete_week` flag. A run recomputes only the weeks it touched, plus the week before.
- **Consequences:** the dashboard is fast and simple, and it can ignore a partly loaded week. Combining districts is an average of averages, so it is a trend, not an exact state-wide price.
- **Alternatives:** a daily mart. Considered; the weekly grain was kept for readability.

## 10. Dashboard filters are parameters

- **Context:** the first version used filter widgets on the data. They offered an "All" option, which mixes commodities that cannot be compared, and their lists shrank when the other filter changed.
- **Decision:** Commodity and Period are dashboard parameters with defaults. Each chart has its own query, which already applies both.
- **Consequences:** a commodity and a period are always selected, and the lists are stable. The top and bottom 5 are chosen in SQL.
- **Alternatives:** keep the filter widgets. Rejected for the two problems above.

## 11. The pipeline signs in without stored secrets

- **Context:** CI needs to deploy to two workspaces.
- **Decision:** workload identity federation. Each environment has an Azure DevOps service connection backed by a federated credential on an Entra app registration, which maps to a Databricks service principal.
- **Consequences:** there is no client secret to store, leak or rotate. One-time setup in Azure is needed.
- **Alternatives:** a personal access token or a client secret in the pipeline variables. Rejected.
