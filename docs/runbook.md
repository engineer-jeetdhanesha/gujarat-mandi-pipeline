# Runbook

How to operate the pipeline: set up an environment, load data, release, roll back and fix common problems. The commands use the Databricks CLI with one profile per workspace (`gujarat-uat`, `gujarat-prod`; use your own profile for `dev`).

Environments: `dev` (runs as you), `uat` and `prod` (run as the service principals `sp-mandi-uat` and `sp-mandi-prod`). Each has its own catalog, `gujarat_mandi_pipeline_<env>_ws`, with schemas `bronze`, `silver` and `gold`.

## 1. Set up a new environment

1. **Deploy the bundle.** For dev: `databricks bundle deploy -t dev`. For UAT and prod the pipeline does it (merge to `main` for UAT, a `v*` tag for prod).
2. **Run `mandi_setup`** once. It creates the schemas, tables and volumes (`bronze.landing_zone`, `bronze.archive`) and loads the market, commodity and date dimensions from `seed/`.
   ```
   databricks bundle run mandi_setup -t <env> -p <profile>
   ```
3. **Grant access by hand** if someone other than the owner must upload files: `USE CATALOG` on the catalog, `USE SCHEMA` on `bronze`, and `READ VOLUME` and `WRITE VOLUME` on `bronze.landing_zone`. Setup grants nothing.
4. **Upload the source file** to `/Volumes/gujarat_mandi_pipeline_<env>_ws/bronze/landing_zone/`. The name must match `mandi_*.csv`.
5. **Run `mandi_daily`:**
   ```
   databricks bundle run mandi_daily -t <env> -p <profile>
   ```
   It loads bronze, silver (clean, rejected and fact) and gold, then moves the loaded file to `bronze.archive/<run_id>/`.
6. **Check the result.** The job is green, the file is in `archive`, and `gold.mart_price_weekly` has rows (`SELECT COUNT(*), MIN(week_start), MAX(week_start) FROM gold.mart_price_weekly`).

## 2. Load a new file

Upload it to `landing_zone` as `mandi_<anything>.csv` and run `mandi_daily`. A file may hold one day or many. Silver is a `MERGE` on the natural key, so a day loaded twice is updated, not duplicated. If no file matches, `load_bronze` fails and the files stay where they are.

## 3. Re-run only one step

Run a single task of a job, for example gold for a run that already loaded silver:

```
databricks bundle run mandi_daily -t <env> -p <profile> --only load_gold --params run_id=<run_id>
```

Gold recomputes the weeks that `<run_id>` touched, so use the run id that loaded the data. `mandi_setup` has a single-task form too: `--only setup`.

## 4. Wipe an environment and start clean

This deletes the data of that environment. Use it only when you mean to.

1. Drop every table in the `bronze`, `silver` and `gold` schemas of the catalog.
2. Delete the files in `bronze.landing_zone` and `bronze.archive`.
3. Run `mandi_setup`, upload the file, run `mandi_daily` (section 1, steps 2 to 5).

## 5. Change a table's columns

`CREATE TABLE IF NOT EXISTS` skips a table that already exists, so a column change in the code never reaches an existing table, and a later `MERGE` can fail. Drop the table, deploy the new code, run `mandi_setup` (it recreates the table), then re-run the load that fills it (section 3). Do this in each environment as part of the release.

## 6. Release to production

1. Merge the pull request to `main`. The pipeline checks the change and deploys UAT.
2. In UAT, run `mandi_setup` (if tables changed) and `mandi_daily` with a file, and check it is green.
3. Tag the release on `main` and push the tag (the tag must start with `v`):
   ```
   git checkout main && git pull
   git tag -a v1.0.1 -m "Release notes in one line"
   git push origin v1.0.1
   ```
4. The tag run checks, deploys UAT again, then waits for approval on the `prod` environment. Approve it in Azure DevOps (**Pipelines > Environments > prod**).
5. In prod, run `mandi_setup` and `mandi_daily` as in section 1.

The pipeline only deploys. It never runs a job.

## 7. Roll back

Re-run the pipeline for an older tag. Do not move or reuse a tag; make a new one (`v1.0.2`) for a fix. Rolling back the code does not undo data; to undo a bad load, wipe and reload (section 4), or use Delta time travel on the table.

## 8. Common problems

| Symptom | Cause | Fix |
|---|---|---|
| `Deploy UAT` fails with "only workspace admins can change the owner of a job" | The jobs were deployed by a person and now by a service principal | `databricks bundle destroy -t <env>` as the person, then deploy again |
| `bundle deploy` stops with "dashboard has been modified remotely" | Someone edited the dashboard in the editor | Copy the editor change into `resources/dashboards/mandi_prices.lvdash.json`, or deploy with `--force` to overwrite the editor version |
| `load_bronze` fails: no file matches | The file is missing or not named `mandi_*.csv` | Upload it to `landing_zone` with a matching name |
| Gold `MERGE` fails on unknown columns | The table has the old columns | Section 5 |
| A user cannot upload to the volume | No grant | Step 3 of section 1 |
| Dashboard shows an error for a viewer | The viewer has no `SELECT` on the gold tables | Grant `SELECT` on `gold.mart_price_weekly` or publish the dashboard with embedded credentials |
