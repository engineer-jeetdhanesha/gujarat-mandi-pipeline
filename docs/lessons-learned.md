# Lessons learned

Problems met while building and releasing the pipeline, what caused each one and how it was fixed. Each is something to check early next time.

## 1. A quoted value with a line break failed a whole load

- **Symptom:** a backfill file failed with `FAILED_READ_FILE`: 278 rows were unreadable.
- **Cause:** some commodity names contain a line break inside quotes (for example `"Spiny Gourd / Kartali(Kantola)\n"`). Spark read each physical line as a row, and the read mode was `FAILFAST`, so one broken row stopped the file.
- **Fix:** read the files with `multiLine` on, so a quoted value can span lines. Added a regression test with a quoted line break.
- **Lesson:** keep the strict read mode (a bad file should fail loudly), but test the reader with the ugly cases of the real data.

## 2. A deploy by a person, then by a service principal, failed with 403

- **Symptom:** Deploy UAT failed: "only workspace admins can change the owner of a job".
- **Cause:** the first bundle deploy was done by a person, so the jobs belonged to them. The pipeline's service principal could not take them over.
- **Fix:** `databricks bundle destroy` as the person, then let the pipeline deploy, so the service principal owns the jobs.
- **Lesson:** decide who owns a deployed bundle before the first deploy. Deploy UAT and prod only through the pipeline.

## 3. Unity Catalog refuses grants to the all-users group

- **Symptom:** `mandi_setup` failed in every environment at the volume grant step, with `UC_SYSTEM_WORKSPACE_GROUP_PERMISSION_UNSUPPORTED`.
- **Cause:** setup tried to grant volume access to the built-in `_workspace_users_...` group. Unity Catalog does not allow privileges on that system group.
- **Fix:** removed the grants from setup completely. Access to a volume is granted by hand.
- **Lesson:** try a new permission step in a real workspace before it goes into a shared job.

## 4. `CREATE TABLE IF NOT EXISTS` hides a schema change

- **Symptom:** after columns were removed from the gold table in the code, the existing tables in every environment still had them, and the next `MERGE` would have failed.
- **Cause:** `IF NOT EXISTS` skips a table that exists. It never alters it.
- **Fix:** drop the table, deploy, let setup recreate it, and re-run the gold load. The steps are in the [runbook](runbook.md).
- **Lesson:** a column change is a migration. Plan the drop and rebuild for each environment as part of the release.

## 5. Editing a deployed dashboard blocks the next deploy

- **Symptom:** `bundle deploy` stopped with "This dashboard has been modified remotely".
- **Cause:** a change made in the dashboard editor is not in the repo, and the CLI refuses to overwrite it silently.
- **Fix:** read the editor version, copy the change into the dashboard file in the repo, then deploy (with `--force` only when the editor version is known to be safe to overwrite).
- **Lesson:** the repo is the source of truth. Treat the editor as a preview.

## 6. Dashboard filters: an "All" option and lists that shrank

- **Symptom:** the commodity filter sometimes offered "All" and sometimes only a few commodities.
- **Cause:** a filter's list is built from the rows left after the other filters, and a filter widget can be cleared to "All". With all commodities selected, an average mixes things that cannot be compared.
- **Fix:** Commodity and Period became dashboard parameters, each with a default and no "All". The top and bottom 5 districts are chosen in SQL.
- **Lesson:** for a dashboard that compares prices, a required choice is better than an optional one.

## 7. An average of averages is a trend, not an exact number

- **Symptom:** a doubt about whether averaging is right once weeks and districts are combined.
- **Cause:** gold stores an average per district and week. Averaging those again gives each district the same weight whatever its volume.
- **Fix:** the extra columns for exact roll-ups (`sum_modal_price`, `price_records`) were removed to keep gold simple, and the dashboard text states that a price is the average of the district weekly averages.
- **Lesson:** decide up front whether a figure is exact or a trend, and say so on the chart.

## 8. Check what the source really holds before explaining it

- **Symptom:** an early statement that the source returns only recent days was wrong.
- **Cause:** it was assumed, not checked. The source had full history, as the per-day totals of the earlier pull showed.
- **Fix:** corrected after looking at the data.
- **Lesson:** look at the real data before putting a claim in a design or a doc.

## 9. Every small change costs a pipeline run

- **Symptom:** several small pull requests in a row each triggered the full checks and, after the merge, a UAT deploy.
- **Fix:** batch related changes into one pull request and push once.
- **Lesson:** on a metered account, plan changes so each push carries a finished piece of work.
