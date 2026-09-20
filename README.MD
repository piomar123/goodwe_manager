# Goodwe PV energy manager

Quick overview:

https://github.com/user-attachments/assets/0824e02f-0407-40a6-8b6f-845b09fb819b

* Create venv `python3 -m venv venv` and activate it `. venv/bin/activate`
* Install dependencies `pip install -r requirements.txt`
* Copy `.env.example` to `.env` and fill in the values
* Replace the rest of `poor-man's config` in `main.py`
* Run with `python main.py` (TODO: instructions for gunicorn)
* Run with `--dry-run` parameter to disable communication with the inverter (used for testing)

## Upgrading

Pulling a checkout that's behind may require a one-off migration step - see
[`CHANGELOG.md`](CHANGELOG.md) for what changed and why. Most notably: if
you have old `data-*.csv` files from before the switch to SQLite, run
`python _migrate_csv_to_sqlite.py` once to import them into `data.db`
(safe to re-run; already-migrated files are skipped).

## Tariff-aware import pricing (optional)

`_calculate_income.py` and the MQTT bridge (see PR #35) can price
imported energy using a real time-of-use tariff schedule
instead of a flat rate. Copy an example from `tariff_examples/` (e.g.
`g12w_pge.yaml`) to `tariff_import.yaml` (already gitignored), fill in
your actual rates, and set `TARIFF_IMPORT_CONFIG=tariff_import.yaml` in
`.env`. Without this set, both keep using the flat `IMPORT_PRICE_KWH`
constant, unchanged from today's behavior.

## MQTT bridge for Home Assistant / Predbat (optional)

Set `MQTT_HOST` (and `MQTT_USERNAME`/`MQTT_PASSWORD` if your broker
requires auth) in `.env` to publish live telemetry, daily energy
counters, RCE export prices, tariff-based import prices (if
`TARIFF_IMPORT_CONFIG` is also set), and the combined PV forecast to your
MQTT broker. Unset `MQTT_HOST` (the default) disables this entirely - no
behavior change from before this feature existed. See PR #35 for the
full topic list and Home Assistant/Predbat setup.

RCE export prices publish at 15-minute granularity by default (the
market's own settlement period). Set `RCE_EXPORT_GRANULARITY=hourly` to
average each hour's four quarters first instead, matching how PGE
settles prosument export credit on an hourly basis.

A negative RCE price publishes as `0.0` by default (`RCE_EXPORT_NEGATIVE_PRICES=zero`),
matching net-billing paying nothing for it. Set `RCE_EXPORT_NEGATIVE_PRICES=raw`
to publish the true negative value instead - note the 23% VAT bonus is
never applied to a negative price either way, since the law doesn't
define a bonus on a loss.

## Architecture

Data is pulled every few seconds from the inverter in the local network using the Goodwe API 
and streaming data to the frontend using Server Sent Events (SSE) for real-time updates.
Data is written into a SQLite database (`data.db`, `inverter_history` table) and can be processed later
by some scripts calculating the cost savings. Older `data-xxxx-xx-xx_xx-xx-xx.csv` files are legacy/historical
only - the app no longer writes to them (see "Upgrading" above for importing them).
## Documentation conventions

Design docs for a feature under development live in `docs/superpowers/{specs,plans,notes}/`
(dated filenames, e.g. `docs/superpowers/notes/2026-09-11-diagram-mockup-checklist.md`).
Once the feature is implemented and merged, its spec/plan get deleted in a follow-up
commit - the code and tests are the source of truth for current behavior, and the
design rationale/trade-offs stay recoverable via that PR's commit history rather than
bloating `main`'s tree with docs that will otherwise silently go stale. `notes/` (recorded
investigations/decisions, e.g. bug root-causes, calibration data) are longer-lived than
`specs`/`plans` and are kept unless truly obsolete.

**Caveat**: if you delete a spec/plan, grep the repo for links to it first (e.g. this
file used to link straight to a since-deleted sqlite storage spec) - a dangling
reference is easy to miss and won't fail any build.

Scripts starting from underscore `_` are not used by the main application, they are some drafts, experiments or utils.
Dates for script input can be specified in formats: `YYYY-MM-DD` or `DD.MM.YYYY`.

There are a lot of poor-man's solutions, quick hacks and bad conventions in the code that should be fixed. 
Some of them include:
* mixing async (Goodwe API) and sync (Flask) code - async code runs in a separate thread, this might cause unexpected problems, 
although a special care has been taken to ensure proper synchronization between them and finalizing on exit - 
a solution would be to use an alternative to Flask that supports async (and also supports Server Sent Events)
* PV forecast assumes that there are two PV strings with the same peak power and tilt but with a different orientation angle (basically east-west configuration)
and that logic is quite hard-coded even though it looks like it's configurable
* too much code in main.py - needs to be extracted for readability and maintainability
* Jinja templates use a lot of code repetition - needs to extract meta-templates and macros

TODO: instructions for running as a linux service
