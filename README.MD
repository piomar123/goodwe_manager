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

## Architecture

Data is pulled every few seconds from the inverter in the local network using the Goodwe API 
and streaming data to the frontend using Server Sent Events (SSE) for real-time updates.
Data is written into a SQLite database (`data.db`, `inverter_history` table) and can be processed later
by some scripts calculating the cost savings; see `docs/superpowers/specs/2026-08-27-sqlite-storage-design.md`
for the storage design. Older `data-xxxx-xx-xx_xx-xx-xx.csv` files are legacy/historical only - the app no
longer writes to them (see "Upgrading" above for importing them).

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
