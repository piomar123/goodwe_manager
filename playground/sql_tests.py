import asyncio

import aiosqlite

print(aiosqlite.sqlite_version)
storage_path = "test.db"

# https://stackoverflow.com/questions/9561832/what-if-i-dont-close-the-database-connection-in-python-sqlite

# https://antonz.org/json-virtual-columns/


async def main():
    await create_tables()
    async with aiosqlite.connect(storage_path) as db:
        async with db.execute("SELECT *, datetime(timestamp, 'unixepoch') FROM inverter_history") as cursor:
            fetchall = await cursor.fetchall()
            print(fetchall)


async def create_tables():
    async with aiosqlite.connect(storage_path) as db:
        # https://stackoverflow.com/questions/1601151/how-do-i-check-in-sqlite-whether-a-table-exists
        async with db.execute('SELECT name FROM sqlite_master WHERE type="table" AND name="inverter_history"') as cursor:
            if await cursor.fetchone():
                return
        await db.execute('CREATE TABLE inverter_history (id INTEGER PRIMARY KEY, data TEXT)')
        await db.execute('ALTER TABLE inverter_history ADD COLUMN timestamp INTEGER AS (unixepoch(json_extract(data, "$.timestamp")))')  # STORED?
        await db.execute('CREATE INDEX IF NOT EXISTS timestamp_index ON inverter_history (timestamp)')

        await db.execute('INSERT INTO inverter_history (data) VALUES (json(?))', ('{"timestamp": "2024-07-31 14:22:06", "ppv": 1234}',))
        await db.commit()


if __name__ == '__main__':
    asyncio.run(main())
