import asyncio as aio
import json

import aiosqlite
import click
from quart import current_app
from quart import g
from quart.cli import pass_script_info


def init_app(app):
    app.cli.add_command(init_db_command)


async def init_db():
    db = await get_db()

    with current_app.open_resource('schema.sql') as f:
        await db.executescript(f.read().decode('utf8'))


@click.command('init-db')
@pass_script_info
def init_db_command(info):
    """Clear the existing data and create new tables."""
    app = info.load_app()

    async def f():
        async with app.app_context():
            await init_db()
            await close_db()

    aio.run(f())
    click.echo('Initialized the database.')


async def get_db():
    if 'db' not in g:
        g.db = await aiosqlite.connect(current_app.config['DATABASE'])
        g.db.row_factory = aiosqlite.Row

    return g.db


async def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        await db.close()


async def insert(key: str, val: dict):
    db = await get_db()
    await db.execute('INSERT INTO cache VALUES (?, ?)', (key, json.dumps(val)))
    await db.commit()


async def get(key):
    db = await get_db()
    query = 'SELECT data FROM cache WHERE id=?'
    async with db.execute(query, (key, )) as cursor:
        result = await cursor.fetchone()
        return json.loads(result[0]) if result is not None else None
