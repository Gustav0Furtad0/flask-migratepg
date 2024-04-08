import os
import re
import click
import psycopg
import importlib.util
from flask import Blueprint, current_app
from datetime import datetime


def migrate(conn, e):
    with open(e.path) as f:
        with psycopg.ClientCursor(conn) as cur:
            if not begin(cur, e.name):
                return
            cur.execute(f.read())
            finalise(cur, e.name)


def begin(cur, name):
    cur.execute('select true from migrations where filename = %s',
                [ name ])
    if (cur.fetchone()):
        return False

    print(name)
    cur.execute('begin')
    return True


def finalise(cur, name):
    cur.execute('insert into migrations (filename) values (%s)',
                [ name ])
    cur.execute('commit')


def init(conn):
    table = '''
    create table if not exists migrations (
        migration_id serial not null,
        filename char(120) not null,
        migrated_at timestamp not null default current_timestamp,
        constraint migrations_primary primary key (migration_id),
        unique (filename)
    )
    '''
    cur = conn.cursor()
    cur.execute('begin')
    cur.execute(table)
    cur.execute('commit')


class MigratePg:
    def __init__(self, app=None):
        if app is not None:
            self.init(app)


    # Establish connection to the database.
    def connect(self):
        return psycopg.connect(
                current_app.config.get('PSYCOPG_CONNINFO'))


    # Where the migrations files are stored.
    def migrations_path(self):
        return current_app.config.get(
                'MIGRATIONS_PATH',
                os.path.join(current_app.root_path, 'database/migrations'))

    # Register command blueprints with Flask.
    def init(self, app):
        bp = Blueprint('migrate', __name__)
        bp.cli.short_help = 'PostgreSQL database migrations.'


        @bp.cli.command('execute', help='Run migrations.')
        def execute():
            migrations_path = self.migrations_path()

            with self.connect() as conn:
                init(conn)

                # Check for new migrations files.
                with os.scandir(migrations_path) as d:
                    ls = list(d)
                    ls.sort(key = lambda e: e.name)
                    for e in ls:
                        if not e.is_file() or e.name.startswith('.'):
                            continue # Ignored file.

                        # SQL migration.
                        if e.name.endswith('.sql'):
                            migrate(conn, e)

            print('Done.')


        @bp.cli.command('new', help='Create a new migration file.')
        @click.argument('name')
        def new(name):
            # Diretório.
            migrations_path = self.migrations_path()

            # Obtendo o timestamp atual em UTC.
            now = datetime.utcnow()
            timestamp = int(now.timestamp())  # Convertendo para inteiro para evitar pontos decimais

            # Sanitizando o nome para garantir que é válido para um nome de arquivo.
            name = re.sub(r'\W', '_', name)

            # Criando o nome do arquivo usando timestamp e o nome sanitizado.
            filename = f'{timestamp}_{name}.sql'
            filepath = os.path.join(migrations_path, filename)  # Usando os.path.join para a correta formação do caminho do arquivo

            # Criando o arquivo.
            with open(filepath, 'a') as f:
                pass  # Simplesmente cria o arquivo se ele não existir.

            print(f'New file: {filepath}')

        app.register_blueprint(bp)
