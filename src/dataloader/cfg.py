import os
from sqlalchemy import text
from urllib.parse import urlparse

from dataloader import logging
from dataloader.helper import (
    clean_csv_value, StringIteratorIO
)
from dataloader.error import ConfigError, UnsupportError

logger = logging.getLogger(__name__)


def _csv_value(rec):
    if rec.__class__.__retain_pkey__:
        if rec.__class__.__table_name__ not in rec.__class__.__dbcfg_ref__['retain_cache']:
            rec.__class__.__dbcfg_ref__['retain_cache'][
                rec.__class__.__table_name__
            ] = []

        rec.__class__.__dbcfg_ref__['retain_cache'][
            rec.__class__.__table_name__
        ].append(rec.pkey_value_map())

    return '|'.join(map(clean_csv_value, rec.tuple_value())) + "\n"


def _mysql_rec_filter(ref, rec):
    ref["buff"].write(_csv_value(rec))


def _postgres_rec_filter(ref, rec):
    ref["buff"].append(_csv_value(rec))


def _mysql_flusher(db_session, full_tbname, sql, buff):
    try:
        buff.close()  # MUST close(flush) FIRST!

        logger.info(f'[FLSH] buff file {buff.name}')

        db_session.execute(text(sql % buff.name))

        db_session.commit()
    except Exception as exc:
        db_session.rollback()
        logger.error(exc)
    finally:
        try:
            os.remove(buff.name)
        except Exception as exc:
            logger.error(exc)

    return None


def _postgres_flusher(db_session, full_tbname, sql, buff):
    try:
        cursor = db_session.connection().connection.cursor()
        std_data_iter = StringIteratorIO(iter(buff))
        cursor.copy_from(std_data_iter, full_tbname, sep='|')
        db_session.commit()
    except Exception as exc:
        db_session.rollback()
        logger.error(exc)

    return std_data_iter


class Configuration(object):
    def __init__(self, config_class):
        self.dbconfigs = Parser().parse(config_class).get_dbconfigs()


class Parser(object):
    def __init__(self):
        self.dbconfigs = {}

    def parse(self, config_class):
        if not any([
            hasattr(config_class, 'DATABASE_URL'),
            hasattr(config_class, 'DATABASE_URLS')
        ]):
            raise ConfigError("'DATABASE_URL' or 'DATABASE_URLS' is needed.")

        db_urls = []
        conn_url = getattr(config_class, 'DATABASE_URL', None)
        if conn_url:
            if isinstance(conn_url, str):
                db_urls = [conn_url]
            else:
                raise ConfigError(f"Invalid config of 'DATABASE_URL' {conn_url}, should be str")

        conn_urls = getattr(config_class, 'DATABASE_URLS', None)
        if conn_urls:
            if isinstance(conn_urls, list):
                db_urls += conn_urls
            else:
                raise ConfigError(
                    f"Invalid config of 'DATABASE_URLS' {conn_urls}, should be list of str: [dburl1, dburl2]."
                )
            db_urls = list(set(db_urls))

        logger.info("[CONF] User configuration of database urls:\n %s", db_urls)

        # flush buff size, count of recs, 5w
        flush_buff_size = getattr(config_class, 'FLUSH_BUFF_SIZE', 5 * 10000)

        # count of recs in eatch iter chunk, 10w <==> 50~60M+
        iter_chunk_size = getattr(config_class, 'ITER_CHUNK_SIZE', 10 * 10000)

        logger.info(f"[CONF] ITER_CHUNK_SIZE: {iter_chunk_size}")
        logger.info(f"[CONF] FLUSH_BUFF_SIZE: {flush_buff_size}")

        try:
            for db_url in db_urls:
                ret = urlparse(db_url)

                if ret.scheme.lower() not in ('mysql', 'postgresql'):
                    raise ConfigError(f"Currently we only support MySQL and PostgresSQL: {db_url}")

                if ret.scheme.lower() == 'mysql':
                    flusher = _mysql_flusher
                    rec_filter = _mysql_rec_filter
                    db_url = db_url[:5] + '+pymysql' + db_url[5:]
                    db_url += '?' if '?' not in db_url else '&'
                    db_url += 'local_infile=1'
                else:  # postgresql
                    flusher = _postgres_flusher
                    rec_filter = _postgres_rec_filter

                database = ret.path[1:]
                if len(database) == 0:
                    raise ConfigError(f"Missing database name in schema: (db_url)")

                if len(ret.username) == 0:
                    raise ConfigError(f"Missing connection username in schema: (db_url)")

                if len(ret.password) == 0:
                    raise ConfigError(f"Missing connection password in schema: (db_url)")

                if len(ret.hostname) == 0:
                    raise ConfigError(f"Missing connection host in schema: (db_url)")

                if ret.port <= 0 or ret.port >= 65535:
                    raise ConfigError(f"Invalid port {ret.port} in schema: (db_url)")

                self.dbconfigs[database] = {
                    'url': db_url,
                    'tables': set(),
                    'session': None,
                    'port': ret.port,
                    'scheme': ret.scheme,
                    'database': database,
                    'username': ret.username,
                    'password': ret.password,
                    'hostname': ret.hostname,
                    'flusher': flusher,
                    'rec_filter': rec_filter,
                    'tables_sql': None,
                    'columns_sql': None,
                    'data_types': {},
                    'retain_cache': {},
                    'flush_buff_size': flush_buff_size,
                    'iter_chunk_size': iter_chunk_size
                }

                if ret.scheme == 'postgresql':
                    self.dbconfigs[database]['tables_sql'] = """
                        SELECT tablename AS table_name,
                               CONCAT(schemaname, '.', tablename) AS full_table_name
                          FROM pg_tables
                         WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                           AND tablename NOT IN (
                            'alembic_version', 'flyway_schema_history');
                    """
                    self.dbconfigs[database]['columns_sql'] = """
                        SELECT a.attname AS field,
                               CASE WHEN ( t.typname LIKE '%%int%%'
                                   OR t.typname LIKE '%%number%%'
                                   OR t.typname LIKE '%%float%%'
                                   OR t.typname LIKE '%%double%%'
                               ) THEN 'number'
                               WHEN t.typname LIKE '%%char%%' THEN
                                   'varchar'
                               ELSE t.typname END AS type,
                               a.attlen AS length,
                               a.atttypid AS type_id,
                               a.attnotnull AS not_null,
                               t.typinput
                          FROM pg_class c, pg_attribute a, pg_type t
                         WHERE c.relname = '%s'
                           AND a.attnum > 0
                           AND a.attrelid = c.oid
                           AND a.atttypid = t.oid
                      ORDER BY a.attnum ASC;
                    """
                    self.dbconfigs[database]['pkey_sql'] = """
                        SELECT kcu.column_name
                          FROM information_schema.table_constraints AS tco
                          JOIN information_schema.key_column_usage kcu
                            ON kcu.constraint_name = tco.constraint_name
                           AND kcu.constraint_schema = tco.constraint_schema
                           AND kcu.constraint_name = tco.constraint_name
                         WHERE tco.constraint_type = 'PRIMARY KEY'
                           AND kcu.table_schema = 'public'
                           AND kcu.table_name = '%s'
                      ORDER BY kcu.ordinal_position;
                    """
                    self.dbconfigs[database]['data_types'] = {
                        'number': 'factories.randint(0, 10)',
                        'varchar': 'factories.FuzzyText(5)',
                        'bytea': 'factories.FuzzyText(5)',
                        'uuid': 'factories.FuzzyText(5)',
                        # 'uuid': 'factories.FuzzyUuid()',
                        'date': '"2020-05-01"',
                        'time': '"12:00:00"',
                        'year': '"2020"',
                        'datetime': '"2020-05-01 12:00:00"',
                        'timestamp': '"2020-05-01 12:00:00"',
                        'bool': 'True', 'jsonb': '{}', 'array': '{}'
                    }
                elif ret.scheme == 'mysql':
                    self.dbconfigs[database]['tables_sql'] = """
                        SELECT table_name,
                               CONCAT(table_schema, '.', table_name) AS full_table_name
                          FROM information_schema.tables
                         WHERE table_schema = '%s'
                           AND table_name NOT IN ('alembic_version', 'flyway_schema_history');
                    """ % database
                    self.dbconfigs[database]['columns_sql'] = """
                        SELECT c.column_name AS field,
                               c.data_type AS type,
                               IFNULL(c.character_maximum_length, -1) AS length,
                               -1 AS type_id,
                               CASE WHEN c.is_nullable = 'NO' THEN
                                   't' ELSE 'f'
                               END AS not_null,
                               'null' AS typinput
                          FROM information_schema.columns c
                         WHERE c.table_schema = '""" + database + """'
                           AND c.table_name = '%s'
                    """
                    self.dbconfigs[database]['pkey_sql'] = """
                        SELECT sta.column_name
                          FROM information_schema.TABLES AS tab
                    INNER JOIN information_schema.statistics AS sta
                            ON sta.table_schema = tab.table_schema
                           AND sta.table_name = tab.table_name
                           AND sta.index_name = 'primary'
                         WHERE tab.table_schema = '""" + database + """'
                           AND tab.table_name = '%s'
                           AND tab.table_type = 'BASE TABLE'
                       ORDER BY tab.table_name;
                    """
                    self.dbconfigs[database]['data_types'] = {
                        # Integer Types
                        'tinyint': '1',  # 'factories.randint(0, 255)',
                        'smallint': 'factories.randint(0, 255)',
                        'mediumint': 'factories.randint(0, 65536)',
                        'int': 'factories.randint(0, 65536)',
                        'integer': 'factories.randint(0, 65536)',
                        'bigint': 'factories.randint(0, 65536)',
                        'float': 'factories.randint(0, 65536)',
                        'double': 'factories.randint(0, 65536)',
                        'decimal': 'factories.randint(0, 65536)',
                        # Date Types
                        'date': '"2020-05-01"',
                        'time': '"12:00:00"',
                        'year': '"2020"',
                        'datetime': '"2020-05-01 12:00:00"',
                        'timestamp': '"2020-05-01 12:00:00"',
                        # String Types
                        'char': 'factories.FuzzyText(1)',
                        'varchar': 'factories.FuzzyText(5)',
                        'tinyblob': 'factories.FuzzyText()',
                        'tinytext': 'factories.FuzzyText()',
                        'blob': 'factories.FuzzyText()',
                        'text': 'factories.FuzzyText()',
                        'mediumblob': 'factories.FuzzyText()',
                        'mediumtext': 'factories.FuzzyText()',
                        'longblob': 'factories.FuzzyText()',
                        'longtext': 'factories.FuzzyText()',
                        # Object Types
                        'json': '{}', 'array': '[]'
                    }
                else:
                    raise UnsupportError(
                        f"Scheme of (ret.scheme) is not support yet at this moment."
                    )
        except:
            logger.exception(f"Can not parse {db_urls}.")
            raise ConfigError("Can not parse DATABASE_URL, invalid URL schema.")

        return self

    def get_dbconfigs(self):
        return self.dbconfigs
