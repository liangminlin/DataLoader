from urllib.parse import urlparse

from dataloader import logging
from dataloader.error import ConfigError, UnsupportError

logger = logging.getLogger(__name__)

# count of recs in eatch iter chunk, 10w recs <==> 60M+
ITER_CHUNK_SIZE = 10 * 10000  # 10w

# flush buff size, count of recs
FLUSH_BUFF_SIZE = 5 * 10000   # 5w


class Configuration(object):
    def __init__(self, config_class):
        self.dbconfigs = Parser().parse(config_class).get_dbconfigs()


class Parser(object):
    def __init__(self):
        self.dbconfigs = {}

        logger.info(f"[CONF] ITER_CHUNK_SIZE: {ITER_CHUNK_SIZE}")
        logger.info(f"[CONF] FLUSH_BUFF_SIZE: {FLUSH_BUFF_SIZE}")

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

        logger.debug("[CONF] user configuration of database urls:\n %s", db_urls)

        try:
            for db_url in db_urls:
                ret = urlparse(db_url)

                if ret.scheme.lower() not in (
                    'mysql', 'mysql+mysqlconnector', 'postgresql'
                ):
                    raise ConfigError(f"Currently we only support MySQL and PostgresSQL: {db_url}")

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
                    'hostname': ret.hostname
                }

                if ret.scheme == 'postgresql':
                    self.dbconfigs[database]['tables_sql'] = """
                        SELECT tablename AS table_name,
                               CONCAT(schemaname, '.', tablename) AS full_table_name
                          FROM pg_tables
                         WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                           AND tablename NOT IN ('alembic_version');
                    """
                    self.dbconfigs[database]['columns_sql'] = """
                        SELECT a.attname AS field,
                               t.typinput AS type,
                               a.attlen AS length,
                               a.atttypid AS type_id,
                               a.attnotnull AS not_null
                          FROM pg_class c, pg_attribute a, pg_type t
                         WHERE c.relname = '%s'
                           AND a.attnum > 0
                           AND a.attrelid = c.oid
                           AND a.atttypid = t.oid
                      ORDER BY a.attnum ASC;
                    """
                elif ret.scheme in ('mysql', 'mysql+mysqlconnector'):
                    self.dbconfigs[database]['tables_sql'] = """
                        SELECT table_name,
                               CONCAT(table_schema, '.', table_name) AS full_table_name
                          FROM information_schema.tables
                         WHERE table_schema = '%s'
                    """ % database
                    self.dbconfigs[database]['columns_sql'] = """
                        SELECT c.column_name AS field,
                               c.data_type AS type,
                               IFNULL(c.character_maximum_length, -1) AS length,
                               -1 AS type_id,
                               CASE WHEN c.is_nullable = 'NO' THEN
                                   't' ELSE 'f'
                               END AS not_null
                          FROM information_schema.columns c
                         WHERE c.table_schema = '""" + database + """'
                           AND c.table_name = '%s'
                    """
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
