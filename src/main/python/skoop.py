from __future__ import print_function

import argparse
import copy
import logging
import os
import re
import sys
from datetime import datetime

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import types as T

logger = logging.getLogger("skoop")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
log_formatter = logging.Formatter(
    "%(asctime)s %(levelname)s %(filename)s(%(lineno)d) %(message)s"
)
ch.setFormatter(log_formatter)
logger.addHandler(ch)

DEFAULT_WRITE_DISPOSITION = "overwrite"
DEFAULT_READ_DISPOSITION = "failfast"
DEFAULT_SRC_FORMAT = "jdbc"
DEFAULT_DST_FORMAT = "orc"
DEFAULT_NUM_CONNECTIONS = 1
DEFAULT_CREATE_BACKUP = False
DEFAULT_COMPUTE_STATISTICS = False
DEFAULT_CREATE_DATABASE = False
DEFAULT_BACKUP_SUFFIX = "_bak"
WRITE_DISPOSITIONS = ["overwrite", "append", "error", "ignore"]
READ_DISPOSITIONS = ["permissive", "dropmfalformed", "failfast", "ignore"]
DEFAULT_LOG_LEVEL = "WARN"
LOG_LEVELS = [
    "ALL",
    "DEBUG",
    "ERROR",
    "FATAL",
    "INFO",
    "OFF",
    "TRACE",
    "TRACE_INT",
    "WARN",
]


def interpolate_value(value, context):

    if value is None or not isinstance(value, (dict, list, str)):
        return value

    if isinstance(value, dict):
        return dict(
            [
                (interpolate_value(k, context), interpolate_value(v, context))
                for k, v in value.iteritems()
            ]
        )

    if isinstance(value, list):
        return [interpolate_value(v, context) for v in value]

    matcher = PatternMatcher(value)
    while matcher.match("(.*)\${(.*)}(.*)"):
        prefix = matcher.group(1).strip()
        target = matcher.group(2).strip()
        suffix = matcher.group(3).strip()

        if ":" in target:
            components = [t.strip() for t in target.split(":")]
            if len(components) != 2:
                raise ValueError(
                    "Defined actions must be in the form of action:argument"
                )
            action, argument = components
            action = action.lower()
            if action == "timestamp":
                target = context.now.strftime(argument)
            elif action == "d" or action == "define":
                if argument not in context.properties:
                    raise ValueError(
                        "Defined property {0} has not been provided".format(argument)
                    )
                else:
                    target = context.properties.get(argument)
            else:
                raise ValueError("Invalid interoplation action: {0}".format(action))
        elif target in context.properties:
            target = context.properties.get(target)
        else:
            raise ValueError("Defined property ${%s} has not been provided" % target)

        value = prefix + target + suffix

        matcher = PatternMatcher(value)

    return value


class Loader(yaml.SafeLoader):

    _context = None

    def __init__(self, stream):
        self._root = os.path.split(stream.name)[0]
        super(Loader, self).__init__(stream)

    def include(self, node):
        filename = os.path.join(self._root, self.construct_scalar(node))
        if Loader._context:
            filename = interpolate_value(str(filename), Loader._context)
        with open(filename, "r") as f:
            return yaml.load(f, Loader)

    @classmethod
    def set_context(clazz, context):
        clazz._context = context


class PatternMatcher(object):

    def __init__(self, matchstring):
        self.matchstring = matchstring

    def match(self, regexp):
        self.rematch = re.match(regexp, self.matchstring)
        return bool(self.rematch)

    def group(self, i):
        return self.rematch.group(i)


def parse_options(argv):

    class StoreKeyValue(argparse.Action):

        def __call__(self, parser, namespace, values, option_string=None):
            try:
                k, v = values.split("=", 1)
            except ValueError:
                raise argparse.ArgumentError(self, "Format must be key=value")

            items = copy.copy(argparse._ensure_value(namespace, self.dest, {}))
            items[k] = v
            setattr(namespace, self.dest, items)

    parser = argparse.ArgumentParser(argv)
    parser.add_argument(
        "--conf", help="The location of the mappings configuration", required=True
    )
    parser.add_argument(
        "--define",
        help="Define a property in the format of key=value",
        action=StoreKeyValue,
        dest="define",
        required=False,
    )
    parser.add_argument(
        "--log-level",
        help="The log level",
        choices=LOG_LEVELS,
        default=DEFAULT_LOG_LEVEL,
        required=False,
    )
    parser.add_argument(
        "--dry-run",
        help="Perform a dry run",
        action="store_true",
        default=False,
        required=False,
    )

    options, additional = parser.parse_known_args()

    return options


def show_options(options):
    print("show_options")
    print("conf: {0}".format(options.conf))
    print("define: {0}".format(options.define))
    print("log-level: {0}".format(options.log_level))
    print("dry-run: {0}".format(options.dry_run))

    if options.dry_run:
        sys.exit(0)


class ModelBase(object):

    def __init__(self, data, defaults={}):
        self._data = data
        self._defaults = defaults if defaults else {}
        self._description = data.get("description")

    def get_value(self, key, default_value=None):
        value = self.data.get(key, self.defaults.get(key, default_value))
        return value

    @property
    def data(self):
        return self._data

    @property
    def defaults(self):
        return self._defaults

    @property
    def description(self):
        return self._description


class Environments(object):

    @staticmethod
    def from_file(context, filename):
        Loader.add_constructor("!include", Loader.include)
        Loader.set_context(context)
        logger.info("from_config(filename={0})".format(filename))
        config = {}
        with open(filename, "r") as f:
            try:
                config = yaml.load(f, Loader)
            except yaml.YAMLError:
                logger.exception("An exception has occurred loading file".format())

        config = interpolate_value(config, context)
        environments = []
        for name, data in config.iteritems():
            environments.append(Environment(context.spark, name, data))

        return environments


class Context(object):

    def __init__(self, spark, properties):
        self._spark = spark
        self._properties = properties
        self._now = datetime.today()

    @property
    def spark(self):
        return self._spark

    @property
    def properties(self):
        return self._properties

    @property
    def now(self):
        return self._now


class Environment(ModelBase):

    def __init__(self, spark, name, data):
        logger.info("Environment(name={0}, data={1})".format(name, data))
        ModelBase.__init__(self, data, {})
        self._spark = spark
        self._name = name
        self._connections = []
        self._mappings = []
        self._set_connections(data)
        self._set_mappings(data)

    def accept(self, visitor):
        visitor.visit(self)
        for connection in self.connections:
            connection.accept(visitor)
        for mapping in self.mappings:
            mapping.accept(visitor)

    def update(self, visitor):
        pass

    @property
    def connections(self):
        return self._connections

    @property
    def spark(self):
        return self._spark

    @property
    def mappings(self):
        return sorted(self._mappings, key=lambda m: m.ordinal, reverse=True)

    def _set_connections(self, data):
        if "connections" not in data:
            raise ValueError("connections entry is required")
        connection_data = self.get_value("connections")
        connection_factory = ConnectionFactory(self)
        for name, data in connection_data.iteritems():
            connection = connection_factory.create(name, data)
            self._connections.append(connection)

    def _set_mappings(self, data):
        mapping_data = self.get_value("mappings")
        defaults = mapping_data.pop("defaults", {})
        for name, data in mapping_data.iteritems():
            self._mappings.append(Mapping(self, name, data, defaults))

    def __str__(self):
        return "connections=[{0}], mappings=[{1}]".format(
            self.connections, self.mappings
        )


class Mapping(ModelBase):

    def __init__(self, environment, name, data, defaults={}):
        logger.info(
            "Mapping(name={0}, data={1}, defaults={2})".format(name, data, defaults)
        )
        ModelBase.__init__(self, data, defaults)
        self._name = name
        self._source = Source(
            environment, data.get("source"), defaults.get("source", {})
        )
        self._destination = Destination(
            environment, data.get("destination"), defaults.get("destination", {})
        )
        self._ignore = self.get_value("ignore", False)
        self._ordinal = self.get_value("ordinal", 0)

    def accept(self, visitor):
        if not self.ignore:
            self.source.accept(visitor)
            self.destination.accept(visitor)
            visitor.visit(self)

    def update(self, visitor):
        if not self.ignore:
            visitor.commit_task()

    @property
    def name(self):
        return self._name

    @property
    def source(self):
        return self._source

    @property
    def destination(self):
        return self._destination

    @property
    def ignore(self):
        return self._ignore

    @property
    def ordinal(self):
        return int(self._ordinal)

    def __str__(self):
        return "name=[{0}], source=[{1}], destination=[{2}], ignore=[{3}, ordinal={4}]".format(
            self.name, self.source, self.destination, self.ignore, self.ordinal
        )


class Datasource(ModelBase):

    def __init__(self, environment, data, defaults={}):
        ModelBase.__init__(self, data, defaults)
        self._database = self.get_value("database")
        self._table = self.get_value("table")
        connection_name = self.get_value("connection")
        if any(c for c in environment.connections if c.name == connection_name):
            self._connection = next(
                c for c in environment.connections if c.name == connection_name
            )
        else:
            raise ValueError("Invalid connection name: {0}".format(connection_name))

        self._num_connections = self.get_value(
            "num-connections", DEFAULT_NUM_CONNECTIONS
        )

    def accept(self, visitor):
        pass

    def update(self, visitor):
        pass

    @property
    def connection(self):
        return self._connection

    @property
    def database(self):
        return self._database

    @property
    def table(self):
        return self._table

    @property
    def num_connections(self):
        return self._num_connections

    def __str__(self):
        return "Datasource(connection=[{0}], database=[{1}], table=[{2}])".format(
            self.connection, self.database, self.table
        )


class Source(Datasource):

    def __init__(self, environment, data, defaults={}):
        logger.info("Source(data={0}, defaults={1})".format(data, defaults))
        Datasource.__init__(self, environment, data, defaults)
        self._split_column = self.get_value("column")
        self._lower_bound = self.get_value("lower-bound")
        self._upper_bound = self.get_value("upper-bound")
        self._format = self.get_value("format", DEFAULT_SRC_FORMAT)
        self._schema = self.get_value("schema")
        self._query = self.get_value("query")
        self._include_columns = self.get_value("include-columns", [])
        self._exclude_columns = self.get_value("exclude-columns", [])
        self._read_disposition = self.get_value(
            "read-disposition", DEFAULT_READ_DISPOSITION
        )
        self._options = self.get_value("options", {})
        if self._read_disposition.lower() not in READ_DISPOSITIONS:
            raise ValueError(
                "read-disposition must be in {0}".format(READ_DISPOSITIONS)
            )
        if self.query and self.table:
            raise ValueError("query can not be provided with table")
        if not self.query and not self.table and not self.schema:
            raise ValueError("table, query or schema is required")

    def accept(self, visitor):
        visitor.visit(self)

    def update(self, visitor):
        visitor.source(self)

    def read(self):
        reader = self.connection.reader()
        df = reader.read(self)
        return df

    @property
    def split_column(self):
        return self._split_column

    @property
    def lower_bound(self):
        return self._lower_bound

    @property
    def upper_bound(self):
        return self._upper_bound

    @property
    def format(self):
        return self._format

    @property
    def schema(self):
        return self._schema

    @property
    def query(self):
        return self._query

    @property
    def read_disposition(self):
        return self._read_disposition

    @property
    def include_columns(self):
        return [c.strip().lower() for c in self._include_columns]

    @property
    def exclude_columns(self):
        return [c.strip().lower() for c in self._exclude_columns]

    @property
    def options(self):
        return self._options


class Destination(Datasource):

    def __init__(self, environment, data, defaults={}):
        logger.info("Destination(data={0}, defaults={1})".format(data, defaults))
        Datasource.__init__(self, environment, data, defaults)
        if not self.database:
            raise ValueError("Database is required")
        if not self.table:
            raise ValueError("Table is required")
        self._create_backup = self.get_value("create-backup", DEFAULT_CREATE_BACKUP)
        self._write_disposition = self.get_value(
            "write-disposition", DEFAULT_WRITE_DISPOSITION
        )
        if self._write_disposition.lower() not in WRITE_DISPOSITIONS:
            raise ValueError(
                "write-disposition must be in {0}".format(WRITE_DISPOSITIONS)
            )
        self._format = self.get_value("format", DEFAULT_DST_FORMAT)
        self._partition_by = self.get_value("partition-by", [])
        self._schema = self.get_value("schema")
        self._create_database = self.get_value(
            "create-database", DEFAULT_CREATE_DATABASE
        )
        self._compute_statistics = self.get_value(
            "compute-statistics", DEFAULT_COMPUTE_STATISTICS
        )
        self._options = self.get_value("options", {})

    def accept(self, visitor):
        visitor.visit(self)

    def update(self, visitor):
        visitor.destination(self)

    def write(self, df):
        writer = self.connection.writer()
        writer.write(df, self)

    @property
    def create_backup(self):
        return str(self._create_backup).lower() == str(True).lower()

    @property
    def write_disposition(self):
        return self._write_disposition

    @property
    def format(self):
        return self._format

    @property
    def partition_by(self):
        return self._partition_by

    @property
    def schema(self):
        return self._schema

    @property
    def create_database(self):
        return str(self._create_database).lower() == str(True).lower()

    @property
    def compute_statistics(self):
        return str(self._compute_statistics).lower() == str(True).lower()

    @property
    def options(self):
        return self._options


class DatasourceReader(object):

    def __init__(self, spark):
        self._spark = spark

    @property
    def spark(self):
        return self._spark


class DatasourceWriter(object):

    def __init__(self, spark):
        self._spark = spark

    @property
    def spark(self):
        return self._spark


class Db2Reader(DatasourceReader):

    def __init__(self, spark):
        DatasourceReader.__init__(self, spark)

    def read(self, source):
        logger.info("Db2Reader.read(source={0})".format(source))

        schema = self._load_schema(source)
        query = self._build_query(source, schema)
        reader = self._jdbc_reader(source, query)

        logger.info("count={0}".format(self._count(source, query)))

        if source.num_connections > 1:
            column = self._find_id_column(source, schema)
            if column:
                upper_bound = self._find_upper_bound(source, column)
                lower_bound = self._find_lower_bound(source, column)
                reader = (
                    reader.option("numPartitions", source.num_connections)
                    .option("partitionColumn", column)
                    .option("lowerBound", lower_bound)
                    .option("upperBound", upper_bound)
                )

        if source.connection.driver:
            reader = reader.option("driver", source.connection.driver)

        if source.schema:
            reader = reader.option("customSchema", source.schema)

        df = reader.load()

        return df

    def _build_query(self, source, schema):
        if source.query:
            query = source.query
        else:
            columns = [f.name for f in schema.fields]
            if source.include_columns:
                columns = [c for c in columns if c.lower() in source.include_columns]
            if source.exclude_columns:
                columns = [
                    c for c in columns if c.lower() not in source.exclude_columns
                ]
            query = "SELECT {0} FROM {1}.{2}".format(
                ",".join(columns), source.database, source.table
            )
        return query

    def _load_schema(self, source):
        if source.query:
            query = "SELECT * FROM ({0}) a WHERE 1 = 0".format(source.query)
        else:
            query = "SELECT * FROM {0}.{1} WHERE 1 = 0".format(
                source.database, source.table
            )
        df = self._jdbc_reader(source, query).load()
        df.printSchema()
        return df.schema

    def _find_id_column(self, source, schema):
        if source.split_column:
            return source.split_column

        def rchop(s, suffix):
            result = s
            if s.endswith(suffix):
                result = s[: -len(suffix)]
            return result

        possible_ids = [
            f.name
            for f in schema.fields
            if rchop(f.name.lower(), "_id") in source.table
        ]
        if possible_ids:
            possible_ids = [max(possible_ids, key=len)]
            logger.info("derived_from_name={0}".format(possible_ids))
        else:
            first_column = schema.fields[0]
            if isinstance(
                first_column.dataType, T.IntegralType
            ) and first_column.name.endswith("_id"):
                possible_ids = [first_column.name]
                logger.info("derived_from_first_column={0}".format(possible_ids))

        column = possible_ids[0] if possible_ids else None
        logger.info("column={0}".format(column))
        return column

    def _count(self, source, query):
        query = "SELECT COUNT(1) AS count FROM ({0})".format(query)
        result_set = self._jdbc_reader(source, query).load().collect()
        count = result_set[0].COUNT if len(result_set) else 0
        return count

    def _find_upper_bound(self, source, column):
        logger.info(
            "find_upper_bound(split_column={0},src={1})".format(column, source.table)
        )
        if source.upper_bound:
            return source.upper_bound
        if source.query:
            query = "SELECT MAX({0}) AS id FROM ({1})".format(column, source.query)
        else:
            query = "SELECT MAX({0}) AS id FROM {1}.{2}".format(
                column, source.database, source.table
            )
        result_set = self._jdbc_reader(source, query).load().collect()
        upper_bound = result_set[0].ID if len(result_set) else 0
        logger.info("upper_bound: {0}".format(upper_bound))
        return upper_bound

    def _find_lower_bound(self, source, column):
        logger.info(
            "find_lower_bound(split_column={0},src={1})".format(column, source.table)
        )
        if source.lower_bound:
            return source.lower_bound
        if source.query:
            query = "SELECT MIN({0}) AS id FROM ({1})".format(column, source.query)
        else:
            query = "SELECT MIN({0}) AS id FROM {1}.{2}".format(
                column, source.database, source.table
            )
        result_set = self._jdbc_reader(source, query).load().collect()
        lower_bound = result_set[0].ID if len(result_set) else 0
        logger.info("lower_bound: {0}".format(lower_bound))
        return lower_bound

    def _jdbc_reader(self, source, query):
        logger.info("_jdbc_reader(query={0})".format(query))
        query = "({0}) AS tmp".format(query)
        logger.info("query={0}".format(query))
        reader = (
            self.spark.read.format("jdbc")
            .option("url", source.connection.url)
            .option("dbtable", query)
            .option("user", source.connection.username)
            .option("password", source.connection.password)
        )
        return reader


class Db2Writer(DatasourceWriter):

    def __init__(self, spark):
        DatasourceWriter.__init__(self, spark)

    def write(self, df, destination):
        logger.info("Db2Writer.write(destination={0})".format(destination))

        destination.connection.register_dialect(self.spark)

        table = "{0}.{1}".format(destination.database, destination.table)
        writer = (
            df.write.format("jdbc")
            .option("url", destination.connection.url)
            .option("dbtable", table)
            .option("user", destination.connection.username)
            .option("password", destination.connection.password)
        )

        writer = writer.mode(destination.write_disposition)
        if destination.num_connections > 1:
            writer = writer.option("numPartitions", destination.num_connections)

        if destination.connection.driver:
            writer = writer.option("driver", destination.connection.driver)

        if destination.schema:
            writer = writer.option("createTableColumnTypes", destination.schema)

        if destination.options:
            for key, value in destination.options.iteritems():
                writer = writer.option(key, value)

        writer.save()

        destination.connection.unregister_dialect(self.spark)


class HiveReader(DatasourceReader):

    def __init__(self, spark):
        DatasourceReader.__init__(self, spark)

    def read(self, source):
        logger.info("HiveReader.read(source={0})".format(source))
        query = (
            source.query
            if source.query
            else "SELECT * FROM {0}.{1}".format(source.database, source.table)
        )
        df = self.spark.sql(query)
        return df


class NullReader(DatasourceReader):

    def __init__(self, spark):
        DatasourceReader.__init__(self, spark)

    def read(self, source):
        logger.info("NullReader.read(source={0})".format(source))
        if not source.schema:
            raise ValueError("schema is required")

        schema = T.StructType()
        columns = [s.strip() for s in source.schema.split(",")]
        for column in columns:
            column_info = column.split()
            column_name = column_info[0]
            column_type = column_info[1] if len(column_info) > 1 else "string"
            schema.add(column_name, column_type, True)

        logger.info("schema={0}".format(schema))
        df = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema)
        return df


class Hive3Reader(DatasourceReader):

    def __init__(self, spark):
        DatasourceReader.__init__(self, spark)

    def read(self, source):
        logger.info("Hive3Reader.read(source={0})".format(source))
        query = (
            source.query
            if source.query
            else "SELECT * FROM {0}.{1}".format(source.database, source.table)
        )
        from pyspark_llap import HiveWarehouseSession

        hive = HiveWarehouseSession.session(self.spark).build()
        df = hive.executeQuery(query)
        return df


class Hive3Writer(DatasourceWriter):

    def __init__(self, spark):
        DatasourceWriter.__init__(self, spark)

    def write(self, df, destination):
        logger.info("Hive3Writer.write(destination={0})".format(destination))
        from pyspark_llap import HiveWarehouseSession

        hive = HiveWarehouseSession.session(self.spark).build()
        if destination.create_database:
            hive.createDatabase(destination.database, True)
        dst = "{0}.{1}".format(destination.database, destination.table)
        if destination.create_backup:
            if self._table_exists(hive, destination):
                backup_table = "{0}{1}".format(dst, DEFAULT_BACKUP_SUFFIX)
                hive.dropTable(backup_table, True, True)
                hive.executeUpdate(
                    "ALTER TABLE {0} RENAME TO {1}".format(dst, backup_table)
                )

        writer = (
            df.write.mode(destination.write_disposition)
            .format(HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR)
            .option("table", dst)
        )

        if destination.options:
            for key, value in destination.options.iteritems():
                writer = writer.option(key, value)

        if destination.partition_by:
            writer = writer.partitionBy(*destination.partition_by)

        writer.save()

        if destination.compute_statistics:
            hive.executeUpdate("ANALYZE TABLE {0} COMPUTE STATISTICS".format(dst))

    def _table_exists(self, hive, destination):
        hive.setDatabase(destination.database)
        tables = hive.showTables().collect()
        logger.info("tables={0}".format(tables))
        found = any(tbl.tab_name == destination.table for tbl in tables)
        logger.debug(
            "Table {0}.{1}: {2}".format(
                destination.database,
                destination.table,
                "Found" if found else "Not Found",
            )
        )
        return found


class HiveWriter(DatasourceWriter):

    def __init__(self, spark):
        DatasourceWriter.__init__(self, spark)

    def write(self, df, destination):
        logger.info("HiveWriter.write(destination={0})".format(destination))
        if destination.create_database:
            self.spark.sql(
                "CREATE DATABASE IF NOT EXISTS {0}".format(destination.database)
            )
        dst = "{0}.{1}".format(destination.database, destination.table)
        if destination.create_backup:
            if self._table_exists(destination):
                backup_table = "{0}{1}".format(dst, DEFAULT_BACKUP_SUFFIX)
                self.spark.sql("DROP TABLE IF EXISTS {0}".format(backup_table))
                self.spark.sql(
                    "ALTER TABLE {0} RENAME TO {1}".format(dst, backup_table)
                )

        writer = df.write.mode(destination.write_disposition).format(destination.format)

        if destination.options:
            for key, value in destination.options.iteritems:
                writer = writer.option(key, value)

        writer.saveAsTable(dst)

        if destination.compute_statistics:
            self.spark.sql("ANALYZE TABLE {0} COMPUTE STATISTICS".format(dst))

    def _table_exists(self, destination):
        tables = self.spark.catalog.listTables(destination.database)
        found = any(tbl.name == destination.table for tbl in tables)
        logger.debug(
            "Table {0}.{1}: {2}".format(
                destination.database,
                destination.table,
                "Found" if found else "Not Found",
            )
        )
        return found


class ConnectionFactory(object):

    def __init__(self, environment):
        self._environment = environment

    def create(self, name, data):
        connection_type = data.get("type")
        if not connection_type:
            raise ValueError("A connection type is required")

        connection = {
            "db2": Db2Connection(self._environment, name, data),
            "hive": HiveConnection(self._environment, name, data),
            "hive3": Hive3Connection(self._environment, name, data),
        }.get(connection_type.lower(), NullConnection(self._environment, name, data))

        return connection


class Connection(ModelBase):

    def __init__(self, environment, name, data):
        ModelBase.__init__(self, data)
        self._environment = environment
        self._name = name
        self._type = data.get("type")
        if not self._type:
            raise ValueError("A connection type is required")

    def accept(self, visitor):
        visitor.visit(self)

    def update(self, visitor):
        pass

    def reader(self):
        pass

    def writer(self):
        pass

    @property
    def environment(self):
        return self._environment

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type


class NullConnection(Connection):

    def __init__(self, environment, name, data):
        Connection.__init__(self, environment, name, data)
        logger.info("NullConnection(name={0}, data={1})".format(name, data))

    def reader(self):
        return NullReader(self.environment.spark)

    def writer(self):
        pass


class JdbcConnection(Connection):

    def __init__(self, environment, name, data):
        Connection.__init__(self, environment, name, data)
        self._url = data.get("url")
        self._username = data.get("username")
        self._password = data.get("password")
        self._driver = data.get("driver")
        self._jdbc_dialect_class = data.get("jdbc-dialect")
        self._jdbc_dialect = None
        if not self._type:
            raise ValueError("A connection type is required")

    def accept(self, visitor):
        pass

    def update(self, visitor):
        pass

    def reader(self):
        pass

    def writer(self):
        pass

    @property
    def name(self):
        return self._name

    @property
    def url(self):
        return self._url

    @property
    def username(self):
        return self._username

    @property
    def password(self):
        return self._password

    @property
    def driver(self):
        return self._driver

    @property
    def type(self):
        return self._type

    @property
    def jdbc_dialect(self):
        return self._jdbc_dialect_class

    def register_dialect(self, spark):
        if not self._jdbc_dialect_class:
            return
        from py4j.java_gateway import java_import

        gw = spark.sparkContext._gateway
        java_import(gw.jvm, self._jdbc_dialect_class)
        logger.info("register_dialect(dialect={0})".format(self._jdbc_dialect_class))
        self._jdbc_dialect = gw.jvm.java.lang.Class.forName(
            self._jdbc_dialect_class
        ).newInstance()
        gw.jvm.org.apache.spark.sql.jdbc.JdbcDialects.registerDialect(
            self._jdbc_dialect
        )

    def unregister_dialect(self, spark):
        if not self.jdbc_dialect:
            return
        gw = spark.sparkContext._gateway
        logger.info("unregister_dialect(dialect={0})".format(self._jdbc_dialect_class))
        gw.jvm.org.apache.spark.sql.jdbc.JdbcDialects.unregisterDialect(
            self._jdbc_dialect
        )


class Db2Connection(JdbcConnection):

    def __init__(self, environment, name, data):
        JdbcConnection.__init__(self, environment, name, data)
        logger.info("Db2Connection(name={0}, data={1})".format(name, data))

    def reader(self):
        return Db2Reader(self.environment.spark)

    def writer(self):
        return Db2Writer(self.environment.spark)


class HadoopConnection(Connection):

    def __init__(self, environment, name, data):
        Connection.__init__(self, environment, name, data)
        self._username = data.get("username")
        if self._username:
            os.environ["HADOOP_USER_NAME"] = self._username

    @property
    def username(self):
        return self._username


class HiveConnection(HadoopConnection):

    def __init__(self, environment, name, data):
        HadoopConnection.__init__(self, environment, name, data)
        logger.info("HiveConnection(name={0}, data={1})".format(name, data))

    def reader(self):
        return HiveReader(self.environment.spark)

    def writer(self):
        return HiveWriter(self.environment.spark)


class Hive3Connection(HadoopConnection):

    def __init__(self, environment, name, data):
        HadoopConnection.__init__(self, environment, name, data)
        logger.info("Hive3Connection(name={0}, data={1})".format(name, data))

    def reader(self):
        return Hive3Reader(self.environment.spark)

    def writer(self):
        return Hive3Writer(self.environment.spark)


class SparkVisitor(object):

    def __init__(self):
        self._tasks = []
        self._current_task = SparkVisitor.Task()

    def visit(self, visitable):
        visitable.update(self)

    def commit_task(self):
        self._tasks.append(copy.copy(self._current_task))
        self._current_task = SparkVisitor.Task()

    def execute(self):
        for task in self._tasks:
            task.execute()

    def source(self, value):
        self._current_task.source = value

    def destination(self, value):
        self._current_task.destination = value

    def __str__(self):
        return "{0}".format(self._tasks)

    class Task(object):

        def __init__(self):
            self.source = None
            self.destination = None

        def __str__(self):
            return "source=[{0}], destination=[{1}]".format(
                self.source, self.destination
            )

        def execute(self):
            df = self.source.read()
            self.destination.write(df)


def create_session(options):
    app_name = "skoop:{0}".format(os.path.basename(options.conf))
    logger.info("app_name={0}".format(app_name))
    spark = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel(options.log_level)

    return spark


def main(argv):
    logger.info("main(argv={0})".format(argv))
    options = parse_options(argv)
    show_options(options)

    spark = create_session(options)
    context = Context(spark, options.define)
    environments = Environments.from_file(context, options.conf)
    for environment in environments:
        visitor = SparkVisitor()
        environment.accept(visitor)
        visitor.execute()

    spark.stop()


if __name__ == "__main__":
    main(sys.argv)
