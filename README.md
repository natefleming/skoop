### Skoop

```
./skoop.sh --help

Usage: skoop.sh --conf [mappings-file] --properties-file [spark-defaults]

  --mappings-conf        The location of a mappings file.
                         If not provided it defaults to the value from the env config file.

```

## Configuration

Mappings files contain source and destination information which is used to copy table from source to destination

# Mapping File

The datasource mappings file used to import tables from db2 into hive has a number of configuration options which aim to
cover the majority of the common decisions made during imports. Below is an exclaimation of the format and parameters
```
# The top level element represents the 'environment' name for the following mappings. 
# There can be be multiple environments per file
ie2:

  # Connections represent logical datasource connections. These are referenced by name from mappings
  # defined below
  connections:

    # This defines the connections 'name'
    db2-mte:
      type: db2
      # Provide an optional class name for an implementation of JdbcDialect
      jdbc-dialect: com.ngc.spark.sql.jdbc.Db2Dialect
      url: jdbc:db2://
      username: [REDACTED]
      password: [REDACTED]
      description: The DB2 Instance

    # The hive entry isnt very interesting at the moment. Spark assumes its configuration is provided
    # from the edge node HDP config files.
    hive-mte:
      type: [hive|hive3]
      description:

  # Mappings define data source and sink which data will be copied to and from
  mappings:

    # The 'defaults' mappings is here for convenience in order to limit the verbosity of the configuration.
    # In the event that a mapping does not provide a value, the value configured from defaults will be used.
    defaults:

      # Defaults for data sources
      source:
        connection: db2-mte
        database: FPS2
        num-connections: 1
        format: jdbc

      # Defaults for data destinations
      destination:
        connection: hive-mte
        database: ie2_testing
        create-backup: True|False
        write-disposition: overwrite|append|error|ignore
        compute-statistics: True|False
        format: orc
        # Specify the number of concurrent writers if the destination connection supports it.
        num-connections: 10

    # This is the 'name' of this mapping entry
    my-mapping:

      # Skip over this mapping
      ignore: True|False 
     
      # The source table name. The database and connection defined in defaults will be used
      source:

        table: mysourcetable

        # Define the number of concurrent connections
        num-connections: 10

        # Define the column used to split on when defining multiple connections.
        # If the number of connections is > 1 and column is not defined as best guess will be made.
        column: my_split_col

        # Defined the upper bound of splits. If not defined the MAX value of 'column' will be used.
        upper-bound: 10000

        # Defined the lower bound of splits. If not defined the MIN value of 'column' will be used.
        lower-bound: 0

	    # Provide a custom schema on read. This can be used to change data types
	    schema: id DECIMAL(38, 0), name STRING

	    # Provide a specific list of columns to include
	    include-columns: [mycol1, mycol2]

	    # Provide a specific list of columns to exclude
	    exclude-columns: [mycol1, mycol2]

      destination:
        table: mydesttable

        # Override the default destination database
        database: non_default_database

        # Defined the format of the resulting table
        format: csv

		# Provide a list of column to create hive partitions for
		partition-by: [mycol1, mycol2]

	# Provide a custom schema on write
	schema: id DECIMAL(38, 0), name STRING

        # Compute statistics for the table once it has been created
        compute-statistics: True|False

```

## Variable Interpolation

Variables can be injected into the configuration file by using ${variable} as any value in the configuration file.

```
For example:

    my-mapping:

      ignore: ${define:should_ignore}
     
      source:
	  	table: my_table_${extension}
```

These value are defined from the command line using the --define arguments
```
	./skoop.sh --conf myconfig.yaml --define should_ignore=True --define extension=bak
```

The same format can be used to reference built-in actions. For doing things like injecting formatted timestamps. 
Actions take the format of ${action:argument}

The argument for the timestamp action is the date format provided in python dialect. 

```
For example:

      source:
	  	table: my_table_${timestamp:%y%m%d%H%M%S%f}
```


## Including Configuration

It is sometimes convenient to externalize parts of the mapping file. For example to support multiple environments you may define connection information for each environment. Then import the appropriate connection into the mappings file at run time.
This can be accomplished by using ```!include``` directive

```
For example:

	# In this example ${d:environment} is provided on the command line using --define environment=[my-env]
	connections: !include ${define:environment}-connnections.yaml
```

## JdbcDialect

Due to the inability for the default implementation of the spark jdbc dialect for DB2 to properly truncate tables, a custom implementation was created.
This code is implemented in Scala and can be built by simply running `mvn` from the top level directory:
```
$> mvn package
```
The jar files produced will be copied by maven into the top level lib/ directory and the run scripts will include them with job submission.


