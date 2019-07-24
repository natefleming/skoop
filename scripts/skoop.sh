#!/bin/bash

export SPARK_MAJOR_VERSION=2
if command -v /opt/fps2/anaconda2_python/anaconda2/bin/python2; then
	export PYSPARK_PYTHON=/opt/fps2/anaconda2_python/anaconda2/bin/python2
	export PYSPARK_DRIVER_PYTHON=/opt/fps2/anaconda2_python/anaconda2/bin/python2
fi

CUR_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

SRC_DIR=$CUR_DIR/../src/main/python
LIB_DIR=$CUR_DIR/../lib
CONF_DIR=$CUR_DIR/../conf
HWC_DIR=/usr/hdp/current/hive_warehouse_connector/

BIN=$SRC_DIR/skoop.py
SPARK=/usr/bin/spark-submit
ADDITIONAL_CLASSPATH=$(find "$LIB_DIR" -name '*.jar' | xargs echo | tr ' ' ':')

if [ -d "$HWC_DIR" ]; then
	LIB_DIR="$LIB_DIR $HWC_DIR"
fi
JARS=$(find $LIB_DIR -name '*.jar' | xargs echo | tr ' ' ',')
if [[ ! -z "$JARS" ]]; then
	JARS="--jars $JARS"
fi
PY_FILES=$(find $LIB_DIR -name '*.zip' | xargs echo | tr ' ' ',')
if [[ ! -z "$PY_FILES" ]]; then
	PY_FILES="--py-files $PY_FILES"
fi
CONFIG_FILES=$(find "$CONF_DIR" -name '*.yaml' | xargs echo | tr ' ' ',')
if [[ ! -z "$CONFIG_FILES" ]]; then
	CONFIG_FILES="--files $CONFIG_FILES"
fi


function usage {
	cat <<EOF
Usage: $(basename $0) --conf [filename] --properties-file [filename]

  --conf                   The location of a mappings file. 
  --properties-file        The location of the spark-defaults file

EOF
}

function parse_args {
	while [ "${1:-}" != "" ]; do
		case "$1" in
				"--properties-file")
				properties_file="--properties-file $2"
				shift
				;;
				"--conf")
				conf="--conf $2"
				shift
				;;
				"-h" | "--help")
				has_help=1
				additional_args+=("$1")
				;;
				*)
				additional_args+=("$1")
				;;
		esac
    	shift
	done
	if [[ -z ${conf+x} ]]; then 
		usage $(basename $0)
		exit 1
	elif [[ -n "$has_help" ]]; then
		usage $(basename $0)
	fi
}

parse_args $@

echo $SPARK $properties_file $CONF_FILE $JARS $PY_FILES $CONFIG_FILES --driver-class-path $ADDITIONAL_CLASSPATH --conf spark.executor.extraClassPath=$ADDITIONAL_CLASSPATH $BIN $conf ${additional_args[*]} 
$SPARK $properties_file $CONF_FILE $JARS $PY_FILES $CONFIG_FILES --driver-class-path $ADDITIONAL_CLASSPATH --conf spark.executor.extraClassPath=$ADDITIONAL_CLASSPATH $BIN $conf ${additional_args[*]} 
exit_code=$?
if [ $exit_code -ne 0 ]; then
	echo "An exception has occurred running $BIN"
	exit $exit_code
fi
