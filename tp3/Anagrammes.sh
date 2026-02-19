#!/usr/bin/env bash

if [ -z "$CLASSPATH" ] ; then
  export CLASSPATH="$(hadoop classpath):$PIG_HOME/lib:$CLASSPATH:$JAVA_HOME";
else
  export CLASSPATH="$(hadoop classpath):$PIG_HOME/lib:$JAVA_HOME";
fi

2>&1 mvn package >/dev/null

if [ "$?" != "0" ] ; then
  echo "Maven package error, run 'mvn package -q' for more information" >&2;
  exit 1;
fi

chmod +x target/tp3-jar-with-dependencies.jar;
hdfs dfs -put "/user/hadoop/gutenberg";
java -cp "target/tp3-jar-with-dependencies.jar$CLASSPATH" Anagrammes.class "/user/hadoop/gutenberg" "/user/hadoop/Anagrammes-pig-out";
