text = LOAD '%INPUT%' USING TextLoader() AS (line:chararray);

-- REGISTER 'path/to/udfs.jar'; -- only relevant for manual execution, not from jar

lines = FOREACH text GENERATE LOWER(line) AS line;
clean = FOREACH lines GENERATE REPLACE(line, '[^a-z]', ' ') AS line;

tokens = FOREACH clean GENERATE FLATTEN(TOKENIZE((chararray)$0)) AS word;

tokens2 = FOREACH tokens GENERATE ANAGRAM(word) AS key, word;

grouped = GROUP tokens2 BY key;

STORE grouped INTO '%OUTPUT%';