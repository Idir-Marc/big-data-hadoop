text = LOAD '/user/hadoop/gutenberg' USING TextLoader() AS (line:chararray);

REGISTER './target/tp3.jar';

lines = FOREACH text GENERATE LOWER(line) AS line;
clean = FOREACH lines GENERATE REPLACE(line, '[^a-z]', ' ') AS line;

tokens = FOREACH clean GENERATE FLATTEN(TOKENIZE((chararray)$0)) AS word;

grouped = GROUP tokens BY word;

counted = FOREACH grouped GENERATE COUNT(tokens), group;

STORE counted INTO '/user/hadoop/gutenberg-pig-out';