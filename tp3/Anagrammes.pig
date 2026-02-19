text = LOAD '/user/hadoop/gutenberg' USING TextLoader() AS (line:chararray);

REGISTER './target/tp3.jar';

lines = FOREACH text GENERATE LOWER(line) AS line;
clean = FOREACH lines GENERATE REPLACE(line, '[^a-z]', ' ') AS line;

tokens = FOREACH clean GENERATE FLATTEN(TOKENIZE((chararray)$0)) AS word;

tokens2 = FOREACH tokens GENERATE ANAGRAM(word) AS key, word;

grouped = GROUP tokens2 BY key;

STORE grouped INTO '%OUTPUT%';

/* -- résultat alternatif avec jonction des mots en une seule chaîne de charactères
final = FOREACH grouped GENERATE key, StrJoin(tokens2) AS words;

STORE final INTO '%OUTPUT%';
*/