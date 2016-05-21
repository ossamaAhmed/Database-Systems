--load the data from HDFS and define the schema
raw = LOAD '/data2/cl03.csv' USING PigStorage(',') AS  (date, type:chararray, parl:int, prov:chararray, riding:chararray, lastname:chararray, firstname:chararray, gender:chararray, occupation:chararray, party:chararray, votes:int, percent:double, elected:int);

fltrd = FILTER raw by percent >= 60;

--since the distinct operator cannot be applied to a subset of fields, it is exeucted before the projection
distnct = DISTINCT fltrd;

--project only the needed fields
gen = foreach distnct generate CONCAT(firstname, CONCAT(' ', lastname));

STORE gen INTO 'q1';
