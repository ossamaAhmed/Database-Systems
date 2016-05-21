SET default_parallel 4;

raw = LOAD '/data2/cl03.csv' USING PigStorage(',') AS  (date, type:chararray, parl:int, prov:chararray, riding:chararray, lastname:chararray, firstname:chararray, gender:chararray, occupation:chararray, party:chararray, votes:int, percent:double, elected:int);
fltrd = FILTER raw by votes >= 100;
won = FILTER fltrd by elected == 1;
lost = FILTER fltrd by elected == 0;
joint  = JOIN won BY (parl,prov,riding), lost BY (parl,prov,riding);
gen = foreach joint generate won::lastname,lost::lastname,ABS(won::votes-lost::votes) AS diff;
results = FILTER gen BY diff <  10;
STORE results INTO 'q2';