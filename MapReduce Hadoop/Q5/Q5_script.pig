raw = LOAD '/data2/emp.csv' USING PigStorage(',') AS (empid:int, fname:chararray, lname:chararray, deptname:chararray, isManager:chararray, mgrid:int, salary:int);
managers = filter raw by isManager=='Y' and deptname=='Finance';
managersSelected = FOREACH managers GENERATE empid as id, lname as myname;
jn = join managersSelected by id, raw by mgrid;
grp = group jn by (id,myname);
dist = DISTINCT grp;
resu = FOREACH dist generate flatten(group), COUNT($1);
dump resu;
STORE resu INTO 'q5';