/*
 *      How many members added between each Parliament?
 *      Consider only general election winners. Dump to the screen a list of (Parliament, count) tuples
 *      showing the difference between that Parliament and the previous one sorted ascending by Parliament number.
 */

--load the data from HDFS and define the schema
raw = LOAD '/data2/cl03.csv' USING PigStorage(',') AS
        (       date, type:chararray,   -- The date of the election, The election type (either Gen or B/P for by-election).
                parl:int,                               -- The number of the Parliament session.
                prov:chararray,                 -- Province where the election took place.
                riding:chararray,               -- Electoral district.
                lastname:chararray,     -- Last name of the candidate.
                firstname:chararray,    -- first name of the candidate
                gender:chararray,               -- Gender (F, otherwise either M or '').
                occupation:chararray,   -- Occupation of the candidate.
                party:chararray,                -- Party of the candidate.
                votes:int,                              -- Number of votes received.
                percent:double,                 -- Percentage of total votes.
                elected:int);                   -- Whether the candidate won (Elected = 1, Defeated = 0).

-- Filter the data by election == "Gen"
genElec = FILTER raw BY type == 'Gen';

-- Filter by winners of the general elections
winners = FILTER genElec BY elected == 1;

-- Group the result by parliament
Grpd = GROUP winners BY parl;   -- Now we have have a relation where we have a group by parliament

-- Computing the aggregate function of parliament and count
Smmd = FOREACH Grpd GENERATE ($0), COUNT($1) as clicks;         -- This relation has all the (Parliament, count) tuples

DUMP Smmd;

-- A duplicate of the previous relation
Smmd2 = FOREACH Grpd GENERATE ($0), COUNT($1) as clicks;                -- This relation has all the (Parliament, count) tuples

-- Join the Smmd relation with itself
Jnd = JOIN Smmd BY ($0), Smmd2 BY ($0) + 1;

Result = FOREACH Jnd GENERATE Smmd::group AS Parl, Smmd::clicks, (Smmd::clicks - Smmd2::clicks) AS diff;

Srtd = ORDER Result BY Parl;

STORE Result INTO 'q3';
