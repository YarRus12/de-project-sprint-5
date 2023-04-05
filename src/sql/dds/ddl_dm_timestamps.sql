CREATE TABLE IF NOT EXISTS dds.dm_timestamps (
id serial PRIMARY KEY,
ts timestamp NOT NULL,
year smallint NOT NULL CHECK(((year >= 2022) AND (year < 2500))),
month smallint NOT NULL CHECK(((month >= 1) AND (month <= 12))),
day smallint NOT NULL CHECK(((day >= 1) AND (day <= 31))),
time time NOT NULL,
date date NOT NULL,
special_mark int DEFAULT 0);