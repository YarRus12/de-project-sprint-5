CREATE TABLE IF NOT EXISTS dds.dm_users
(
id serial UNIQUE NOT NULL,
user_id varchar NOT NULL,
user_name  varchar NOT NULL,
user_login  varchar NOT NULL UNIQUE);