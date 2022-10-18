create TABLE IF NOT EXISTS stg.bonussystem_users (
     id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
     order_user_id text NOT NULL,
     CONSTRAINT users_pkey PRIMARY KEY (id)
);