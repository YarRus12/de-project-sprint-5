create TABLE IF NOT EXISTS stg.bonussystem_events (
     id int4 NOT NULL,
     event_ts timestamp NOT NULL,
     event_type varchar NOT NULL,
     event_value text NOT NULL,
     CONSTRAINT outbox_pkey PRIMARY KEY (id)
);
create index IF NOT EXISTS idx_bonussystem_events__event_ts on stg.bonussystem_events USING btree (event_ts);