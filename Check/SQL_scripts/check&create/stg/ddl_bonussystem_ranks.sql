create TABLE IF NOT EXISTS stg.bonussystem_ranks(
	id int4 NOT NULL,
     "name" varchar(2048) NOT NULL,
     bonus_percent numeric(19, 5) NOT NULL DEFAULT 0,
     min_payment_threshold numeric(19, 5) NOT NULL DEFAULT 0,
     CONSTRAINT ranks_bonus_percent_check CHECK ((bonus_percent >= (0)::numeric)),
     CONSTRAINT ranks_bonus_percent_check1 CHECK ((bonus_percent >= (0)::numeric)),
     CONSTRAINT ranks_pkey PRIMARY KEY (id)
);