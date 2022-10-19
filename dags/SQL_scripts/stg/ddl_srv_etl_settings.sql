create TABLE IF NOT EXISTS stg.srv_etl_settings (
	id serial NOT NULL,
	workflow_key varchar NULL,
	workflow_settings text NULL,
	CONSTRAINT srv_etl_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_etl_settings_workflow_settings_key UNIQUE (workflow_settings)
);