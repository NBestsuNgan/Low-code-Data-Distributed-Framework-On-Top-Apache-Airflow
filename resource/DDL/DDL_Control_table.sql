-- cntl_af.cntl_cfg_log definition
CREATE TABLE cntl_af.cntl_cfg_log (
	prcs_nm varchar NOT NULL,
	data_dt timestamp NULL,
	start_dt timestamp NOT NULL,
	end_dt timestamp NOT NULL,
	status int4 NOT NULL,
	message text NULL,
	source_row int4 NULL,
	target_row int4 NULL
);

-- cntl_af.cntl_cfg_prcs definition
CREATE TABLE cntl_af.cntl_cfg_prcs (
	prcs_nm varchar NOT NULL,
	prcs_grp varchar NOT NULL,
	prir int4 NOT NULL,
	src_schm_nm varchar NULL,
	src_tbl varchar NULL,
	tgt_schm_nm varchar NOT NULL,
	tgt_tbl varchar NOT NULL,
	prcs_typ int4 NOT NULL,
	af_path_nm varchar NULL,
	af_parm varchar NULL,
	act_f int4 NOT NULL,
	upt_dt timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT cntl_cfg_prcs_pkey PRIMARY KEY (prcs_nm)
);

-- cntl_af.cntl_cfg_prcs_depn definition
CREATE TABLE cntl_af.cntl_cfg_prcs_depn (
	prcs_nm varchar NOT NULL,
	dpnd_prcs_nm varchar NOT NULL,
	act_f int4 NOT NULL,
	upt_dt timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT cntl_cfg_prcs_depn_pkey PRIMARY KEY (prcs_nm, dpnd_prcs_nm)
);

-- cntl_af.cntl_cfg_prcs_grp definition
CREATE TABLE cntl_af.cntl_cfg_prcs_grp (
	prcs_grp varchar NOT NULL,
	strem_nm varchar NOT NULL,
	prir int4 NOT NULL,
	act_f int4 NOT NULL,
	upt_dt timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT cntl_cfg_prcs_grp_pkey PRIMARY KEY (prcs_grp)
);

-- cntl_af.cntl_cfg_schedule definition
CREATE TABLE cntl_af.cntl_cfg_schedule (
	prcs_nm varchar NOT NULL,
	minutes varchar NOT NULL,
	hours varchar NOT NULL,
	day_month varchar NOT NULL,
	months varchar NOT NULL,
	day_week varchar NULL,
	upt_dt timestamp DEFAULT CURRENT_TIMESTAMP NULL
);

-- cntl_af.cntl_cfg_strem definition
CREATE TABLE cntl_af.cntl_cfg_strem (
	strem_nm varchar NOT NULL,
	upt_dt timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	act_f int4 NULL,
	"owner" varchar NULL,
	CONSTRAINT cntl_cfg_strem_pkey PRIMARY KEY (strem_nm)
);