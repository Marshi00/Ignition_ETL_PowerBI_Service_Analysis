-- Table: public.records

-- DROP TABLE IF EXISTS public.records;

CREATE TABLE IF NOT EXISTS public.records
(
    id integer NOT NULL DEFAULT nextval('records_id_seq'::regclass),
    ignition_path text COLLATE pg_catalog."default",
    value double precision,
    t_stamp timestamp without time zone,
    CONSTRAINT records_pkey PRIMARY KEY (id),
    CONSTRAINT unique_tstamp_tag UNIQUE (t_stamp, ignition_path)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.records
    OWNER to postgres;