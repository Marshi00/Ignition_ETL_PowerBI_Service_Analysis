-- Table: public.records

-- DROP TABLE IF EXISTS public.records;

CREATE TABLE IF NOT EXISTS public.records
(
    id SERIAL PRIMARY KEY,
    tag_name text COLLATE pg_catalog."default",
    value double precision,
    t_stamp timestamp without time zone,
    CONSTRAINT records_pkey PRIMARY KEY (id),
    CONSTRAINT unique_tstamp_tag UNIQUE (t_stamp, tag_name)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.records
    OWNER to postgres;