-- Table: public.script_run_info

-- DROP TABLE IF EXISTS public.script_run_info;

CREATE TABLE IF NOT EXISTS public.script_run_info
(
    path text COLLATE pg_catalog."default",
    start_time timestamp without time zone,
    finish_time timestamp without time zone,
    status text COLLATE pg_catalog."default",
    details text COLLATE pg_catalog."default",
    start_date timestamp without time zone,
    end_date timestamp without time zone
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.script_run_info
    OWNER to postgres;