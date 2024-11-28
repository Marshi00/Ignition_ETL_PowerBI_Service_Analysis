-- Table: public.tag_details

-- DROP TABLE IF EXISTS public.tag_details;

CREATE TABLE IF NOT EXISTS public.tag_details
(
    id integer NOT NULL DEFAULT nextval('tag_details_id_seq'::regclass),
    tagname character varying(50) COLLATE pg_catalog."default" NOT NULL,
    description text COLLATE pg_catalog."default",
    unit character varying(20) COLLATE pg_catalog."default",
    path text COLLATE pg_catalog."default" NOT NULL,
    type character varying(10) COLLATE pg_catalog."default",
    device character varying(100) COLLATE pg_catalog."default",
    location character varying(100) COLLATE pg_catalog."default",
    CONSTRAINT tag_details_pkey PRIMARY KEY (id),
    CONSTRAINT tag_details_path_key UNIQUE (path),
    CONSTRAINT tag_details_tagname_key UNIQUE (tagname)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.tag_details
    OWNER to postgres;