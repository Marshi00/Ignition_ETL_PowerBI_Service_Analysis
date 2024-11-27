-- DROP TABLE IF EXISTS public.tag_details;

CREATE TABLE IF NOT EXISTS public.tag_details
(
    id SERIAL PRIMARY KEY,  -- id is now a serial column, automatically incrementing
    tagname character varying(50) COLLATE pg_catalog."default" NOT NULL,
    description text COLLATE pg_catalog."default",
    unit character varying(20) COLLATE pg_catalog."default",
    path text COLLATE pg_catalog."default" UNIQUE NOT NULL, -- path is now unique and part of the primary key
    type character varying(10) COLLATE pg_catalog."default",  -- type can be AI, DI, etc.
    device character varying(100) COLLATE pg_catalog."default", -- device info
    sensor character varying(100) COLLATE pg_catalog."default", -- sensor info
    CONSTRAINT tag_details_tagname_key UNIQUE (tagname) -- enforcing uniqueness for tagname
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.tag_details
    OWNER to postgres;
