CREATE TABLE IF NOT EXISTS public.dim_region
(
    region_id integer NOT NULL,
    region_name "char",
    state "char",
    CONSTRAINT dim_region_pkey PRIMARY KEY (region_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.dim_region
    OWNER to postgres;