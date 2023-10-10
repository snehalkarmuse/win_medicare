CREATE TABLE IF NOT EXISTS public.dim_sales_representative
(
    sales_rep_id bigint NOT NULL,
    sales_rep_name text COLLATE pg_catalog."default",
    region_name text COLLATE pg_catalog."default",
    CONSTRAINT sales_rep_id_pkey PRIMARY KEY (sales_rep_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.dim_sales_representative
    OWNER to postgres;