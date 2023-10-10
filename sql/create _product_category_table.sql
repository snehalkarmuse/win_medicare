-- Table: public.dim_product_category

-- DROP TABLE IF EXISTS public.dim_product_category;

CREATE TABLE IF NOT EXISTS public.dim_product_category
(
    product_code character varying COLLATE pg_catalog."default",
    product_category character varying COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.dim_product_category
    OWNER to postgres;