
-- Table: public.dim_products

-- DROP TABLE IF EXISTS public.dim_products;

CREATE TABLE IF NOT EXISTS public.dim_products
(
    product_id serial,
    product_name "varchar" NOT NULL,
    product_category "varchar",
    CONSTRAINT dim_products_pkey PRIMARY KEY (product_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.dim_products
    OWNER to postgres;