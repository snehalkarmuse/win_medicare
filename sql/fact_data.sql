-- Table: public.fact_data

-- DROP TABLE IF EXISTS public.fact_data;

CREATE TABLE IF NOT EXISTS public.fact_data
(
    data_id serial,
    product_id varchar,
    region_id integer,
    sales_rep_id integer,
    month character varying(15) COLLATE pg_catalog."default",
    year character varying(4) COLLATE pg_catalog."default",
	unit_price double precision,
    total_price double precision,
    quantity integer,
    CONSTRAINT fact_data_pkey PRIMARY KEY (data_id),
    CONSTRAINT product_id_fkey FOREIGN KEY (product_id)
        REFERENCES public.dim_products (product_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT region_id_fkey FOREIGN KEY (region_id)
        REFERENCES public.dim_region (region_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT sales_rep_id_fkey FOREIGN KEY (sales_rep_id)
        REFERENCES public.dim_sales_representative (sales_rep_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.fact_data
    OWNER to postgres;
-- Index: fki_product_id_fkey

-- DROP INDEX IF EXISTS public.fki_product_id_fkey;

CREATE INDEX IF NOT EXISTS fki_product_id_fkey
    ON public.fact_data USING btree
    (product_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: fki_region_id_fkey

-- DROP INDEX IF EXISTS public.fki_region_id_fkey;

CREATE INDEX IF NOT EXISTS fki_region_id_fkey
    ON public.fact_data USING btree
    (region_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: fki_sales_rep_id_fkey

-- DROP INDEX IF EXISTS public.fki_sales_rep_id_fkey;

CREATE INDEX IF NOT EXISTS fki_sales_rep_id_fkey
    ON public.fact_data USING btree
    (sales_rep_id ASC NULLS LAST)
    TABLESPACE pg_default;