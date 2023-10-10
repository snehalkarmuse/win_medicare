-- Table: public.fact_data_table

-- DROP TABLE IF EXISTS public.fact_data_table;

CREATE TABLE IF NOT EXISTS public.fact_data_table
(
    data_id integer NOT NULL,
    product_id integer,
    region_id integer,
    sale_rep_id integer,
    date date,
    sales_amt double precision,
    CONSTRAINT fact_data_table_pkey PRIMARY KEY (data_id),
    CONSTRAINT product_id_fkey FOREIGN KEY (product_id)
        REFERENCES public.dim_products (product_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT region_id_fkey FOREIGN KEY (region_id)
        REFERENCES public.dim_region (region_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT sales_rep_id_fkey FOREIGN KEY (sale_rep_id)
        REFERENCES public.dim_sales_representative (sales_rep_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.fact_data_table
    OWNER to postgres;
-- Index: fki_product_id_fkey

-- DROP INDEX IF EXISTS public.fki_product_id_fkey;

CREATE INDEX IF NOT EXISTS fki_product_id_fkey
    ON public.fact_data_table USING btree
    (product_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: fki_region_id_fkey

-- DROP INDEX IF EXISTS public.fki_region_id_fkey;

CREATE INDEX IF NOT EXISTS fki_region_id_fkey
    ON public.fact_data_table USING btree
    (region_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: fki_sales_rep_id_fkey

-- DROP INDEX IF EXISTS public.fki_sales_rep_id_fkey;

CREATE INDEX IF NOT EXISTS fki_sales_rep_id_fkey
    ON public.fact_data_table USING btree
    (sale_rep_id ASC NULLS LAST)
    TABLESPACE pg_default;