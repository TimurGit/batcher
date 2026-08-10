-- Простейший случай: батчинг по первичному ключу таблицы.
-- Ключ по умолчанию — public.sku.id, поэтому здесь источник границ задаётся явно:
--
--   ../run_batched_sql.py examples/products.sql --key-table public.product --out products.tsv
--
-- lo — исключительно, hi — включительно.
SELECT *
FROM public.product
WHERE id > %(lo)s AND id <= %(hi)s
ORDER BY id
