-- То же самое, но условие окна записано токеном /*BATCH*/ — он разворачивается
-- в `<--key-expr> > %(lo)s AND <--key-expr> <= %(hi)s`.
-- Источник границ по умолчанию (public.sku.id) подходит, флаги не нужны:
--
--   ../run_batched_sql.py examples/skus.sql --out skus.tsv
--
-- Фильтровать окна можно и по своей колонке:
--
--   ../run_batched_sql.py examples/skus.sql --key-expr s.id --key-where "status = 'ACTIVE'" --out skus.tsv
SELECT *
FROM public.sku s
WHERE /*BATCH*/
ORDER BY s.id
