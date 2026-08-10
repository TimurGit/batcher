# batcher

Гоняет тяжёлый `SELECT`, который не влезает в `statement_timeout`, окнами по ключу
и складывает результат в TSV. Read-only, с резюмом после обрыва.

## Установка

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # и заполнить
```

`.env`:

```
DB_HOST=replica-host
DB_PORT=6432
DB_NAME=mydb
DB_USER=myuser
DB_PASSWORD=...
```

`.env` ищется сначала в текущем каталоге и выше, потом рядом со скриптом. Любую
переменную можно перебить флагом (`--host`, `--port`, `--db`, `--user`, `--password`).
Другой набор имён — `--env-prefix`, например `--env-prefix PG_` подхватит `PG_HOST`,
`PG_PORT`, `PG_NAME`, `PG_USER`, `PG_PASSWORD`.

## Как это работает

Границы окон берутся **keyset-ом** (`WHERE id > lo ORDER BY id LIMIT N`), а не делением
диапазона на равные интервалы: id обычно разрежены (например 8 … 5 000 000 006 на 8.8 млн
строк), и равные интервалы дали бы тысячи пустых батчей.

Дальше каждое окно — отдельный запрос с параметрами `%(lo)s` / `%(hi)s`
(**lo исключительно, hi включительно**), результат дописывается в TSV, а готовые окна
отмечаются в сайдкар-файле `<out>.progress`.

## Подготовка запроса

Плейсхолдеры `%(lo)s` / `%(hi)s` надо расставить во **все** места, которые иначе
сканируют таблицу целиком — в том числе внутри CTE:

```sql
WITH stocks AS (
    SELECT sku_id, sum(quantity_active) AS qty
    FROM sku_stock
    WHERE sku_id > %(lo)s AND sku_id <= %(hi)s   -- без этого CTE агрегирует всю таблицу
    GROUP BY sku_id                              -- на КАЖДОМ батче
)
SELECT s.id, st.qty
FROM sku s
LEFT JOIN stocks st ON st.sku_id = s.id
WHERE s.id > %(lo)s AND s.id <= %(hi)s
  AND <остальные условия>
ORDER BY s.id
```

Это главный подводный камень: предикат **не** проталкивается через `LEFT JOIN` на
агрегирующий CTE, и без явного условия внутри батчинг только замедлит запрос.

Сокращение: токен `/*BATCH*/` разворачивается в `<--key-expr> > %(lo)s AND <...> <= %(hi)s`.

Режим `--wrap COL` — для запроса вообще без плейсхолдеров: оборачивает его в
`SELECT * FROM (<запрос>) t WHERE t.COL > %(lo)s AND t.COL <= %(hi)s`. Работает, только
если планировщик может протолкнуть предикат внутрь (нет агрегатов на верхнем уровне,
оконных функций, LIMIT). Всегда проверяй результат через `--explain`.

`ORDER BY` внутри окна дешёвый (сортируются ~batch-size строк), а окна идут по
возрастанию ключа — так что при `--jobs 1` выходной файл упорядочен целиком.

## Запуск

```bash
# план первого окна — убедиться, что нет Seq Scan по большим таблицам
./run_batched_sql.py query.sql --explain

# итоговый SQL и список окон, без выполнения
./run_batched_sql.py query.sql --dry-run

# выгрузка
./run_batched_sql.py query.sql --out out.tsv

# в 4 потока (порядок строк между окнами не гарантирован)
./run_batched_sql.py query.sql --out out.tsv --jobs 4

# продолжить после обрыва
./run_batched_sql.py query.sql --out out.tsv --resume

# прогнать первые 3 окна, чтобы прикинуть время
./run_batched_sql.py query.sql --out out.tsv --max-batches 3
```

## Флаги

| Флаг | По умолчанию | Зачем |
|---|---|---|
| `--out` | — | выходной TSV; без него только считает строки |
| `--key-table` | `public.sku` | таблица-источник границ окон |
| `--key-column` | `id` | колонка ключа в ней |
| `--key-expr` | `= --key-column` | как ключ пишется в запросе (для `/*BATCH*/`) |
| `--key-where` | — | доп. фильтр источника границ, напр. `status <> 'DELETED'` |
| `--batch-size` | `50000` | значений ключа в окне |
| `--jobs` | `1` | параллельных окон |
| `--stmt-timeout-ms` | `300000` | `statement_timeout` на окно (`0` — без лимита) |
| `--wrap COL` | — | обернуть запрос без плейсхолдеров |
| `--explain` | — | `EXPLAIN (ANALYZE, BUFFERS)` первого окна |
| `--dry-run` | — | показать SQL и окна, не выполнять |
| `--max-batches` | — | выполнить только первые N окон |
| `--resume` | — | продолжить, пропустив окна из `.progress` |
| `--env-prefix` | `DB_` | префикс env-переменных с кредами |

## Поведение

- **Таймаут окна** — окно автоматически делится пополам и части выполняются отдельно;
  если не делится дальше и всё равно не влезает, окно пропускается с явным сообщением.
- **Обрыв** (Ctrl-C, разрыв связи) — перезапуск с `--resume` дочитает недостающие окна,
  шапка в TSV не дублируется.
- **Read-only** — сессия открывается с `default_transaction_read_only=on`, а
  `statement_timeout` ставится опциями коннекта: за пулером (pgbouncer в transaction mode)
  обычный `SET` может уехать в чужую сессию.
- Из файла берётся **первый** стейтмент до `;` — с учётом комментариев и литералов,
  так что `;` внутри `--`/`/* */`/строк запрос не обрезает.

## Примеры

В `examples/` — два минимальных запроса:

- `products.sql` — `SELECT * FROM product` с явными `%(lo)s` / `%(hi)s` и источником
  границ через `--key-table public.product`;
- `skus.sql` — то же через токен `/*BATCH*/` и источник границ по умолчанию.

```bash
./run_batched_sql.py examples/skus.sql --out skus.tsv
./run_batched_sql.py examples/products.sql --key-table public.product --out products.tsv
```
