#!/usr/bin/env python3
"""
Универсальный батчер для тяжёлых SELECT-ов, которые уходят в statement timeout.

Идея: запрос гоняется не целиком, а окнами по ключу (по умолчанию public.sku.id).
Границы окон берутся keyset-ом (`... WHERE id > lo ORDER BY id LIMIT N`), а не
равными числовыми диапазонами — id обычно разрежены (например 8 .. 5e9 на 8.8M строк),
и деление на равные интервалы дало бы тысячи пустых батчей.

Как подготовить SQL-файл
------------------------
В запросе должны быть параметры диапазона `%(lo)s` / `%(hi)s` (lo — исключительно,
hi — включительно). Ставить их надо во ВСЕ места, которые иначе сканируют таблицу
целиком, в том числе внутри CTE:

    WITH stocks AS (
        SELECT sku_id, sum(...) FROM sku_stock
        WHERE sku_id > %(lo)s AND sku_id <= %(hi)s      -- <= иначе CTE агрегирует всё
        GROUP BY sku_id
    )
    SELECT ... FROM sku s ...
    WHERE s.id > %(lo)s AND s.id <= %(hi)s
      AND <остальные условия>
    ORDER BY s.id

Сокращение: токен `/*BATCH*/` разворачивается в `<--key-expr> > %(lo)s AND <...> <= %(hi)s`.

Режим `--wrap COL` — для запроса без плейсхолдеров: оборачивает его в
`SELECT * FROM (<запрос>) t WHERE t.COL > %(lo)s AND t.COL <= %(hi)s`.
Работает только если планировщик может протолкнуть предикат внутрь (нет агрегатов
на верхнем уровне, оконных функций, LIMIT). Через LEFT JOIN на агрегирующий CTE
предикат НЕ проталкивается — там нужны явные %(lo)s/%(hi)s. Проверяй `--explain`.

Примеры
-------
  ./run_batched_sql.py sku_model_stocks.sql --out sku_model_stocks.tsv
  ./run_batched_sql.py q.sql --batch-size 20000 --jobs 4 --out q.tsv
  ./run_batched_sql.py q.sql --out q.tsv --resume          # продолжить после обрыва
  ./run_batched_sql.py q.sql --explain                     # план первого батча
  ./run_batched_sql.py q.sql --out q.tsv --max-batches 2   # прогнать пару окон и замерить

Креды — из .env (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD), либо через
--host/--port/--db/--user/--password. .env ищется сначала в текущем каталоге и выше,
затем рядом со скриптом. Другой набор переменных — через --env-prefix, напр.
`--env-prefix PG_` подхватит PG_HOST, PG_PORT, PG_NAME, PG_USER, PG_PASSWORD.

Сессия принудительно read-only, гонять на реплике.
"""

import argparse
import csv
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import psycopg2
from dotenv import find_dotenv, load_dotenv

# .env: сначала из текущего каталога и выше (usecwd — иначе поиск идёт от каталога
# скрипта и .env рабочей директории не подхватывается), затем рядом со скриптом.
load_dotenv(find_dotenv(usecwd=True))
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))


def parse_args():
    ap = argparse.ArgumentParser(
        description="Батчевое выполнение тяжёлого SELECT по окнам ключа",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("sql_file", help="файл с запросом (с %%(lo)s/%%(hi)s или /*BATCH*/)")
    ap.add_argument("--out", help="выходной TSV (без него — только счёт строк)")
    ap.add_argument("--key-table", default="public.sku", help="таблица-источник границ (default public.sku)")
    ap.add_argument("--key-column", default="id", help="колонка ключа в key-table (default id)")
    ap.add_argument("--key-expr", help="как ключ пишется в запросе, для /*BATCH*/ (default = --key-column)")
    ap.add_argument("--key-where", help="доп. фильтр для источника границ, напр. \"status <> 'DELETED'\"")
    ap.add_argument("--batch-size", type=int, default=10_000, help="строк ключа в окне (default 10000)")
    ap.add_argument("--jobs", type=int, default=1, help="параллельных батчей (default 1; >1 ломает порядок строк)")
    ap.add_argument("--stmt-timeout-ms", type=int, default=60_000,
                    help="statement_timeout на батч, мс (default 60000; 0 = без лимита)")
    ap.add_argument("--sleep", type=float, default=3,
                    help="пауза после каждого окна, сек (default 3) — троттлинг, чтобы не давить "
                         "реплику; 0 = гнать вплотную. При --jobs > 1 пауза у каждого воркера своя")
    ap.add_argument("--wrap", metavar="COL", help="обернуть запрос без плейсхолдеров, фильтруя по колонке COL")
    ap.add_argument("--explain", action="store_true", help="показать EXPLAIN первого батча и выйти")
    ap.add_argument("--dry-run", action="store_true", help="показать итоговый SQL и список окон, не выполнять")
    ap.add_argument("--max-batches", type=int, help="прогнать только первые N окон (для замера)")
    ap.add_argument("--resume", action="store_true", help="продолжить: дописывать out, пропустив окна из .progress")
    ap.add_argument("--env-prefix", default="DB_",
                    help="префикс env-переменных с кредами (default DB_ -> DB_HOST, DB_PORT, ...)")
    ap.add_argument("--host")
    ap.add_argument("--port", type=int)
    ap.add_argument("--db")
    ap.add_argument("--user")
    ap.add_argument("--password")
    args = ap.parse_args()

    # что не задано флагами — берём из env с префиксом
    pref = args.env_prefix
    args.host = args.host or os.getenv(pref + "HOST")
    args.port = args.port or int(os.getenv(pref + "PORT", 5432))
    args.db = args.db or os.getenv(pref + "NAME")
    args.user = args.user or os.getenv(pref + "USER")
    args.password = args.password or os.getenv(pref + "PASSWORD")
    return args


def first_statement(raw):
    """Первый стейтмент до ';' — игнорируя ';' внутри комментариев и литералов."""
    i, n = 0, len(raw)
    while i < n:
        c = raw[i]
        if c == ";":
            return raw[:i], raw[i + 1:]
        if raw.startswith("--", i):
            i = raw.find("\n", i)
            if i == -1:
                break
        elif raw.startswith("/*", i):
            depth, i = 1, i + 2          # блочные комментарии в PG вкладываются
            while i < n and depth:
                if raw.startswith("/*", i):
                    depth, i = depth + 1, i + 2
                elif raw.startswith("*/", i):
                    depth, i = depth - 1, i + 2
                else:
                    i += 1
            continue
        elif c in "'\"":
            i += 1
            while i < n:
                if raw[i] == c:
                    if i + 1 < n and raw[i + 1] == c:   # экранирование удвоением
                        i += 2
                        continue
                    break
                i += 1
        i += 1
    return raw, ""


def build_sql(args):
    raw = open(args.sql_file, encoding="utf-8").read()

    # хвостовой ';' (и мусор после него — напр. болтающийся 'LIMIT 100;') убираем:
    # psycopg2 не выполняет несколько стейтментов с параметрами.
    head, tail = first_statement(raw)
    if tail.strip():
        print(f"[warn] всё после первого ';' отброшено: {' '.join(tail.split())[:80]!r}")
    sql = head.strip()

    key_expr = args.key_expr or args.key_column
    if "/*BATCH*/" in sql:
        sql = sql.replace("/*BATCH*/", f"{key_expr} > %(lo)s AND {key_expr} <= %(hi)s")

    if args.wrap:
        if "%(lo)s" in sql:
            raise SystemExit("[FATAL] --wrap несовместим с уже проставленными %(lo)s в запросе")
        sql = (f"SELECT * FROM (\n{sql}\n) __batch\n"
               f"WHERE __batch.{args.wrap} > %(lo)s AND __batch.{args.wrap} <= %(hi)s")

    if "%(lo)s" not in sql or "%(hi)s" not in sql:
        raise SystemExit("[FATAL] в запросе нет %(lo)s/%(hi)s (или /*BATCH*/). "
                         "Расставь их вручную — в т.ч. внутри CTE — либо используй --wrap COL.")
    return sql


def connect(args):
    opts = f"-c default_transaction_read_only=on -c statement_timeout={args.stmt_timeout_ms}"
    conn = psycopg2.connect(host=args.host, port=args.port, dbname=args.db,
                            user=args.user, password=args.password,
                            connect_timeout=15, options=opts)
    conn.autocommit = True
    return conn


def bounds_sql(args):
    """Границы окна keyset-ом: max ключа среди следующих N значений после lo."""
    where = f"{args.key_column} > %(lo)s"
    if args.key_where:
        where += f" AND ({args.key_where})"
    return (f"SELECT max(k) FROM (SELECT {args.key_column} AS k FROM {args.key_table} "
            f"WHERE {where} ORDER BY {args.key_column} LIMIT %(n)s) t")


def next_bound(cur, args, lo, n):
    cur.execute(bounds_sql(args), {"lo": lo, "n": n})
    return cur.fetchone()[0]


def plan_windows(cur, args, done=(), limit=None):
    """Разбивает диапазон ключа на окна по batch-size значений.

    Возвращает (всего окон, окна к выполнению): готовые окна из .progress отсеиваются
    сразу, а limit (--max-batches) останавливает и само планирование — иначе на мелком
    --batch-size границы всех окон считались бы впустую.
    """
    where = f"1=1 AND ({args.key_where})" if args.key_where else "true"
    cur.execute(f"SELECT min({args.key_column}), max({args.key_column}) FROM {args.key_table} WHERE {where}")
    lo_min, hi_max = cur.fetchone()
    if lo_min is None:
        return [], []
    windows, todo, lo = [], [], lo_min - 1
    while lo < hi_max:
        hi = next_bound(cur, args, lo, args.batch_size)
        if hi is None:
            break
        windows.append((lo, hi))
        if (lo, hi) not in done:
            todo.append((lo, hi))
            if limit and len(todo) >= limit:
                break
        lo = hi
    return windows, todo


def main():
    args = parse_args()
    if not args.host or not args.user:
        raise SystemExit(f"[FATAL] нет кредов БД: заполни {args.env_prefix}HOST/{args.env_prefix}USER/... "
                         f"в .env (см. .env.example) или передай --host/--user/--password")
    if args.jobs > 1 and not args.out:
        raise SystemExit("[FATAL] --jobs > 1 требует --out")

    sql = build_sql(args)
    progress_path = (args.out + ".progress") if args.out else None

    done = set()
    if args.resume and progress_path and os.path.exists(progress_path):
        with open(progress_path, encoding="utf-8") as f:
            done = {tuple(int(x) for x in line.split("\t")) for line in f if line.strip()}
        print(f"[resume] окон уже готово: {len(done)}")
    elif args.out and not args.resume:
        for p in (args.out, progress_path):
            if os.path.exists(p):
                raise SystemExit(f"[FATAL] {p} уже существует — удали или запусти с --resume")

    conn = connect(args)
    with conn.cursor() as cur:
        if args.explain:
            lo = next_bound(cur, args, -1, 1)
            hi = next_bound(cur, args, lo - 1, args.batch_size)
            cur.execute("EXPLAIN (ANALYZE, BUFFERS) " + sql, {"lo": lo - 1, "hi": hi})
            print(f"-- окно ({lo - 1}, {hi}]")
            print("\n".join(r[0] for r in cur.fetchall()))
            return
        t_plan = time.time()
        windows, todo = plan_windows(cur, args, done, args.max_batches)
        capped = args.max_batches and len(todo) >= args.max_batches
        print(f"Окон по {args.batch_size} ключей: {len(windows)}{'+ (планирование прервано по --max-batches)' if capped else ''}"
              f"; к выполнению: {len(todo)} (границы посчитаны за {time.time() - t_plan:.1f}с)")

    if args.dry_run:
        print("\n----- SQL -----\n" + sql)
        print(f"\n----- окна ({len(todo)}) -----")
        for w in todo[:5]:
            print(f"  ({w[0]}, {w[1]}]")
        if len(todo) > 5:
            print(f"  ... ещё {len(todo) - 5}")
        return

    lock = threading.Lock()
    state = {"rows": 0, "batches": 0, "header": None}
    out_f = writer = progress_f = None
    if args.out:
        out_f = open(args.out, "a", newline="", encoding="utf-8")
        writer = csv.writer(out_f, delimiter="\t")
        progress_f = open(progress_path, "a", encoding="utf-8")
        if out_f.tell() > 0:
            state["header"] = True  # дописываем в непустой файл — шапка уже есть

    local = threading.local()

    def cursor_conn():
        if not hasattr(local, "conn"):
            local.conn = conn if threading.current_thread() is threading.main_thread() else connect(args)
        return local.conn

    def run_window(lo, hi, depth=0):
        c = cursor_conn()
        t0 = time.time()
        try:
            with c.cursor() as cur:
                cur.execute(sql, {"lo": lo, "hi": hi})
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
        except psycopg2.errors.QueryCanceled:
            c.rollback()
            with lock:
                print(f"  [timeout] окно ({lo}, {hi}] — делю пополам")
            with c.cursor() as cur:
                n = max(1, args.batch_size // (2 ** (depth + 1)))
                mid = next_bound(cur, args, lo, n)
            if mid is None or mid >= hi:
                with lock:
                    print(f"  [!] окно ({lo}, {hi}] не делится и не влезает в timeout — пропущено")
                return
            run_window(lo, mid, depth + 1)
            run_window(mid, hi, depth + 1)
            return

        with lock:
            if writer:
                if not state["header"]:
                    writer.writerow(cols)
                    state["header"] = True
                writer.writerows(rows)
                out_f.flush()
                progress_f.write(f"{lo}\t{hi}\n")
                progress_f.flush()
            state["rows"] += len(rows)
            state["batches"] += 1
            print(f"  [{state['batches']}/{len(todo)}] ({lo}, {hi}]: "
                  f"+{len(rows)} строк за {time.time() - t0:.1f}с (всего {state['rows']})")

        if args.sleep:            # троттлинг — вне lock, иначе воркеры ждали бы друг друга
            time.sleep(args.sleep)

    t0 = time.time()
    try:
        if args.jobs > 1:
            with ThreadPoolExecutor(max_workers=args.jobs) as pool:
                list(pool.map(lambda w: run_window(*w), todo))
        else:
            for lo, hi in todo:
                run_window(lo, hi)
    finally:
        if out_f:
            out_f.close()
            progress_f.close()
        conn.close()

    el = time.time() - t0
    print(f"\nГотово: {state['rows']} строк за {f'{el:.1f}с' if el < 60 else f'{el / 60:.1f} мин'}"
          + (f" -> {args.out}" if args.out else ""))
    if progress_path:
        print(f"{progress_path} можно удалить после успешного завершения.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[прервано; перезапусти с --resume]")
        sys.exit(130)
