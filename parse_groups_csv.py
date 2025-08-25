#!/usr/bin/env python3
"""
Парсер для output/groups.csv.

Назначение:
- Фильтрует группы, у которых колонка "Публикация" == "Создать пост"
- Выдаёт "чистые" ссылки на VK-группы без query-параметров, например:
  https://vk.com/club208512229

Использование:
  python3 parse_groups_csv.py --input output/groups.csv [--output output/clean_links.txt]

По умолчанию печатает ссылки в stdout по одной на строку.
"""

from __future__ import annotations

import argparse
import csv
import sys
from urllib.parse import urlparse


CSV_HEADER_LINK = "Ссылка"
CSV_HEADER_PUBLICATION = "Публикация"
PUBLICATION_VALUE_OPEN = "Создать пост"
PUBLICATION_VALUE_CLOSED = "Предложить пост"


def clean_vk_url(raw_url: str) -> str:
    """Возвращает ссылку без query/fragment, только схема+хост+путь.

    Пример:
    https://vk.com/club208512229?search_track_code=abc -> https://vk.com/club208512229
    """
    raw_url = (raw_url or "").strip()
    if not raw_url:
        return ""

    parsed = urlparse(raw_url)

    scheme = parsed.scheme or "https"
    netloc = parsed.netloc or "vk.com"
    path = parsed.path or ""

    # Убираем завершающий слэш, если он есть (для консистентности)
    if path.endswith("/"):
        path = path[:-1]

    return f"{scheme}://{netloc}{path}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Извлечение чистых ссылок VK из groups.csv")
    parser.add_argument(
        "--input",
        default="output/groups.csv",
        help="Путь к входному CSV (по умолчанию: output/groups.csv)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Путь к файлу вывода. Если не указан, печатает в stdout",
    )
    parser.add_argument(
        "--wall-type",
        choices=["open", "closed", "all"],
        default="open",
        help="Тип стены для фильтрации: 'open' (Создать пост), 'closed' (Предложить пост), 'all' (все группы). По умолчанию: 'open'.",
    )
    parser.add_argument(
        "--no-dedupe",
        action="store_true",
        help="Не удалять дубликаты ссылок (по умолчанию дубликаты удаляются)",
    )
    return parser.parse_args()


def collect_clean_links(
    input_csv_path: str, wall_type: str, dedupe: bool = True
) -> list[str]:
    clean_links: list[str] = []
    seen: set[str] = set()

    with open(input_csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        # Валидация наличия нужных колонок
        fieldnames = reader.fieldnames or []
        if CSV_HEADER_LINK not in fieldnames or CSV_HEADER_PUBLICATION not in fieldnames:
            raise ValueError(
                f"В CSV отсутствуют требуемые колонки: '{CSV_HEADER_LINK}', '{CSV_HEADER_PUBLICATION}'."
            )

        for row in reader:
            publication_value = (row.get(CSV_HEADER_PUBLICATION) or "").strip()
            if wall_type == "open" and publication_value != PUBLICATION_VALUE_OPEN:
                continue
            if wall_type == "closed" and publication_value != PUBLICATION_VALUE_CLOSED:
                continue

            raw_link = row.get(CSV_HEADER_LINK) or ""
            link = clean_vk_url(raw_link)
            if not link:
                continue

            if dedupe:
                if link in seen:
                    continue
                seen.add(link)

            clean_links.append(link)

    return clean_links


def main() -> None:
    args = parse_args()
    links = collect_clean_links(
        args.input, wall_type=args.wall_type, dedupe=not args.no_dedupe
    )

    if args.output:
        with open(args.output, "w", encoding="utf-8") as out:
            for link in links:
                out.write(f"{link}\n")
    else:
        for link in links:
            print(link)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)



