#!/usr/bin/env python3
import argparse
import csv
import os
import sys
import time
import urllib.parse
import re
import math
from dataclasses import dataclass
from typing import Iterable, List, Optional, Set, Tuple, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from tqdm import tqdm

# Excel
from openpyxl import Workbook, load_workbook

# Playwright
from playwright.sync_api import Playwright, sync_playwright, BrowserContext, Page, TimeoutError as PlaywrightTimeoutError


SEARCH_URL_TEMPLATE = "https://vk.com/search?c%5Bq%5D={query}&c%5Bsection%5D=communities"
VK_GROUPS_URL = "https://vk.com/groups"
STORAGE_STATE_FILE = "./.vk_storage_state.json"


@dataclass
class GroupItem:
    index: int
    name: str
    url: str
    subscribers: Optional[int]
    posting_status: Optional[str]


csv_lock = threading.Lock()
index_lock = threading.Lock()
seen_lock = threading.Lock()


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


def _read_csv_header(file_path: str) -> Optional[List[str]]:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            return next(reader)
    except FileNotFoundError:
        return None
    except StopIteration:
        return []


def ensure_csv(file_path: str) -> None:
    ensure_parent_dir(file_path)
    desired = ["№", "Название", "Ссылка", "Подписчики", "Публикация"]
    header = _read_csv_header(file_path)
    if header is None:
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(desired)
    else:
        if not header:
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(desired)
        else:
            missing = [c for c in desired if c not in header]
            if missing:
                # Upgrade: add missing columns and pad rows
                rows: List[List[str]] = []
                with open(file_path, "r", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    rows = list(reader)
                # Build new header preserving order of existing and appending missing in desired order
                new_header = header[:]
                for c in desired:
                    if c not in new_header:
                        new_header.append(c)
                # Map old -> new width
                for i in range(1, len(rows)):
                    while len(rows[i]) < len(new_header):
                        rows[i].append("")
                rows[0] = new_header
                with open(file_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerows(rows)


def ensure_xlsx(file_path: str) -> None:
    ensure_parent_dir(file_path)
    if not os.path.exists(file_path):
        wb = Workbook()
        ws = wb.active
        ws.title = "groups"
        ws.append(["№", "Название", "Ссылка", "Подписчики", "Публикация"])
        wb.save(file_path)
    else:
        wb = load_workbook(file_path)
        ws = wb.active
        headers = [cell.value for cell in next(ws.iter_rows(min_row=1, max_row=1))]
        changed = False
        for col in ["Подписчики", "Публикация"]:
            if col not in headers:
                headers.append(col)
                changed = True
        if changed:
            # rewrite header row
            for c, val in enumerate(headers, start=1):
                ws.cell(row=1, column=c, value=val)
            wb.save(file_path)


def append_csv(file_path: str, item: GroupItem) -> None:
    with csv_lock:
        with open(file_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                item.index,
                item.name,
                item.url,
                item.subscribers if item.subscribers is not None else "",
                item.posting_status if item.posting_status is not None else "",
            ])


def append_xlsx(file_path: str, item: GroupItem) -> None:
    with csv_lock:
        wb = load_workbook(file_path)
        ws = wb.active
        ws.append([
            item.index,
            item.name,
            item.url,
            item.subscribers if item.subscribers is not None else None,
            item.posting_status if item.posting_status is not None else None,
        ])
        wb.save(file_path)


def has_auth_cookie(page: Page) -> bool:
    try:
        cookies = page.context.cookies()
        for c in cookies:
            if c.get("name") == "remixsid" and c.get("value"):
                return True
    except Exception:
        pass
    try:
        cookie_str = page.evaluate("() => document.cookie")
        return ("remixsid=" in (cookie_str or ""))
    except Exception:
        return False


def looks_like_login_page(page: Page) -> bool:
    markers = [
        "Вход ВКонтакте",
        "QR-код",
        "Войти другим способом",
        "Создать аккаунт",
    ]
    try:
        content = page.content()
        return any(m in content for m in markers)
    except Exception:
        return False


def is_logged_in(page: Page) -> bool:
    if has_auth_cookie(page):
        return True
    if looks_like_login_page(page):
        return False
    try:
        page.wait_for_selector('a[href^="/feed"], a[href^="/im"], a[href^="/logout"]', timeout=1500)
        return True
    except PlaywrightTimeoutError:
        return False


def wait_for_login(page: Page, timeout_sec: int) -> None:
    start = time.time()
    while time.time() - start < timeout_sec:
        if is_logged_in(page):
            return
        time.sleep(1.0)
    raise TimeoutError("Авторизация не была завершена вовремя. Перезапустите и войдите в VK.")


def open_or_create_persistent_context(playwright: Playwright, user_data_dir: str, headless: bool) -> Tuple[BrowserContext, Page]:
    context = playwright.chromium.launch_persistent_context(
        user_data_dir=user_data_dir,
        headless=headless,
        viewport={"width": 1400, "height": 900},
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
        ],
    )
    page = context.pages[0] if context.pages else context.new_page()
    return context, page


def ensure_login_and_export_state(user_data_dir: str, headless: bool, login_timeout: int, state_file: str) -> None:
    with sync_playwright() as p:
        context, page = open_or_create_persistent_context(p, user_data_dir=user_data_dir, headless=headless)
        try:
            page.goto(VK_GROUPS_URL)
        except Exception:
            page.goto("https://vk.com/")
        if not is_logged_in(page):
            print("Открылся VK. Пожалуйста, выполните вход (QR, пароль или через VK ID). Ожидаю завершения авторизации…", flush=True)
            if headless:
                print("Внимание: headless режим усложняет авторизацию. Рекомендуется без --headless.")
            page.goto("https://vk.com/")
            wait_for_login(page, timeout_sec=login_timeout)
            print("Авторизация распознана, продолжаю…", flush=True)
        context.storage_state(path=state_file)
        context.close()


def ensure_on_groups_page(page: Page) -> None:
    for _ in range(3):
        try:
            page.goto(VK_GROUPS_URL)
            try:
                page.wait_for_timeout(300)
                if not page.locator('input[data-testid="search_input"][placeholder="Поиск сообществ"]').count():
                    link = page.query_selector('a[href="/groups"], a[href*="/groups"]')
                    if link:
                        link.click()
                        page.wait_for_timeout(500)
            except Exception:
                pass
            loc = page.locator('input[data-testid="search_input"][placeholder="Поиск сообществ"]')
            loc.wait_for(state="visible", timeout=4000)
            return
        except Exception:
            continue
    try:
        page.goto(VK_GROUPS_URL)
    except Exception:
        pass


def navigate_to_communities_search(page: Page, query: str) -> None:
    ensure_on_groups_page(page)
    try:
        locator = page.locator('input[data-testid="search_input"][placeholder="Поиск сообществ"]')
        locator.wait_for(state="visible", timeout=5000)
        locator.click()
        locator.fill("")
        locator.type(query, delay=20)
        page.keyboard.press("Enter")
    except Exception:
        input_selectors = [
            'input[data-testid="search_input"]',
            '[role="searchbox"] input',
            'input[placeholder*="Поиск" i]',
            'input[type="search"]',
        ]
        for sel in input_selectors:
            try:
                el = page.query_selector(sel)
                if not el:
                    continue
                el.click()
                el.fill("")
                el.type(query, delay=20)
                page.keyboard.press("Enter")
                break
            except Exception:
                continue
        else:
            encoded = urllib.parse.quote(query)
            page.goto(SEARCH_URL_TEMPLATE.format(query=encoded))
    selectors = [
        'div[class*="search_results"]',
        'div[class*="SearchResults"]',
        'div[class*="_list"]',
        'div[class*="groups_list"]',
        'div:has(a.search_item__title)',
    ]
    for sel in selectors:
        try:
            page.wait_for_selector(sel, timeout=4000)
            break
        except PlaywrightTimeoutError:
            continue
    page.wait_for_timeout(800)


def parse_int_from_text(text: str) -> Optional[int]:
    if not text:
        return None
    t = text.lower().replace("\xa0", " ")
    m = re.search(r"(\d+[\s\d]*([\.,]\d+)?)\s*(тыс\.?|k)\b", t)
    if m:
        num = m.group(1).replace(" ", "").replace(",", ".")
        try:
            return int(float(num) * 1000)
        except Exception:
            pass
    m = re.search(r"(\d+[\s\d]*([\.,]\d+)?)\s*(млн\.?|m)\b", t)
    if m:
        num = m.group(1).replace(" ", "").replace(",", ".")
        try:
            return int(float(num) * 1000000)
        except Exception:
            pass
    numbers = re.findall(r"\d+[\s\d]*", t)
    if not numbers:
        return None
    def to_int(s: str) -> int:
        digits = re.sub(r"\D", "", s)
        return int(digits) if digits else 0
    values = [to_int(n) for n in numbers]
    return max(values) if values else None


def extract_group_cards(page: Page) -> List[Tuple[str, str, Optional[int]]]:
    try:
        data = page.evaluate(
            """
            () => {
              function parseCount(text){
                if(!text) return null;
                const t = text.toLowerCase().replace(/\u00a0/g, ' ');
                let m = t.match(/(\d+[\s\d]*([\.,]\d+)?)\s*(тыс\.?|k)\b/);
                if(m){
                  const num = parseFloat(m[1].replace(/\s/g,'').replace(',','.'));
                  if(!isNaN(num)) return Math.round(num*1000);
                }
                m = t.match(/(\d+[\s\d]*([\.,]\d+)?)\s*(млн\.?|m)\b/);
                if(m){
                  const num = parseFloat(m[1].replace(/\s/g,'').replace(',', '.'));
                  if(!isNaN(num)) return Math.round(num*1000000);
                }
                const digits = (t.match(/\d+[\s\d]*/g)||[]).map(s=>parseInt(s.replace(/\D/g,''))).filter(n=>!isNaN(n));
                if(digits.length===0) return null;
                return Math.max(...digits);
              }
              function center(el){
                const r = el.getBoundingClientRect();
                return {x: r.left + r.width/2, y: r.top + r.height/2};
              }
              const anchors = Array.from(document.querySelectorAll("a[href^='/public'], a[href^='/club'], a[href^='/event'], a[href^='/community']"));
              const candidates = Array.from(document.querySelectorAll("span, div"))
                .filter(n => {
                  const t = (n.textContent||'').toLowerCase();
                  return t.includes('подпис') || t.includes('участ');
                });
              const spans = candidates.map(n => ({ node:n, c:center(n), text:(n.textContent||'').trim() }));
              const seen = new Set();
              const out = [];
              for(const a of anchors){
                const href = a.getAttribute('href')||'';
                const name = (a.textContent||a.getAttribute('aria-label')||'').trim();
                if(!href || !name) continue;
                const full = href.startsWith('/') ? 'https://vk.com'+href : href;
                if(seen.has(full)) continue;
                seen.add(full);
                const ac = center(a);
                let best = null, bestD = Infinity;
                for(const s of spans){
                  const dy = Math.abs(s.c.y - ac.y);
                  const dx = Math.abs(s.c.x - ac.x);
                  const d = dy + dx*0.1;
                  if(dy < 80 && d < bestD){
                    best = s;
                    bestD = d;
                  }
                }
                let subs = null;
                if(best){
                  const val = parseCount(best.text);
                  if(typeof val === 'number' && val>0) subs = val;
                }
                out.push([name, full, subs]);
              }
              return out;
            }
            """
        )
        results: List[Tuple[str, str, Optional[int]]] = []
        for name, url, subs in data:
            results.append((str(name), str(url), int(subs) if subs is not None else None))
        return results
    except Exception:
        return []


def extract_posting_status(page: Page) -> Optional[str]:
    selectors = [
        'button[data-testid="posting_create_post_button"]',
        'div.PostingReactBlock__root button',
        'section.vkitGroup__group--vFKdo button',
        'button:has-text("Создать пост")',
    ]
    for sel in selectors:
        try:
            btn = page.query_selector(sel)
            if btn:
                txt = (btn.inner_text() or btn.text_content() or "").strip()
                if txt:
                    return txt
        except Exception:
            continue
    try:
        # Ancillary heading near posting form
        h2 = page.query_selector('h2:has-text("Добавление нового контента")')
        if h2:
            # Try to find any button inside same block
            parent = h2
            for _ in range(3):
                parent = parent.query_selector('xpath=..') or parent
            btn = parent.query_selector('button')
            if btn:
                txt = (btn.inner_text() or btn.text_content() or "").strip()
                if txt:
                    return txt
    except Exception:
        pass
    return None


def infinite_scroll_and_collect(
    page: Page,
    already: Set[str],
    max_per_query: int,
    on_item: Callable[[str, str, Optional[int]], None],
    stop_event: threading.Event,
    progress: Optional[tqdm] = None,
) -> None:
    collected_urls_local: Set[str] = set()
    stagnant_rounds = 0
    unlimited = max_per_query is None or max_per_query <= 0

    while not stop_event.is_set() and (unlimited or len(collected_urls_local) < max_per_query) and stagnant_rounds < 10:
        found = extract_group_cards(page)
        new_count = 0
        for name, url, subs in found:
            if stop_event.is_set():
                break
            if url in already or url in collected_urls_local:
                continue
            collected_urls_local.add(url)
            try:
                on_item(name, url, subs)
                new_count += 1
                if progress is not None:
                    progress.update(1)
            except Exception:
                continue
        if new_count == 0:
            stagnant_rounds += 1
        else:
            stagnant_rounds = 0
        try:
            page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        except Exception:
            pass
        page.wait_for_timeout(1200)


def worker_collect_and_write(
    query: str,
    state_file: str,
    headless: bool,
    max_per_query: int,
    output_csv: str,
    output_xlsx: str,
    existing_urls: Set[str],
    index_ref: List[int],
    pbar: Optional[tqdm],
    stop_event: threading.Event,
) -> None:
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=headless,
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
            )
            context = browser.new_context(storage_state=state_file, viewport={"width": 1400, "height": 900})
            page = context.new_page()
            detail_page = context.new_page()
            navigate_to_communities_search(page, query)

            def on_item(name: str, url: str, subs: Optional[int]) -> None:
                if stop_event.is_set():
                    return
                with seen_lock:
                    if url in existing_urls:
                        return
                    existing_urls.add(url)
                posting: Optional[str] = None
                try:
                    detail_page.goto(url)
                    detail_page.wait_for_timeout(800)
                    posting = extract_posting_status(detail_page)
                except Exception:
                    posting = None
                with index_lock:
                    index_ref[0] += 1
                    idx = index_ref[0]
                item = GroupItem(index=idx, name=name, url=url, subscribers=subs, posting_status=posting)
                append_csv(output_csv, item)
                append_xlsx(output_xlsx, item)

            infinite_scroll_and_collect(
                page=page,
                already=existing_urls,
                max_per_query=max_per_query,
                on_item=on_item,
                stop_event=stop_event,
                progress=pbar,
            )
            context.close()
            browser.close()
    except Exception as e:
        if not stop_event.is_set():
            print(f"Поток для запроса '{query}' завершился с ошибкой: {e}", file=sys.stderr)


def run(
    query_list: List[str],
    output_csv: str,
    output_xlsx: str,
    user_data_dir: str,
    headless: bool,
    max_per_query: int,
    login_timeout: int,
) -> None:
    ensure_csv(output_csv)
    ensure_xlsx(output_xlsx)

    state_file = os.path.abspath(STORAGE_STATE_FILE)
    ensure_parent_dir(state_file)
    ensure_login_and_export_state(
        user_data_dir=user_data_dir,
        headless=headless,
        login_timeout=login_timeout,
        state_file=state_file,
    )

    existing_urls: Set[str] = set()
    try:
        with open(output_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = (row.get("Ссылка") or "").strip()
                if url:
                    existing_urls.add(url)
    except FileNotFoundError:
        pass

    current_index = 0
    try:
        with open(output_csv, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            for i, _ in enumerate(reader):
                current_index = i
    except FileNotFoundError:
        current_index = 0

    index_ref = [current_index]
    stop_event = threading.Event()

    total_target = None if (max_per_query is None or max_per_query <= 0) else len(query_list) * max_per_query
    pbar_cm = tqdm(total=total_target, desc="Сбор результатов", unit="grp") if total_target is not None else tqdm(desc="Сбор результатов", unit="grp")
    with pbar_cm as pbar:
        try:
            with ThreadPoolExecutor(max_workers=max(1, len(query_list))) as executor:
                futures = [
                    executor.submit(
                        worker_collect_and_write,
                        query,
                        state_file,
                        headless,
                        max_per_query,
                        output_csv,
                        output_xlsx,
                        existing_urls,
                        index_ref,
                        pbar,
                        stop_event,
                    )
                    for query in query_list
                ]
                for _ in as_completed(futures):
                    pass
        except KeyboardInterrupt:
            print("Остановка по Ctrl+C. Завершаю потоки…", file=sys.stderr)
            stop_event.set()
            time.sleep(1.0)
        finally:
            stop_event.set()


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Парсер сообществ VK: многопоточность по запросам, потоковое сохранение, подписчики и статус публикации.")
    parser.add_argument("--queries", nargs="*", default=[
        "Авто из Кореи",
        "Авто из Японии",
        "Авто из Китая",
    ], help="Список поисковых запросов. По умолчанию три предустановленных.")
    parser.add_argument("--csv", default="./output/groups.csv", help="Путь к CSV файлу для сохранения.")
    parser.add_argument("--xlsx", default="./output/groups.xlsx", help="Путь к Excel файлу для сохранения.")
    parser.add_argument("--user-data-dir", default="./.vk_user_data", help="Каталог для хранения сессии браузера (куки).")
    parser.add_argument("--headless", action="store_true", help="Запуск без интерфейса (для авторизации лучше без него).")
    parser.add_argument("--max-per-query", type=int, default=0, help="Максимум результатов на запрос. 0 или меньше = без лимита.")
    parser.add_argument("--login-timeout", type=int, default=300, help="Таймаут ожидания авторизации, сек.")

    args = parser.parse_args(list(argv) if argv is not None else None)

    csv_path = os.path.abspath(args.csv)
    xlsx_path = os.path.abspath(args.xlsx)
    user_data_dir = os.path.abspath(args.user_data_dir)

    try:
        run(
            query_list=args.queries,
            output_csv=csv_path,
            output_xlsx=xlsx_path,
            user_data_dir=user_data_dir,
            headless=args.headless,
            max_per_query=args.max_per_query,
            login_timeout=args.login_timeout,
        )
        print(f"Готово. CSV: {csv_path}, XLSX: {xlsx_path}")
        return 0
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
