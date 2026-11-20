#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
МЕХАНИЗМ inv-01 — инвентаризация GitHub-универсума по живым данным,
а не по описаниям README.

Принцип: каждая цифра инвентаря приходит из живого ответа GitHub API,
git-протокола, raw.githubusercontent или отрендеренной страницы GitHub —
и несёт метку своего источника. Цифры из README/био/витрин превращаются
в claims и сверяются с API-фактами. Verdict: PASS / FAIL / DRIFT /
UNVERIFIABLE + перекрёстные противоречия между источниками.

Источники (бюджет-осведомлённые):
  core    — api.github.com REST (60/ч без токена; ждёт сброса, если исчерпан)
  search  — api.github.com/search (10/мин без токена)
  raw     — raw.githubusercontent.com (README и data-файлы)
  git     — git clone --bare --filter=blob:none (точные коммиты и деревья,
            blobs не скачиваются — дёшево даже для репо с аудио)
  html    — github.com / github.io страницы (календарь контрибуций, витрина)

stdlib-only: urllib, json, re, git через subprocess. 0 зависимостей.

Использование:
  python3 inventory_mechanism.py --user rtynbirf \
      --out /home/z/my-project/download
Ключи:
  --token TOKEN       GitHub PAT (или env GITHUB_TOKEN/GH_TOKEN) → core 5000/ч
  --core-wait SEC     сколько ждать сброса core-лимита (по умолчанию 480)
  --workers N         параллельные клоны (по умолчанию 4)
  --skip-clones / --skip-deep / --no-html   деградация для отладки
Выход:
  {out}/{user}-inventory.json         — полный инвентарь + claims + meta
  {out}/{user}-inventory-report.html  — самодостаточный отчёт (тёмный, gold)
"""

import argparse
import html as html_mod
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

API = "https://api.github.com"
UA = "inv-mechanism/01 (stdlib-only; evidence-over-readme)"
DATE_FMT = "%Y-%m-%dT%H:%M:%SZ"

CLONE_ENV = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}

README_CANDIDATES = [
    "README.md", "readme.md", "README.ru.md", "Readme.md", "README.txt",
]


def log(msg):
    print(f"[inv {datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}",
          flush=True)


def now_iso():
    return datetime.now(timezone.utc).strftime(DATE_FMT)


def days_between(iso, ref=None):
    """Дней от ISO-даты до ref (по умолчанию сейчас). None при ошибке."""
    try:
        d = datetime.strptime(iso[:10], "%Y-%m-%d")
        r = datetime.strptime((ref or now_iso())[:10], "%Y-%m-%d")
        return (r - d).days
    except Exception:
        return None


# ---------------------------------------------------------------- HTTP-слой
class Http:
    """Бюджет-осведомлённый HTTP: считает вызовы по типам источников,
    соблюдает pacing для search, делает один retry на 403/429/сеть."""

    def __init__(self, token=None):
        self.token = token
        self.calls = {"core": 0, "search": 0, "raw": 0, "html": 0}
        self.core_remaining = None
        self.search_remaining = None
        self._last_search = 0.0
        self.last_headers = {}

    def _kind(self, url):
        if "/search/" in url:
            return "search"
        if "api.github.com" in url:
            return "core"
        if "raw.githubusercontent" in url:
            return "raw"
        return "html"

    def _track(self, url, headers):
        kind = self._kind(url)
        self.calls[kind] = self.calls.get(kind, 0) + 1
        rem = headers.get("x-ratelimit-remaining")
        if rem is not None and rem.isdigit():
            if kind == "search":
                self.search_remaining = int(rem)
            elif kind == "core":
                self.core_remaining = int(rem)

    def get(self, url, accept="application/vnd.github+json", timeout=45):
        kind = self._kind(url)
        # pacing для search: не чаще 1 вызова в 6.4 с (лимит 10/мин)
        if kind == "search":
            gap = time.time() - self._last_search
            if gap < 6.4:
                time.sleep(6.4 - gap)
            self._last_search = time.time()
        for attempt in (1, 2):
            req = urllib.request.Request(url)
            req.add_header("User-Agent", UA)
            req.add_header("Accept", accept)
            if self.token:
                req.add_header("Authorization", "Bearer " + self.token)
            try:
                with urllib.request.urlopen(req, timeout=timeout) as r:
                    body = r.read()
                    self.last_headers = {k.lower(): v for k, v in r.headers.items()}
                    self._track(url, self.last_headers)
                    return r.status, body, self.last_headers
            except urllib.error.HTTPError as e:
                self.last_headers = {k.lower(): v for k, v in (e.headers or {}).items()}
                self._track(url, self.last_headers)
                st = e.code
                if st in (403, 429) and attempt == 1:
                    ra = self.last_headers.get("retry-after", "")
                    wait = int(ra) if ra.isdigit() else 15
                    log(f"  HTTP {st} на {url} → пауза {wait}с (retry 1/1)")
                    time.sleep(wait)
                    continue
                return st, (e.read() or b"")[:1000], self.last_headers
            except Exception as e:  # сеть: одна повторная попытка
                if attempt == 1:
                    time.sleep(5)
                    continue
                return -1, str(e).encode(), {}
        return -1, b"", {}


def gh_json(http, url, timeout=45):
    """GET api.github.com → JSON. None при 404/422. Исключение при 403 после retry."""
    st, body, hdrs = http.get(url, timeout=timeout)
    if st == 200:
        try:
            return json.loads(body.decode("utf-8"))
        except Exception as e:
            raise RuntimeError(f"битый JSON с {url}: {e}")
    if st in (404, 422):
        return None
    raise RuntimeError(f"HTTP {st} на {url}: {body[:200].decode('utf-8', 'replace')!r}")


def fetch_text(http, url, timeout=30):
    """Любая страница/raw как текст. None при ошибке."""
    st, body, _ = http.get(url, accept="text/plain, text/html, */*",
                           timeout=timeout)
    if st != 200:
        return None
    return body.decode("utf-8", "replace")


# ---------------------------------------------------------------- git-слой
def git_clone_bare(url, dest, timeout=150):
    if os.path.exists(dest):
        return True, "cached"
    try:
        p = subprocess.run(
            ["git", "clone", "-q", "--bare", "--single-branch",
             "--filter=blob:none", url, dest],
            timeout=timeout, capture_output=True, env=CLONE_ENV)
        return p.returncode == 0, (p.stderr or b"").decode("utf-8", "replace")[-200:]
    except subprocess.TimeoutExpired:
        return False, "timeout"
    except Exception as e:
        return False, str(e)


def git_info(dest):
    """Точные факты из голого клона: коммиты, последний коммит, дерево файлов."""
    info = {}
    p = subprocess.run(["git", "-C", dest, "rev-list", "--count", "HEAD"],
                       capture_output=True, env=CLONE_ENV)
    if p.returncode == 0 and p.stdout.strip().isdigit():
        info["commits"] = int(p.stdout.strip())
    p = subprocess.run(["git", "-C", dest, "log", "-1", "--format=%cI"],
                       capture_output=True, env=CLONE_ENV)
    if p.returncode == 0 and p.stdout.strip():
        info["last_commit_at"] = p.stdout.decode().strip()
    p = subprocess.run(
        ["git", "-c", "core.quotepath=off", "-C", dest,
         "ls-tree", "-r", "--name-only", "HEAD"],
        capture_output=True, env=CLONE_ENV)
    if p.returncode == 0:
        names = p.stdout.decode("utf-8", "replace").splitlines()
        info["file_count"] = len(names)
        info["files"] = names
    return info


# ---------------------------------------------------------------- сборщики
def check_rate_limit(http):
    """Возвращает словарь бюджетов. /rate_limit не тратит core-лимит."""
    try:
        d = gh_json(http, f"{API}/rate_limit")
        res = d.get("resources", {})
        out = {}
        for k in ("core", "search"):
            v = res.get(k, {})
            out[k] = {"limit": v.get("limit"), "remaining": v.get("remaining"),
                      "reset": v.get("reset")}
        return out
    except Exception as e:
        log(f"  rate_limit недоступен: {e}")
        return {}


def wait_for_core(http, need=20, max_wait=480):
    """Ждёт сброса core-лимита, опрашивая /rate_limit раз в 20 с."""
    waited = 0
    while waited < max_wait:
        b = check_rate_limit(http)
        rem = (b.get("core") or {}).get("remaining") or 0
        if rem >= need:
            log(f"  core-лимит доступен: {rem} вызовов")
            return True
        reset = (b.get("core") or {}).get("reset")
        if reset:
            log(f"  core={rem}; жду сброса ({waited}/{max_wait}с)")
        else:
            log(f"  core недоступен; жду ({waited}/{max_wait}с)")
        time.sleep(20)
        waited += 20
    log("  core-лимит так и не освободился — работаю на деградации")
    return False


def search_repos(http, user):
    """Инвентарь репозиториев через search API (работает при нулевом core)."""
    q = urllib.parse.quote(f"user:{user}")
    url = f"{API}/search/repositories?q={q}&per_page=100&sort=updated&order=desc"
    d = gh_json(http, url)
    if not d:
        return [], 0
    items = list(d.get("items", []))
    total = int(d.get("total_count", len(items)))
    page = 2
    while len(items) < total and page <= 5:
        d = gh_json(http, f"{url}&page={page}")
        if not d or not d.get("items"):
            break
        items.extend(d["items"])
        page += 1
    return items, total


def core_repos(http, user, max_pages=5):
    """Канонический инвентарь через core REST (полный набор полей)."""
    out = []
    for page in range(1, max_pages + 1):
        url = (f"{API}/users/{user}/repos?per_page=100&sort=pushed"
               f"&page={page}")
        d = gh_json(http, url)
        if not d:
            break
        out.extend(d)
        if len(d) < 100:
            break
    return out


def core_user(http, user):
    return gh_json(http, f"{API}/users/{user}")


def core_events(http, user, max_pages=3):
    out = []
    for page in range(1, max_pages + 1):
        d = gh_json(http, f"{API}/users/{user}/events/public?per_page=100&page={page}")
        if not d:
            break
        out.extend(d)
        if len(d) < 100:
            break
    return out


def search_commits_author(http, user):
    """Глобальный тотал коммитов автора через search API."""
    q = urllib.parse.quote(f"author:{user}")
    d = gh_json(http, f"{API}/search/commits?q={q}&per_page=1")
    if d and "total_count" in d:
        return int(d["total_count"])
    return None


def parse_contributions_svg(svg):
    """Календарь контрибуций со страницы github.com/users/<user>/contributions.
    Новый рендер — это <td> с data-date/data-level (без чисел), старый —
    <rect>. Числа берём из aria-label/data-count/<title>, если есть."""
    days, levels = {}, {}
    for m in re.finditer(r"<[a-zA-Z]+[^>]*data-date=\"(\d{4}-\d{2}-\d{2})\"[^>]*>",
                         svg):
        tag, date = m.group(0), m.group(1)
        ml = re.search(r'data-level="(\d+)"', tag)
        if ml:
            levels[date] = int(ml.group(1))
        c = None
        mc = re.search(r'data-count="(\d+)"', tag)
        ma = re.search(r'aria-label="(\d+)\s+contributions?', tag)
        if mc:
            c = int(mc.group(1))
        elif ma:
            c = int(ma.group(1))
        if c is not None:
            days[date] = c
    if len(days) < 100:
        for m in re.finditer(
                r"<[a-zA-Z]+[^>]*data-date=\"(\d{4}-\d{2}-\d{2})\"[^>]*>\s*"
                r"<title[^>]*>\s*(\d+)\s+contributions?", svg):
            days[m.group(1)] = int(m.group(2))
    return days, levels


def repo_tab_count(http, user):
    """Счётчик 'Repositories N' со страницы вкладки (кросс-чек total_count)."""
    text = fetch_text(http, f"https://github.com/{user}?tab=repositories")
    if not text:
        return None
    i = text.find("Repositories")
    if i < 0:
        return None
    seg = text[i:i + 400]
    m = re.search(r'title="(\d{1,5})"', seg) or re.search(r">([\d,]{1,7})<", seg)
    if m:
        try:
            return int(m.group(1).replace(",", ""))
        except ValueError:
            return None
    return None


def fetch_readme(http, user, repo, branch):
    """README репозитория через raw (HEAD-подобный перебор кандидатов)."""
    for cand in README_CANDIDATES:
        text = fetch_text(http,
                          f"https://raw.githubusercontent.com/{user}/{repo}/"
                          f"{branch}/{cand}")
        if text and len(text) > 10:
            return text, cand
    return None, None


# ------------------------------------------------------- claims-движок
# величина → ключевые слова (ru+en). Порядок: число ПЕРЕД словом или ПОСЛЕ.
KW = {
    "repositories": r"(?:репозитор|repositor|репо\b)",
    "commits": r"(?:коммит(?:ов|ам|ами|ах|ом|е|а|ы|у)?\b|commits?\b)",
    "contributions": r"(?:контрибуц|contributions?\b|вклад)",
    "followers": r"(?:подписчик|followers?\b)",
    "following": r"(?:following\b|подписок)",
    "songs": r"(?:песен|песни|песня|songs?\b|трек(?:ов|а|е|и|ам|ами|ах|ом|у)?\b)",
    "languages": r"(?:язык|languages?\b)",
    "tools": r"(?:инструмент|tools?\b)",
    "tests": r"(?:тест|tests?\b|PASS\b)",
    "worlds": r"(?:миров|мир[аы]?|worlds?\b)",
    "cells": r"(?:клет|геном)",
    "paintings": r"(?:полотен|полотна|полотно|картин|paintings?\b)",
    "pages": r"(?:страниц|pages?\b)",
    "rooms": r"(?:комнат|rooms?\b)",
    "dependencies": r"(?:зависимост|dependencies?\b)",
}
NUM = r"(\d[\d\s\u00a0.,]{0,9})"
# не брать числа, вклеенные в версии/пути: v107, /64, .5
NUM_GUARD = r"(?<![./\w-])"
# для after-паттерна: ещё и не в скобках (год «(2026)») и не в слове
NUM_GUARD2 = r"(?<![()\w./-])"
# строки-шум: HTML-теги, CSS, бейджи
SKIP_SUBSTR = ("<", "{", "}", "shields.io", "![", "width=")

CLAIM_ID = [0]


def parse_num(raw):
    try:
        return int(re.sub(r"[\s\u00a0,.]", "", raw))
    except Exception:
        return None


def strip_html(text):
    """HTML → текст (для витрин): script/style вырезать, теги → перевод
    строки (чтобы соседние текстовые узлы не склеивались в одну строку)."""
    text = re.sub(r"<script[\s\S]*?</script>", " ", text, flags=re.I)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "\n", text)
    return text.replace("&nbsp;", " ").replace("&amp;", "&")


def extract_claims(text, source_type, source_ref, scope="global", user=""):
    """Достаёт численные утверждения из текста: '64 песни', 'N коммитов',
    '3800+', '33/33 PASS'. Ссылка github.com/user/repo в строке привязывает
    claim к этому репо; '- **имя** — N коммитов' тоже."""
    claims = []
    seen = set()

    def add(qty, val, plus, line, fscope):
        if val is None or val > 10**7 or val == 0:
            return
        if 1990 <= val <= 2099 and re.search(
                rf"{val}\s*(?:года|год|году|г\.)", line, re.IGNORECASE):
            return  # год-контекст («каталог 2026 года»), не счёт
        key = (qty, val, fscope)
        if key in seen:
            return
        seen.add(key)
        CLAIM_ID[0] += 1
        claims.append({"id": CLAIM_ID[0], "qty": qty, "value": val,
                       "plus": plus, "ratio": None, "scope": fscope,
                       "source_type": source_type,
                       "source_ref": source_ref, "quote": line[:160]})

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or len(line) > 300:
            continue
        if any(x in line for x in SKIP_SUBSTR):
            continue
        if re.match(r"^\|?\s*v\d+\.\d+", line):
            continue  # changelog-строки: исторические состояния, не текущие
        # ссылка на репо в строке → claims строки относятся к этому репо
        line_scope = scope
        if user and scope == "global":
            mrepo = re.search(
                r"github\.com/" + re.escape(user) + r"/([A-Za-z0-9_.-]+)",
                line)
            if mrepo:
                line_scope = mrepo.group(1)
        # «- **имя** — N коммитов» → точная проверка коммитов репо «имя»
        mb = re.match(
            r"^[-*|]\s*\*{0,2}([^*<|]+?)\*{0,2}\s*[—–-]\s*~?(\d[\d\s\u00a0.,]*)"
            r"\s*\+?\s*(?:коммит|commit)", line, re.IGNORECASE)
        if mb:
            nm = mb.group(1).strip().strip("`")
            v = parse_num(mb.group(2))
            if v and nm and "всего" not in nm.lower():
                add("commits", v, "+" in mb.group(0), line, fscope=nm)
                continue
        for qty, kw in KW.items():
            if qty == "tests":
                ratio = re.search(
                    r"(\d{1,4})\s*/\s*(\d{1,4})\s*(?:" + kw + r")",
                    line, re.IGNORECASE)
                if ratio:
                    rv = f"{ratio.group(1)}/{ratio.group(2)}"
                    key = ("tests_ratio", rv, line_scope)
                    if key not in seen:
                        seen.add(key)
                        CLAIM_ID[0] += 1
                        claims.append({"id": CLAIM_ID[0],
                                       "qty": "tests_ratio", "value": None,
                                       "ratio": rv, "plus": False,
                                       "scope": line_scope,
                                       "source_type": source_type,
                                       "source_ref": source_ref,
                                       "quote": line[:160]})
            before = re.search(
                NUM_GUARD + NUM +
                r"\s*\+?\s*(?:public\s+|публичн\w*\s+|private\s+)?" +
                r"(?:шт\.?\s*)?(?:" + kw + r")",
                line, re.IGNORECASE)
            after = re.search(
                r"(?:" + kw + r")[^0-9%]{0,24}" + NUM_GUARD2 + NUM +
                r"(?![\w-])",
                line, re.IGNORECASE)
            m = before or after
            if not m:
                continue
            plus = bool(re.search(
                NUM_GUARD + NUM + r"\s*\+\s*(?:" + kw + r")", line,
                re.IGNORECASE))
            add(qty, parse_num(m.group(1)), plus, line, fscope=line_scope)
    return claims


# ------------------------------------------------------- deep-проверки
DEEP_TARGETS = [
    {"repo": "singulyar", "qty": "songs", "handler": "corpus_index"},
    {"repo": "singulyar", "qty": "languages", "handler": "corpus_index"},
    {"repo": "polymath-tools-atlas", "qty": "tools",
     "pats": [r"(?i)catalog", r"(?i)tool", r"(?i)atlas"]},
]
DATA_EXT = (".json", ".csv", ".md", ".txt", ".yaml", ".yml")

_CORPUS_CACHE = {}


def deep_corpus_index(files, http, user, branch):
    """RUVSON-корпус: машиночитаемый ИНДЕКС_МУЛЬТИЯЗЫК.md → песни × языки.
    Файлы-семплы в дереве не отражают корпус (31 RUVSON-файл = сэмплы),
    поэтому счёт — по индексу, дата сборки фиксируется в методе."""
    if "corpus_index" in _CORPUS_CACHE:
        return _CORPUS_CACHE["corpus_index"]
    out, note = {}, ""
    cand = sorted(f for f in files
                  if "ИНДЕКС_МУЛЬТИЯЗЫК" in f and f.endswith(".md"))
    if not cand:
        note = "ИНДЕКС_МУЛЬТИЯЗЫК.md не найден в дереве"
        _CORPUS_CACHE["corpus_index"] = (out, note)
        return out, note
    path = cand[0]
    text = fetch_text(http, f"https://raw.githubusercontent.com/{user}/"
                      f"singulyar/{branch}/{urllib.parse.quote(path)}")
    if not text:
        note = "ИНДЕКС не скачался через raw"
        _CORPUS_CACHE["corpus_index"] = (out, note)
        return out, note
    mdate = re.search(r"Дата сборки:\s*(\d{4}-\d{2}-\d{2})", text)
    stamp = mdate.group(1) if mdate else "дата не указана"
    msongs = re.search(r"Песен в корпусе[^0-9]{0,40}(\d+)", text)
    mlang = re.search(r"полным набором\s+(\d+)\s+языков", text)
    if msongs:
        out["songs"] = (int(msongs.group(1)),
                        f"индекс корпуса RUVSON @ {path} (сборка {stamp})")
    else:
        note = "в ИНДЕКСЕ нет строки «Песен в корпусе»"
    if mlang:
        out["languages"] = (int(mlang.group(1)),
                            f"индекс корпуса RUVSON @ {path} (сборка {stamp})")
    elif msongs:
        mpairs = re.search(r"«песня × язык»[^0-9]{0,60}?(\d+)\s*из", text)
        if mpairs:
            out["languages"] = (
                round(int(mpairs.group(1)) / int(msongs.group(1))),
                f"пары «песня × язык» ÷ песни @ {path} (сборка {stamp})")
    _CORPUS_CACHE["corpus_index"] = (out, note)
    return out, note


DEEP_HANDLERS = {"corpus_index": deep_corpus_index}


def count_text(fname, text):
    """Считает записи в data-файле. Возвращает (число|None, метод)."""
    low = fname.lower()
    if low.endswith(".json"):
        try:
            d = json.loads(text)
        except Exception:
            return None, "json-битый"
        if isinstance(d, list):
            return len(d), "json-list"
        if isinstance(d, dict):
            if len(d) >= 5:
                return len(d), "json-dict"
            for k, v in d.items():
                if isinstance(v, list) and len(v) >= 5:
                    return len(v), f"json-dict[{k}]"
            return None, "json-мелкий"
    if low.endswith(".csv"):
        lines = [l for l in text.splitlines() if l.strip()]
        return max(0, len(lines) - 1), "csv-строки-данных"
    if low.endswith((".md", ".txt")):
        rows = sum(1 for l in text.splitlines()
                   if l.strip().startswith("|") and not
                   re.match(r"^\|[\s\-|:]+\|$", l.strip()))
        bullets = sum(1 for l in text.splitlines()
                      if re.match(r"^\s*[-*]\s+\S", l))
        if rows >= 10:
            return rows, "md-таблица-строк"
        if bullets >= 10:
            return bullets, "md-буллеты"
    return None, "не-распарсено"


def deep_check(target, gitdata, http, user, branches):
    """Считает величину (songs/tools/...) по живым файлам репо:
    дерево из blob-less клона → хендлер/кандидаты → raw → парсер."""
    repo, qty = target["repo"], target["qty"]
    g = gitdata.get(repo) or {}
    files = g.get("files")
    if not files:
        return {"qty": qty, "repo": repo, "value": None,
                "note": "нет клона/дерева — deep недоступен"}
    handler = target.get("handler")
    if handler:
        res, note = DEEP_HANDLERS[handler](files, http, user,
                                           branches.get(repo, "HEAD"))
        hit = res.get(qty)
        if hit:
            return {"qty": qty, "repo": repo, "value": hit[0],
                    "method": hit[1], "note": ""}
        return {"qty": qty, "repo": repo, "value": None,
                "note": note or "хендлер не дал числа"}
    pats = target["pats"]
    matches = [f for f in files
               if any(re.search(p, f) for p in pats)
               and f.lower().endswith(DATA_EXT)]
    if not matches:
        return {"qty": qty, "repo": repo, "value": None,
                "note": "в дереве нет файлов-кандидатов "
                        f"(файлов всего {g.get('file_count')})"}
    if len(matches) >= 10:
        return {"qty": qty, "repo": repo, "value": len(matches),
                "method": f"file-count ({len(matches)} файлов-кандидатов)",
                "note": "много одноструктурных файлов → счёт по файлам"}
    branch = branches.get(repo, "HEAD")
    matches.sort(key=lambda f: (not f.lower().endswith(".json"),
                                f.count("/"), f))
    tried = []
    for f in matches[:6]:
        text = fetch_text(http,
                          f"https://raw.githubusercontent.com/{user}/{repo}/"
                          f"{branch}/{urllib.parse.quote(f)}")
        tried.append(f)
        if not text:
            continue
        n, method = count_text(f, text)
        if n:
            return {"qty": qty, "repo": repo, "value": n,
                    "method": f"{method} @ {f}", "note": ""}
    return {"qty": qty, "repo": repo, "value": None,
            "note": "кандидаты не распарсились: " + ", ".join(tried[:4])}


# ------------------------------------------------------- верификация
def verify_claim(claim, facts, deep_map):
    """Сверяет claim с живыми фактами. Заполняет fact/verdict/delta/note."""
    qty, scope = claim["qty"], claim["scope"]
    value, plus = claim.get("value"), claim.get("plus")
    ratio = claim.get("ratio")
    out = {"fact": None, "verdict": "UNVERIFIABLE", "delta": None,
           "note": "", "method": "api"}

    def finish(verdict, fact, note="", method="api", delta=None):
        out.update(verdict=verdict, fact=fact, note=note, method=method)
        if delta is not None:
            out["delta"] = delta

    if qty.endswith("_ratio"):
        finish("UNVERIFIABLE", None,
               "требует прогона CI/make verify — API-факта не существует")
        return out

    if qty == "repositories":
        if scope != "global":
            finish("UNVERIFIABLE", None,
                   "внутрирепозиторная семантика («собран из N репо») — "
                   "не сверяется с public_repos")
            return out
        fact = facts.get("public_repos")
        note = "core /users"
        if fact is None:
            fact = facts.get("search_repos_total")
            note = "search /repositories"
        if fact is None:
            finish("UNVERIFIABLE", None, "нет источника")
        else:
            extra = [f"len(core_repos)={facts.get('core_repos_len')}"
                     if facts.get("core_repos_len") is not None else "",
                     f"search={facts.get('search_repos_total')}"
                     if facts.get("search_repos_total") is not None else "",
                     f"html-tab={facts.get('repos_tab_count')}"
                     if facts.get("repos_tab_count") is not None else ""]
            finish("PASS" if fact == value else "FAIL", fact,
                   (note + "; " + "; ".join(e for e in extra if e)).strip("; "))
        return out

    if qty == "commits":
        q = (claim.get("quote") or "")
        if re.search(r"за\s+(?:январ|феврал|март|апрел|ма[йея]|июн|июл|"
                     r"август|сентябр|октябр|ноябр|декабр|\d{4})", q,
                     re.IGNORECASE):
            finish("UNVERIFIABLE", None,
                   "статистика за период, не совокупный тотал")
            return out
        if scope != "global" and "всего" not in q.lower() \
                and "живых репо" not in q.lower():
            per = (facts.get("per_repo") or {}).get(scope)
            if per is None:
                finish("UNVERIFIABLE", None, "нет клона репозитория")
            elif per >= value:
                finish("PASS", per,
                       "git rev-list --count (blob-less клон, default "
                       f"branch); вырос на +{per - value} с даты утверждения",
                       method="git", delta=per - value)
            else:
                drift = abs(per - value) / max(1, value)
                finish("DRIFT" if drift <= 0.05 else "FAIL", per,
                       "git rev-list --count (blob-less клон); расхождение "
                       f"{per - value:+d}", method="git")
            return out
        cands = []
        a = facts.get("commits_author_search")
        s = facts.get("commits_sum_clones")
        if a is not None:
            cands.append(("search /commits (автор)", a))
        if s is not None:
            cands.append(("сумма git rev-list по клонам (все авторы)", s))
        if not cands:
            finish("UNVERIFIABLE", None, "нет источника")
            return out
        if plus:
            best = max(cands, key=lambda x: x[1])
            extra = ("; прочие: " + "; ".join(f"{n}={v}" for n, v in cands
                    if (n, v) != best)) if len(cands) > 1 else ""
            finish("PASS" if best[1] >= value else "FAIL", best[1],
                   best[0] + f"; требовалось ≥{value}" + extra)
            return out
        best = min(cands, key=lambda x: abs(x[1] - value))
        note = best[0]
        if len(cands) > 1:
            note += " | прочие источники: " + "; ".join(
                f"{n}={v}" for n, v in cands if (n, v) != best)
        if best[1] >= value:
            finish("PASS", best[1], note +
                   f"; вырос на +{best[1] - value} с даты утверждения",
                   delta=best[1] - value)
        else:
            drift = abs(best[1] - value) / max(1, value)
            finish("DRIFT" if drift <= 0.05 else "FAIL", best[1],
                   note + f"; расхождение {best[1] - value:+d} "
                   f"({drift:.1%})")
        return out

    if qty == "contributions":
        fact = facts.get("contrib_year_sum")
        if fact is None:
            finish("UNVERIFIABLE", None,
                   "SVG-календарь не дал чисел (только уровни)")
        elif plus:
            finish("PASS" if fact >= value else "FAIL", fact,
                   "сумма за календарный год из SVG-календаря")
        else:
            finish("PASS" if fact >= value else "DRIFT", fact,
                   f"SVG-календарь; расхождение {fact - value:+d}")
        return out

    if qty in ("followers", "following"):
        fact = facts.get(qty)
        if fact is None:
            finish("UNVERIFIABLE", None, "нет core /users")
        else:
            finish("PASS" if fact == value else "FAIL", fact, "core /users")
        return out

    if qty in ("songs", "languages", "tools"):
        hit = deep_map.get((scope, qty))
        if hit:
            ok = hit["value"] == value or (plus and hit["value"] >= value)
            finish("PASS" if ok else "FAIL", hit["value"],
                   f"deep: {hit.get('method', '')} [{hit.get('repo')}]",
                   method="deep")
            return out
        if scope == "global":
            g = deep_map.get(("*", qty))
            if g:
                ok = g["value"] == value or (plus and g["value"] >= value)
                finish("PASS" if ok else "FAIL", g["value"],
                       "deep (единственный источник): "
                       f"{g.get('method', '')} [{g.get('repo')}]",
                       method="deep")
                return out
            finish("UNVERIFIABLE", None, "deep-источник не дал числа")
            return out
        known = [f"{r}: {q}={v.get('value')}"
                 for (r, q), v in deep_map.items()
                 if r != "*" and v.get("value") is not None]
        finish("UNVERIFIABLE", None,
               "в этом репо нет deep-источника; глубокий счёт сделан для: "
               + (", ".join(known) or "ничего"))
        return out

    finish("UNVERIFIABLE", None,
           "структурная/внутренняя величина: в GitHub API факта нет "
           "(нужна проверка внутри репо или CI)")
    return out


def consistency_flags(claims):
    """Одна и та же величина с разными числами в разных источниках."""
    groups = {}
    for c in claims:
        if c.get("value") is None:
            continue
        key = (c["qty"], c["scope"])
        groups.setdefault(key, []).append(c)
    flags = []
    for (qty, scope), cs in groups.items():
        vals = sorted({c["value"] for c in cs})
        if len(vals) > 1:
            flags.append({
                "qty": qty, "scope": scope, "values": vals,
                "sources": [{"value": c["value"], "source_ref":
                             c["source_ref"], "quote": c["quote"][:100]}
                            for c in cs]})
    return flags


# ------------------------------------------------------- агрегаты
def build_aggregates(repos, gitdata, facts):
    langs, licenses, topics = {}, {}, {}
    stars = forks = size_kb = issues = 0
    archived, is_fork, no_desc, no_license = [], [], [], []
    stale30, fresh7 = [], []
    newest = oldest = None
    for r in repos:
        name = r.get("name")
        lang = r.get("language")
        if lang:
            langs[lang] = langs.get(lang, 0) + 1
        lic = (r.get("license") or {}).get("spdx_id")
        if lic and lic != "NOASSERTION":
            licenses[lic] = licenses.get(lic, 0) + 1
        else:
            no_license.append(name)
        for t in r.get("topics") or []:
            topics[t] = topics.get(t, 0) + 1
        stars += r.get("stargazers_count") or 0
        forks += r.get("forks_count") or 0
        size_kb += r.get("size") or 0
        issues += r.get("open_issues_count") or 0
        if r.get("archived"):
            archived.append(name)
        if r.get("fork"):
            is_fork.append(name)
        if not (r.get("description") or "").strip():
            no_desc.append(name)
        pushed = r.get("pushed_at")
        d = days_between(pushed) if pushed else None
        if d is not None:
            if d > 30:
                stale30.append({"name": name, "days": d})
            if d <= 7:
                fresh7.append(name)
        if pushed and (newest is None or pushed > newest[1]):
            newest = (name, pushed)
        created = r.get("created_at")
        if created:
            if oldest is None or created < oldest[1]:
                oldest = (name, created)
    commits_sum = sum((g.get("commits") or 0) for g in gitdata.values())
    return {
        "repos_count": len(repos),
        "stars_total": stars,
        "forks_total": forks,
        "size_kb_total": size_kb,
        "size_gb_total": round(size_kb / 1024 / 1024, 2),
        "open_issues_total": issues,
        "languages": dict(sorted(langs.items(), key=lambda x: -x[1])),
        "licenses": licenses,
        "topics_top": dict(sorted(topics.items(), key=lambda x: -x[1])[:12]),
        "archived": archived,
        "forks_of_others": is_fork,
        "no_description": no_desc,
        "no_license": no_license,
        "stale_30d": sorted(stale30, key=lambda x: -x["days"])[:40],
        "fresh_7d": fresh7,
        "newest_push": newest,
        "oldest_created": oldest,
        "commits_sum_clones": commits_sum,
        "clone_ok": sum(1 for g in gitdata.values() if g.get("commits") is
                        not None),
        "clone_fail": sum(1 for g in gitdata.values()
                          if g.get("commits") is None),
    }


# ------------------------------------------------------- HTML-отчёт
CSS = """
:root{--bg:#0b0b0e;--panel:#131318;--panel2:#181820;--line:#26262e;
--gold:#d9a441;--gold2:#f0c76a;--text:#ece7db;--muted:#8f8a7f;
--pass:#3f9d6b;--fail:#cf5555;--drift:#d9a441;--unver:#7c766a;--git:#6f9dc9}
*{box-sizing:border-box}html{scroll-behavior:smooth}
body{margin:0;background:var(--bg);color:var(--text);
font:16px/1.55 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
a{color:var(--gold2);text-decoration:none}a:hover{text-decoration:underline}
.wrap{max-width:1200px;margin:0 auto;padding:0 clamp(12px,3vw,28px)}
header{border-bottom:2px solid var(--gold);padding:clamp(18px,4vw,34px) 0;
background:linear-gradient(180deg,#141018, var(--bg))}
h1{font-size:clamp(1.5rem,5vw,2.6rem);margin:0 0 6px;letter-spacing:.04em}
h1 b{color:var(--gold)}
.sub{color:var(--muted);font-size:clamp(.85rem,2.5vw,1rem)}
.sub code{color:var(--gold2)}
h2{font-size:clamp(1.15rem,3.5vw,1.5rem);margin:34px 0 12px;
border-left:4px solid var(--gold);padding-left:10px}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));
gap:10px;margin:18px 0}
.tile{background:var(--panel);border:1px solid var(--line);border-radius:10px;
padding:14px 12px}
.tile .v{font-size:clamp(1.3rem,4vw,1.9rem);font-weight:700;color:var(--gold2);
font-variant-numeric:tabular-nums}
.tile .k{color:var(--muted);font-size:.78rem;text-transform:uppercase;
letter-spacing:.06em;margin-top:2px}
.tile .s{color:var(--muted);font-size:.72rem}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;
padding:clamp(10px,2vw,18px);margin:10px 0}
.scroll{overflow-x:auto;-webkit-overflow-scrolling:touch}
table{border-collapse:collapse;width:100%;font-size:.86rem;min-width:640px}
th{background:var(--panel2);color:var(--gold);text-align:left;
padding:7px 9px;border-bottom:2px solid var(--line);white-space:nowrap;
cursor:pointer;user-select:none}
td{padding:6px 9px;border-bottom:1px solid var(--line);vertical-align:top}
tr:hover td{background:#191922}
.num{font-variant-numeric:tabular-nums;white-space:nowrap}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:.82rem}
.muted{color:var(--muted)}.small{font-size:.78rem}
.badge{display:inline-block;padding:1px 8px;border-radius:20px;font-size:.72rem;
font-weight:700;letter-spacing:.04em;white-space:nowrap}
.v-PASS{background:#14301f;color:#5fd394;border:1px solid #2c6e46}
.v-FAIL{background:#331515;color:#f08a8a;border:1px solid #7e3434}
.v-DRIFT{background:#33260f;color:#e9be62;border:1px solid #7e6128}
.v-UNVERIFIABLE{background:#1c1c1c;color:#9a948a;border:1px solid #3a3a3a}
.v-archived{background:#2b1a1a;color:#d98a8a;border:1px solid #6e3a3a}
.v-fork{background:#1a2430;color:#8ab6e0;border:1px solid #2e4a66}
.fbar{margin:6px 0 12px;display:flex;flex-wrap:wrap;gap:6px}
.fbtn{background:var(--panel2);color:var(--text);border:1px solid var(--line);
border-radius:20px;padding:3px 12px;font-size:.78rem;cursor:pointer}
.fbtn.on{border-color:var(--gold);color:var(--gold2)}
.flag{border:1px solid #6e4a20;background:#241a0c;border-radius:8px;
padding:10px 12px;margin:8px 0;font-size:.88rem}
.flag b{color:var(--gold2)}
.method{font-size:.8rem;color:var(--muted)}
footer{border-top:1px solid var(--line);margin-top:36px;padding:18px 0 40px;
color:var(--muted);font-size:.78rem}
svg.chart{width:100%;height:auto;display:block}
details{margin:8px 0}summary{cursor:pointer;color:var(--gold2);font-size:.85rem}
pre{background:#0e0e12;border:1px solid var(--line);border-radius:8px;
padding:10px;overflow-x:auto;font-size:.72rem;color:#b8b2a4}
@media(max-width:640px){table{min-width:560px}.tile{padding:10px}}
"""

JS = """
function sortTable(th){
  var table=th.closest('table'),tbody=table.querySelector('tbody');
  var idx=Array.prototype.indexOf.call(th.parentNode.children,th);
  var rows=Array.prototype.slice.call(tbody.rows),dir=th.dataset.dir==='asc'?-1:1;
  th.dataset.dir=dir===1?'asc':'desc';
  rows.sort(function(a,b){
    var x=a.children[idx].dataset.v||a.children[idx].textContent.trim();
    var y=b.children[idx].dataset.v||b.children[idx].textContent.trim();
    var nx=parseFloat(x.replace(/\\s/g,'')),ny=parseFloat(y.replace(/\\s/g,''));
    if(!isNaN(nx)&&!isNaN(ny)){return (nx-ny)*dir}
    return x.localeCompare(y)*dir});
  rows.forEach(function(r){tbody.appendChild(r)});
}
document.querySelectorAll('th[data-sort]').forEach(function(th){
  th.addEventListener('click',function(){sortTable(th)})});
var fbtns=document.querySelectorAll('.fbtn');
fbtns.forEach(function(b){b.addEventListener('click',function(){
  var v=b.dataset.v,rows=document.querySelectorAll('#claims tbody tr');
  fbtns.forEach(function(x){x.classList.toggle('on',x===b)});
  rows.forEach(function(r){
    r.style.display=(v==='all'||r.dataset.verdict===v)?'':'none'})})});
"""


def esc(s):
    return html_mod.escape(str(s if s is not None else ""), quote=True)


def fmt_int(n):
    if n is None:
        return "—"
    return f"{n:,}".replace(",", " ")


def badge(v):
    return f'<span class="badge v-{esc(v)}">{esc(v)}</span>'


def svg_calendar(days, levels):
    """Годовой SVG-график: столбики по числу контрибуций, иначе по уровню."""
    dates = sorted(set(days) | set(levels))
    if not dates:
        return ("<p class='muted'>Календарь недоступен (SVG без числовых "
                "данных).</p>")
    dates = dates[-365:]
    use_counts = len(days) >= 100
    src = days if use_counts else levels
    maxv = max(src.values()) or 1
    W, H = 760, 132
    bw = W / len(dates)
    parts = [f"<svg class='chart' viewBox='0 0 {W} {H}' role='img' "
             f"aria-label='Календарь контрибуций за год'>"]
    parts.append(f"<line x1='0' y1='{H-14}' x2='{W}' y2='{H-14}' "
                 f"stroke='#26262e'/>")
    last_month = None
    months = ["янв", "фев", "мар", "апр", "май", "июн", "июл", "авг",
              "сен", "окт", "ноя", "дек"]
    for i, d in enumerate(dates):
        v = src.get(d, 0)
        frac = (v / maxv) if use_counts else ((v + 1) / 5)
        h = 6 + frac * 100
        x = i * bw
        op = 0.35 + 0.65 * frac
        color = "#d9a441" if v else "#33322b"
        parts.append(f"<rect x='{x:.1f}' y='{H-14-h:.1f}' width='{bw*0.7:.1f}' "
                     f"height='{h:.1f}' fill='{color}' opacity='{op:.2f}'/>")
        m = d[5:7]
        if m != last_month and i > 0:
            parts.append(f"<text x='{x:.1f}' y='{H-3}' font-size='9' "
                         f"fill='#8f8a7f'>{months[int(m)-1]}</text>")
        last_month = m
    parts.append("</svg>")
    cap = "число контрибуций в день" if use_counts else "уровень активности 0–4 (SVG без точных чисел)"
    peak = max(src, key=lambda k: src[k])
    return ("".join(parts) +
            f"<p class='method'>Источник: github.com/users/*/contributions "
            f"(HTML). Режим: {cap}. Пик: {peak} = {src[peak]}.</p>")


def month_totals(days):
    out = {}
    for d, c in days.items():
        out[d[:7]] = out.get(d[:7], 0) + c
    return dict(sorted(out.items()))


def build_html(data):
    meta = data["meta"]
    prof = data["profile"]
    agg = data["aggregates"]
    claims = data["claims"]
    flags = data["contradictions"]
    repos = data["repos"]
    gitd = data["git"]
    act = data["activity"]
    facts = data["facts"]

    verd = {}
    for c in claims:
        verd[c["verdict"]] = verd.get(c["verdict"], 0) + 1
    verd_str = " · ".join(f"{k}: {v}" for k, v in sorted(verd.items()))

    tiles = [
        ("Репозитории", fmt_int(facts.get("public_repos",
            facts.get("search_repos_total"))), "core /users"),
        ("Коммиты автора", fmt_int(facts.get("commits_author_search")),
         "search /commits"),
        ("Коммиты (клоны)", fmt_int(agg.get("commits_sum_clones")),
         f"git rev-list × {agg.get('clone_ok')} репо"),
        ("Контрибуции, год", fmt_int(facts.get("contrib_year_sum")),
         "SVG-календарь"),
        ("Followers", fmt_int(facts.get("followers")), "core /users"),
        ("Звёзд всего", fmt_int(agg.get("stars_total")), "сумма по репо"),
    ]
    tile_html = "".join(
        f"<div class='tile'><div class='v'>{v}</div>"
        f"<div class='k'>{k}</div><div class='s'>{s}</div></div>"
        for k, v, s in tiles)

    # --- факты профиля
    psrc = ("core /users" if str(meta.get("profile_source", "")).startswith(
        "core") else "кэш core /users")
    prof_rows = [
        ("login", prof.get("login"), psrc),
        ("создан", (prof.get("created_at") or "—")[:10], psrc),
        ("public_repos", fmt_int(facts.get("public_repos")), psrc),
        ("search total_count", fmt_int(facts.get("search_repos_total")),
         "search /repositories"),
        ("len(core repos)", fmt_int(facts.get("core_repos_len")),
         "core /repos"),
        ("вкладка Repositories", fmt_int(facts.get("repos_tab_count")),
         "html:github.com"),
        ("followers / following",
         f"{fmt_int(facts.get('followers'))} / {fmt_int(facts.get('following'))}",
         psrc),
        ("bio", (prof.get("bio") or "—")[:200], psrc),
        ("blog", prof.get("blog") or "—", psrc),
        ("location", prof.get("location") or "—", psrc),
    ]
    prof_html = "".join(
        f"<tr><td class='mono'>{esc(k)}</td><td>{esc(v)}</td>"
        f"<td class='muted small'>{esc(s)}</td></tr>"
        for k, v, s in prof_rows)

    # --- claims
    verdicts = ["PASS", "FAIL", "DRIFT", "UNVERIFIABLE"]
    fbar = "<button class='fbtn on' data-v='all'>все</button>" + "".join(
        f"<button class='fbtn' data-v='{v}'>{v} · {verd.get(v, 0)}</button>"
        for v in verdicts)
    claim_rows = []
    for c in claims:
        quote = esc(c.get("quote") or "")
        fact = c.get("fact")
        fact_s = "—" if fact is None else fmt_int(fact)
        if c.get("ratio"):
            val_s = esc(c["ratio"])
        else:
            val_s = fmt_int(c.get("value")) + ("+" if c.get("plus") else "")
        note = esc(c.get("note") or "")
        src = f"{esc(c['source_type'])}<br><span class='muted small'>{esc(c['source_ref'])}</span>"
        claim_rows.append(
            f"<tr data-verdict='{esc(c['verdict'])}'>"
            f"<td class='num'>{c['id']}</td>"
            f"<td class='mono'>{esc(c['qty'])}"
            f"{' @' + esc(c['scope']) if c['scope'] != 'global' else ''}</td>"
            f"<td class='num'>{val_s}</td><td>{src}<br>"
            f"<span class='muted small'>«{quote}»</span></td>"
            f"<td class='num'>{fact_s}<br><span class='method'>{note}</span></td>"
            f"<td>{badge(c['verdict'])}</td></tr>")
    claims_html = "".join(claim_rows)

    # --- противоречия
    if flags:
        flag_html = "".join(
            f"<div class='flag'><b>{esc(f['qty'])}"
            f"{' @ ' + esc(f['scope']) if f['scope'] != 'global' else ''}"
            f"</b> — разные числа в живых источниках: "
            f"{', '.join(esc(str(v)) for v in f['values'])}. "
            + "".join(
                f"<div class='small muted'>{esc(s['value'])} ← "
                f"{esc(s['source_ref'])}: «{esc(s['quote'][:80])}»</div>"
                for s in f["sources"][:4]) + "</div>"
            for f in flags)
    else:
        flag_html = "<p class='muted'>Перекрёстных противоречий между источниками не найдено.</p>"

    # --- реестр репо
    repo_rows = []
    for r in sorted(repos, key=lambda x: x.get("pushed_at") or "", reverse=True):
        name = r.get("name")
        lic = r.get("license") or "—"
        days_push = r.get("days_since_push")
        status = ""
        if r.get("archived"):
            status += "<span class='badge v-archived'>archived</span> "
        if r.get("fork"):
            status += "<span class='badge v-fork'>fork</span>"
        desc = esc((r.get("description") or "")[:110])
        repo_rows.append(
            f"<tr><td><a href='{esc(r.get('html_url'))}'><b>{esc(name)}</b></a>"
            f"<br><span class='muted small'>{desc}</span></td>"
            f"<td class='mono small'>{esc(r.get('language') or '—')}</td>"
            f"<td class='mono small'>{esc(lic)}</td>"
            f"<td class='num'>{fmt_int(r.get('stars'))}</td>"
            f"<td class='num'>{fmt_int(r.get('forks'))}</td>"
            f"<td class='num'>{fmt_int(r.get('open_issues'))}</td>"
            f"<td class='num'>{fmt_int(r.get('size_kb'))}</td>"
            f"<td class='num' data-v='{r.get('git_commits') or 0}'>"
            f"{fmt_int(r.get('git_commits'))}</td>"
            f"<td class='num' data-v='{r.get('git_file_count') or 0}'>"
            f"{fmt_int(r.get('git_file_count'))}</td>"
            f"<td class='num small'>{esc((r.get('created_at') or '')[:10])}</td>"
            f"<td class='num small' data-v='{days_push or 0}'>"
            f"{esc((r.get('pushed_at') or '')[:10])}"
            f"{'<br><span class=muted>'+str(days_push)+' дн</span>' if days_push is not None else ''}</td>"
            f"<td>{status}</td></tr>")
    repos_html = "".join(repo_rows)

    # --- здоровье
    def lst(items):
        return esc(", ".join(items)) if items else "—"
    health_rows = [
        ("archived (мёртвые клетки)", lst(agg["archived"])),
        ("fork чужого", lst(agg["forks_of_others"])),
        ("без описания", lst(agg["no_description"][:20])),
        ("без лицензии", lst(agg["no_license"][:20])),
        ("не пушались >30 дней",
         esc(", ".join(f"{s['name']} ({s['days']}д)"
                       for s in agg["stale_30d"][:20])) or "—"),
        ("пуш за последние 7 дней", lst(agg["fresh_7d"])),
    ]
    health_html = "".join(
        f"<tr><td class='small'>{esc(k)}</td><td class='small'>{v}</td></tr>"
        for k, v in health_rows)

    # --- языки
    lang_html = "".join(
        f"<div><span class='mono small'>{esc(l)}</span> "
        f"<span class='muted small'>{n}</span></div>"
        for l, n in list(agg["languages"].items())[:12])

    ev = act.get("events")
    ev_html = ""
    if ev:
        types = ", ".join(f"{esc(k)}: {v}" for k, v in
                          sorted(ev["types"].items(), key=lambda x: -x[1]))
        ev_html = (f"<p class='small muted'>События core /events (окно "
                   f"{esc(ev['window'][1][:10])} … {esc(ev['window'][0][:10])}, "
                   f"{ev['count']} шт): {types}; PushEvent коммитов: "
                   f"{fmt_int(ev['push_commits'])}.</p>")

    months = month_totals(act.get("calendar_days") or {})
    months_html = ""
    if months:
        cells = "".join(
            f"<div class='tile'><div class='v'>{fmt_int(v)}</div>"
            f"<div class='k'>{esc(k)}</div></div>"
            for k, v in months.items())
        months_html = f"<div class='tiles'>{cells}</div>"

    calls = meta.get("calls", {})
    budget_line = " · ".join(f"{k}: {v}" for k, v in calls.items())
    data_json = json.dumps(data, ensure_ascii=False,
                           default=str).replace("</", "<\\/")

    return f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(meta['user'])} — инвентаризация по живому GitHub API</title>
<style>{CSS}</style></head><body>
<header><div class="wrap">
<h1><b>{esc(meta['user'].upper())}</b> · ИНВЕНТАРИЗАЦИЯ ПО ЖИВОМУ API</h1>
<div class="sub">МЕХАНИЗМ inv-01 · прогон {esc(meta['run_started'])} →
{esc(meta['run_finished'])} · ни одной цифры из README без сверки</div>
</div></header>
<main class="wrap">
<h2>Сводка</h2>
<div class="tiles">{tile_html}</div>
<div class="panel small muted">Вердикты утверждений: {esc(verd_str)}.
Вызовы источников — {esc(budget_line)}; git-клонов: {meta.get('git_clones', 0)}.
Ошибки прогона: {len(meta.get('errors', []))}.</div>

<h2>Факты профиля</h2>
<div class="panel"><div class="scroll"><table>
<thead><tr><th>Поле</th><th>Живое значение</th><th>Источник</th></tr></thead>
<tbody>{prof_html}</tbody></table></div></div>

<h2>Активность за год</h2>
<div class="panel">{svg_calendar(act.get('calendar_days') or {},
 act.get('calendar_levels') or {})}{months_html}{ev_html}</div>

<h2>Сверка утверждений (claims)</h2>
<p class="small muted">Каждое численное утверждение из био, README,
описаний репо и витрины сверено с живым фактом (API / git / deep).
Фильтр по вердикту:</p>
<div class="fbar">{fbar}</div>
<div class="panel"><div class="scroll"><table id="claims">
<thead><tr><th>#</th><th>Величина</th><th>Заявлено</th><th>Источник</th>
<th>Живой факт</th><th>Вердикт</th></tr></thead>
<tbody>{claims_html}</tbody></table></div></div>

<h2>Перекрёстные противоречия</h2>
{flag_html}

<h2>Реестр репозиториев ({len(repos)})</h2>
<p class="small muted">Клик по заголовку — сортировка. Коммиты и файлы —
из blob-less клонов (git rev-list / ls-tree).</p>
<div class="panel"><div class="scroll"><table>
<thead><tr><th data-sort>Репозиторий</th><th data-sort>Язык</th>
<th data-sort>Лицензия</th><th data-sort>★</th><th data-sort>Forks</th>
<th data-sort>Issues</th><th data-sort>KB</th><th data-sort>Коммиты</th>
<th data-sort>Файлы</th><th data-sort>Создан</th><th data-sort>Pushed</th>
<th>Статус</th></tr></thead>
<tbody>{repos_html}</tbody></table></div></div>

<h2>Здоровье роя</h2>
<div class="panel"><div class="scroll"><table>
<thead><tr><th>Класс</th><th>Клетки</th></tr></thead>
<tbody>{health_html}</tbody></table></div></div>

<h2>Языки и методика</h2>
<div class="panel">{lang_html}
<details><summary>Методика и источники</summary><pre>{esc(json.dumps({
 'sources': ['core api.github.com: /users, /repos, /events, /rate_limit',
  'search api.github.com: /search/repositories, /search/commits',
  'git: clone --bare --filter=blob:none → rev-list --count, ls-tree, log -1',
  'raw: README и data-файлы для deep-счёта',
  'html: github.com (вкладка Repositories, SVG-календарь), github.io (витрина)'],
 'claims': 'regex-извлечение чисел из био/README/описаний → сверка с фактами',
 'verdicts': {'PASS': 'совпало или выросло с даты источника',
  'DRIFT': 'расхождение ≤5% у чисел с датой сверки',
  'FAIL': 'живой факт противоречит утверждению',
  'UNVERIFIABLE': 'в GitHub API факта нет (нужен CI/прогон)'},
 'notes': ['search-индекс может отставать от реальности на минуты',
  'blob:none не скачивает содержимое файлов — только история и деревья',
  'запуск: ' + meta['run_started']]},
 ensure_ascii=False, indent=1))}</pre>
</details></div>

<footer><div class="wrap">МЕХАНИЗМ inv-01 · stdlib-only ·
Obsidian &amp; Molten Gold · прогоны воспроизводимы:
<code>python3 inventory_mechanism.py --user {esc(meta['user'])}</code>
· {esc(meta['run_finished'])}</div></footer>
<script type="application/json" id="inv-data">{data_json}</script>
<script>{JS}</script>
</body></html>"""


# ------------------------------------------------------- оркестратор
def events_summary(events):
    """Сводка последних публичных событий (core /events)."""
    if not events:
        return None
    types = {}
    push_commits = 0
    for e in events:
        t = e.get("type") or "?"
        types[t] = types.get(t, 0) + 1
        if t == "PushEvent":
            push_commits += ((e.get("payload") or {}).get("size") or 0)
    # events[0] — самое свежее
    return {"count": len(events), "types": types,
            "push_commits": push_commits,
            "window": [events[0].get("created_at"),
                       events[-1].get("created_at")]}


def main():
    ap = argparse.ArgumentParser(
        description="МЕХАНИЗМ inv-01: инвентаризация GitHub-универсума "
                    "по живым данным API/git/raw/html, claims-сверка README-цифр")
    ap.add_argument("--user", default="rtynbirf")
    ap.add_argument("--out", default="/home/z/my-project/download")
    ap.add_argument("--clones-dir",
                    default="/home/z/my-project/.tmp-inv/git-clones")
    ap.add_argument("--token",
                    default=os.environ.get("GITHUB_TOKEN") or
                    os.environ.get("GH_TOKEN"))
    ap.add_argument("--core-wait", type=int, default=480,
                    help="макс. ожидание сброса core-лимита, сек")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--profile-cache", default=None,
                    help="JSON прошлого прогона — fallback для профиля, "
                         "если core-лимит снова исчерпан")
    ap.add_argument("--skip-clones", action="store_true")
    ap.add_argument("--skip-deep", action="store_true")
    ap.add_argument("--no-html", action="store_true")
    args = ap.parse_args()

    t0 = time.time()
    http = Http(args.token)
    meta = {"mechanism": "inv-01", "run_started": now_iso(),
            "user": args.user, "errors": [], "calls": {}, "git_clones": 0}
    log(f"МЕХАНИЗМ inv-01 → {args.user} (token={'да' if args.token else 'нет'})")

    budgets = check_rate_limit(http)
    log("бюджеты: " + "; ".join(
        f"{k}: {(v or {}).get('remaining')}/{(v or {}).get('limit')}"
        for k, v in budgets.items()))

    # --- 1) инвентарь через search (работает при нулевом core)
    srepos, stotal = [], 0
    try:
        srepos, stotal = search_repos(http, args.user)
        log(f"search /repositories: {len(srepos)} объектов "
            f"(total_count={stotal})")
    except Exception as e:
        meta["errors"].append(f"search repos: {e}")
        log(f"  !! search repos: {e}")
    names = [r.get("name") for r in srepos if r.get("name")]

    # --- 2) blob-less клоны: точные коммиты и деревья, 0 затрат API
    gitdata = {}
    if not args.skip_clones and names:
        os.makedirs(args.clones_dir, exist_ok=True)

        def work(name):
            dest = os.path.join(args.clones_dir, name + ".git")
            ok, err = git_clone_bare(
                f"https://github.com/{args.user}/{name}.git", dest)
            if not ok:
                return name, {"error": err}
            return name, git_info(dest)

        log(f"клоны --bare --filter=blob:none × {len(names)} "
            f"(workers={args.workers})")
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            done = 0
            for name, info in ex.map(work, names):
                gitdata[name] = info
                done += 1
                if done % 15 == 0 or done == len(names):
                    log(f"  клоны {done}/{len(names)}")
        meta["git_clones"] = len(gitdata)
        ok_n = sum(1 for g in gitdata.values()
                   if g.get("commits") is not None)
        log(f"клоны готовы: {ok_n}/{len(names)} с точным числом коммитов")

    # --- 3) глобальный тотал коммитов автора (search)
    commits_author = None
    try:
        commits_author = search_commits_author(http, args.user)
        log(f"search /commits author: {commits_author}")
    except Exception as e:
        meta["errors"].append(f"search commits: {e}")
        log(f"  !! search commits: {e}")

    # --- 4) core REST: ждём сброс лимита, если нужно
    user_core = crepos = events = None
    if wait_for_core(http, need=20, max_wait=args.core_wait):
        try:
            user_core = core_user(http, args.user)
            log("core /users: ок")
        except Exception as e:
            meta["errors"].append(f"core user: {e}")
            log(f"  !! core user: {e}")
        try:
            crepos = core_repos(http, args.user)
            log(f"core /repos: {len(crepos)} репо")
        except Exception as e:
            meta["errors"].append(f"core repos: {e}")
            log(f"  !! core repos: {e}")
        try:
            events = core_events(http, args.user)
            log(f"core /events: {len(events)} событий")
        except Exception as e:
            meta["errors"].append(f"core events: {e}")
            log(f"  !! core events: {e}")

    profile_src = "core /users (живой)" if user_core else None
    if user_core is None and args.profile_cache and \
            os.path.exists(args.profile_cache):
        try:
            with open(args.profile_cache, encoding="utf-8") as f:
                user_core = json.load(f).get("profile")
            profile_src = "кэш профиля: " + args.profile_cache
            log("профиль взян из кэша прошлого прогона")
        except Exception as e:
            meta["errors"].append(f"profile cache: {e}")
    meta["profile_source"] = profile_src or "нет"

    repos = crepos if crepos else srepos
    if not repos:
        log("!! инвентарь пуст: ни core, ни search не ответили")
        meta["run_finished"] = now_iso()
        out_json = os.path.join(args.out, f"{args.user}-inventory.json")
        os.makedirs(args.out, exist_ok=True)
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump({"meta": meta}, f, ensure_ascii=False, indent=1)
        return 1
    src_label = "core /repos" if crepos else "search /repositories"
    log(f"канонический инвентарь: {len(repos)} репо из {src_label}")

    branches = {r.get("name"): (r.get("default_branch") or "HEAD")
                for r in repos if r.get("name")}

    # --- 5) календарь контрибуций (HTML/SVG)
    days, levels = {}, {}
    svg = fetch_text(http,
                     f"https://github.com/users/{args.user}/contributions")
    if svg:
        days, levels = parse_contributions_svg(svg)
        log(f"календарь: {len(days)} дней с числами, {len(levels)} с уровнем")
    else:
        meta["errors"].append("SVG-календарь недоступен")

    # --- 6) raw-источники для claims
    prof_readme = fetch_text(
        http, f"https://raw.githubusercontent.com/{args.user}/{args.user}/"
              f"HEAD/README.md")
    showcase = fetch_text(http, f"https://{args.user}.github.io/")
    repos_tab = repo_tab_count(http, args.user)
    log("raw: профиль-README " + ("ок" if prof_readme else "нет") +
        ", витрина " + ("ок" if showcase else "нет") +
        f", вкладка-счётчик: {repos_tab}")

    readmes = {}
    log("README всех репо через raw …")
    for i, r in enumerate(repos, 1):
        nm = r.get("name")
        if not nm:
            continue
        text, _ = fetch_readme(http, args.user, nm, branches.get(nm, "HEAD"))
        if text:
            readmes[nm] = text
        if i % 20 == 0 or i == len(repos):
            log(f"  README {i}/{len(repos)} (есть: {len(readmes)})")

    # --- 7) deep-проверки по живым файлам
    deep_results = []
    if not args.skip_deep:
        all_names = set(names) | {r.get("name") for r in repos if r.get("name")}
        for t in DEEP_TARGETS:
            if t["repo"] not in all_names:
                deep_results.append({**t, "value": None,
                                     "note": "репо нет в инвентаре"})
                continue
            deep_results.append(
                deep_check(t, gitdata, http, args.user, branches))
    deep_map = {}
    for d in deep_results:
        deep_map[(d["repo"], d["qty"])] = d
    for qty in ("songs", "languages", "tools"):
        hits = [d for (r, q), d in deep_map.items()
                if q == qty and r != "*" and d.get("value") is not None]
        if len(hits) == 1:
            deep_map[("*", qty)] = hits[0]
    for d in deep_results:
        log(f"deep {d['qty']}@{d['repo']}: {d.get('value')} "
            f"({d.get('method') or d.get('note')})")

    # --- 8) факты и claims
    commits_sum = sum((g.get("commits") or 0) for g in gitdata.values())
    facts = {
        "public_repos": (user_core or {}).get("public_repos"),
        "search_repos_total": stotal,
        "core_repos_len": len(crepos) if crepos is not None else None,
        "repos_tab_count": repos_tab,
        "commits_author_search": commits_author,
        "commits_sum_clones": commits_sum if gitdata else None,
        "per_repo": {n: g.get("commits") for n, g in gitdata.items()
                     if g.get("commits") is not None},
        "followers": (user_core or {}).get("followers"),
        "following": (user_core or {}).get("following"),
        "contrib_year_sum": sum(days.values()) if len(days) >= 100 else None,
    }

    claims = []
    bio = (user_core or {}).get("bio")
    if bio:
        claims += extract_claims(bio, "api:core:/users",
                                 "bio @" + args.user, user=args.user)
    if prof_readme:
        claims += extract_claims(prof_readme, "raw:README",
                                 "README профиля @ " + args.user,
                                 user=args.user)
    if showcase:
        claims += extract_claims(strip_html(showcase), "raw:витрина",
                                 args.user + ".github.io", user=args.user)
    for r in repos:
        nm = r.get("name")
        if not nm or nm == args.user:
            continue  # профиль-репо уже разобран как README профиля
        desc = (r.get("description") or "")
        if desc.strip():
            claims += extract_claims(desc, "api:core:/repos",
                                     "описание " + nm, scope=nm,
                                     user=args.user)
        if nm in readmes:
            claims += extract_claims(readmes[nm], "raw:README",
                                     "README " + nm, scope=nm,
                                     user=args.user)
    log(f"claims извлечено: {len(claims)}")
    for c in claims:
        c.update(verify_claim(c, facts, deep_map))
    flags = consistency_flags(claims)
    log(f"перекрёстных противоречий: {len(flags)}")

    # --- 9) агрегаты
    agg = build_aggregates(repos, gitdata, facts)
    ev_sum = events_summary(events)

    # --- 10) выходы
    repos_rows = []
    for r in repos:
        nm = r.get("name")
        g = gitdata.get(nm) or {}
        repos_rows.append({
            "name": nm, "full_name": r.get("full_name"),
            "html_url": r.get("html_url"),
            "description": r.get("description"),
            "language": r.get("language"),
            "license": (r.get("license") or {}).get("spdx_id"),
            "stars": r.get("stargazers_count"),
            "forks": r.get("forks_count"),
            "open_issues": r.get("open_issues_count"),
            "size_kb": r.get("size"),
            "created_at": r.get("created_at"),
            "updated_at": r.get("updated_at"),
            "pushed_at": r.get("pushed_at"),
            "days_since_push": days_between(r.get("pushed_at")),
            "archived": r.get("archived"), "fork": r.get("fork"),
            "default_branch": r.get("default_branch"),
            "topics": r.get("topics"), "homepage": r.get("homepage"),
            "has_issues": r.get("has_issues"),
            "git_commits": g.get("commits"),
            "git_last_commit_at": g.get("last_commit_at"),
            "git_file_count": g.get("file_count"),
            "git_error": g.get("error"),
        })

    data = {
        "meta": meta,
        "profile": user_core or {"login": args.user},
        "facts": facts,
        "repos": repos_rows,
        "git": {n: {k: v for k, v in g.items() if k != "files"}
                for n, g in gitdata.items()},
        "aggregates": agg,
        "activity": {"calendar_days": days, "calendar_levels": levels,
                     "calendar_month_totals": month_totals(days),
                     "events": ev_sum},
        "claims": claims,
        "contradictions": flags,
        "deep": deep_results,
    }
    meta["calls"] = dict(http.calls)
    meta["budgets_final"] = check_rate_limit(http)
    meta["run_finished"] = now_iso()
    meta["duration_sec"] = round(time.time() - t0, 1)

    os.makedirs(args.out, exist_ok=True)
    json_path = os.path.join(args.out, f"{args.user}-inventory.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1, default=str)
    log(f"JSON: {json_path} ({os.path.getsize(json_path) // 1024} KB)")
    if not args.no_html:
        html_path = os.path.join(args.out,
                                 f"{args.user}-inventory-report.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(build_html(data))
        log(f"HTML: {html_path} ({os.path.getsize(html_path) // 1024} KB)")

    # --- сводка прогона
    verd = {}
    for c in claims:
        verd[c["verdict"]] = verd.get(c["verdict"], 0) + 1
    print("\n=== СВОДКА ПРОГОНА ===")
    print(f"репозитории: core={facts['public_repos']} "
          f"search={stotal} len={facts['core_repos_len']} "
          f"html_таб={repos_tab}")
    print(f"коммиты: author_search={commits_author} "
          f"сумма_клонов={commits_sum}")
    print(f"контрибуции за год: {facts['contrib_year_sum']} "
          f"(дней с числами: {len(days)})")
    print(f"claims: {len(claims)}; вердикты: {verd}; "
          f"противоречий: {len(flags)}")
    print(f"агрегаты: звёзды={agg['stars_total']} "
          f"forks={agg['forks_total']} размер={agg['size_gb_total']}GB "
          f"archived={len(agg['archived'])} "
          f"fork_репо={len(agg['forks_of_others'])}")
    print("deep: " + "; ".join(
        f"{d['qty']}@{d['repo']}={d.get('value')}" for d in deep_results))
    return 0


if __name__ == "__main__":
    sys.exit(main())
