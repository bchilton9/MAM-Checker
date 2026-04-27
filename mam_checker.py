#!/usr/bin/env python3

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import json
import re
import time
import hashlib
import threading
import requests

from rapidfuzz import fuzz
from ebooklib import epub
from pypdf import PdfReader


# =========================
# CONFIG
# =========================

BOOK_DIRS = [
    ("/path/to/ebooks", "ebook"),
    ("/path/to/audiobooks", "audiobook"),
]

PROWLARR_URL = "http://localhost:9696"
PROWLARR_API_KEY = "PASTE_YOUR_PROWLARR_API_KEY_HERE"
INDEXER_IDS = [52]

OUTPUT_CSV = "mam_missing_report.csv"
CACHE_FILE = "prowlarr_search_cache.json"

FOUND_MATCH = 82
PARTIAL_MATCH = 60

SKIP_FOUND_FROM_CSV = True

MAX_WORKERS = 4
SLEEP_BETWEEN_SEARCHES = 0.15

CACHE_ENABLED = True
CACHE_MAX_AGE_DAYS = 30

CATEGORY_MAP = {
    "ebook": 8010,
    "audiobook": 3030,
}

EXTS_BY_TYPE = {
    "ebook": {
        ".epub", ".pdf", ".mobi", ".azw3", ".cbz", ".cbr"
    },
    "audiobook": {
        ".mp3", ".m4b", ".m4a", ".flac", ".aac", ".ogg", ".opus"
    },
}


# =========================
# GLOBAL CACHE
# =========================

cache_lock = threading.Lock()
search_cache = {}


def load_cache():
    global search_cache

    if not CACHE_ENABLED:
        return

    path = Path(CACHE_FILE)
    if not path.exists():
        search_cache = {}
        return

    try:
        with open(path, "r", encoding="utf-8") as f:
            search_cache = json.load(f)
    except Exception:
        search_cache = {}


def save_cache():
    if not CACHE_ENABLED:
        return

    with cache_lock:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(search_cache, f, indent=2, ensure_ascii=False)


def cache_key(query, media_type, category):
    raw = json.dumps(
        {
            "query": query,
            "media_type": media_type,
            "category": category,
            "indexers": INDEXER_IDS,
        },
        sort_keys=True,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def cache_get(query, media_type, category):
    if not CACHE_ENABLED:
        return None

    key = cache_key(query, media_type, category)

    with cache_lock:
        entry = search_cache.get(key)

    if not entry:
        return None

    age = time.time() - entry.get("timestamp", 0)
    max_age = CACHE_MAX_AGE_DAYS * 86400

    if age > max_age:
        return None

    return entry.get("results")


def cache_set(query, media_type, category, results):
    if not CACHE_ENABLED:
        return

    key = cache_key(query, media_type, category)

    with cache_lock:
        search_cache[key] = {
            "timestamp": time.time(),
            "query": query,
            "media_type": media_type,
            "category": category,
            "results": results,
        }


# =========================
# TEXT HELPERS
# =========================

def clean(text):
    if not text:
        return ""
    text = str(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def strip_extension_noise(text):
    return re.sub(
        r"\b(epub|pdf|mobi|azw3|cbz|cbr|mp3|m4b|m4a|flac|aac|ogg|opus)\b",
        " ",
        text,
        flags=re.I,
    )


def normalize_title(text):
    text = clean(text).lower()

    noise = [
        "retail", "ebook", "ebooks", "kindle", "true epub",
        "converted", "scan", "scanned", "audiobook", "audio book",
        "unabridged", "abridged", "mp3", "m4b", "audible",
        "read by", "narrated by", "full cast",
    ]

    text = re.sub(r"\[[^\]]*\]", " ", text)
    text = re.sub(r"\([^\)]*\)", " ", text)
    text = strip_extension_noise(text)

    for word in noise:
        text = re.sub(rf"\b{re.escape(word)}\b", " ", text)

    text = re.sub(r"\bbook\s+\d+(\.\d+)?\b", " ", text)
    text = re.sub(r"\bvol(?:ume)?\s+\d+(\.\d+)?\b", " ", text)
    text = re.sub(r"\bpart\s+\d+(\.\d+)?\b", " ", text)

    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def normalize_author(text):
    text = normalize_title(text)

    # Handle "Last, First"
    if "," in clean(text):
        parts = [p.strip() for p in clean(text).split(",")]
        if len(parts) >= 2:
            text = f"{parts[1]} {parts[0]}"

    return normalize_title(text)


def remove_series_bits(text):
    text = clean(text)

    patterns = [
        r"\bbook\s+\d+(\.\d+)?\b",
        r"\b#\s*\d+(\.\d+)?\b",
        r"\bvol(?:ume)?\.?\s*\d+(\.\d+)?\b",
        r"\bpart\s+\d+(\.\d+)?\b",
        r"\bseries\s+\d+(\.\d+)?\b",
    ]

    for p in patterns:
        text = re.sub(p, " ", text, flags=re.I)

    text = re.sub(r"\s+", " ", text)
    return text.strip(" -_[]()")


# =========================
# SERIES / AUTHOR PARSING
# =========================

def guess_series_from_path(path):
    parts = list(path.parts)

    # Usually:
    # /Audiobooks/Author/Series/Book
    # /Books/Author/Series/Book
    if len(parts) >= 3:
        parent = path.parent.name
        grandparent = path.parent.parent.name

        # Avoid using author folder as series when there is no series folder.
        if parent and parent.lower() not in {"ebooks", "books", "audiobooks", "audio"}:
            if normalize_title(parent) != normalize_title(grandparent):
                return clean(parent)

    return ""


def guess_author_from_path(path):
    if len(path.parts) >= 3:
        grandparent = path.parent.parent.name
        parent = path.parent.name

        # Pattern: /Author/Series/File
        if grandparent and parent and normalize_title(grandparent) != normalize_title(parent):
            return clean(grandparent)

        # Pattern: /Author/File
        if parent:
            return clean(parent)

    return ""


def parse_book_number(text):
    patterns = [
        r"\bbook\s*(\d+(?:\.\d+)?)\b",
        r"\b#\s*(\d+(?:\.\d+)?)\b",
        r"\bvol(?:ume)?\.?\s*(\d+(?:\.\d+)?)\b",
        r"\bpart\s*(\d+(?:\.\d+)?)\b",
    ]

    for p in patterns:
        m = re.search(p, text, flags=re.I)
        if m:
            return m.group(1)

    return ""


def parse_from_filename(path):
    name = path.stem
    name = clean(name)

    book_num = parse_book_number(name)

    # Author - Series 01 - Title
    m = re.match(
        r"^(?P<author>.+?)\s+-\s+(?P<series>.+?)\s+(?P<num>\d+(?:\.\d+)?)\s+-\s+(?P<title>.+)$",
        name,
    )
    if m:
        return {
            "title": clean(m.group("title")),
            "author": clean(m.group("author")),
            "series": clean(m.group("series")),
            "book_number": clean(m.group("num")),
        }

    # Author - Series - 01 - Title
    m = re.match(
        r"^(?P<author>.+?)\s+-\s+(?P<series>.+?)\s+-\s+(?P<num>\d+(?:\.\d+)?)\s+-\s+(?P<title>.+)$",
        name,
    )
    if m:
        return {
            "title": clean(m.group("title")),
            "author": clean(m.group("author")),
            "series": clean(m.group("series")),
            "book_number": clean(m.group("num")),
        }

    # Author - Title
    if " - " in name:
        author, title = name.split(" - ", 1)
        return {
            "title": remove_series_bits(title),
            "author": clean(author),
            "series": guess_series_from_path(path),
            "book_number": book_num,
        }

    return {
        "title": remove_series_bits(name),
        "author": guess_author_from_path(path),
        "series": guess_series_from_path(path),
        "book_number": book_num,
    }


# =========================
# METADATA
# =========================

def meta_from_epub(path):
    book = epub.read_epub(str(path))

    titles = book.get_metadata("DC", "title")
    creators = book.get_metadata("DC", "creator")

    title = clean(titles[0][0]) if titles else ""
    author = clean(creators[0][0]) if creators else ""

    parsed = parse_from_filename(path)

    return {
        "title": title or parsed["title"],
        "author": author or parsed["author"],
        "series": parsed["series"],
        "book_number": parsed["book_number"],
    }


def meta_from_pdf(path):
    reader = PdfReader(str(path))
    metadata = reader.metadata or {}

    parsed = parse_from_filename(path)

    title = clean(metadata.get("/Title")) or parsed["title"]
    author = clean(metadata.get("/Author")) or parsed["author"]

    return {
        "title": title,
        "author": author,
        "series": parsed["series"],
        "book_number": parsed["book_number"],
    }


def meta_from_file(path):
    parsed = parse_from_filename(path)

    try:
        if path.suffix.lower() == ".epub":
            return meta_from_epub(path)

        if path.suffix.lower() == ".pdf":
            return meta_from_pdf(path)

    except Exception:
        pass

    return parsed


# =========================
# CSV RESUME
# =========================

def load_existing_csv():
    existing = {}

    path = Path(OUTPUT_CSV)
    if not path.exists():
        return existing

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            local_file = row.get("local_file")
            if local_file:
                existing[local_file] = row

    return existing


# =========================
# PROWLARR
# =========================

thread_local = threading.local()


def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session


def search_prowlarr(query, media_type):
    category = CATEGORY_MAP[media_type]

    cached = cache_get(query, media_type, category)
    if cached is not None:
        return cached, True

    url = f"{PROWLARR_URL.rstrip('/')}/api/v1/search"

    params = {
        "query": query,
        "apikey": PROWLARR_API_KEY,
        "categories": category,
    }

    for indexer_id in INDEXER_IDS:
        params.setdefault("indexerIds", [])
        params["indexerIds"].append(indexer_id)

    session = get_session()
    response = session.get(url, params=params, timeout=30)
    response.raise_for_status()

    results = response.json()
    cache_set(query, media_type, category, results)

    time.sleep(SLEEP_BETWEEN_SEARCHES)

    return results, False


def get_result_title(result):
    return clean(
        result.get("title")
        or result.get("releaseTitle")
        or result.get("guid")
        or ""
    )


def classify_score(score):
    if score >= FOUND_MATCH:
        return "FOUND"
    if score >= PARTIAL_MATCH:
        return "PARTIAL"
    return "MISSING"


# =========================
# QUERY + MATCHING
# =========================

def build_queries(meta):
    title = clean(meta.get("title", ""))
    author = clean(meta.get("author", ""))
    series = clean(meta.get("series", ""))
    book_number = clean(meta.get("book_number", ""))

    queries = []

    def add(q):
        q = clean(q)
        if q and q not in queries:
            queries.append(q)

    # Most useful first.
    add(title)

    if author and title:
        add(f"{title} {author}")

    if series and title:
        add(f"{series} {title}")

    if author and series and title:
        add(f"{author} {series} {title}")

    if series and book_number:
        add(f"{series} book {book_number}")

    if author and series and book_number:
        add(f"{author} {series} book {book_number}")

    if series:
        add(series)

    return queries[:6]


def score_match(meta, result_title):
    title = meta.get("title", "")
    author = meta.get("author", "")
    series = meta.get("series", "")
    book_number = meta.get("book_number", "")

    result_norm = normalize_title(result_title)

    title_score = fuzz.token_set_ratio(normalize_title(title), result_norm) if title else 0
    author_score = fuzz.partial_ratio(normalize_author(author), result_norm) if author else 0
    series_score = fuzz.partial_ratio(normalize_title(series), result_norm) if series else 0

    score = title_score * 0.78

    if author:
        score += author_score * 0.12

    if series:
        score += series_score * 0.10

    # Tiny bonus if book number appears.
    if book_number and re.search(rf"\b{re.escape(book_number)}\b", result_norm):
        score += 3

    return min(100, int(score))


def check_book(meta, media_type):
    queries = build_queries(meta)

    best_score = 0
    best_match = ""
    best_query = ""
    cache_hits = 0
    live_hits = 0
    error = ""

    if not queries:
        return "ERROR", 0, "", "", "No searchable title/query", 0, 0

    for query in queries:
        try:
            results, from_cache = search_prowlarr(query, media_type)
        except Exception as e:
            error = str(e)
            continue

        if from_cache:
            cache_hits += 1
        else:
            live_hits += 1

        for result in results:
            result_title = get_result_title(result)
            if not result_title:
                continue

            score = score_match(meta, result_title)

            if score > best_score:
                best_score = score
                best_match = result_title
                best_query = query

        # Stop early on strong hit.
        if best_score >= FOUND_MATCH + 8:
            break

    status = classify_score(best_score)

    if not best_match and error:
        status = "ERROR"

    return status, best_score, best_match, best_query, error, cache_hits, live_hits


# =========================
# FILE GATHERING
# =========================

def gather_files():
    all_files = []

    for base, media_type in BOOK_DIRS:
        base_path = Path(base)

        if not base_path.exists():
            print(f"Skipping missing path: {base}")
            continue

        allowed_exts = EXTS_BY_TYPE[media_type]

        for path in base_path.rglob("*"):
            if path.is_file() and path.suffix.lower() in allowed_exts:
                all_files.append((path, media_type))

    return all_files


# =========================
# WORKER
# =========================

def process_file(path, media_type):
    meta = meta_from_file(path)

    status, score, best_match, searched_query, error, cache_hits, live_hits = check_book(
        meta,
        media_type,
    )

    return {
        "status": status,
        "match_score": score,
        "title": meta.get("title", ""),
        "author": meta.get("author", ""),
        "series": meta.get("series", ""),
        "book_number": meta.get("book_number", ""),
        "best_mam_match": best_match,
        "searched_query": searched_query,
        "type": media_type,
        "cache_hits": cache_hits,
        "live_hits": live_hits,
        "error": error,
        "local_file": str(path),
    }


# =========================
# OUTPUT HELPERS
# =========================

def format_eta(seconds):
    seconds = int(seconds)

    if seconds < 60:
        return f"{seconds}s"

    minutes, seconds = divmod(seconds, 60)

    if minutes < 60:
        return f"{minutes}m {seconds}s"

    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m"


def safe_int(value):
    try:
        return int(float(value))
    except Exception:
        return 0


# =========================
# MAIN
# =========================

def main():
    load_cache()

    files = gather_files()
    existing = load_existing_csv()

    rows = []
    files_to_check = []
    skipped = 0

    for path, media_type in files:
        path_str = str(path)
        old_row = existing.get(path_str)

        if (
            SKIP_FOUND_FROM_CSV
            and old_row
            and old_row.get("status") == "FOUND"
        ):
            rows.append(old_row)
            skipped += 1
        else:
            files_to_check.append((path, media_type))

    total = len(files_to_check)

    print(f"Total files: {len(files)}")
    print(f"Existing CSV rows: {len(existing)}")
    print(f"Skipped existing FOUND: {skipped}")
    print(f"Checking now: {total}")
    print(f"Threads: {MAX_WORKERS}")
    print(f"Cache entries loaded: {len(search_cache)}\n")

    start = time.time()
    completed = 0

    if total > 0:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_file = {
                executor.submit(process_file, path, media_type): (path, media_type)
                for path, media_type in files_to_check
            }

            for future in as_completed(future_to_file):
                path, media_type = future_to_file[future]
                completed += 1

                try:
                    row = future.result()
                except Exception as e:
                    row = {
                        "status": "ERROR",
                        "match_score": 0,
                        "title": path.stem,
                        "author": "",
                        "series": "",
                        "book_number": "",
                        "best_mam_match": "",
                        "searched_query": "",
                        "type": media_type,
                        "cache_hits": 0,
                        "live_hits": 0,
                        "error": str(e),
                        "local_file": str(path),
                    }

                rows.append(row)

                elapsed = time.time() - start
                percent = (completed / total) * 100
                rate = completed / elapsed if elapsed else 0
                remaining = total - completed
                eta = remaining / rate if rate else 0

                print(
                    f"[{completed}/{total} | {percent:5.1f}% | ETA {format_eta(eta):>8}] "
                    f"{row['status']:8} | {safe_int(row['match_score']):3} | "
                    f"{row['title']} - {row['author']} "
                    f"({row['type']}) "
                    f"[cache:{row.get('cache_hits', 0)} live:{row.get('live_hits', 0)}]"
                )

                if row["status"] in {"PARTIAL", "MISSING"} and row["best_mam_match"]:
                    print(f"          best: {row['best_mam_match']}")

    rows.sort(
        key=lambda r: (
            r.get("type", ""),
            r.get("status", ""),
            safe_int(r.get("match_score", 0)),
            r.get("author", "").lower(),
            r.get("series", "").lower(),
            r.get("title", "").lower(),
        )
    )

    fieldnames = [
        "status",
        "match_score",
        "title",
        "author",
        "series",
        "book_number",
        "best_mam_match",
        "searched_query",
        "type",
        "cache_hits",
        "live_hits",
        "error",
        "local_file",
    ]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    save_cache()

    print(f"\nDone. Updated report written to: {OUTPUT_CSV}")
    print(f"Cache saved to: {CACHE_FILE}")


if __name__ == "__main__":
    main()
