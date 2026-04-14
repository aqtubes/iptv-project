#!/usr/bin/env python3
import base64
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ============================================================
# HARD-CODED IPTV + TMDB
# ============================================================

IPTV_URL = "http://ipro.tv:80/get.php?username=iaacoBdOQo&password=i4qtuDgzDx&type=m3u&output=ts"
TMDB_API_KEY = "699a362c1f60b16e30ce26bb858d9b71"

# ============================================================
# GOOGLE DRIVE CONFIG
# ============================================================
# For GitHub Actions, set these as repository secrets:
# - GDRIVE_SERVICE_ACCOUNT_JSON_B64 : base64-encoded service account JSON
# - GDRIVE_FOLDER_ID                : destination folder ID in Google Drive
#
# The script will still run locally without upload if these are missing.

GDRIVE_SERVICE_ACCOUNT_JSON_B64 = os.getenv("GDRIVE_SERVICE_ACCOUNT_JSON_B64", "")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")

# ============================================================
# GENERAL CONFIG
# ============================================================

WORK_DIR = Path("./iptv_work")
PLAYLIST_FILE = WORK_DIR / "playlist.m3u"
REPORT_FILE = WORK_DIR / "report.json"
MANIFEST_FILE = WORK_DIR / "manifest.json"
TMDB_CACHE_FILE = WORK_DIR / "tmdb_cache.json"
BOLLYWOOD_PLAYLIST = WORK_DIR / "bollywood_only.m3u"

HTTP_TIMEOUT = 60
MAX_TMDB_WORKERS = 10
MIN_YEAR = 1990

MOVIES_ROOT = "Movies"
TV_ROOT = "TV Shows"
ADULT_ROOT = "Exra's"
BOLLYWOOD_ROOT = "Bollywood"

# ============================================================
# FILTERS
# ============================================================

REMOVE_CODE_TAGS = {
    "GR", "EX", "YU", "DE", "NL", "FR", "ES", "TR", "IT", "AR",
    "PT", "MK", "MA", "LAT", "RU", "TN", "IR", "SE", "DK"
}

REMOVE_WORD_TAGS = {
    "FRENCH", "SPANISH", "GERMAN", "ITALIAN", "ARABIC",
    "TURKISH", "DUTCH", "GREEK", "RUSSIAN"
}

SPORTS_LIVE_WORDS = {
    "UFC", "WWE", "FIFA", "FOOTBALL", "SOCCER", "NBA", "NFL", "NHL", "MLB",
    "BOXING", "CRICKET", "SPORT", "SPORTS", "MATCH", "MATCHES", "PPV",
    "LIVE PERFORMANCE", "LIVE PERFORMANCES", "CONCERT", "CONCERTS",
    "MUSICAL", "MUSIC VIDEO", "PERFORMANCE", "PERFORMANCES", "EVENT", "EVENTS",
    "ROUND 2LEG", "2LEG", "PLAYOFF", "PLAY-OFF"
}

ADULT_WORDS = {
    "XXX", "PORN", "ADULT", "18+", "SEX", "HUSTLER", "BRAZZERS",
    "REALITY KINGS", "NAUGHTY", "BLACKED", "ONLYFANS"
}

BOLLYWOOD_WORDS = {
    "BOLLYWOOD", "INDIA", "INDIAN", "HINDI", "TAMIL", "TELUGU",
    "PUNJABI", "PUNJAB", "PAK", "PAKISTAN", "PAKISTANI",
    "BANGLA", "BENGALI", "BANGLADESH", "BANGLADESHI",
    "MALAYALAM", "KANNADA"
}

TV_GROUP_WORDS = {
    "SERIES", "TV SHOW", "TV SHOWS", "SHOW", "SHOWS",
    "SEASON", "EPISODE", "EPISODES", "SERIE"
}

MOVIE_GROUP_WORDS = {
    "MOVIE", "MOVIES", "VOD", "FILM", "FILMS", "CINEMA"
}

JUNK_TOKENS = [
    "WEB-DL", "WEBDL", "WEBRIP", "BLURAY", "BRRIP", "BDRIP", "HDRIP",
    "DVDRIP", "X264", "X265", "H264", "H265", "HEVC", "AAC", "DDP5",
    "DD5", "MULTI", "REMUX", "PROPER", "REPACK", "UNRATED", "EXTENDED",
    "DIRECTORS CUT", "DIRECTOR'S CUT", "NF", "AMZN", "DSNP", "HMAX",
    "HDTV", "CAM", "TS", "TC", "CODEC", "DUBBED", "DUAL AUDIO",
    "TRUEHD", "ATMOS", "PO", "ROUND", "ENTERTAI", "ENTERTAINMENT",
]

QUALITY_RULES = [
    (r"\b2160P\b|\b4K\b|\bUHD\b|\bUUHD\b", 600),
    (r"\b1080P\b|\bFHD\b", 500),
    (r"\b720P\b", 400),
    (r"\bHD\b", 300),
    (r"\bSD\b", 200),
]

TV_PATTERNS = [
    r"\bS\d{1,2}E\d{1,3}\b",
    r"\b\d{1,2}x\d{1,3}\b",
    r"\bSEASON\s+\d+\b",
    r"\bEPISODE\s+\d+\b",
    r"\bEP\s*\d+\b",
]

NONCONTENT_PATTERNS = [
    r"^[=\-_\s]+$",
    r"^\d{1,4}$",
    r"^(?:=+\s*)?[A-Z][A-Z\s&'\-]{2,}(?:\s*=+)?$",
    r"^(?:-+\s*)?[A-Z][A-Z\s&'\-]{2,}(?:\s*-+)?$",
]

# ============================================================
# DATA MODEL
# ============================================================

@dataclass
class MediaItem:
    url: str
    raw_title: str
    clean_title: str
    content_type: str   # movie or tv
    group: str
    quality: int
    is_adult: bool
    is_bollywood: bool
    english_pref: bool
    final_name: str = ""
    show_name: str = ""
    year: str = ""
    tmdb_id: Optional[int] = None
    tmdb_type: Optional[str] = None

# ============================================================
# UTIL
# ============================================================

def requests_get(url: str, **kwargs) -> requests.Response:
    headers = kwargs.pop("headers", {})
    headers.setdefault("User-Agent", "Mozilla/5.0")
    return requests.get(url, headers=headers, timeout=HTTP_TIMEOUT, **kwargs)

def ensure_dirs() -> None:
    WORK_DIR.mkdir(parents=True, exist_ok=True)

def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default

def save_json(path: Path, data) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

def sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

def sanitize_filename(name: str) -> str:
    name = re.sub(r'[\\/:"*?<>|]+', "", name)
    name = re.sub(r"\s+", " ", name).strip(" .-_")
    return name[:180]

def bucket_letter(name: str) -> str:
    if not name:
        return "#"
    c = name[0].upper()
    return c if c.isalpha() else "#"

def upper_text(*parts: str) -> str:
    return " | ".join([p for p in parts if p]).upper()

def normalize_separators(text: str) -> str:
    text = text.replace("_", " ").replace(".", " ").replace("|", " ").replace("\'96", "-")
    return re.sub(r"\s+", " ", text).strip()

def extract_year(text: str) -> str:
    years = re.findall(r"\b(19\d{2}|20\d{2})\b", text)
    return years[-1] if years else ""

def extract_episode_code(title: str) -> str:
    m = re.search(r"\b(S\d{1,2}E\d{1,3})\b", title, flags=re.I)
    if m:
        return m.group(1).upper()
    m = re.search(r"\b(\d{1,2})x(\d{1,3})\b", title, flags=re.I)
    if m:
        return f"S{int(m.group(1)):02d}E{int(m.group(2)):02d}"
    m = re.search(r"\bEP(?:ISODE)?\s*(\d{1,3})\b", title, flags=re.I)
    if m:
        return f"E{int(m.group(1)):02d}"
    return ""

def quality_score(text: str) -> int:
    text_u = text.upper()
    best = 100
    for pattern, score in QUALITY_RULES:
        if re.search(pattern, text_u, flags=re.I):
            best = max(best, score)
    return best

def smart_title_case(text: str) -> str:
    if not text:
        return text

    keep_upper = {"USA", "UK", "CSI", "NCIS", "FBI", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X"}
    small_words = {"a", "an", "and", "of", "the", "in", "on", "at", "to", "for", "vs"}

    out = []
    for i, word in enumerate(text.split()):
        wu = word.upper()
        wl = word.lower()
        if wu in keep_upper:
            out.append(wu)
        elif re.fullmatch(r"S\d{2}E\d{2}", wu):
            out.append(wu)
        elif re.fullmatch(r"E\d{2}", wu):
            out.append(wu)
        elif re.fullmatch(r"[ivxlcdm]+", wl):
            out.append(wu)
        elif wl in small_words and i != 0:
            out.append(wl)
        elif re.fullmatch(r"\d+", word):
            out.append(word)
        else:
            out.append(word[:1].upper() + word[1:].lower())
    return " ".join(out)

# ============================================================
# PARSE / FILTER
# ============================================================

def download_playlist() -> None:
    print("Downloading playlist...")
    res = requests_get(IPTV_URL)
    res.raise_for_status()
    PLAYLIST_FILE.write_bytes(res.content)
    print(f"Saved: {PLAYLIST_FILE}")

def parse_m3u(path: Path) -> list[dict]:
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    entries = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("#EXTINF:"):
            url = lines[i + 1].strip() if i + 1 < len(lines) else ""
            title = line.split(",", 1)[-1].strip() if "," in line else ""
            entries.append({"extinf": line, "title": title, "url": url})
            i += 2
        else:
            i += 1
    return entries

def extract_attrs(extinf: str) -> dict:
    attrs = {}
    for key in ["group-title", "tvg-name", "tvg-id", "tvg-logo"]:
        m = re.search(rf'{key}="([^"]*)"', extinf, flags=re.I)
        if m:
            attrs[key.lower()] = m.group(1).strip()
    return attrs

def looks_vod(entry: dict) -> bool:
    attrs = extract_attrs(entry["extinf"])
    group_u = upper_text(attrs.get("group-title", ""))
    combo = upper_text(entry["title"], attrs.get("tvg-name", ""), group_u, entry["url"])
    url = entry["url"].lower().strip()

    if not url.startswith(("http://", "https://")):
        return False
    if any(word in combo for word in SPORTS_LIVE_WORDS):
        return False
    if any(word in group_u for word in ["LIVE", "CHANNEL", "CHANNELS", "NEWS", "SPORT"]):
        return False
    if re.search(r"(===|---|___)", entry["title"]):
        return False
    return True

def has_code_tag(text_upper: str) -> bool:
    for tag in REMOVE_CODE_TAGS:
        if re.search(rf"\[{re.escape(tag)}\]|#{re.escape(tag)}\b|\b{re.escape(tag)}\b", text_upper, flags=re.I):
            return True
    return False

def has_word_tag(text_upper: str) -> bool:
    return any(re.search(rf"\b{re.escape(word)}\b", text_upper) for word in REMOVE_WORD_TAGS)

def ends_with_fr(title_upper: str) -> bool:
    return bool(re.search(r"\bFR\s*$", title_upper))

def is_definitely_junk(raw_title: str, group: str) -> bool:
    raw = normalize_separators(raw_title).upper().strip()
    combo = upper_text(raw, group)
    if not raw:
        return True
    if any(re.match(p, raw) for p in NONCONTENT_PATTERNS):
        return True
    if "====" in raw or "----" in raw:
        return True
    if len(re.findall(r"[^\w\s]", raw)) > 8:
        return True
    if any(word in combo for word in SPORTS_LIVE_WORDS):
        return True
    return False

def is_tv_like(raw_title: str, group: str) -> bool:
    combo = upper_text(raw_title, group)
    if any(re.search(p, combo, flags=re.I) for p in TV_PATTERNS):
        return True
    if any(word in combo for word in TV_GROUP_WORDS):
        return True
    return False

def is_movie_like(raw_title: str, group: str) -> bool:
    combo = upper_text(raw_title, group)
    if any(word in combo for word in MOVIE_GROUP_WORDS):
        return True
    if re.search(r"\b(19\d{2}|20\d{2})\b", combo):
        return True
    return False

def infer_content_type(raw_title: str, group: str) -> str:
    tv_like = is_tv_like(raw_title, group)
    movie_like = is_movie_like(raw_title, group)
    if tv_like and not movie_like:
        return "tv"
    if movie_like and not tv_like:
        return "movie"
    if tv_like and movie_like:
        return "tv" if extract_episode_code(raw_title) else "movie"
    return "movie"

def remove_noise(text: str) -> str:
    patterns = [
        r"\[[^\]]*\]",
        r"\{[^}]*\}",
        r"\(\s*#?[A-Z]{2,5}\s*\)",
        r"#\s*(?:IN|IND|PK|PUN|BAN|BANG)\b",
        r"\b(?:IN|IND|PK|PUN|BAN|BANG)\b$",
        r"\b2160P\b|\b1080P\b|\b720P\b|\b480P\b",
        r"\b4K\b|\bUHD\b|\bUUHD\b|\bFHD\b|\bHD\b|\bSD\b",
        r"\b\d{5,}\b",
        r"^[=\-_\s]+",
        r"[=\-_\s]+$",
    ]
    for pattern in patterns:
        text = re.sub(pattern, " ", text, flags=re.I)
    for token in JUNK_TOKENS:
        text = re.sub(rf"\b{re.escape(token)}\b", " ", text, flags=re.I)
    return re.sub(r"\s+", " ", text).strip()

def clean_title(raw: str, content_type: str) -> str:
    text = normalize_separators(raw)
    text = remove_noise(text)
    year = extract_year(text)
    episode = extract_episode_code(text)
    text = re.sub(r"[^A-Za-z0-9 '&:\-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    if content_type == "tv":
        if episode:
            text = re.sub(r"\bS\d{1,2}E\d{1,3}\b", " ", text, flags=re.I)
            text = re.sub(r"\b\d{1,2}x\d{1,3}\b", " ", text, flags=re.I)
            text = re.sub(r"\bEP(?:ISODE)?\s*\d+\b", " ", text, flags=re.I)
        text = re.sub(r"\bSEASON\s+\d+\b", " ", text, flags=re.I)
        text = re.sub(r"\bEPISODE\s+\d+\b", " ", text, flags=re.I)
        text = smart_title_case(re.sub(r"\s+", " ", text).strip())
        return f"{text} {episode}".strip() if episode else text

    if year:
        base = re.sub(r"\b(19\d{2}|20\d{2})\b", " ", text)
        base = re.sub(r"\s+", " ", base).strip()
        base = smart_title_case(base)
        return f"{base} ({year})" if base else year

    return smart_title_case(text)

def is_garbage_title(title: str, content_type: str) -> bool:
    if not title or len(title) < 2:
        return True
    if len(re.findall(r"[A-Za-z]", title)) < 2 and not re.search(r"\b(19\d{2}|20\d{2})\b", title):
        return True
    if content_type == "movie" and extract_episode_code(title):
        return True
    return False

def is_englishish(title: str) -> bool:
    letters = re.findall(r"[A-Za-z]", title)
    non_ascii = re.findall(r"[^\x00-\x7F]", title)
    return len(letters) >= 2 and len(non_ascii) == 0

# ============================================================
# TMDB
# ============================================================

def load_tmdb_cache() -> dict:
    return load_json(TMDB_CACHE_FILE, {})

TMDB_CACHE = load_tmdb_cache()

def tmdb_search_movie(query: str, year: str) -> dict | None:
    clean_title = re.sub(r"\(\d{4}\)", "", query)
clean_title = re.sub(r"\(\d{4}\)", "", item.clean_title)
key = f"movie:{clean_title.strip().lower()}"

if key in TMDB_CACHE:
    return TMDB_CACHE[key]

    url = "https://api.themoviedb.org/3/search/movie"
    params = {"api_key": TMDB_API_KEY, "query": query}
    if year:
        params["year"] = year

    result = None
    try:
        res = requests_get(url, params=params)
        res.raise_for_status()
        items = res.json().get("results", [])
        result = items[0] if items else None
    except Exception:
        result = None

    TMDB_CACHE[key] = result
    return result

def tmdb_search_tv(query: str, year: str) -> dict | None:
    key = f"tv::{query}::{year}"
    if key in TMDB_CACHE:
        return TMDB_CACHE[key]

    url = "https://api.themoviedb.org/3/search/tv"
    params = {"api_key": TMDB_API_KEY, "query": query}
    if year:
        params["first_air_date_year"] = year

    result = None
    try:
        res = requests_get(url, params=params)
        res.raise_for_status()
        items = res.json().get("results", [])
        result = items[0] if items else None
    except Exception:
        result = None

    TMDB_CACHE[key] = result
    return result

def enrich_item(item: MediaItem) -> Optional[MediaItem]:
    title = item.clean_title
    year_guess = extract_year(title)

    if item.content_type == "movie":
        query = re.sub(r"\(\d{4}\)", "", title).strip()
        tmdb = tmdb_search_movie(query, year_guess)

        if tmdb:
            release_year = (tmdb.get("release_date") or "")[:4]
            if not release_year or int(release_year) < MIN_YEAR:
                return None
            movie_title = sanitize_filename(tmdb.get("title", query))
            item.final_name = f"{movie_title} ({release_year})"
            item.tmdb_id = tmdb.get("id")
            item.tmdb_type = "movie"
            item.year = release_year
        else:
            if not year_guess or int(year_guess) < MIN_YEAR:
                return None
            item.final_name = f"{sanitize_filename(query)} ({year_guess})"
            item.tmdb_id = None
            item.tmdb_type = "movie"
            item.year = year_guess

        return item

    show_query = re.sub(r"\bS\d{1,2}E\d{1,3}\b", "", title, flags=re.I)
    show_query = re.sub(r"\b\d{1,2}x\d{1,3}\b", "", show_query, flags=re.I)
    show_query = re.sub(r"\bE\d{2}\b", "", show_query, flags=re.I).strip()
    tmdb = tmdb_search_tv(show_query, year_guess)
    episode = extract_episode_code(title)

    if not episode:
        return None

    if tmdb:
        first_year = (tmdb.get("first_air_date") or "")[:4]
        if first_year and int(first_year) < MIN_YEAR:
            return None
        item.show_name = sanitize_filename(tmdb.get("name", show_query))
        item.tmdb_id = tmdb.get("id")
        item.tmdb_type = "tv"
        item.year = first_year
    else:
        if year_guess and int(year_guess) < MIN_YEAR:
            return None
        item.show_name = sanitize_filename(show_query)
        item.tmdb_id = None
        item.tmdb_type = "tv"
        item.year = year_guess

    item.final_name = f"{item.show_name} {episode}"
    return item

# ============================================================
# OUTPUT PATHS / DEDUPE
# ============================================================

def movie_relpath(item: MediaItem) -> str:
    name = sanitize_filename(item.final_name)
    if item.is_adult:
        return f"{ADULT_ROOT}/{name}.strm"
    if item.is_bollywood:
        return f"{BOLLYWOOD_ROOT}/{name}.strm"
    return f"{MOVIES_ROOT}/{bucket_letter(name)}/{name}.strm"

def movie_nfo_relpath(item: MediaItem) -> str:
    name = sanitize_filename(item.final_name)
    if item.is_adult:
        return f"{ADULT_ROOT}/{name}.nfo"
    if item.is_bollywood:
        return f"{BOLLYWOOD_ROOT}/{name}.nfo"
    return f"{MOVIES_ROOT}/{bucket_letter(name)}/{name}.nfo"

def tv_relpath(item: MediaItem) -> str:
    show = sanitize_filename(item.show_name)
    ep = sanitize_filename(item.final_name)
    return f"{TV_ROOT}/{bucket_letter(show)}/{show}/{ep}.strm"

def tvshow_nfo_relpath(item: MediaItem) -> str:
    show = sanitize_filename(item.show_name)
    return f"{TV_ROOT}/{bucket_letter(show)}/{show}/tvshow.nfo"

def normalize_movie_key(item: MediaItem) -> str:
    return re.sub(r"\s+", " ", item.final_name.lower()).strip()

def normalize_tv_key(item: MediaItem) -> str:
    return f"{item.show_name.lower()}::{item.final_name.lower()}"

def preference_tuple(item: MediaItem) -> tuple:
    return (
        1 if item.english_pref else 0,
        item.quality,
        1 if item.tmdb_id else 0,
        len(item.url),
    )

def write_movie_nfo(item: MediaItem) -> str:
    if not item.tmdb_id:
        return ""
    return f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<movie>
  <title>{item.final_name}</title>
  <year>{item.year}</year>
  <uniqueid type="tmdb" default="true">{item.tmdb_id}</uniqueid>
</movie>
"""

def write_tvshow_nfo(item: MediaItem) -> str:
    if not item.tmdb_id:
        return ""
    return f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<tvshow>
  <title>{item.show_name}</title>
  <uniqueid type="tmdb" default="true">{item.tmdb_id}</uniqueid>
</tvshow>
"""

# ============================================================
# GOOGLE DRIVE UPLOAD
# ============================================================

def gdrive_enabled() -> bool:
    return bool(GDRIVE_SERVICE_ACCOUNT_JSON_B64 and GDRIVE_FOLDER_ID)

def get_gdrive_service():
    if not gdrive_enabled():
        return None

    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    raw = base64.b64decode(GDRIVE_SERVICE_ACCOUNT_JSON_B64).decode("utf-8")
    info = json.loads(raw)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def list_children(service, parent_id: str) -> dict:
    q = f"'{parent_id}' in parents and trashed = false"
    fields = "files(id,name,mimeType)"
    files = {}
    page_token = None

    while True:
        resp = service.files().list(
            q=q,
            fields=f"nextPageToken,{fields}",
            pageToken=page_token,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        for f in resp.get("files", []):
            files[(f["name"], f["mimeType"])] = f["id"]
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files

def ensure_drive_folder(service, name: str, parent_id: str, cache: dict) -> str:
    key = f"{parent_id}/{name}"
    if key in cache:
        return cache[key]

    mime = "application/vnd.google-apps.folder"
    children = list_children(service, parent_id)
    existing = children.get((name, mime))
    if existing:
        cache[key] = existing
        return existing

    meta = {
        "name": name,
        "mimeType": mime,
        "parents": [parent_id],
    }
    created = service.files().create(
        body=meta,
        fields="id",
        supportsAllDrives=True
    ).execute()
    cache[key] = created["id"]
    return created["id"]

def upload_file_to_drive(service, local_bytes: bytes, relpath: str, parent_root_id: str, folder_cache: dict, file_cache: dict):
    from googleapiclient.http import MediaIoBaseUpload
    import io

    parts = relpath.split("/")
    folder_parts = parts[:-1]
    filename = parts[-1]

    parent_id = parent_root_id
    for folder in folder_parts:
        parent_id = ensure_drive_folder(service, folder, parent_id, folder_cache)

    file_key = f"{parent_id}/{filename}"
    children = file_cache.get(parent_id)
    if children is None:
        children = list_children(service, parent_id)
        file_cache[parent_id] = children

    existing_id = children.get((filename, "application/octet-stream")) or children.get((filename, "text/plain")) or None

    media = MediaIoBaseUpload(io.BytesIO(local_bytes), resumable=False, mimetype="text/plain")
    meta = {"name": filename, "parents": [parent_id]}

    if existing_id:
        service.files().update(
            fileId=existing_id,
            media_body=media,
            supportsAllDrives=True
        ).execute()
    else:
        created = service.files().create(
            body=meta,
            media_body=media,
            fields="id,mimeType",
            supportsAllDrives=True
        ).execute()
        children[(filename, created.get("mimeType", "text/plain"))] = created["id"]

def delete_drive_path(service, relpath: str, parent_root_id: str, folder_cache: dict):
    parts = relpath.split("/")
    parent_id = parent_root_id
    mime_folder = "application/vnd.google-apps.folder"

    for folder in parts[:-1]:
        children = list_children(service, parent_id)
        folder_id = children.get((folder, mime_folder))
        if not folder_id:
            return
        parent_id = folder_id

    filename = parts[-1]
    children = list_children(service, parent_id)
    for (name, _mime), file_id in children.items():
        if name == filename:
            service.files().delete(fileId=file_id, supportsAllDrives=True).execute()
            return

# ============================================================
# MAIN
# ============================================================

def main() -> None:
    start = time.time()
    ensure_dirs()

    # Optional dependencies only when Drive upload is enabled
    if gdrive_enabled():
        try:
            import google.oauth2  # noqa
            import googleapiclient  # noqa
        except Exception as exc:
            raise RuntimeError("Google Drive upload enabled, but google-api-python-client is not installed.") from exc

    manifest_old = load_json(MANIFEST_FILE, {})
    report = {
        "raw_entries": 0,
        "after_filtering": 0,
        "after_rough_dedupe": 0,
        "after_tmdb_year_filter": 0,
        "movies_written": 0,
        "tv_written": 0,
        "adult_written": 0,
        "bollywood_written": 0,
        "changed_files": 0,
        "deleted_files": 0,
        "elapsed_seconds": 0,
    }

    download_playlist()
    raw_entries = parse_m3u(PLAYLIST_FILE)
    report["raw_entries"] = len(raw_entries)
    print(f"Parsed entries: {len(raw_entries)}")

    filtered: list[MediaItem] = []
    bolly_playlist_entries: list[dict] = []

    for entry in raw_entries:
        attrs = extract_attrs(entry["extinf"])
        group = attrs.get("group-title", "")
        combo = upper_text(entry["title"], attrs.get("tvg-name", ""), group, entry["url"])
        title_upper = entry["title"].upper()

        if not looks_vod(entry):
            continue
        if is_definitely_junk(entry["title"], group):
            continue
        if has_code_tag(combo):
            continue
        if has_word_tag(combo):
            continue
        if ends_with_fr(title_upper):
            continue

        content_type = infer_content_type(entry["title"], group)
        clean = clean_title(entry["title"], content_type)
        if is_garbage_title(clean, content_type):
            continue

        is_bollywood = content_type == "movie" and any(w in combo for w in BOLLYWOOD_WORDS)
        english_pref = is_englishish(clean)

        if not english_pref and not is_bollywood:
            continue

        item = MediaItem(
            url=entry["url"],
            raw_title=entry["title"],
            clean_title=clean,
            content_type=content_type,
            group=group,
            quality=quality_score(entry["title"]),
            is_adult=any(w in combo for w in ADULT_WORDS),
            is_bollywood=is_bollywood,
            english_pref=english_pref,
        )

        if is_bollywood:
            bolly_playlist_entries.append({"title": clean, "url": entry["url"]})

        filtered.append(item)

    report["after_filtering"] = len(filtered)
    print(f"After filtering: {len(filtered)}")

    with BOLLYWOOD_PLAYLIST.open("w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for e in bolly_playlist_entries:
            f.write(f"#EXTINF:-1,{e['title']}\n{e['url']}\n")

    # Rough dedupe before TMDB
    rough_best = {}
    for item in filtered:
        if item.content_type == "movie":
            key = f"movie::{re.sub(r'\\(\\d{4}\\)', '', item.clean_title).strip().lower()}"
        else:
            show = re.sub(r"\bS\d{1,2}E\d{1,3}\b", "", item.clean_title, flags=re.I)
            show = re.sub(r"\b\d{1,2}x\d{1,3}\b", "", show, flags=re.I).strip().lower()
            ep = extract_episode_code(item.clean_title)
            key = f"tv::{show}::{ep.lower()}"

        current = rough_best.get(key)
        if current is None or preference_tuple(item) > preference_tuple(current):
            rough_best[key] = item

    candidates = list(rough_best.values())
    report["after_rough_dedupe"] = len(candidates)
    print(f"After rough dedupe: {len(candidates)}")

    # TMDB enrichment
    enriched: list[MediaItem] = []
    with ThreadPoolExecutor(max_workers=MAX_TMDB_WORKERS) as pool:
        futures = [pool.submit(enrich_item, item) for item in candidates]
        for fut in as_completed(futures):
            result = fut.result()
            if result:
                enriched.append(result)

    report["after_tmdb_year_filter"] = len(enriched)
    print(f"After TMDB/year filter: {len(enriched)}")

    # Final dedupe
    best_movies = {}
    best_tv = {}

    for item in enriched:
        if item.content_type == "movie":
            key = normalize_movie_key(item)
            current = best_movies.get(key)
            if current is None or preference_tuple(item) > preference_tuple(current):
                best_movies[key] = item
        else:
            key = normalize_tv_key(item)
            current = best_tv.get(key)
            if current is None or preference_tuple(item) > preference_tuple(current):
                best_tv[key] = item

    final_movies = list(best_movies.values())
    final_tv = list(best_tv.values())

    print(f"Final movies: {len(final_movies)}")
    print(f"Final TV episodes: {len(final_tv)}")

    # Build new manifest
    manifest_new = {}

    for item in final_movies:
        rel = movie_relpath(item)
        manifest_new[rel] = {
            "sha1": sha1_text(item.url),
            "content": item.url + "\n",
            "type": "strm",
        }

        nfo = write_movie_nfo(item)
        if nfo:
            rel_nfo = movie_nfo_relpath(item)
            manifest_new[rel_nfo] = {
                "sha1": sha1_text(nfo),
                "content": nfo,
                "type": "nfo",
            }

        report["movies_written"] += 1
        if item.is_adult:
            report["adult_written"] += 1
        elif item.is_bollywood:
            report["bollywood_written"] += 1

    for item in final_tv:
        rel = tv_relpath(item)
        manifest_new[rel] = {
            "sha1": sha1_text(item.url),
            "content": item.url + "\n",
            "type": "strm",
        }

        tvshow_nfo = write_tvshow_nfo(item)
        if tvshow_nfo:
            rel_nfo = tvshow_nfo_relpath(item)
            manifest_new[rel_nfo] = {
                "sha1": sha1_text(tvshow_nfo),
                "content": tvshow_nfo,
                "type": "nfo",
            }

        report["tv_written"] += 1

    # Incremental local sync
    for relpath, meta in manifest_new.items():
        old = manifest_old.get(relpath)
        if old and old.get("sha1") == meta["sha1"]:
            continue

        full = OUTPUT_ROOT_LOCAL_FALLBACK(relpath)
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_text(meta["content"], encoding="utf-8")
        report["changed_files"] += 1

    stale = set(manifest_old) - set(manifest_new)
    for relpath in stale:
        full = OUTPUT_ROOT_LOCAL_FALLBACK(relpath)
        if full.exists():
            full.unlink()
            report["deleted_files"] += 1

    # Incremental Google Drive sync
    if gdrive_enabled():
        service = get_gdrive_service()
        folder_cache = {}
        file_cache = {}

        for relpath, meta in manifest_new.items():
            old = manifest_old.get(relpath)
            if old and old.get("sha1") == meta["sha1"]:
                continue
            upload_file_to_drive(
                service=service,
                local_bytes=meta["content"].encode("utf-8"),
                relpath=relpath,
                parent_root_id=GDRIVE_FOLDER_ID,
                folder_cache=folder_cache,
                file_cache=file_cache,
            )

        for relpath in stale:
            delete_drive_path(
                service=service,
                relpath=relpath,
                parent_root_id=GDRIVE_FOLDER_ID,
                folder_cache=folder_cache,
            )

    save_json(MANIFEST_FILE, {k: {"sha1": v["sha1"], "type": v["type"]} for k, v in manifest_new.items()})
    save_json(TMDB_CACHE_FILE, TMDB_CACHE)

    report["elapsed_seconds"] = round(time.time() - start, 2)
    save_json(REPORT_FILE, report)

    print("\nDone.")
    print(f"Movies written: {report['movies_written']}")
    print(f"TV written: {report['tv_written']}")
    print(f"Adult written: {report['adult_written']}")
    print(f"Bollywood written: {report['bollywood_written']}")
    print(f"Changed files: {report['changed_files']}")
    print(f"Deleted files: {report['deleted_files']}")
    print(f"Report: {REPORT_FILE}")

# Local fallback mirror in workflow workspace
def OUTPUT_ROOT_LOCAL_FALLBACK(relpath: str) -> Path:
    return WORK_DIR / "output" / relpath

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("\nStopped by user.")
    except Exception as exc:
        sys.exit(f"\nError: {exc}")
