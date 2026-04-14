#!/usr/bin/env python3
import json
import os
import re
import shutil
import unicodedata
from pathlib import Path

from openai import OpenAI

BASE_DIR = Path(".")
PLAYLIST = BASE_DIR / "playlist.m3u"
OUTPUT_DIR = BASE_DIR / "InfuseMedia"
CACHE_FILE = BASE_DIR / "ai_cache.json"

USE_AI = bool(os.getenv("OPENAI_API_KEY"))
AI_MODEL = "gpt-4.1-mini"
MIN_AI_CHECKS_PER_RUN = 50
MAX_AI_CHECKS_PER_RUN = 300
CURRENT_AI_BUDGET = 100

client = OpenAI() if USE_AI else None

SEASON_EPISODE_RE = re.compile(r"\bS(\d{1,2})\s*E(\d{1,3})\b", 
re.IGNORECASE)
YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")
BRACKET_LANG_RE = re.compile(r"\[\s*[A-Z]{2,}\s*\]", re.IGNORECASE)
HASH_TAG_RE = re.compile(
    r"(?:\s*#\s*(?:"
    
r"[A-Z]{2,}\d*|French|Spanish|German|Italian|Arabic|Turkish|Dutch|Greek|Russian|"
    r"Punjabi|Hindi|Urdu|Bengali|Tamil|Telugu|Malayalam"
    r")\b)",
    re.IGNORECASE,
)

JUNK_WORDS = [
    "UHD", "FHD", "HD", "HEVC", "X265", "X264", "H264", "H265",
    "4K", "2160P", "1080P", "720P", "DVDSCR", "SCREENER", "WEBRIP",
    "WEB-DL", "WEB", "BLURAY", "BDRIP", "HDRIP", "CAM", "TS", "TC",
    "SUB", "KORSUB", "MULTI", "ENG", "DUAL", "AUDIO", "PROPER",
    "REPACK", "EXTENDED", "UNCUT", "HDR", "DV", "ATMOS", "REMUX",
    "CODEC"
]
JUNK_WORD_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(w) for w in JUNK_WORDS) + r")\b",
    re.IGNORECASE,
)

THREED_RE = re.compile(r"\b3D\b|\b3d\b|3d[-\s]*", re.IGNORECASE)
INVERTED_PUNCT_RE = re.compile(r"[¡¿]")
NON_ASCII_RE = re.compile(r"[^\x00-\x7F]")
ARABIC_CHAR_RE = re.compile(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]")
INVALID_FS_CHARS_RE = re.compile(r'[<>:"/\\|?*\x00-\x1F]')
MULTISPACE_RE = re.compile(r"\s+")
EMPTY_BRACKETS_RE = re.compile(r"\(\s*\)|\[\s*\]")

ADULT_TERMS = [
    "adult", "xxx", "porn", "erotic", "sex", "nsfw",
    "brazzers", "naughtyamerica", "realitykings", "mofos",
    "bangbros", "vixen", "blacked", "tushy", "deeper",
    "fake taxi", "fakehub", "onlyfans"
]
ADULT_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(term) for term in ADULT_TERMS) + r")\b",
    re.IGNORECASE,
)

SPORT_EVENT_TERMS = [
    "ufc", "wwe", "fifa", "boxing", "fight night", "pay per view", "ppv",
    "premier league", "laliga", "serie a", "bundesliga", "champions 
league",
    "europa league", "fa cup", "copa del rey", "world cup", "friendly 
match",
    "real madrid", "manchester united", "arsenal", "chelsea", "liverpool",
    "juventus", "milan", "fc ", "cf ", " vs ", "vs.", "round 2leg"
]
SPORT_EVENT_RE = re.compile(
    r"(?:" + "|".join(re.escape(x) for x in SPORT_EVENT_TERMS) + r")",
    re.IGNORECASE,
)

PERFORMANCE_TERMS = [
    "concert", "live at", "live in", "live from", "theatre", "theater",
    "opera", "ballet", "broadway", "festival", "unplugged",
    "stand up", "stand-up", "special", "tour", "session"
]
PERFORMANCE_RE = re.compile(
    r"(?:" + "|".join(re.escape(x) for x in PERFORMANCE_TERMS) + r")",
    re.IGNORECASE,
)

MUSIC_OR_COMEDY_TERMS = [
    "adele", "alanis morissette", "henry rollins", "madonna", "beyonce",
    "taylor swift", "drake", "rihanna", "avicii", "aziz ansari"
]
MUSIC_OR_COMEDY_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(x) for x in MUSIC_OR_COMEDY_TERMS) + 
r")\b",
    re.IGNORECASE,
)

YU_RE = re.compile(
    r"\b(?:YU|YUGO|YUGOSLAV|EX[- 
]?YU|BALKAN|SERBIA|CROATIA|BOSNIA|SLOVENIA|MONTENEGRO)\b",
    re.IGNORECASE,
)

BOLLYWOOD_RE = re.compile(
    r"(?:"
    r"\b(?:IN|IND|PK|PUN)\b|"
    r"\b(?:Tamil|Hindi|Urdu|Punjabi|Telugu|Malayalam|Bengali)\b|"
    r"\[\s*(?:IN|IND|PK|PUN)\s*\]|"
    r"#\s*(?:IN|IND|PK|PUN)\b"
    r")",
    re.IGNORECASE,
)

FOREIGN_RE = re.compile(
    r"\b(?:"
    r"yalniz|kurt|yarim|yarin|yanimda|yahsi|yeni|yil|soygu|yeralti|"
    r"calikusu|cici|cinar|cilgin|cinayet|borcu|zaman|zeytin|agaci|"
    r"zehir|hafiye|zindagi|dobara|arkadasim|beni|yakari|yabanci|yasahime|"
    r"ghaziabad|zhu|zhuzhu|zouzounia|yaadein|yaarana|yaraana|yardie|"
    r"yazi|yol|yodha|zombi|settat|cadaver|calvaire|calmos|forajidos|"
    r"cajas|tempo|homme|legendado|weten|zwijgen|tijd|zwei|trumpfen|auf|"
    r"oleum|miel|vie|moi|pas|nubi|zee|van"
    r")\b",
    re.IGNORECASE,
)

ENGLISH_HINT_RE = re.compile(
    r"\b(?:"
    
r"the|and|you|your|with|from|night|day|house|man|woman|girl|boy|story|"
    
r"movie|love|life|world|home|dead|dark|light|blood|city|road|war|king|"
    
r"queen|crime|killer|ghost|return|rise|fall|planet|last|first|american|"
    r"christmas|summer|winter|wanted|yellow|young|year|years|rabbit|"
    r"rock|call|before|after|inside|outside|circle|circus|citizen|"
    r"cinderella|cake|calm|cabin|calendar|captain|doctor|family|dream|"
    r"angel|hunter|hall|lake|jane|rose|bird|yes|here|worst|master|"
    r"princess|justice|zero|zebra|zombie|zone|zoom|storm|future|row|"
    r"secret|translated|intervention|captivating|midwife|theorem|"
    r"tolerance|contact|space|adventure|child|town|gold|sheldon|"
    r"honor|garden|perfect|sherlock|masterchef|yellowstone|yesterday|"
    r"again|mother|bear|bestie|chef|virginia|days|friends|neighbor|"
    r"wallander|royals|camp|castle|carnival|cars|carol|cat|captivity|"
    r"future|fall|midwife|rock|wanted|royals"
    r")\b",
    re.IGNORECASE,
)

BAD_TITLE_RE = re.compile(
    r"^\d{6,}$|"
    r"^\d{8,}-|"
    r"^\d{8,}[a-z0-9._-]*$|"
    r"^[a-z]{2,8}-[a-z0-9._-]{3,}$|"
    r"\b(?:asat|wcq|sprtsch|gc-720|bl1-f|codec)\b|"
    r"\b(?:720|1080|2160)\b",
    re.IGNORECASE,
)

ALLOWED_NUMERIC_TITLES = {
    "1883", "1899", "1917", "1922", "1923", "1984", "1985", "1987",
    "1992", "2010", "2012", "1408", "1670", "211", "300", "360",
    "365 Days", "600 Miles", "825 Forest Road"
}

def load_cache() -> dict:
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

AI_CACHE = load_cache()
AI_CHECK_COUNT = 0

def save_cache() -> None:
    CACHE_FILE.write_text(json.dumps(AI_CACHE, indent=2, 
ensure_ascii=False), encoding="utf-8")

def estimate_ai_budget(total_entries: int) -> int:
    if total_entries <= 5000:
        return MIN_AI_CHECKS_PER_RUN
    if total_entries <= 20000:
        return 100
    if total_entries <= 50000:
        return 175
    if total_entries <= 100000:
        return 225
    return MAX_AI_CHECKS_PER_RUN

def normalize_spaces(text: str) -> str:
    text = text.replace("|", " ")
    text = MULTISPACE_RE.sub(" ", text)
    return text.strip(" -._,:")

def sanitize_fs_name(name: str) -> str:
    name = INVALID_FS_CHARS_RE.sub("", name)
    name = MULTISPACE_RE.sub(" ", name).strip()
    return name.rstrip(".") or "Unknown"

def bucket_letter(name: str) -> str:
    if not name:
        return "#"
    first = name[0].upper()
    return first if "A" <= first <= "Z" else "#"

def ascii_fold(text: str) -> str:
    return unicodedata.normalize("NFKD", text).encode("ascii", 
"ignore").decode("ascii")

def canonical_text(text: str) -> str:
    text = ascii_fold(text).lower()
    text = re.sub(r"\b(19\d{2}|20\d{2})\b", "", text)
    text = re.sub(r"[^\w\s]", " ", text)
    return MULTISPACE_RE.sub(" ", text).strip()

def extract_best_year(title: str):
    years = YEAR_RE.findall(title)
    if not years:
        return None
    paren_years = re.findall(r"\(\s*(19\d{2}|20\d{2})\s*\)", title)
    return paren_years[-1] if paren_years else years[-1]

def is_allowed_numeric_title(title: str) -> bool:
    if title in ALLOWED_NUMERIC_TITLES:
        return True
    return bool(re.fullmatch(r"\d{4}", title) and title in 
ALLOWED_NUMERIC_TITLES)

def strip_tags_and_junk(title: str) -> str:
    title = BRACKET_LANG_RE.sub("", title)
    title = HASH_TAG_RE.sub("", title)
    title = JUNK_WORD_RE.sub("", title)
    title = THREED_RE.sub("", title)

    title = re.sub(
        
r"\b(?:FR|DE|NL|GR|ES|TR|IT|AR|PT|RU|MA|MK|LAT|TN|YU|IN|IND|PK|PUN)\b$",
        "",
        title,
        flags=re.IGNORECASE,
    )

    title = re.sub(
        
r"\b(French|Spanish|German|Italian|Arabic|Turkish|Dutch|Greek|Russian|Punjabi|Hindi|Urdu|Tamil|Telugu|Malayalam|Bengali)\b",
        "",
        title,
        flags=re.IGNORECASE,
    )

    title = re.sub(r"\s*#\s*", " ", title)
    title = EMPTY_BRACKETS_RE.sub("", title)
    return normalize_spaces(title)

def clean_tv_title(title: str) -> str:
    match = SEASON_EPISODE_RE.search(title)
    show_name = strip_tags_and_junk(title[:match.start()])
    season = int(match.group(1))
    episode = int(match.group(2))
    return f"{show_name} S{season:02d} E{episode:02d}"

def clean_movie_title(title: str) -> str:
    year = extract_best_year(title)
    cleaned = strip_tags_and_junk(title)
    cleaned = YEAR_RE.sub("", cleaned)
    cleaned = EMPTY_BRACKETS_RE.sub("", cleaned)
    cleaned = normalize_spaces(cleaned)
    return f"{cleaned} ({year})" if year else cleaned

def clean_title(raw_title: str) -> str:
    title = normalize_spaces(raw_title)
    if SEASON_EPISODE_RE.search(title):
        return clean_tv_title(title)
    return clean_movie_title(title)

def is_adult_title(raw_title: str, cleaned_title: str) -> bool:
    return bool(ADULT_RE.search(raw_title) or 
ADULT_RE.search(cleaned_title))

def is_bollywood_entry(raw_title: str, cleaned_title: str) -> bool:
    return bool(BOLLYWOOD_RE.search(f"{raw_title} {cleaned_title}"))

def is_provider_garbage(title: str) -> bool:
    t = title.lower()

    if t.startswith("aaf-"):
        return True
    if title.count(".") >= 3:
        return True
    if re.search(r"^[a-z0-9._-]{18,}$", t):
        return True
    if re.match(r"^[a-z0-9]+(\.[a-z0-9]+){2,}$", t):
        return True

    letters = re.findall(r"[A-Za-z]", title)
    if len(letters) < 2 and not is_allowed_numeric_title(title):
        return True

    if letters:
        uppercase_ratio = sum(1 for c in letters if c.isupper()) / 
len(letters)
        if uppercase_ratio < 0.18 and len(title) > 14:
            return True

    return False

def english_score(title: str) -> int:
    score = 0
    words = re.findall(r"[a-z]+", ascii_fold(title).lower())
    for word in words:
        if ENGLISH_HINT_RE.search(word):
            score += 2
        elif len(word) >= 4:
            score += 1
    return score

def rule_looks_foreign(cleaned_title: str) -> bool:
    lowered = ascii_fold(cleaned_title).lower()

    if NON_ASCII_RE.search(cleaned_title):
        return True
    if "'" in lowered:
        return True
    if FOREIGN_RE.search(lowered):
        return True

    words = re.findall(r"[a-z]+", lowered)
    if not words:
        return True

    score = english_score(cleaned_title)
    long_words = [w for w in words if len(w) >= 5]

    if len(long_words) >= 2 and score < 3:
        return True
    if score == 0 and len(words) <= 2:
        return True

    return False

def clearly_english(cleaned_title: str) -> bool:
    lowered = ascii_fold(cleaned_title).lower()
    if ENGLISH_HINT_RE.search(lowered):
        return True
    return english_score(cleaned_title) >= 4

def ai_keep_title(cleaned_title: str, is_tv: bool) -> bool:
    global AI_CHECK_COUNT

    key = f"{'tv' if is_tv else 'movie'}::{cleaned_title.lower()}"
    if key in AI_CACHE:
        return bool(AI_CACHE[key])

    if not USE_AI or AI_CHECK_COUNT >= CURRENT_AI_BUDGET:
        return True

    prompt = (
        "Return only KEEP or DROP.\n"
        "KEEP only if this is most likely a real English movie or TV title 
that belongs in a clean English media library.\n"
        "DROP if it is foreign-language, unclear, junk metadata, provider 
garbage, badly named, live event, concert, comedy special, sports, or 
suspicious.\n"
        "Be strict.\n\n"
        f"Title: {cleaned_title}\n"
        f"Type: {'TV' if is_tv else 'Movie'}"
    )

    try:
        AI_CHECK_COUNT += 1
        resp = client.responses.create(
            model=AI_MODEL,
            input=prompt,
        )
        text = (resp.output_text or "").strip().upper()
        keep = text.startswith("KEEP")
    except Exception:
        keep = True

    AI_CACHE[key] = keep
    if len(AI_CACHE) % 25 == 0:
        save_cache()
    return keep

def should_skip_entry(raw_title: str, cleaned_title: str, is_bollywood: 
bool) -> bool:
    text = f"{raw_title} {cleaned_title}"

    if INVERTED_PUNCT_RE.search(text):
        return True
    if ARABIC_CHAR_RE.search(text):
        return True
    if SPORT_EVENT_RE.search(text) or PERFORMANCE_RE.search(text):
        return True
    if MUSIC_OR_COMEDY_RE.search(text):
        return True
    if YU_RE.search(text):
        return True
    if BAD_TITLE_RE.search(cleaned_title) and not 
is_allowed_numeric_title(cleaned_title):
        return True
    if is_provider_garbage(cleaned_title):
        return True
    if len(re.findall(r"\d", cleaned_title)) >= 6:
        return True
    if len(cleaned_title) < 2:
        return True

    if is_bollywood:
        return False

    if clearly_english(cleaned_title):
        return False
    if rule_looks_foreign(cleaned_title):
        return True

    is_tv = bool(SEASON_EPISODE_RE.search(cleaned_title))
    return not ai_keep_title(cleaned_title, is_tv)

def parse_entries(lines):
    entries = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("#EXTINF:") and i + 1 < len(lines):
            url = lines[i + 1].strip()
            if "," in line:
                _, raw_title = line.split(",", 1)
                raw_title = raw_title.strip()
                cleaned_title = clean_title(raw_title)

                if cleaned_title:
                    is_bollywood = is_bollywood_entry(raw_title, 
cleaned_title)
                    if not should_skip_entry(raw_title, cleaned_title, 
is_bollywood):
                        entries.append({
                            "raw_title": raw_title,
                            "title": cleaned_title,
                            "url": url,
                            "is_tv": 
bool(SEASON_EPISODE_RE.search(cleaned_title)),
                            "is_adult": is_adult_title(raw_title, 
cleaned_title),
                            "is_bollywood": is_bollywood,
                        })
            i += 2
        else:
            i += 1
    return entries

def better_movie_entry(a, b):
    a_has_year = bool(YEAR_RE.search(a["title"]))
    b_has_year = bool(YEAR_RE.search(b["title"]))
    if a_has_year != b_has_year:
        return a if a_has_year else b
    if len(a["title"]) != len(b["title"]):
        return a if len(a["title"]) > len(b["title"]) else b
    return a if len(a["url"]) <= len(b["url"]) else b

def dedupe_entries(entries):
    deduped_movies = {}
    deduped_tv = {}

    for entry in entries:
        if entry["is_tv"]:
            m = SEASON_EPISODE_RE.search(entry["title"])
            show = entry["title"][:m.start()].strip()
            season = int(m.group(1))
            episode = int(m.group(2))
            key = (
                canonical_text(show),
                season,
                episode,
                entry["is_bollywood"],
                entry["is_adult"],
            )
            existing = deduped_tv.get(key)
            if existing is None or len(entry["title"]) > 
len(existing["title"]):
                deduped_tv[key] = entry
        else:
            movie_name = normalize_spaces(YEAR_RE.sub("", entry["title"]))
            key = (
                canonical_text(movie_name),
                entry["is_bollywood"],
                entry["is_adult"],
            )
            existing = deduped_movies.get(key)
            if existing is None:
                deduped_movies[key] = entry
            else:
                deduped_movies[key] = better_movie_entry(existing, entry)

    return list(deduped_movies.values()) + list(deduped_tv.values())

def ensure_base_dirs():
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)

    (OUTPUT_DIR / "Movies").mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "TV Shows").mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "Extras").mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "Bollywood" / "Movies").mkdir(parents=True, 
exist_ok=True)
    (OUTPUT_DIR / "Bollywood" / "TV Shows").mkdir(parents=True, 
exist_ok=True)

def preferred_movie_name(title: str) -> str:
    return sanitize_fs_name(title)

def write_strm_files(entries):
    ensure_base_dirs()

    movie_count = 0
    tv_count = 0
    adult_count = 0
    bolly_movie_count = 0
    bolly_tv_count = 0

    for entry in sorted(entries, key=lambda e: e["title"].lower()):
        title = entry["title"]
        url = entry["url"]

        if entry["is_tv"]:
            m = SEASON_EPISODE_RE.search(title)
            show_name = sanitize_fs_name(title[:m.start()].strip())
            season = int(m.group(1))
            episode = int(m.group(2))
            filename = sanitize_fs_name(f"{show_name} S{season:02d} 
E{episode:02d}.strm")

            if entry["is_bollywood"]:
                show_dir = OUTPUT_DIR / "Bollywood" / "TV Shows" / 
show_name
                show_dir.mkdir(parents=True, exist_ok=True)
                (show_dir / filename).write_text(url + "\n", 
encoding="utf-8")
                bolly_tv_count += 1
            else:
                letter = bucket_letter(show_name)
                show_dir = OUTPUT_DIR / "TV Shows" / letter / show_name
                show_dir.mkdir(parents=True, exist_ok=True)
                (show_dir / filename).write_text(url + "\n", 
encoding="utf-8")
                tv_count += 1
            continue

        movie_name = preferred_movie_name(title)

        if entry["is_adult"]:
            extras_dir = OUTPUT_DIR / "Extras"
            extras_dir.mkdir(parents=True, exist_ok=True)
            (extras_dir / f"{movie_name}.strm").write_text(url + "\n", 
encoding="utf-8")
            adult_count += 1
        elif entry["is_bollywood"]:
            movie_dir = OUTPUT_DIR / "Bollywood" / "Movies"
            movie_dir.mkdir(parents=True, exist_ok=True)
            (movie_dir / f"{movie_name}.strm").write_text(url + "\n", 
encoding="utf-8")
            bolly_movie_count += 1
        else:
            letter = bucket_letter(movie_name)
            movie_dir = OUTPUT_DIR / "Movies" / letter
            movie_dir.mkdir(parents=True, exist_ok=True)
            (movie_dir / f"{movie_name}.strm").write_text(url + "\n", 
encoding="utf-8")
            movie_count += 1

    return movie_count, tv_count, adult_count, bolly_movie_count, 
bolly_tv_count

def main():
    global CURRENT_AI_BUDGET

    if not PLAYLIST.exists():
        print(f"Missing playlist: {PLAYLIST}")
        return

    if not USE_AI:
        print("OPENAI_API_KEY not set. Running in fast local-only mode.")

    lines = PLAYLIST.read_text(encoding="utf-8", 
errors="ignore").splitlines()
    extinf_count = sum(1 for line in lines if line.startswith("#EXTINF:"))
    CURRENT_AI_BUDGET = estimate_ai_budget(extinf_count)

    entries = parse_entries(lines)
    entries = dedupe_entries(entries)

    movie_count, tv_count, adult_count, bolly_movies, bolly_tv = 
write_strm_files(entries)
    save_cache()

    print("Done.")
    print(f"Playlist used: {PLAYLIST.resolve()}")
    print(f"Output folder rebuilt: {OUTPUT_DIR.resolve()}")
    print(f"Movies written: {movie_count}")
    print(f"TV episodes written: {tv_count}")
    print(f"Adult extras written: {adult_count}")
    print(f"Bollywood movies written: {bolly_movies}")
    print(f"Bollywood TV episodes written: {bolly_tv}")
    print(f"AI cache entries: {len(AI_CACHE)}")
    print(f"AI checks used this run: {AI_CHECK_COUNT}")
    print(f"Adaptive AI budget this run: {CURRENT_AI_BUDGET}")

if __name__ == "__main__":
    main()
