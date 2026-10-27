import requests
import gzip
import json
import os
import random
import re
import shutil
import sys
import time
from collections import deque

BASE = "https://www.deezer.com"
SITEMAP_URL = f"{BASE}/catalog_sitemap/en/album.xml.gz"
LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 100000
POOL_EVERY = 1000
TIMEOUT = 15
SLEEP_RANGE = (0, 0.3)
RETRY_STATUS = (429, 500, 502, 503, 504)
MIN_FREE_DISK_GB = 10
BLOCK_LIMIT = 20
MARKERS = (
    "__DZR_APP_STATE__",
    'href="{BASE}/en/{kind}/{entity_id}"',
)
CHECKPOINT_INTERVAL = 10000
CHECKPOINT_FILE = os.path.join("data", "checkpoint.json")
PAGES_DIR = os.path.join("data", "pages")
INITIAL_STATS = {"album": 0, "artist": 0, "skipped": 0, "dead": 0}
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; StudentCrawler/1.0; STU Bratislava; contact: xrado@stuba.sk)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

def load_seeds():
    response = requests.get(SITEMAP_URL, headers=HEADERS, timeout=TIMEOUT)
    xml = gzip.decompress(response.content).decode("utf-8")
    return re.findall(rf"<loc>({re.escape(BASE)}/en/album/\d+)</loc>", xml)

def parse_url(url):
    for kind in ("album", "artist"):
        match = re.search(f"/en/{kind}/(\\d+)", url)
        if match:
            return kind, match.group(1)
    return None, None

def page_valid(kind, entity_id, html):
    return all(marker.format(BASE=BASE, kind=kind, entity_id=entity_id) in html for marker in MARKERS)

def fetch(session, url):
    last_status = None
    for attempt in range(3):
        try:
            response = session.get(url, timeout=TIMEOUT)
            last_status = response.status_code
            if response.status_code == 200:
                return response, False
            if response.status_code not in RETRY_STATUS:
                return None, response.status_code == 403
        except requests.RequestException:
            pass
        time.sleep(2 ** (attempt + 1))
    return None, last_status in RETRY_STATUS

def links_from(html, kind):
    if kind == "album":
        return [f"{BASE}/en/artist/{i}" for i in set(re.findall(r'href="/en/artist/(\d+)"', html))]
    if kind == "artist":
        album_links = [f"{BASE}/en/album/{i}" for i in set(re.findall(r'href="/en/album/(\d+)"', html))]
        artist_links = [f"{BASE}/en/artist/{i}" for i in set(re.findall(r'href="/en/artist/(\d+)"', html))]
        return album_links + artist_links
    return []

def save(kind, entity_id, content):
    path = os.path.join(PAGES_DIR, f"{kind}_{entity_id}.html.gz")
    with gzip.open(path, "wb") as file:
        file.write(content)

def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return deque(), {"album": set(), "artist": set()}, dict(INITIAL_STATS)
    with open(CHECKPOINT_FILE, encoding="utf-8") as file:
        payload = json.load(file)
    seen = {kind: set(ids) for kind, ids in payload["seen"].items()}
    return deque(payload["frontier"]), seen, payload["stats"]

def save_checkpoint(frontier, seen, stats):
    payload = {
        "frontier": list(frontier),
        "seen": {kind: sorted(ids) for kind, ids in seen.items()},
        "stats": stats,
    }
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as file:
        json.dump(payload, file)

def print_stats(done, stats, started):
    files = os.listdir(PAGES_DIR)
    size_mb = sum(os.path.getsize(os.path.join(PAGES_DIR, f)) for f in files) / 1024 / 1024
    relevant_pct = stats["album"] / done * 100 if done else 0
    
    print("---")
    print(f"downloaded: {done} (album {stats['album']}, artist {stats['artist']})")
    print(f"skipped: {stats['skipped']}\t\tdead: {stats['dead']}")
    print(f"files: {len(files)}\t\tsize: {size_mb:.2f} MB\t\trelevant: {stats['album']}/{done} ({relevant_pct:.1f}%)")
    print(f"elapsed this round: {time.time() - started:.1f} s")

def main():
    os.makedirs(PAGES_DIR, exist_ok=True)
    session = requests.Session()
    session.headers.update(HEADERS)

    frontier, seen, stats = load_checkpoint()
    done = stats["album"] + stats["artist"]
    if done:
        print(f"resumed: {done} downloaded")

    seeds = load_seeds()
    random.shuffle(seeds)
    pool = deque(seeds)
    print(f"seeds: {len(pool)}")

    started = time.time()
    block_streak = 0
    while done < LIMIT and (frontier or pool):
        free_gb = shutil.disk_usage(PAGES_DIR).free / 1024 ** 3
        if free_gb < MIN_FREE_DISK_GB:
            print(f"low disk space ({free_gb:.1f} GB left), stopping")
            break
        if pool and done % POOL_EVERY == 0:
            url = pool.popleft()
        elif frontier:
            url = frontier.popleft()
        else:
            url = pool.popleft()

        kind, entity_id = parse_url(url)
        if kind is None or entity_id in seen[kind]:
            stats["skipped"] += 1
            continue
        seen[kind].add(entity_id)

        response, blocked = fetch(session, url)
        html = response.content.decode("utf-8", "replace") if response is not None else ""
        if response is not None and response.status_code == 200 and not page_valid(kind, entity_id, html):
            blocked = True

        if blocked:
            seen[kind].discard(entity_id)
            frontier.append(url)
            block_streak += 1
            print(f"block suspicion {block_streak}/{BLOCK_LIMIT}: {url}")
            if block_streak >= BLOCK_LIMIT:
                print("blocked, stopping")
                break
            time.sleep(random.uniform(*SLEEP_RANGE))
            continue

        if response is None:
            stats["dead"] += 1
            print("dead:", url)
            continue

        block_streak = 0

        done += 1
        stats[kind] += 1
        save(kind, entity_id, response.content)
        print(f"[{done}/{LIMIT}] {kind} {entity_id}")

        for link in links_from(html, kind):
            link_kind, link_id = parse_url(link)
            if link_id not in seen[link_kind]:
                frontier.append(link)

        if done % CHECKPOINT_INTERVAL == 0:
            save_checkpoint(frontier, seen, stats)
        time.sleep(random.uniform(*SLEEP_RANGE))

    save_checkpoint(frontier, seen, stats)
    print_stats(done, stats, started)

if __name__ == "__main__":
    main()
