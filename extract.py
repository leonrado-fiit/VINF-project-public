import gzip
import os
import re
import sys
import time

PAGES_DIR = os.path.join("data", "pages")
OUTPUT_FILE = os.path.join("data", "albums.tsv")
LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else None
SEPARATOR = "\x1f"
COLUMNS = [
    "album_id",
    "artist_id",
    "title",
    "artist",
    "release_date",
    "label",
    "num_tracks",
    "total_duration",
    "cover_url",
    "tracklist",
    "track_popularity_avg",
    "fans",
    "explicit",
    "featurings",
]

def album_id_from_path(path):
    name = os.path.basename(path)
    return name[len("album_"):-len(".html.gz")]

def load_seen():
    seen = set()
    if not os.path.exists(OUTPUT_FILE) or os.path.getsize(OUTPUT_FILE) == 0:
        return seen
    with open(OUTPUT_FILE, encoding="utf-8") as file:
        file.readline()
        for line in file:
            album_id = line.split("\t", 1)[0]
            if album_id:
                seen.add(album_id)
    return seen

def extract(html, album_id):
    match = re.search(r"window\.__DZR_APP_STATE__ = (\{.*\})</script>", html)
    if not match:
        return None
    state = match.group(1)

    match = re.search(r'"ALB_TITLE":"(.*?)","', state)
    title = match.group(1) if match else ""

    match = re.search(r'"ART_NAME":"(.*?)","', state)
    artist = match.group(1) if match else ""

    match = re.search(r'"ART_ID":"(\d+)"', state)
    artist_id = match.group(1) if match else ""

    match = re.search(r'"DIGITAL_RELEASE_DATE":"(.*?)","', state)
    release_date = match.group(1) if match else ""
    if not release_date:
        match = re.search(r'"PHYSICAL_RELEASE_DATE":"(.*?)","', state)
        release_date = match.group(1) if match else ""
    if not release_date:
        match = re.search(r'"ORIGINAL_RELEASE_DATE":"(.*?)","', state)
        release_date = match.group(1) if match else ""

    match = re.search(r'"LABEL_NAME":"(.*?)","', state)
    label = match.group(1) if match else ""

    match = re.search(r'"NUMBER_TRACK":"?(\d+)"?', state)
    num_tracks = match.group(1) if match else ""

    match = re.search(r'"DURATION":"?(\d+)"?', state)
    total_duration = match.group(1) if match else ""

    match = re.search(r'"ALB_PICTURE":"(.*?)","', state)
    cover_hash = match.group(1) if match else ""
    cover_url = f"https://cdn-images.dzcdn.net/images/cover/{cover_hash}/500x500.jpg" if cover_hash else ""

    match = re.search(r'"NB_FAN":(\d+)', state)
    fans = match.group(1) if match else ""

    explicit = 0
    match = re.search(r'"EXPLICIT_LYRICS_STATUS":(\d+)', state)
    album_status = match.group(1) if match else ""
    match = re.search(r'"EXPLICIT_COVER_STATUS":(\d+)', state)
    cover_status = match.group(1) if match else ""
    if album_status == "1" or cover_status == "1":
        explicit = 1

    match = re.search(r'"SONGS":\{"data":\[(.*?)\],"count":', state)
    if not match:
        return None
    songs = match.group(1)

    tracklist = []
    ranks = []
    featurings = []

    for chunk in re.split(r'\},\{"SNG_ID":"', songs):
        match = re.search(r'"SNG_TITLE":"(.*?)","', chunk)
        if not match:
            continue
        tracklist.append(match.group(1))

        match = re.search(r'"RANK_SNG":"?(\d+)"?', chunk)
        if match:
            ranks.append(int(match.group(1)))

        match = re.search(r'"EXPLICIT_LYRICS":"([^"]*)"', chunk)
        if match and match.group(1) != "0":
            explicit = 1

        match = re.search(r'"ARTISTS":\[(.*?)\],"ALB_ID"', chunk)
        if match:
            names = re.findall(r'"ART_NAME":"(.*?)","', match.group(1))
            for name in names[1:]:
                if name not in featurings:
                    featurings.append(name)

    popularity = round(sum(rank / 100000 for rank in ranks) / len(ranks), 2) if ranks else ""

    return [
        album_id,
        artist_id,
        title,
        artist,
        release_date,
        label,
        num_tracks,
        total_duration,
        cover_url,
        SEPARATOR.join(tracklist),
        popularity,
        fans,
        explicit,
        SEPARATOR.join(featurings),
    ]

def main():
    all_files = [
        os.path.join(PAGES_DIR, name)
        for name in os.listdir(PAGES_DIR)
        if name.startswith("album_") and name.endswith(".html.gz")
    ]
    seen = load_seen()

    new_files = [path for path in all_files if album_id_from_path(path) not in seen]
    skipped = len(all_files) - len(new_files)
    if LIMIT is not None:
        new_files = new_files[:LIMIT]

    print(f"album files: {len(all_files)}")
    print(f"already extracted: {skipped}")
    print(f"new files: {len(new_files)}")

    if len(new_files) == 0:
        print("nothing new to extract")
        return

    started = time.time()
    done = 0
    errors = 0
    file_exists = os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0

    with open(OUTPUT_FILE, "a" if file_exists else "w", encoding="utf-8", newline="") as output:
        if not file_exists:
            output.write("\t".join(COLUMNS) + "\n")

        for path in new_files:
            album_id = album_id_from_path(path)

            try:
                with gzip.open(path, "rb") as file:
                    html = file.read().decode("utf-8", "replace")

                row = extract(html, album_id)

                if row is None:
                    errors += 1
                    continue

                values = [str(value).replace("\t", " ").replace("\n", " ") for value in row]
                output.write("\t".join(values) + "\n")
                seen.add(album_id)
                done += 1

                if done % 1000 == 0:
                    rate = done / (time.time() - started)
                    print(f"[{done}/{len(new_files)}] {rate:.1f}/s\terrors: {errors}")
            except Exception as error:
                errors += 1
                print(f"error: {os.path.basename(path)}: {error}")

    elapsed = time.time() - started
    size_mb = os.path.getsize(OUTPUT_FILE) / 1024 / 1024

    print("---")
    print(f"new files:\t{len(new_files)}")
    print(f"extracted:\t{done}")
    print(f"errors:\t\t{errors}")
    print(f"output:\t\t{OUTPUT_FILE}\t{size_mb:.2f} MB")
    print(f"elapsed:\t{elapsed:.1f} s")

if __name__ == "__main__":
    main()
