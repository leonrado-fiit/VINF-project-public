import glob
import json
import os
import re
import shutil
import unicodedata
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

ALBUMS = "data/albums.tsv"
WIKI = "data/wiki/dumps"
OUTPUT = "data/wiki/out"

ARTIST_FIELDS = ["origin", "years_active", "members", "associated_acts", "website"]
ALBUM_FIELDS = ["producer", "genre", "type", "studio", "recorded"]

NEW_COLUMNS = [
    "artist_wiki_page_id",
    "artist_wiki_title",
    *["artist_" + field for field in ARTIST_FIELDS],
    "album_wiki_page_id",
    "album_wiki_title",
    *["album_" + field for field in ALBUM_FIELDS],
]

PARSE_SCHEMA = StructType([
    StructField("kind", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("years_active", StringType(), True),
    StructField("members", StringType(), True),
    StructField("associated_acts", StringType(), True),
    StructField("website", StringType(), True),
    StructField("producer", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("type", StringType(), True),
    StructField("studio", StringType(), True),
    StructField("recorded", StringType(), True),
    StructField("album_artist", StringType(), True),
])

PAGE_SCHEMA = StructType([
    StructField("title", StringType(), True),
    StructField("ns", LongType(), True),
    StructField("id", LongType(), True),
    StructField("revision", StructType([StructField("text", StringType(), True)]), True),
])

def normalize(value):
    value = unicodedata.normalize("NFKD", value or "")
    value = value.lower().replace("&", " and ").replace("+", " and ")
    value = re.sub(r"\([^)]*\)", " ", value)
    value = re.sub(r"[^a-z0-9]+", " ", value)

    return re.sub(r"\s+", " ", value).strip()

def infobox(text, name):
    match = re.search(r"\{\{\s*Infobox\s+" + name + r"(.*?)\n\}\}", text or "", re.S | re.I,)
    return match.group(1) if match else ""

def clean(value):
    value = value or ""
    value = re.sub(r"<ref[^>]*>.*?</ref>", " ", value, flags=re.S | re.I)
    value = re.sub(r"\[\[(?:[^|\]]*\|)?([^\]]+)\]\]", r"\1", value)

    for _ in range(3):
        value = re.sub(r"\{\{([^{}]*)\}\}", r"\1", value)

    value = re.sub(r"\b(?:hlist|flatlist|ubl|plainlist)\|", "", value, flags=re.I)
    value = value.replace("|", ", ").replace("*", ", ")
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\s*,\s*", ", ", value)

    return re.sub(r"\s+", " ", value).strip(" ,;")

def field(block, name):
    match = re.search(r"\|\s*" + re.escape(name) + r"\s*=\s*([^\n]*)", block, re.I,)
    return clean(match.group(1)) if match else ""

def parse_page(text):
    artist = infobox(text, "musical artist") or infobox(text, "musician")
    if artist:
        return (
            "artist",
            field(artist, "origin") or field(artist, "birth_place"),
            field(artist, "years_active"),
            field(artist, "members"),
            field(artist, "associated_acts"),
            field(artist, "website"),
            "", "", "", "", "", "",
        )

    album = infobox(text, "album")
    if album:
        return (
            "album", "", "", "", "", "",
            field(album, "producer"),
            field(album, "genre"),
            field(album, "type"),
            field(album, "studio"),
            field(album, "recorded"),
            field(album, "artist"),
        )

    return ("other", "", "", "", "", "", "", "", "", "", "", "")

def load_albums(spark):
    raw = spark.read.text(ALBUMS)
    first = raw.first()[0]
    header = first.rstrip("\r\n").split("\t")
    rows = raw.filter(F.col("value") != first).filter(F.length("value") > 0)
    parts = F.split(F.col("value"), "\\t", -1)

    return rows.select(*[parts[i].alias(name) for i, name in enumerate(header)])

def load_wiki(spark):
    files = []
    for pattern in ("*.bz2", "*.xml"):
        files.extend(glob.glob(os.path.join(WIKI, pattern)))

    files = sorted(files)

    if not files:
        raise FileNotFoundError(f"no wiki files in {WIKI}")

    wiki = (
        spark.read
        .schema(PAGE_SCHEMA)
        .format("xml")
        .option("rowTag", "page")
        .load(files)
    )

    wiki = wiki.filter(F.col("ns").cast("int") == 0).withColumn("text", F.col("revision")["text"])

    return (
        wiki.select(
            F.col("id").cast("string").alias("page_id"),
            F.col("title").cast("string").alias("title"),
            F.col("text").cast("string").alias("text"),
        ),
        len(files),
    )

def write_tsv(df, path):
    temp = path + ".tmp"
    shutil.rmtree(temp, ignore_errors=True)

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("sep", "\t")
        .option("header", "true")
        .option("quote", "")
        .csv(temp)
    )

    part = next(iter(glob.glob(os.path.join(temp, "part-*"))), None)
    if part is None:
        raise RuntimeError(f"no output in {temp}")

    os.replace(part, path)
    shutil.rmtree(temp, ignore_errors=True)

def run(spark):
    norm = F.udf(normalize, StringType())
    parse_udf = F.udf(parse_page, PARSE_SCHEMA)

    albums = load_albums(spark).cache()
    original_columns = list(albums.columns)

    base = (
        albums
        .withColumn("artist_key", norm(F.col("artist")))
        .withColumn("album_key", norm(F.col("title")))
        .cache()
    )

    wiki, wiki_files = load_wiki(spark)
    wiki = wiki.withColumn("title_key", norm(F.col("title")))

    print("wiki loaded:", wiki_files, "files", flush=True)

    targets = (
        base.select(F.col("artist_key").alias("key"))
        .union(base.select(F.col("album_key").alias("key")))
        .filter(F.length("key") > 0)
        .distinct()
    )

    candidates = (
        wiki.join(targets, wiki.title_key == targets.key)
        .select("page_id", "title", "text")
        .dropDuplicates(["page_id"])
    )

    parsed = (
        candidates
        .withColumn("info", parse_udf(F.col("text")))
        .select("page_id", "title", "info.*")
        .cache()
    )

    wiki_artists = (
        parsed.filter(F.col("kind") == "artist")
        .select(
            F.col("page_id").alias("wiki_page_id"),
            F.col("title").alias("wiki_title"),
            *ARTIST_FIELDS,
            norm(F.col("title")).alias("artist_key"),
        )
    )
    wiki_albums = (
        parsed.filter(F.col("kind") == "album")
        .select(
            F.col("page_id").alias("wiki_page_id"),
            F.col("title").alias("wiki_title"),
            *ALBUM_FIELDS,
            norm(F.col("album_artist")).alias("artist_key"),
            norm(F.col("title")).alias("album_key"),
        )
    )

    artist_lookup = (
        wiki_artists.dropDuplicates(["artist_key"])
        .select(
            "artist_key",
            F.col("wiki_page_id").alias("artist_wiki_page_id"),
            F.col("wiki_title").alias("artist_wiki_title"),
            *[F.col(field).alias("artist_" + field) for field in ARTIST_FIELDS],
        )
    )
    album_lookup = (
        wiki_albums.dropDuplicates(["album_key", "artist_key"])
        .select(
            "album_key", "artist_key",
            F.col("wiki_page_id").alias("album_wiki_page_id"),
            F.col("wiki_title").alias("album_wiki_title"),
            *[F.col(field).alias("album_" + field) for field in ALBUM_FIELDS],
        )
    )

    result = (
        base.join(artist_lookup, "artist_key", "left")
        .join(album_lookup, ["album_key", "artist_key"], "left")
        .cache()
    )

    rows_total = albums.count()
    rows_output = result.count()

    if rows_output != rows_total:
        raise RuntimeError(f"row count changed: {rows_total} -> {rows_output}")

    artist_mask = F.col("artist_wiki_page_id").isNotNull()
    album_mask = F.col("album_wiki_page_id").isNotNull()
    rows_with_artist = result.filter(artist_mask).count()
    rows_with_album = result.filter(album_mask).count()
    rows_with_both = result.filter(artist_mask & album_mask).count()
    artist_names_total = base.select("artist_key").distinct().count()
    artist_names_joined = result.filter(artist_mask).select("artist_key").distinct().count()
    albums_total = base.select("album_id").distinct().count()
    albums_joined = result.filter(album_mask).select("album_id").distinct().count()

    wiki_pages_total = (
        result.select(F.col("artist_wiki_page_id").alias("page_id"))
        .union(result.select(F.col("album_wiki_page_id").alias("page_id")))
        .filter(F.col("page_id").isNotNull())
        .distinct()
        .count()
    )

    stats = {
        "rows_total": rows_total,
        "rows_output": rows_output,
        "rows_with_artist": rows_with_artist,
        "rows_with_album": rows_with_album,
        "rows_with_both": rows_with_both,
        "artist_names_total": artist_names_total,
        "artist_names_joined": artist_names_joined,
        "albums_total": albums_total,
        "albums_joined": albums_joined,
        "wiki_pages_total": wiki_pages_total,
        "wiki_files": wiki_files,
    }

    final_result = result.select(*original_columns, *NEW_COLUMNS)
    write_tsv(final_result, os.path.join(OUTPUT, "albums_wiki.tsv"))

    return stats

def main():
    os.makedirs(OUTPUT, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("VINF wiki join")
        .master("local[3]")
        .config("spark.sql.shuffle.partitions", "12")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        stats = run(spark)
        with open(os.path.join(OUTPUT, "wiki_stats.json"), "w", encoding="utf-8") as file:
            json.dump(stats, file, ensure_ascii=False, indent=2)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
