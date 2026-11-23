import csv
import os
import sys
import time
import lucene

lucene.initVM(vmargs=["-Djava.awt.headless=true", "-Xmx6g"])

from java.lang import Float
from java.nio.file import Paths
from java.util import HashMap

from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, DoublePoint, Field, IntPoint, LongPoint, StoredField, StringField, TextField
from org.apache.lucene.index import DirectoryReader, IndexWriter, IndexWriterConfig
from org.apache.lucene.queryparser.classic import MultiFieldQueryParser, QueryParser, QueryParserBase
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.store import FSDirectory

INPUT_FILE = os.path.join("data", "wiki", "out", "albums_wiki.tsv")
INDEX_DIR = os.path.join("data", "lucene_index")
SEPARATOR = "\x1f"
LIMIT = 10

SEARCH_FIELDS = [
    "title",
    "artist",
    "tracklist",
    "label",
    "year_text",
    "featurings",
    "album_genre",
    "album_producer",
    "artist_origin",
    "artist_years_active",
    "album_studio",
    "album_recorded",
]

SEARCH_BOOSTS = {
    "title": 4.0,
    "artist": 3.0,
    "tracklist": 1.5,
    "label": 0.8,
    "year_text": 8.0,
    "featurings": 1.0,
    "album_genre": 1.2,
    "album_producer": 1.0,
    "artist_origin": 0.8,
    "artist_years_active": 0.5,
    "album_studio": 0.5,
    "album_recorded": 0.5,
}

def int_value(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None

def double_value(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def add_text(document, name, value, store=True):
    if value:
        document.add(TextField(name, value, Field.Store.YES if store else Field.Store.NO))

def add_string(document, name, value, store=True):
    if value:
        document.add(StringField(name, value, Field.Store.YES if store else Field.Store.NO))

def add_int(document, name, value):
    number = int_value(value)
    if number is not None:
        document.add(IntPoint(name, number))
        document.add(StoredField(name, str(number)))

def add_long(document, name, value):
    number = int_value(value)
    if number is not None:
        document.add(LongPoint(name, number))
        document.add(StoredField(name, str(number)))

def add_double(document, name, value):
    number = double_value(value)
    if number is not None:
        document.add(DoublePoint(name, number))
        document.add(StoredField(name, str(number)))

def make_document(row):
    document = Document()

    add_string(document, "album_id", row["album_id"])
    add_string(document, "artist_id", row["artist_id"])

    add_text(document, "title", row["title"])
    add_string(document, "title_exact", row["title"].lower(), False)
    
    add_text(document, "artist", row["artist"])
    add_string(document, "artist_exact", row["artist"].lower(), False)

    document.add(StoredField("release_date", row["release_date"]))
    year = row["release_date"][:4]
    add_int(document, "year", year)
    add_string(document, "year_text", year, False)

    add_text(document, "label", row["label"])
    add_string(document, "label_exact", row["label"].lower(), False)

    add_int(document, "num_tracks", row["num_tracks"])
    add_int(document, "total_duration", row["total_duration"])
    add_long(document, "fans", row["fans"])
    add_double(document, "track_popularity_avg", row["track_popularity_avg"])
    add_string(document, "explicit", row["explicit"])

    add_text(document, "tracklist", row["tracklist"].replace(SEPARATOR, " "), False)
    add_text(document, "featurings", row["featurings"].replace(SEPARATOR, " "))

    document.add(StoredField("artist_website", row["artist_website"]))
    add_text(document, "artist_origin", row["artist_origin"])
    add_text(document, "artist_years_active", row["artist_years_active"])
    add_text(document, "artist_members", row["artist_members"])
    add_text(document, "artist_associated_acts", row["artist_associated_acts"])

    add_text(document, "album_genre", row["album_genre"])
    add_string(document, "album_genre_exact", row["album_genre"].lower(), False)
   
    add_text(document, "album_producer", row["album_producer"])
    add_string(document, "album_type", row["album_type"])
    add_text(document, "album_studio", row["album_studio"])
    add_text(document, "album_recorded", row["album_recorded"])

    add_string(document, "artist_wiki_page_id", row["artist_wiki_page_id"])
    add_string(document, "album_wiki_page_id", row["album_wiki_page_id"])

    return document

def build_index():
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(INPUT_FILE)

    os.makedirs(INDEX_DIR, exist_ok=True)
    
    analyzer = StandardAnalyzer()
    config = IndexWriterConfig(analyzer)
    
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    config.setRAMBufferSizeMB(2048.0)
    
    directory = FSDirectory.open(Paths.get(INDEX_DIR))
    writer = IndexWriter(directory, config)

    started = time.time()
    count = 0

    try:
        with open(INPUT_FILE, encoding="utf-8", newline="") as file:
            reader = csv.DictReader(file, delimiter="\t")
            for row in reader:
                writer.addDocument(make_document(row))
                count += 1
                if count % 50000 == 0:
                    print("indexed:", count, flush=True)
        
        writer.commit()
        writer.forceMerge(1)
        writer.commit()
    finally:
        writer.close()
        directory.close()
        analyzer.close()

    print("documents:", count)
    print("index:", INDEX_DIR)
    print("elapsed:", round(time.time() - started, 1), "s")

def open_searcher():
    if not os.path.exists(INDEX_DIR):
        raise FileNotFoundError(INDEX_DIR)
    
    directory = FSDirectory.open(Paths.get(INDEX_DIR))
    reader = DirectoryReader.open(directory)
    searcher = IndexSearcher(reader)
    
    return directory, reader, searcher

def get_or_empty(document, field):
    return document.get(field) or ""

def print_hits(searcher, hits):
    for rank, score_doc in enumerate(hits, 1):
        document = searcher.doc(score_doc.doc)
        print(
            "\t".join([
                str(rank),
                f"{score_doc.score:.4f}",
                get_or_empty(document, "album_id"),
                get_or_empty(document, "title"),
                get_or_empty(document, "artist"),
                get_or_empty(document, "release_date"),
                get_or_empty(document, "label"),
                get_or_empty(document, "album_genre"),
                get_or_empty(document, "album_type"),
                get_or_empty(document, "artist_origin"),
                get_or_empty(document, "album_producer"),
            ])
        )

def search_text(text):
    directory, reader, searcher = open_searcher() 
    analyzer = StandardAnalyzer()
    boosts = HashMap()
    
    for field, value in SEARCH_BOOSTS.items():
        boosts.put(field, Float(value))

    try:
        parser = MultiFieldQueryParser(SEARCH_FIELDS, analyzer, boosts)
        query = QueryParserBase.parse(parser, text)
        hits = searcher.search(query, LIMIT).scoreDocs
        
        print("query:", text)
        print("hits:", len(hits))
        print_hits(searcher, hits)
    finally:
        analyzer.close()
        reader.close()
        directory.close()

def query_text(text):
    directory, reader, searcher = open_searcher()
    analyzer = StandardAnalyzer()

    try:
        parser = QueryParser("title", analyzer)
        query = parser.parse(text)
        hits = searcher.search(query, LIMIT).scoreDocs
        
        print("query:", text)
        print("hits:", len(hits))
        print_hits(searcher, hits)
    finally:
        analyzer.close()
        reader.close()
        directory.close()

def range_query(field, low, high):
    if field in ("year", "num_tracks", "total_duration"):
        query = IntPoint.newRangeQuery(field, int(low), int(high))
    elif field == "fans":
        query = LongPoint.newRangeQuery(field, int(low), int(high))
    elif field == "track_popularity_avg":
        query = DoublePoint.newRangeQuery(field, float(low), float(high))
    else:
        raise ValueError(f"unsupported range field: {field}")

    directory, reader, searcher = open_searcher()

    try:
        hits = searcher.search(query, LIMIT).scoreDocs
        
        print("range:", field, low, high)
        print("hits:", len(hits))
        print_hits(searcher, hits)
    finally:
        reader.close()
        directory.close()

def usage():
    print("usage:")
    print("  python lucene_app.py build")
    print("  python lucene_app.py search \"daft punk 2008\"")
    print("  python lucene_app.py query \"artist:daft AND album_type:studio\"")
    print("  python lucene_app.py range year 1990 1999")

def main():
    args = sys.argv[1:]
    if not args:
        usage()
        return

    command = args[0]

    if command == "build":
        build_index()
    elif command == "search" and len(args) > 1:
        search_text(" ".join(args[1:]))
    elif command == "query" and len(args) > 1:
        query_text(" ".join(args[1:]))
    elif command == "range" and len(args) == 4:
        range_query(args[1], args[2], args[3])
    else:
        usage()

if __name__ == "__main__":
    main()
