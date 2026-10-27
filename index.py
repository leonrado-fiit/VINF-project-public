import math
import os
import re
import sys
import time

ALBUMS_FILE = os.path.join("data", "albums.tsv")
INDEX_FILE = os.path.join("data", "index.tsv")
SEPARATOR = "\x1f"

def tokenize(text):
    return re.findall(r"\w+", text.lower())

def load_albums():
    with open(ALBUMS_FILE, encoding="utf-8") as file:
        header = file.readline().rstrip("\n").split("\t")
        rows = []
        for line in file:
            values = line.rstrip("\n").split("\t")
            rows.append(dict(zip(header, values)))
    return rows

def build_index(rows):
    index = {}
    for doc_id, row in enumerate(rows):
        text = " ".join([
            row["title"],
            row["artist"],
            row["label"],
            row["release_date"][:4],
            row["tracklist"].replace(SEPARATOR, " "),
            row["featurings"].replace(SEPARATOR, " "),
            "explicit" if row["explicit"] == "1" else "",
        ])
        counts = {}
        for term in tokenize(text):
            counts[term] = counts.get(term, 0) + 1
        for term, tf in counts.items():
            index.setdefault(term, []).append((doc_id, tf))
    return index

def save_index(index):
    started = time.time()
    
    with open(INDEX_FILE, "w", encoding="utf-8", newline="") as file:
        file.write("term\tdf\tdoc_ids\ttfs\n")
        for term in sorted(index):
            postings = index[term]
            doc_ids = ",".join(str(doc_id) for doc_id, tf in postings)
            tfs = ",".join(str(tf) for doc_id, tf in postings)
            file.write(f"{term}\t{len(postings)}\t{doc_ids}\t{tfs}\n")
    
    elapsed = time.time() - started
    size_mb = os.path.getsize(INDEX_FILE) / 1024 / 1024
    postings = sum(len(items) for items in index.values())
    
    print(f"index saved:\t{INDEX_FILE}")
    print(f"terms:\t\t{len(index)}")
    print(f"postings:\t{postings}")
    print(f"file:\t\t{size_mb:.2f} MB")
    print(f"save time:\t{elapsed:.1f} s")

def load_index():
    started = time.time()
    index = {}
    
    with open(INDEX_FILE, encoding="utf-8") as file:
        file.readline()
        for line in file:
            term, df, doc_ids_text, tfs_text = line.rstrip("\n").split("\t")
            doc_ids = [int(value) for value in doc_ids_text.split(",")]
            tfs = [int(value) for value in tfs_text.split(",")]
            index[term] = list(zip(doc_ids, tfs))
    
    print(f"index loaded:\t{len(index)} terms in {time.time() - started:.1f} s")
    return index

def build_and_save():
    rows = load_albums()
    started = time.time()
    index = build_index(rows)
    print(f"index built:\t{len(index)} terms in {time.time() - started:.1f} s")
    save_index(index)
    return rows, index

def ensure_index(rebuild):
    if rebuild or not os.path.exists(INDEX_FILE):
        return build_and_save()
    try:
        return load_albums(), load_index()
    except Exception as error:
        print(f"index load failed: {error}")
        return build_and_save()

def search(rows, index, query, method):
    scores = {}
    total = len(rows)
    for term in tokenize(query):
        postings = index.get(term)
        if not postings:
            continue
        df = len(postings)
        if method == "smooth":
            idf = math.log((total + 1) / (df + 1)) + 1
        else:
            idf = math.log(total / df) if df else 0
        for doc_id, tf in postings:
            scores[doc_id] = scores.get(doc_id, 0) + tf * idf
    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)[:10]
    return [(rows[doc_id], score) for doc_id, score in ranked]

def show(query, rows, index):
    print(f"\nquery: {query}")
    print("method\trank\tscore\talbum\tartist")
    for method in ("classic", "smooth"):
        results = search(rows, index, query, method)
        if not results:
            print(f"{method}\t-\t-\tno results\t-")
            continue
        for rank, (row, score) in enumerate(results, 1):
            print(f"{method}\t{rank}\t{score:.4f}\t{row['title']}\t{row['artist']}")

def main():
    args = sys.argv[1:]
    rebuild = "--build" in args
    args = [arg for arg in args if arg != "--build"]

    if not os.path.exists(ALBUMS_FILE):
        print(f"missing file: {ALBUMS_FILE}")
        return

    rows, index = ensure_index(rebuild)

    for query in args:
        show(query, rows, index)

if __name__ == "__main__":
    main()
