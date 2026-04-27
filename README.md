![License](https://img.shields.io/github/license/bchilton9/MAM-Checker)
![Last Commit](https://img.shields.io/github/last-commit/bchilton9/MAM-Checker)
![Python](https://img.shields.io/badge/Python-3.9%2B-3776AB?logo=python&logoColor=white)

# MAM Library Checker

Scan your local ebook and audiobook collection and compare it against MyAnonamouse (via Prowlarr) to see what you already have vs what might be missing.

## ✨ Features

- 📚 Supports ebooks + audiobooks
- ⚡ Multithreaded (fast scanning)
- 📊 Live progress + ETA
- 🧠 Smart matching:
  - title
  - author
  - series
  - book number
- 🔁 Resume support (skips already FOUND items)
- 💾 Caching (dramatically faster re-runs)
- 📈 Outputs CSV with:
  - FOUND / PARTIAL / MISSING
  - match score
  - best match on MAM
  - search query used

___

## 🧰 Requirements

- Python 3.9+
- Prowlarr with MyAnonamouse configured

Install dependencies:

```bash
pip install requests rapidfuzz ebooklib pypdf
```

___

## ⚙️ Configuration

Edit these values in the script:

```python
BOOK_DIRS = [
    ("/your/ebooks", "ebook"),
    ("/your/audiobooks", "audiobook"),
]

PROWLARR_URL = "http://your-prowlarr:9696"
PROWLARR_API_KEY = "your_api_key"
INDEXER_IDS = [52]  # your MAM indexer ID
```

___

## ▶️ Usage

```bash
python3 mam_checker.py
```

___

## 📊 Output

Creates:

```
mam_missing_report.csv
```

With columns:

- status → FOUND / PARTIAL / MISSING
- match_score
- title
- author
- series
- book_number
- best_mam_match
- searched_query
- type (ebook/audiobook)
- cache_hits
- live_hits
- error
- local_file

___

## 🧠 Matching Logic

The script uses fuzzy matching:

- Title = primary signal
- Author = secondary
- Series = bonus signal
- Book number = small boost

### Status meanings:

| Status   | Meaning |
|----------|--------|
| FOUND    | Very strong match |
| PARTIAL  | Likely match but uncertain |
| MISSING  | No good match found |
| ERROR    | API or parsing issue |

___

## ⚡ Performance

- First run: slower (builds cache)
- Future runs: much faster due to:
  - cached search results
  - skipping FOUND items

Cache file:

```
prowlarr_search_cache.json
```

___

## 🗂️ Recommended Folder Structure

```
/Books/
  Author/
    Series/
      Author - Series 01 - Title.epub

/Audiobooks/
  Author/
    Series/
      Author - Series 01 - Title.m4b
```

___

## 🔧 Tips

- Lower FOUND_MATCH if too many false MISSING
- Raise it if too many false positives

Start with:

```python
MAX_WORKERS = 4
```

___

## ⚠️ Notes

- Matching is heuristic — not perfect
- Torrent titles are messy
- Expect some PARTIAL results

___

## 📜 License

MIT – free to use and modify. Not affiliated with theme.park or any of the apps.

___

## 🙌 Credits

Based on the incredible work of [MyAnonamouse](https://www.myanonamouse.net).

___

## 🛠 Made By

[ChilSoft.com](https://chilsoft.com) with caffeine and questionable commits.

___

## ⚠️ Disclaimer

This site and its contents are provided for informational and educational purposes only.

Use any code, tools, or instructions at your own risk.  
We are **not responsible** for any damage to your device, data loss, or unintended consequences.

Always proceed with care — and make backups.

© **2025 ChilSoft**. All rights reserved.

