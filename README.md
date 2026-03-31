# mf4_to_databricks

Tools for analyzing MF4 measurement files, converting to Parquet, and uploading to Databricks.

## Project Structure

```
mf4_to_databricks/
├── pyproject.toml
├── requirements.txt
├── README.md
├── src/
│   └── mf4_to_databricks/
│       ├── __init__.py
│       ├── __main__.py     # Entry point (GUI default, --cli for terminal)
│       ├── analyze_mf4.py  # MF4 metadata analysis
│       ├── converter.py    # MF4 → DataFrame → Parquet conversion
│       └── gui/
│           ├── __init__.py
│           └── app.py      # Tkinter GUI
├── scripts/
└── tests/
```

## Setup

```powershell
cd C:\Users\ero4abt\Documents\Python\mf4_to_databricks
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

## Usage

### GUI (Standard)

```powershell
python -m mf4_to_databricks
```

### CLI-Analyse

```powershell
python -m mf4_to_databricks --cli "path\to\file.mf4"
python -m mf4_to_databricks --cli "path\to\file.mf4" --channels
```

### Als Bibliothek

```python
from mf4_to_databricks.analyze_mf4 import analyze_mf4
from mf4_to_databricks.converter import mf4_to_parquet, mf4_to_dataframe

# Metadata
info = analyze_mf4(r"path\to\file.mf4")

# Direkt-Konvertierung MF4 → Parquet
mf4_to_parquet(r"path\to\file.mf4")

# Mit Optionen: bestimmte Gruppen, Resampling
mf4_to_parquet(r"file.mf4", output_path="out.parquet", group_indices=[0, 2], raster=0.01)

# Nur DataFrame erzeugen
df = mf4_to_dataframe(r"file.mf4", channels=["Speed", "RPM"])
```

## Erweiterung

Neue Analysefunktionen können als Funktionen in eigenen Modulen unter
`src/mf4_to_databricks/` angelegt und über die GUI oder als Library-API
bereitgestellt werden.
print(info["channel_count"])
```
