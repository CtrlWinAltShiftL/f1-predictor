# f1-predictor

## 🛠️ Tools & Technologies

- 🐍 Python 3.10+
- 🏁 [`Fast-F1`](https://github.com/theOehrly/Fast-F1) — Access F1 session data
- 📦 [`uv`](https://github.com/astral-sh/uv) — Modern Python packaging (PEP 582)
- 🗃️ SQLite or PostgreSQL — Structured storage of sessions, drivers, and results
- 🐘 SQL — Feature extraction and querying (schema-first approach)
- 📘 Pandas — Data manipulation
- 🤖 Scikit-learn — Model training and evaluation
- 📊 Matplotlib / Seaborn — Visualizations

---

## 🧱 Data Architecture

The project uses a **SQL database** (SQLite by default) to store parsed data from the Fast-F1 API. This enables:

- Flexible, queryable access to session and driver data
- Hands-on experience with SQL joins, filters, and aggregations
- Separation between raw data ingestion and model training

Example tables (to be confirmed):
- `drivers`
- `races`
- `qualifying_results`
- `race_results`
- `constructor_standings`

Python scripts handle data ingestion, cleaning, and insertion into SQL.

---

## 🔧 Setup & Installation

```bash
# Clone the repo
git clone https://github.com/CtrlWinAltShiftL/f1-predictor

# Set up Python environment (using uv)
uv sync