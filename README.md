# Project 112 - Distributed Watermark Tracker

Distributed Watermark Tracker ("Log Delay Compensator") is a distributed
stream-processing project for `Web_Server_Logs` with out-of-order arrivals.
The system compares:

- **Strict Watermark**: waits until an event-time window is safe to close,
  prioritizing completeness.
- **Heuristic Watermark**: estimates lateness from observed delay, closes
  earlier, and sends late records to a correction path.

The required analysis output is **Data Completeness (%) vs Wait Time (ms)**.

## Prerequisites

- Python 3.10 or newer.
- Docker Desktop for the distributed deployment.
- PowerShell on Windows, or a POSIX shell on Linux/macOS.

## Local Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install pytest matplotlib
```

Linux/macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install pytest matplotlib
```

## Run Tests

```powershell
python -m pytest -q
```

## Generate Benchmark Artifacts

```powershell
python reports/watermark_tradeoff_benchmark.py
```

Generated outputs:

- `reports/artifacts/dataset_summary.csv`
- `reports/artifacts/empirical_wait_curve.csv`
- `reports/artifacts/implemented_heuristic_curve.csv`
- `reports/artifacts/strict_safe_close.csv`
- `reports/artifacts/watermark_tradeoff.png`

Current benchmark snapshot on 5,000 sampled web log events:

| Policy | Wait Time (ms) | Data Completeness (%) |
|---|---:|---:|
| Strict safe close | 29,988.666 | 100.00 |
| Heuristic P95 | 7,421.000 | 96.74 |
| Heuristic P99 | 26,529.000 | 99.46 |
| Heuristic P99.9 | 28,283.000 | 99.76 |

## Minimal Local Strict-Mode Demo

Open separate terminals from the repository root:

```powershell
python run.py --role coordinator --mode strict --port 8000
python run.py --role worker --mode strict --port 8101 --partitions 0,1,2
python run.py --role ingestor --mode strict --source dataset/access.log/access_full.csv --port 8200
```

Health check:

```powershell
curl http://localhost:8000/health
```

## Docker Compose Deployment

Repository-root scaled deployment:

```powershell
docker compose up --build
```

Full strict profile:

```powershell
cd deploy
$env:MODE = "strict"
$env:DATASET_FILE = "access.log/access_full.csv"
docker compose --profile strict up --build
```

Full heuristic profile:

```powershell
cd deploy
$env:MODE = "heuristic"
$env:DATASET_FILE = "access.log/access_full.csv"
docker compose --profile heuristic up --build
```

Stop and remove containers/volumes:

```powershell
docker compose down -v
```

## Final Deliverables

The generated academic submission bundle is under
`reports/final_deliverables/`:

- `01_Project_Proposal_Distributed_Watermark_Tracker.docx`
- `02_Two_Page_Design_Document.docx`
- `03_Code_Repository_README_Instructions.md`
- `04_Analysis_Ozsu_Valduriez_Report.docx`
- `05_Proof_Video_Boilerplate.md`

Regenerate them with:

```powershell
python reports/build_final_deliverables.py
```
