# Run dashboard with frontend theme and styles. From repo root: .\dashboard\run.ps1
Set-Location $PSScriptRoot
Set-Location frontend
python -m streamlit run ../backend/app.py
