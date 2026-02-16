#!/usr/bin/env bash
# Run dashboard with frontend theme and styles. From repo root: ./dashboard/run.sh
cd "$(dirname "$0")/frontend"
streamlit run ../backend/app.py
