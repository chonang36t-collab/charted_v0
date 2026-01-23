@echo off
taskkill /F /IM python.exe /T 2>nul
.\venv\Scripts\python.exe run.py
pause
