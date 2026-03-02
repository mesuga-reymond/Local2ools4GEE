@echo off
if not exist "venv" (
    echo [!] Virtual environment not found. Running setup first...
    call setup_env.bat
)
call venv\Scripts\activate
echo Launching GEE2DB...
python main.py
pause