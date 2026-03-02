@echo off
setlocal
title GEE2DB Environment Setup

echo ============================================
echo      GEE2DB: AUTOMATED VENV SETUP
echo ============================================

:: 1. Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python not found. Please install Python and add it to your PATH.
    pause
    exit /b
)

:: 2. Create Virtual Environment if it doesn't exist
if not exist "venv" (
    echo [1/4] Creating virtual environment...
    python -m venv venv
) else (
    echo [SKIP] Virtual environment already exists.
)

:: 3. Activate the environment
echo [2/4] Activating environment...
call venv\Scripts\activate

:: 4. Upgrade pip for stability
echo [3/4] Upgrading pip...
python -m pip install --upgrade pip

:: 5. Install requirements
if exist "requirements.txt" (
    echo [4/4] Installing dependencies from requirements.txt...
    pip install -r requirements.txt
) else (
    echo [WARN] requirements.txt not found. Skipping dependency install.
)

echo ============================================
echo   SETUP COMPLETE! 
echo   To start the app, use: run_app.bat
echo ============================================
pause