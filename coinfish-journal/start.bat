@echo off
cd /d "%~dp0"
echo Installing/updating required packages...
python -m pip install --quiet flask
if errorlevel 1 (
    echo "python" not found, trying "py" launcher...
    py -m pip install --quiet flask
    py import_trades.py
    py app.py
) else (
    python import_trades.py
    python app.py
)
pause
