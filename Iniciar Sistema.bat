@echo off
echo Iniciando aplicação...

start "" pythonw app.py

timeout /t 3 > nul

start "" http://localhost:5000
exit