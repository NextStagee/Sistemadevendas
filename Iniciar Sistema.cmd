@echo off
echo Iniciando aplicação...

start cmd /k python app.py

timeout /t 3 > nul

start http://localhost:5000