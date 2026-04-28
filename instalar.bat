@echo off
chcp 65001 >nul
echo Instalando ambiente CH Contestado...
python -m venv venv
call venv\Scripts\activate
pip install -r requirements.txt
echo.
echo Instalacao concluida.
pause
