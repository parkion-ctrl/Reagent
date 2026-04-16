@echo off
cd /d C:\Users\ajou\Desktop\autoRe-agent
call C:\Users\ajou\anaconda3\Scripts\activate.bat
waitress-serve --host=0.0.0.0 --port=8000 config.wsgi:application
