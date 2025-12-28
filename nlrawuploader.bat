@echo off
REM Ganti path di bawah ini dengan path lengkap ke folder proyek Anda
cd "C:\Users\NamaAnda\Documents\proyek_streamlit_upload"

echo Mengaktifkan lingkungan virtual...
call .\venv\Scripts\activate

echo Menjalankan aplikasi Streamlit...
streamlit run app.py

pause