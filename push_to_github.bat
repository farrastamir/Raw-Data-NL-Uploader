@echo off
if not exist .git (
    echo Inisialisasi Git Repository...
    git init
    git branch -M main
)

echo Mengatur Remote Origin...
git remote add origin https://github.com/farrastamir/Raw-Data-NL-Uploader.git 2>nul
git remote set-url origin https://github.com/farrastamir/Raw-Data-NL-Uploader.git

echo Menambahkan semua file...
git add .

echo Melakukan Commit...
set /p commit_msg="Masukkan pesan commit (tekan Enter untuk default 'Update content'): "
if "%commit_msg%"=="" set commit_msg=Update content
git commit -m "%commit_msg%"

echo Melakukan Push ke Main...
git push origin main

echo Selesai!
pause
