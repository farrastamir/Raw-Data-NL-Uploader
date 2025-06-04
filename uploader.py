# app.py
import streamlit as st
import zipfile
import pandas as pd
import io
import requests
import re
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account

# === Fungsi bantu ===
def clean_dataframe(df):
    return df.applymap(lambda x: str(x).lstrip("'") if isinstance(x, str) else x)

def detect_delimiter(sample_text):
    return ';' if sample_text.count(';') > sample_text.count(',') else ','

def clear_sheet_range(worksheet, has_tier):
    clear_range = 'A:AG' if has_tier else 'A:AZ'
    worksheet.batch_clear([clear_range])

def upload_dataframe_to_sheet(df, worksheet):
    batch_size = 10000
    total_rows = len(df)
    set_with_dataframe(worksheet, df.iloc[:batch_size], include_column_header=True, resize=False)

    for start in range(batch_size, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        set_with_dataframe(
            worksheet,
            df.iloc[start:end],
            row=start + 2,
            include_column_header=False,
            resize=False
        )

st.title("📁 Upload CSV atau ZIP dan Kirim ke Google Spreadsheet")

choice = st.radio("Pilih metode input:", ["Upload ZIP (berisi CSV)", "Link ZIP", "Upload file CSV"])

csv_dfs = []  # List of tuples: (filename, DataFrame)

if choice == "Upload ZIP (berisi CSV)":
    uploaded_zips = st.file_uploader("Unggah file ZIP", type="zip", accept_multiple_files=True)
    if uploaded_zips:
        for zip_file in uploaded_zips:
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                file_list = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')]
                for fname in file_list:
                    with zip_ref.open(fname) as f:
                        sample = f.read(1024).decode('utf-8')
                        delim = detect_delimiter(sample)
                    with zip_ref.open(fname) as f:
                        df = pd.read_csv(f, delimiter=delim)
                        df = clean_dataframe(df)
                        csv_dfs.append((fname, df))

elif choice == "Link ZIP":
    url = st.text_input("Masukkan URL file ZIP:")
    if url:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(response.content), 'r') as zip_ref:
                    file_list = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')]
                    for fname in file_list:
                        with zip_ref.open(fname) as f:
                            sample = f.read(1024).decode('utf-8')
                            delim = detect_delimiter(sample)
                        with zip_ref.open(fname) as f:
                            df = pd.read_csv(f, delimiter=delim)
                            df = clean_dataframe(df)
                            csv_dfs.append((fname, df))
            else:
                st.error("Gagal mengunduh file ZIP.")
        except Exception as e:
            st.error(f"Terjadi error: {e}")

elif choice == "Upload file CSV":
    uploaded_csv = st.file_uploader("Unggah file CSV", type="csv")
    if uploaded_csv:
        sample = uploaded_csv.read(1024).decode('utf-8')
        uploaded_csv.seek(0)
        delimiter = detect_delimiter(sample)
        df = pd.read_csv(uploaded_csv, delimiter=delimiter)
        df = clean_dataframe(df)
        csv_dfs.append(("uploaded_file.csv", df))

# === Proses unggah ke Google Sheets ===
if csv_dfs:
    st.success(f"{len(csv_dfs)} file berhasil diproses.")
    sheet_link = st.text_input("Masukkan link lengkap Google Spreadsheet Anda:")
    json_key = st.file_uploader("Upload file service account JSON Google Anda", type="json")

    if sheet_link and json_key:
        try:
            match = re.search(r"/d/([a-zA-Z0-9-_]+)", sheet_link)
            if not match:
                st.error("Link Spreadsheet tidak valid.")
            else:
                SPREADSHEET_ID = match.group(1)
                creds = service_account.Credentials.from_service_account_info(
                    eval(json_key.read().decode()), scopes=["https://www.googleapis.com/auth/spreadsheets"]
                )
                gc = gspread.authorize(creds)
                sh = gc.open_by_key(SPREADSHEET_ID)
                available_sheets = [ws.title for ws in sh.worksheets()]

                for fname, df in csv_dfs:
                    has_tier = 'tier' in df.columns
                    default_target = 'RONM' if has_tier else 'RSOCMED'
                    if default_target not in available_sheets:
                        target_sheet = st.selectbox(
                            f"Sheet '{default_target}' tidak ditemukan. Pilih sheet tujuan untuk file '{fname}':",
                            available_sheets,
                            key=fname
                        )
                    else:
                        target_sheet = default_target

                    worksheet = sh.worksheet(target_sheet)
                    clear_sheet_range(worksheet, has_tier)
                    upload_dataframe_to_sheet(df, worksheet)

                    st.success(f"✅ File '{fname}' berhasil diunggah ke sheet '{target_sheet}'.")

                st.markdown(f"[📄 Lihat Spreadsheet](https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit)")
        except Exception as e:
            st.error(f"Gagal mengunggah ke Spreadsheet: {e}")
