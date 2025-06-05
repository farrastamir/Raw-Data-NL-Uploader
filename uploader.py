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

st.title("📁 Upload CSV atau ZIP dan Kirim ke Google Spreadsheet")

# === PILIH METODE INPUT ===
choice = st.radio("Pilih metode input:", ["Upload ZIP (berisi CSV)", "Link ZIP", "Upload file CSV"])

csv_df = None

if choice == "Upload ZIP (berisi CSV)":
    uploaded_zip = st.file_uploader("Unggah file ZIP", type="zip")
    if uploaded_zip:
        with zipfile.ZipFile(uploaded_zip, 'r') as zip_ref:
            file_list = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')]
            if not file_list:
                st.error("Tidak ada file CSV dalam ZIP.")
            else:
                selected = st.selectbox("Pilih file CSV", ["SEMUA"] + file_list)
                dfs = []
                if selected == "SEMUA":
                    for fname in file_list:
                        with zip_ref.open(fname) as f:
                            sample = f.read(1024).decode('utf-8')
                            delim = detect_delimiter(sample)
                        with zip_ref.open(fname) as f:
                            df = pd.read_csv(f, delimiter=delim)
                            dfs.append(clean_dataframe(df))
                    csv_df = pd.concat(dfs, ignore_index=True)
                else:
                    with zipfile.ZipFile(uploaded_zip, 'r') as zip_ref:
                        with zip_ref.open(selected) as f:
                            sample = f.read(1024).decode('utf-8')
                            delim = detect_delimiter(sample)
                        with zip_ref.open(selected) as f:
                            csv_df = pd.read_csv(f, delimiter=delim)
                            csv_df = clean_dataframe(csv_df)

elif choice == "Link ZIP":
    url = st.text_input("Masukkan URL file ZIP:")
    if url:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(response.content), 'r') as zip_ref:
                    file_list = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')]
                    if not file_list:
                        st.error("Tidak ada file CSV dalam ZIP.")
                    else:
                        selected = st.selectbox("Pilih file CSV", ["SEMUA"] + file_list)
                        dfs = []
                        if selected == "SEMUA":
                            for fname in file_list:
                                with zip_ref.open(fname) as f:
                                    sample = f.read(1024).decode('utf-8')
                                    delim = detect_delimiter(sample)
                                with zip_ref.open(fname) as f:
                                    df = pd.read_csv(f, delimiter=delim)
                                    dfs.append(clean_dataframe(df))
                            csv_df = pd.concat(dfs, ignore_index=True)
                        else:
                            with zip_ref.open(selected) as f:
                                sample = f.read(1024).decode('utf-8')
                                delim = detect_delimiter(sample)
                            with zip_ref.open(selected) as f:
                                csv_df = pd.read_csv(f, delimiter=delim)
                                csv_df = clean_dataframe(csv_df)
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
        csv_df = pd.read_csv(uploaded_csv, delimiter=delimiter)
        csv_df = clean_dataframe(csv_df)

# === Jika sudah ada DataFrame ===
if csv_df is not None:
    st.success("File berhasil diproses.")
    
    if 'original_id' in csv_df.columns and 'label' in csv_df.columns:
        start_idx = csv_df.columns.get_loc('original_id')
        end_idx = csv_df.columns.get_loc('label')
        df_selected = csv_df.iloc[:, start_idx:end_idx + 1]
        st.dataframe(df_selected.head())

        # === Google Spreadsheet ===
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

                    # Cek apakah kolom 'tier' ada
                    if 'tier' in csv_df.columns:
                        sheet_name = 'RONM'
                    else:
                        sheet_name = 'RSOCMED'
                    
                    try:
                        worksheet = sh.worksheet(sheet_name)
                    except gspread.exceptions.WorksheetNotFound:
                        # Jika sheet tidak ditemukan, tampilkan sheet yang ada untuk dipilih
                        available_sheets = [ws.title for ws in sh.worksheets()]
                        sheet_name = st.selectbox("Sheet tidak ditemukan. Pilih sheet tujuan:", available_sheets)
                        worksheet = sh.worksheet(sheet_name)

                    # Tentukan range berdasarkan ada tidaknya kolom 'tier'
                    range_to_clear = 'A1:AG' if 'tier' in csv_df.columns else 'A1:AZ'
                    worksheet.batch_clear([range_to_clear])

                    batch_size = 10000
                    total_rows = len(df_selected)

                    set_with_dataframe(worksheet, df_selected.iloc[:batch_size], include_column_header=True, resize=False)

                    for start in range(batch_size, total_rows, batch_size):
                        end = min(start + batch_size, total_rows)
                        set_with_dataframe(
                            worksheet,
                            df_selected.iloc[start:end],
                            row=start + 2,
                            include_column_header=False,
                            resize=False
                        )

                    st.success(f"✅ Sukses! Data telah diunggah ke sheet '{sheet_name}'.")
                    st.markdown(f"[📄 Lihat Spreadsheet](https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit)")
            except Exception as e:
                st.error(f"Gagal mengunggah ke Spreadsheet: {e}")
    else:
        st.warning("Kolom 'original_id' atau 'label' tidak ditemukan dalam CSV.")
