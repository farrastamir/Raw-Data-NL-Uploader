import streamlit as st
import zipfile
import pandas as pd
import io
import gspread
import requests
import re
import json
import traceback
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account

# === Fungsi bantu ===
def clean_dataframe(df):
    return df.applymap(lambda x: str(x).lstrip("'") if isinstance(x, str) else x)

def detect_delimiter(sample_text):
    return ';' if sample_text.count(';') > sample_text.count(',') else ','

def limit_body_column(df, column_name='body', max_length=50000, new_length=30000):
    if column_name in df.columns:
        df[column_name] = df[column_name].apply(
            lambda x: x[:new_length] if isinstance(x, str) and len(x) > max_length else x
        )
    return df

# === UI ===
st.title("Upload CSV/ZIP dan Kirim ke Google Spreadsheet")

# === Input ZIP multi-upload ===
uploaded_zips = st.file_uploader("Unggah satu atau lebih file ZIP (berisi CSV)", type="zip", accept_multiple_files=True)

csv_df = None
if uploaded_zips:
    dfs = []
    for uploaded_zip in uploaded_zips:
        with zipfile.ZipFile(uploaded_zip, 'r') as zip_ref:
            file_list = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')]
            for fname in file_list:
                with zip_ref.open(fname) as f:
                    sample = f.read(1024).decode('utf-8')
                    delim = detect_delimiter(sample)
                with zip_ref.open(fname) as f:
                    df = pd.read_csv(f, delimiter=delim)
                    dfs.append(clean_dataframe(df))
    if dfs:
        csv_df = pd.concat(dfs, ignore_index=True)
        st.success(f"{len(dfs)} file CSV berhasil digabung ({len(csv_df)} baris total).")

# === Jika ada data, lanjutkan ===
if csv_df is not None:
    # === Pilihan Replace atau Append ===
    upload_mode = st.radio("Mode upload ke Spreadsheet:", ["Ganti isi lama (Replace)", "Tambahkan di bawah (Append)"])

    # === Deteksi RONM atau RSOCMED ===
    if 'tier' in csv_df.columns:
        target_worksheet = 'RONM'
    elif 'original_id' in csv_df.columns and 'label' in csv_df.columns:
        target_worksheet = 'RSOCMED'
        start_idx = csv_df.columns.get_loc('original_id')
        end_idx = csv_df.columns.get_loc('label')
        csv_df = csv_df.iloc[:, start_idx:end_idx + 1]
    else:
        st.error("Kolom 'tier' atau ('original_id' dan 'label') tidak ditemukan.")
        st.stop()

    st.info(f"Target sheet: **{target_worksheet}**")
    st.write(f"Kolom yang digunakan: {list(csv_df.columns)}")
    st.write(f"Jumlah baris: {len(csv_df)}")

    # === Input Spreadsheet dan JSON ===
    sheet_link = st.text_input("Masukkan link lengkap Google Spreadsheet Anda:")
    json_key = st.file_uploader("Upload file JSON Service Account Anda", type="json")

    if sheet_link and json_key:
        match = re.search(r"/d/([a-zA-Z0-9-_]+)", sheet_link)
        if not match:
            st.error("Link Spreadsheet tidak valid.")
        else:
            SPREADSHEET_ID = match.group(1)
            try:
                json_data = json.loads(json_key.read().decode('utf-8'))
                creds = service_account.Credentials.from_service_account_info(
                    json_data,
                    scopes=["https://www.googleapis.com/auth/spreadsheets"]
                )
                gc = gspread.authorize(creds)
                sh = gc.open_by_key(SPREADSHEET_ID)

                try:
                    worksheet = sh.worksheet(target_worksheet)
                except gspread.exceptions.WorksheetNotFound:
                    worksheet = sh.add_worksheet(title=target_worksheet, rows='1000', cols='26')

                # === Upload Mode: Replace / Append ===
                if upload_mode == "Ganti isi lama (Replace)":
                    worksheet.batch_clear(['A1:AZ'])
                    next_row = 1
                else:  # Append
                    values = worksheet.get_all_values()
                    next_row = len(values) + 1 if values else 1

                csv_df = limit_body_column(csv_df)

                st.write("🚀 Mengunggah data ke Google Spreadsheet...")
                progress_bar = st.progress(0)

                batch_size = 10000
                total_rows = len(csv_df)

                if next_row == 1:
                    set_with_dataframe(worksheet, csv_df.iloc[:batch_size], include_column_header=True, resize=False)
                else:
                    set_with_dataframe(worksheet, csv_df.iloc[:batch_size], row=next_row, include_column_header=False, resize=False)

                progress_bar.progress(min(10, 100))

                for start in range(batch_size, total_rows, batch_size):
                    end = min(start + batch_size, total_rows)
                    set_with_dataframe(
                        worksheet,
                        csv_df.iloc[start:end],
                        row=next_row + start if next_row > 1 else start + 2,
                        include_column_header=False,
                        resize=False
                    )
                    progress = (start + batch_size) / total_rows
                    progress_bar.progress(min(progress, 1.0))

                st.success(f"✅ Sukses upload ke sheet '{target_worksheet}'.")
                st.markdown(f"[📄 Lihat Spreadsheet](https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit)")

            except Exception as e:
                st.error("❌ Gagal mengakses Google Spreadsheet.")
                st.text(traceback.format_exc())
