import streamlit as st
import zipfile
import pandas as pd
import io
import gspread
import requests
import re
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account

# Fungsi bantu
def clean_dataframe(df):
    return df.applymap(lambda x: str(x).lstrip("'") if isinstance(x, str) else x)

def detect_delimiter(sample_text):
    return ';' if sample_text.count(';') > sample_text.count(',') else ','

# === PILIH METODE INPUT ===
st.title("Upload CSV atau ZIP dan Kirim ke Google Spreadsheet")

choice = st.radio("Pilih metode input:", ["Upload ZIP (berisi CSV)", "Link ZIP", "Upload file CSV"])

csv_df = None

# === Pilih Metode Input ===
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
                    with zip_ref.open(selected) as f:
                        sample = f.read(1024).decode('utf-8')
                        delim = detect_delimiter(sample)
                    with zip_ref.open(selected) as f:
                        csv_df = pd.read_csv(f, delimiter=delim)
                        csv_df = clean_dataframe(csv_df)

elif choice == "Link ZIP":
    url = st.text_input("Masukkan URL file ZIP:")
    if url:
        response = requests.get(url)
        if response.status_code == 200:
            zip_filename = "downloaded.zip"
            with open(zip_filename, 'wb') as f:
                f.write(response.content)
            with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
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

elif choice == "Upload file CSV":
    uploaded_csv = st.file_uploader("Unggah file CSV", type="csv")
    if uploaded_csv:
        sample = uploaded_csv.read(1024).decode('utf-8')
        uploaded_csv.seek(0)
        delimiter = detect_delimiter(sample)
        csv_df = pd.read_csv(uploaded_csv, delimiter=delimiter)
        csv_df = clean_dataframe(csv_df)

else:
    st.error("Pilihan tidak valid.")

# === Pilih Kolom original_id dan label ===
if csv_df is not None:
    st.success("File berhasil diproses.")
    start_col = 'original_id'
    end_col = 'label'

    if start_col not in csv_df.columns or end_col not in csv_df.columns:
        st.error(f"Kolom '{start_col}' atau '{end_col}' tidak ditemukan.")
    else:
        start_idx = csv_df.columns.get_loc(start_col)
        end_idx = csv_df.columns.get_loc(end_col)

        df_selected = csv_df.iloc[:, start_idx:end_idx + 1]
        st.write(f"Kolom yang digunakan: {list(df_selected.columns)}")
        st.write(f"Jumlah baris: {len(df_selected)}")

        # === Link Spreadsheet ===
        sheet_link = st.text_input("Masukkan link lengkap Google Spreadsheet Anda:")
        if sheet_link:
            match = re.search(r"/d/([a-zA-Z0-9-_]+)", sheet_link)
            if not match:
                st.error("Link Spreadsheet tidak valid.")
            else:
                SPREADSHEET_ID = match.group(1)

                # === Koneksi dan Sheet ===
                try:
                    json_key = st.file_uploader("Upload file Service Account JSON Anda", type="json")
                    if json_key:
                        creds = service_account.Credentials.from_service_account_info(
                            json_key.read(), scopes=["https://www.googleapis.com/auth/spreadsheets"]
                        )
                        gc = gspread.authorize(creds)
                        sh = gc.open_by_key(SPREADSHEET_ID)

                        try:
                            worksheet = sh.worksheet('RSOCMED')
                        except gspread.exceptions.WorksheetNotFound:
                            worksheet = sh.add_worksheet(title='RSOCMED', rows='1000', cols='26')

                        # Hapus hanya kolom A:AZ
                        clear_range = 'A1:AZ'
                        worksheet.batch_clear([clear_range])

                        # === Upload Data dengan Progress Indicator ===
                        st.write("🚀 Mengunggah data ke Google Spreadsheet...")

                        # Menambahkan indikator loading
                        progress_bar = st.progress(0)
                        with st.spinner('Sedang mengunggah data...'):
                            batch_size = 10000
                            total_rows = len(df_selected)

                            # Upload batch pertama dengan header
                            set_with_dataframe(worksheet, df_selected.iloc[:batch_size], include_column_header=True, resize=False)
                            progress_bar.progress(10)  # Update progress pertama

                            # Upload sisa batch tanpa header
                            for start in range(batch_size, total_rows, batch_size):
                                end = min(start + batch_size, total_rows)
                                set_with_dataframe(
                                    worksheet,
                                    df_selected.iloc[start:end],
                                    row=start + 2,
                                    include_column_header=False,
                                    resize=False
                                )
                                # Update progress bar setiap selesai batch
                                progress = (start + batch_size) / total_rows * 100
                                progress_bar.progress(progress)

                        st.success("✅ Data berhasil diunggah ke sheet 'RSOCMED'.")
                        st.markdown(f"[📄 Lihat Spreadsheet](https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit)")
                except Exception as e:
                    st.error(f"Gagal mengakses Spreadsheet: {e}")
