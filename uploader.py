# =====================  IMPORT  =====================
import streamlit as st
import zipfile, io, re, json, traceback, requests, time
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account
from typing import List, Any

# =====================  FUNGSI BANTU  =====================
# --- Tidak ada perubahan pada fungsi bantu, dibiarkan seperti aslinya ---
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Hilangkan apostrof ('123) yang kadang muncul dari Excel/Sheets."""
    return df.applymap(lambda x: str(x).lstrip("'") if isinstance(x, str) else x)


def detect_delimiter(sample_text: str) -> str:
    """Deteksi delimiter dominan (',' atau ';')."""
    return ";" if sample_text.count(";") > sample_text.count(",") else ","


def truncate_long_texts(df: pd.DataFrame,
                        max_allowed: int = 50_000,
                        trunc_length: int = 20_000) -> pd.DataFrame:
    """
    Jika ada sel string lebih panjang dari `max_allowed`,
    potong menjadi `trunc_length` pertama.
    """
    def _trunc(x):
        return x[:trunc_length] if isinstance(x, str) and len(x) > max_allowed else x
    return df.applymap(_trunc)


def _fix_time_dots(t: str) -> str:
    """14.26.28 → 14:26:28, 13.00 → 13:00."""
    return re.sub(
        r"(\d{1,2})\.(\d{2})(?:\.(\d{2}))?",
        lambda m: f"{m.group(1)}:{m.group(2)}" +
                  (f":{m.group(3)}" if m.group(3) else ""),
        t,
    )


def _to_full_year(year: int) -> int:
    """2-digit year → 4-digit (≤30 → 20xx, sisanya 19xx)."""
    if year < 100:
        return 2000 + year if year <= 30 else 1900 + year
    return year


def standardize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Ubah kolom date_created / date_published → 'dd/mm/yyyy hh.mm.ss'."""
    for col in ("date_created", "date_published"):
        if col not in df.columns:
            continue

        def _convert(val):
            if pd.isna(val):
                return val
            s = str(val).strip()

            date_part, time_part = (s.split(" ", 1) + ["00:00:00"])[:2]
            time_part = _fix_time_dots(time_part)
            date_part = date_part.replace("-", "/")

            if time_part.count(":") == 0:
                time_part += ":00"
            if time_part.count(":") == 1:
                time_part += ":00"

            s_norm = f"{date_part} {time_part}"
            try:
                dt_obj = pd.to_datetime(s_norm, dayfirst=True, errors="coerce")
                if pd.notna(dt_obj) and dt_obj.year < 100:
                    dt_obj = dt_obj.replace(year=_to_full_year(dt_obj.year))
            except Exception:
                dt_obj = pd.NaT
            return dt_obj.strftime("%d/%m/%Y %H.%M.%S") if pd.notna(dt_obj) else val

        df[col] = df[col].apply(_convert)
    return df


def read_csv_from_bytes(b: bytes) -> pd.DataFrame:
    """Baca CSV dari bytes dengan delimiter otomatis."""
    try:
        sample = b[:2048].decode("utf-8", errors="ignore")
        delim = detect_delimiter(sample)
        return pd.read_csv(io.BytesIO(b), delimiter=delim)
    except Exception:
        return pd.read_csv(io.BytesIO(b), delimiter=';')


def load_from_url(url: str) -> List[pd.DataFrame]:
    """Unduh file CSV/ZIP via URL → list DataFrame."""
    dfs: List[pd.DataFrame] = []
    try:
        r = requests.get(url.strip())
        r.raise_for_status()
        content = r.content

        if zipfile.is_zipfile(io.BytesIO(content)):
            with zipfile.ZipFile(io.BytesIO(content), "r") as z:
                for name in z.namelist():
                    if name.lower().endswith(".csv"):
                        dfs.append(
                            clean_dataframe(read_csv_from_bytes(z.read(name)))
                        )
        else:
            dfs.append(clean_dataframe(read_csv_from_bytes(content)))
    except Exception as exc:
        st.error(f"Gagal mengambil {url} → {exc}")
    return dfs

def write_dataframe_in_chunks(ws,
                              df: pd.DataFrame,
                              start_row: int,
                              replace_mode: bool,
                              progress_placeholder: Any):
    """
    Kirim DataFrame ke worksheet dalam batch.
    Menampilkan progres dan menangani error 500.
    """
    rows_per_batch = 10_000
    row_ptr = 0
    total_rows = len(df)
    header_written = False

    while row_ptr < total_rows:
        chunk = df.iloc[row_ptr: row_ptr + rows_per_batch]
        start_display = row_ptr + 1
        end_display = min(row_ptr + len(chunk), total_rows)
        progress_placeholder.info(
            f"⏳ Mengunggah baris {start_display} - {end_display} dari {total_rows}..."
        )

        try:
            set_with_dataframe(
                ws,
                chunk,
                include_column_header=(not header_written and replace_mode),
                row=start_row + row_ptr,
                resize=False,
            )
            header_written = True
            row_ptr += len(chunk)
        except gspread.exceptions.APIError as e:
            if "500" in str(e) and rows_per_batch > 1:
                rows_per_batch = max(1, rows_per_batch // 2)
                st.warning(f"⚠️ 500 error – mengecilkan batch menjadi {rows_per_batch} baris…")
                time.sleep(2)
            else:
                raise
    
    progress_placeholder.empty()

# =====================  UI  =====================
st.set_page_config(
    page_title="Upload CSV/ZIP ➜ Google Sheets",
    page_icon="📄",
    layout="wide",
)

# --- Tombol Reset di bagian atas ---
col1, col2 = st.columns([3, 1])
with col1:
    st.title("Upload File/Link ➜ Google Spreadsheet")
with col2:
    if st.button("🔄 Reset Aplikasi", use_container_width=True, key="reset_top", help="Mulai ulang seluruh proses dari awal."):
        st.session_state.clear()
        st.rerun()


# ----------  1️⃣  PILIH SUMBER DATA  ----------
st.header("1️⃣ Pilih sumber data")

# REVISI 1: Opsi disederhanakan menjadi 2 pilihan.
src_choice = st.selectbox(
    "Bagaimana Anda ingin memasukkan data?",
    ("Unggah File (CSV/ZIP)", "Masukkan Tautan"),
    key="src_choice_key"
)

dfs: List[pd.DataFrame] = []

# REVISI 1: Logika untuk handle upload file digabung.
if src_choice == "Unggah File (CSV/ZIP)":
    uploaded_files = st.file_uploader(
        "Unggah satu / lebih file .CSV atau .ZIP",
        type=["csv", "zip"],
        accept_multiple_files=True,
        key="file_uploader"
    )
    if uploaded_files:
        with st.spinner("Membaca dan memproses file..."):
            for f in uploaded_files:
                # Deteksi otomatis tipe file berdasarkan nama
                if f.name.lower().endswith('.zip'):
                    with zipfile.ZipFile(f, "r") as z:
                        for name in z.namelist():
                            if name.lower().endswith(".csv") and not name.startswith('__MACOSX'):
                                dfs.append(clean_dataframe(read_csv_from_bytes(z.read(name))))
                elif f.name.lower().endswith('.csv'):
                    dfs.append(clean_dataframe(read_csv_from_bytes(f.read())))
                else:
                    st.warning(f"File '{f.name}' diabaikan karena bukan format .zip atau .csv yang didukung.")

else: # src_choice == "Masukkan Tautan"
    url_text = st.text_area("Tempel satu / lebih tautan (pisahkan dengan baris baru atau koma)", key="url_input")
    if url_text:
        with st.spinner("Mengunduh dan memproses data dari tautan..."):
            url_list = [u.strip() for u in re.split(r"[\n,]+", url_text) if u.strip()]
            for u in url_list:
                dfs.extend(load_from_url(u))

if not dfs:
    st.info("⌛ Unggah file atau masukkan tautan untuk melanjutkan.")
    st.stop()

st.success(f"✅ Berhasil mengumpulkan {len(dfs)} file data.")

# ----------  2️⃣  PENGATURAN SPREADSHEET  ----------
st.header("2️⃣ Pengaturan Spreadsheet")

# REVISI 2: Menambahkan form untuk konfirmasi
with st.form("sheet_settings_form"):
    sheet_link = st.text_input("Tempel link Google Spreadsheet tujuan:", key="sheet_link_input")
    upload_mode = st.radio(
        "Mode upload:",
        ("Ganti isi lama (Replace)", "Tambahkan di bawah (Append)"),
        key="upload_mode_key",
        horizontal=True
    )
    
    # Tombol konfirmasi di dalam form
    confirmed = st.form_submit_button("✅ Konfirmasi & Lanjutkan")

if not confirmed or not sheet_link:
    st.info("Masukkan link spreadsheet dan klik 'Konfirmasi' untuk melanjutkan.")
    st.stop()


# ----------  3️⃣  AUTENTIKASI GOOGLE SHEETS  ----------
st.header("3️⃣ Autentikasi Google Sheets")

with st.form("json_auth_form"):
    json_opt = st.radio(
        "Pilih sumber Service-Account JSON:",
        ("Gunakan JSON default di Drive", "Unggah file JSON sendiri"),
        key="json_opt_key"
    )
    uploaded_json = None
    if json_opt == "Unggah file JSON sendiri":
        uploaded_json = st.file_uploader("Unggah file .json", type="json", key="json_uploader")
    
    proceed = st.form_submit_button("🚀 Mulai Proses Upload!")

if not proceed:
    st.stop()

# ----------  Proses Utama (Ekstraksi ID dan Upload) ----------
m = re.search(r"/d/([\w-]+)", sheet_link)
if not m:
    st.error("Link Spreadsheet tidak valid. Pastikan link yang Anda masukkan benar.")
    st.stop()

SPREADSHEET_ID = m.group(1)

try:
    st.info("Mempersiapkan kredensial...")
    if json_opt == "Gunakan JSON default di Drive":
        default_link = "https://drive.google.com/file/d/1VRpKOpI3R918d5voY70wi9CsDRBwDuRl/view?usp=drive_link"
        fid = re.search(r"/d/([\w-]+)", default_link).group(1)
        r = requests.get(f"https://drive.google.com/uc?export=download&id={fid}", timeout=30)
        r.raise_for_status()
        json_data = json.loads(r.content.decode())
        st.success("✅ JSON default berhasil diambil.")
    else:
        if uploaded_json is None:
            st.error("Silakan unggah file JSON terlebih dahulu.")
            st.stop()
        json_data = json.loads(uploaded_json.read().decode())
        st.success("✅ File JSON berhasil diproses.")

    with st.spinner("Mengklasifikasikan data..."):
        ronm_dfs, rsocmed_dfs, rfollower_dfs, unknown_dfs = [], [], [], []
        
        for df in dfs:
            cols = {str(c).lower() for c in df.columns}
            if "tier" in cols:
                ronm_dfs.append(df)
            elif {"original_id", "label"}.issubset(cols):
                start_col = next((c for c in df.columns if str(c).lower() == 'original_id'), None)
                end_col = next((c for c in df.columns if str(c).lower() == 'label'), None)
                if start_col and end_col:
                    start_idx = df.columns.get_loc(start_col)
                    end_idx = df.columns.get_loc(end_col)
                    rsocmed_dfs.append(df.iloc[:, start_idx : end_idx + 1])
            elif "social_media" in cols:
                rfollower_dfs.append(df)
            else:
                unknown_dfs.append(df)

    if not ronm_dfs and not rsocmed_dfs and not rfollower_dfs:
        st.error("❌ Tidak ada data yang cocok dengan skema RONM (kolom 'tier'), RSOCMED (kolom 'original_id' & 'label'), maupun RFOLLOWER (kolom 'social_media'). Proses dihentikan.")
        st.stop()

    targets = {
        "RONM": pd.concat(ronm_dfs, ignore_index=True) if ronm_dfs else None,
        "RSOCMED": pd.concat(rsocmed_dfs, ignore_index=True) if rsocmed_dfs else None,
        "RFOLLOWER": pd.concat(rfollower_dfs, ignore_index=True) if rfollower_dfs else None,
    }

    creds = service_account.Credentials.from_service_account_info(
        json_data, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    st.write("---")
    st.info("🚀 Memulai proses unggah...")
    any_upload_success = False
    
    for ws_name, df in targets.items():
        if df is None or df.empty:
            continue
        
        st.subheader(f"Mengunggah ke sheet: `{ws_name}`")
        df = truncate_long_texts(standardize_dates(df))

        try:
            ws = sh.worksheet(ws_name)
        except gspread.exceptions.WorksheetNotFound:
            st.info(f"Worksheet '{ws_name}' tidak ditemukan, membuat baru...")
            ws = sh.add_worksheet(title=ws_name, rows="1000", cols="50")

        replace = upload_mode.startswith("Ganti")

        if ws_name == "RFOLLOWER":
            st.info(f"Mode RFOLLOWER: Menulis ulang data dari file (termasuk header) mulai dari baris 2.")
            ws.batch_clear(['A2:ZZ']) 
            next_row = 2
            effective_replace_mode = True
        else:
            effective_replace_mode = replace
            if effective_replace_mode:
                if ws_name == "RONM": clear_range = 'A:AG'
                elif ws_name == "RSOCMED": clear_range = 'A:AZ'
                else: clear_range = 'A:ZZ'
                
                st.info(f"Membersihkan kolom {clear_range} di sheet '{ws_name}'...")
                ws.batch_clear([clear_range])
                next_row = 1
            else:
                existing_values = ws.get_all_values()
                next_row = len(existing_values) + 1 if existing_values else 1
                effective_replace_mode = False
        
        progress_placeholder = st.empty()
        write_dataframe_in_chunks(
            ws, df, 
            start_row=next_row, 
            replace_mode=effective_replace_mode,
            progress_placeholder=progress_placeholder
        )
        st.success(f"✅ Selesai! {len(df)} baris berhasil diunggah ke worksheet **{ws_name}**")
        any_upload_success = True

    st.write("---")
    if any_upload_success:
        st.balloons()
        st.success("🎉 Semua proses unggah telah selesai!")
        
    if unknown_dfs:
        st.warning(f"⚠️ Ditemukan {len(unknown_dfs)} file yang tidak cocok dengan skema manapun dan tidak diunggah.")

except Exception:
    st.error("❌ Terjadi kesalahan fatal saat mengakses atau menulis ke Spreadsheet.")
    st.text(traceback.format_exc())

finally:
    st.divider()
    if 'SPREADSHEET_ID' in locals():
        st.markdown(
            f"### [📄 Buka Spreadsheet](https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit)"
        )
    
    # REVISI 3: Tombol reset di bagian bawah untuk kemudahan setelah proses selesai.
    if st.button("Mulai Lagi (Reset)", use_container_width=True, key="reset_bottom"):
        st.session_state.clear()
        st.rerun()
