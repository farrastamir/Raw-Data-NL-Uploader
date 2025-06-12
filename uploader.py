# =====================  IMPORT  =====================
import streamlit as st
import zipfile, io, re, json, traceback, requests, time
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account
from typing import List


# =====================  FUNGSI BANTU  =====================
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

            # Pisah tanggal & waktu
            date_part, time_part = (s.split(" ", 1) + ["00:00:00"])[:2]
            time_part = _fix_time_dots(time_part)
            date_part = date_part.replace("-", "/")

            # Lengkapi waktu
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
    sample = b[:2048].decode("utf-8", errors="ignore")
    delim = detect_delimiter(sample)
    return pd.read_csv(io.BytesIO(b), delimiter=delim)


def load_from_url(url: str) -> List[pd.DataFrame]:
    """Unduh file CSV/ZIP via URL → list DataFrame."""
    dfs: List[pd.DataFrame] = []
    try:
        r = requests.get(url.strip())
        r.raise_for_status()
        content = r.content

        # ZIP?
        if zipfile.is_zipfile(io.BytesIO(content)):
            with zipfile.ZipFile(io.BytesIO(content), "r") as z:
                for name in z.namelist():
                    if name.lower().endswith(".csv"):
                        dfs.append(
                            clean_dataframe(read_csv_from_bytes(z.read(name)))
                        )
        else:  # Anggap CSV
            dfs.append(clean_dataframe(read_csv_from_bytes(content)))
    except Exception as exc:
        st.error(f"Gagal mengambil {url} → {exc}")
    return dfs


def write_dataframe_in_chunks(ws,
                              df: pd.DataFrame,
                              start_row: int,
                              replace_mode: bool):
    """
    Kirim DataFrame ke worksheet dalam batch 10.000 baris.
    Jika kena error 500, batch diperkecil separuh lalu retry.
    """
    rows_per_batch = 10_000  # Tetapkan jumlah baris per batch

    row_ptr = 0
    total_rows = len(df)
    header_written = False

    while row_ptr < total_rows:
        chunk = df.iloc[row_ptr: row_ptr + rows_per_batch]
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


# =====================  UI  =====================
st.set_page_config(
    page_title="Upload CSV/ZIP ➜ Google Sheets",
    page_icon="📄",
    layout="wide",
)
st.title("Upload / Ambil CSV atau ZIP ➜ Google Spreadsheet")

# ----------  1️⃣  PILIH SUMBER DATA  ----------
st.header("1️⃣ Pilih sumber data")

src_choice = st.selectbox(
    "Bagaimana Anda ingin memasukkan data?",
    ("Unggah ZIP", "Unggah CSV", "Masukkan tautan (CSV / ZIP)"),
)

dfs: List[pd.DataFrame] = []

# ----------  Pengumpulan data ----------
if src_choice == "Unggah ZIP":
    zip_files = st.file_uploader(
        "Unggah satu / lebih file ZIP", type="zip", accept_multiple_files=True
    )
    for f in zip_files:
        with zipfile.ZipFile(f, "r") as z:
            for name in z.namelist():
                if name.lower().endswith(".csv"):
                    dfs.append(clean_dataframe(read_csv_from_bytes(z.read(name))))

elif src_choice == "Unggah CSV":
    csv_files = st.file_uploader(
        "Unggah satu / lebih file CSV", type="csv", accept_multiple_files=True
    )
    for f in csv_files:
        dfs.append(clean_dataframe(read_csv_from_bytes(f.read())))

else:  # Masukkan tautan
    url_text = st.text_area("Tempel satu / lebih tautan (pisahkan baris/baris atau koma)")
    url_list = [u.strip() for u in re.split(r"[\n,]+", url_text) if u.strip()]
    for u in url_list:
        dfs.extend(load_from_url(u))

if not dfs:
    st.info("⌛ Unggah atau masukkan tautan untuk melanjutkan.")
    st.stop()

st.success(f"✅ Terkumpul {len(dfs)} file.")

# ----------  2️⃣  PENGATURAN SPREADSHEET  ----------
st.header("2️⃣ Pengaturan Spreadsheet")

sheet_link = st.text_input("Tempel link Google Spreadsheet:")
upload_mode = st.radio(
    "Mode upload:",
    ("Ganti isi lama (Replace)", "Tambahkan di bawah (Append)"),
)

if not sheet_link:
    st.stop()

# ----------  3️⃣  AUTENTIKASI GOOGLE SHEETS  ----------
st.header("3️⃣ Autentikasi Google Sheets")

with st.form("json_auth_form"):
    json_opt = st.radio(
        "Pilih sumber Service-Account JSON:",
        ("Gunakan JSON default di Drive", "Unggah file JSON sendiri"),
    )
    uploaded_json = None
    if json_opt == "Unggah file JSON sendiri":
        uploaded_json = st.file_uploader("Unggah file .json", type="json")
    proceed = st.form_submit_button("Proceed")

if not proceed:
    st.stop()

# ----------  Siapkan kredensial  ----------
try:
    if json_opt == "Gunakan JSON default di Drive":
        default_link = (
            "https://drive.google.com/file/d/1VRpKOpI3R918d5voY70wi9CsDRBwDuRl/view?usp=drive_link"
        )
        fid = re.search(r"/d/([\w-]+)", default_link).group(1)
        r = requests.get(
            f"https://drive.google.com/uc?export=download&id={fid}", timeout=30
        )
        r.raise_for_status()
        json_data = json.loads(r.content.decode())
        st.success("✅ JSON default berhasil diambil.")
    else:
        if uploaded_json is None:
            st.error("Silakan unggah file JSON terlebih dahulu.")
            st.stop()
        json_data = json.loads(uploaded_json.read().decode())
        st.success("✅ File JSON berhasil diproses.")
except Exception as exc:
    st.error(f"Gagal memuat JSON: {exc}")
    st.stop()

# ----------  Klasifikasikan DataFrame  ----------
ronm_dfs, rsocmed_dfs, unknown_dfs = [], [], []
for df in dfs:
    if "tier" in df.columns:
        ronm_dfs.append(df)
    elif {"original_id", "label"}.issubset(df.columns):
        start = df.columns.get_loc("original_id")
        end = df.columns.get_loc("label")
        rsocmed_dfs.append(df.iloc[:, start : end + 1])
    else:
        unknown_dfs.append(df)

if not ronm_dfs and not rsocmed_dfs:
    st.error("❌ Tidak ada data yang cocok dengan skema RONM maupun RSOCMED.")
    st.stop()

targets = {
    "RONM": pd.concat(ronm_dfs, ignore_index=True) if ronm_dfs else None,
    "RSOCMED": pd.concat(rsocmed_dfs, ignore_index=True) if rsocmed_dfs else None,
}

# ----------  Proses Upload  ----------
m = re.search(r"/d/([\w-]+)", sheet_link)
if not m:
    st.error("Link Spreadsheet tidak valid.")
    st.stop()

SPREADSHEET_ID = m.group(1)

try:
    creds = service_account.Credentials.from_service_account_info(
        json_data, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    st.write("🚀 Mengunggah data …")
    for ws_name, df in targets.items():
        if df is None:
            continue

        # Normalisasi
        df = truncate_long_texts(standardize_dates(df))

        # Buka / buat worksheet
        try:
            ws = sh.worksheet(ws_name)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=ws_name, rows="1000", cols="26")

        # Tentukan baris awal
        replace = upload_mode.startswith("Ganti")
        if replace:
            ws.batch_clear(["A1:AZ"])
            next_row = 1
        else:
            existing = ws.get_all_values()
            next_row = len(existing) + 1 if existing else 1

        # Tulis dengan batch kecil
        write_dataframe_in_chunks(
            ws, df, start_row=next_row, replace_mode=replace
        )

        st.success(f"✅ {len(df)} baris → worksheet **{ws_name}**")

    st.markdown(
        f"[📄 Buka Spreadsheet](https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit)"
    )
except Exception:
    st.error("❌ Terjadi kesalahan saat mengakses / menulis Spreadsheet.")
    st.text(traceback.format_exc())
