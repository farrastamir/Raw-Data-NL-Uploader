import streamlit as st
import zipfile, io, re, json, traceback, requests, datetime as dt
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account

# ----------  FUNGSI BANTU  ----------
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Buang leading apostrophe yang kadang muncul dari Excel/Sheets."""
    return df.applymap(lambda x: str(x).lstrip("'") if isinstance(x, str) else x)

def detect_delimiter(sample_text: str) -> str:
    """Deteksi ; atau , sebagai delimiter dominan."""
    return ";" if sample_text.count(";") > sample_text.count(",") else ","

def limit_body_column(df: pd.DataFrame,
                      column_name="body",
                      max_length=50_000,
                      new_length=30_000) -> pd.DataFrame:
    """Pangkas kolom 'body' agar tidak melebihi batas."""
    if column_name in df.columns:
        df[column_name] = df[column_name].apply(
            lambda x: x[:new_length] if isinstance(x, str) and len(x) > max_length else x
        )
    return df

def _fix_time_dots(t: str) -> str:
    """Ganti pemisah jam dari '.' ke ':' agar mudah diparse."""
    # contoh: 14.26.28 → 14:26:28, 13.00 → 13:00
    # hanya ubah bagian jam-menit-detik
    return re.sub(r"(\d{1,2})\.(\d{2})(?:\.(\d{2}))?",
                  lambda m: f"{m.group(1)}:{m.group(2)}" + (f":{m.group(3)}" if m.group(3) else ""),
                  t)

def _to_full_year(year: int) -> int:
    """Konversi 2-digit year ke 4-digit (<=30 → 2000-2030, sisanya 1900-1999)."""
    if year < 100:
        return 2000 + year if year <= 30 else 1900 + year
    return year

def standardize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Ubah `date_created` & `date_published` ke dd/mm/yyyy hh.mm.ss."""
    for col in ["date_created", "date_published"]:
        if col not in df.columns:
            continue
        def _convert(val):
            if pd.isna(val):
                return val
            s = str(val).strip()
            # pisah tanggal & waktu
            if " " in s:
                date_part, time_part = s.split(" ", 1)
            else:
                # mungkin '09/06/2025' saja
                date_part, time_part = s, "00:00:00"
            time_part = _fix_time_dots(time_part)
            # handle date pemisah '-' / '/'
            date_part = date_part.replace("-", "/")
            # pad waktu
            if time_part.count(":") == 0:
                time_part += ":00"        # jam → jam:00
            if time_part.count(":") == 1:
                time_part += ":00"        # jam:menit → jam:menit:00
            s_norm = f"{date_part} {time_part}"
            try:
                dt_obj = pd.to_datetime(s_norm, dayfirst=True, errors="coerce")
                # ganti 2-digit year:
                if pd.notna(dt_obj) and dt_obj.year < 100:
                    dt_obj = dt_obj.replace(year=_to_full_year(dt_obj.year))
            except Exception:
                dt_obj = pd.NaT
            return (dt_obj.strftime("%d/%m/%Y %H.%M.%S")
                    if pd.notna(dt_obj) else val)
        df[col] = df[col].apply(_convert)
    return df

def read_csv_from_bytes(b: bytes) -> pd.DataFrame:
    """Baca CSV (bytes) dengan delimiter otomatis."""
    sample = b[:2048].decode("utf-8", errors="ignore")
    delim = detect_delimiter(sample)
    return pd.read_csv(io.BytesIO(b), delimiter=delim)

def load_from_url(url: str) -> list[pd.DataFrame]:
    """Unduh satu URL (zip / csv) → list DF."""
    try:
        r = requests.get(url.strip())
        r.raise_for_status()
        content = r.content
        if zipfile.is_zipfile(io.BytesIO(content)):
            dfs = []
            with zipfile.ZipFile(io.BytesIO(content), "r") as z:
                for name in z.namelist():
                    if name.lower().endswith(".csv"):
                        dfs.append(clean_dataframe(
                            read_csv_from_bytes(z.read(name))))
            return dfs
        else:  # asumsikan CSV
            return [clean_dataframe(read_csv_from_bytes(content))]
    except Exception as e:
        st.error(f"Gagal mengambil {url}\n{e}")
        return []

# ----------  UI  ----------
st.title("Upload / Ambil CSV atau ZIP ➜ Google Spreadsheet")

# -- PILIHAN UNGGAH FILE --
uploaded_csvs = st.file_uploader("Unggah satu / lebih CSV", type="csv",
                                 accept_multiple_files=True)
uploaded_zips = st.file_uploader("Unggah satu / lebih ZIP (berisi CSV)",
                                 type="zip", accept_multiple_files=True)

st.markdown("**Atau** tempel tautan ke CSV / ZIP (satu per baris atau pisahkan koma):")
url_text = st.text_area("Daftar tautan")
url_list = [u.strip() for u in re.split(r"[\n,]+", url_text) if u.strip()]

dfs = []

# -- PROSES FILE CSV --
for f in uploaded_csvs:
    dfs.append(clean_dataframe(read_csv_from_bytes(f.read())))

# -- PROSES FILE ZIP --
for f in uploaded_zips:
    with zipfile.ZipFile(f, "r") as z:
        for name in z.namelist():
            if name.lower().endswith(".csv"):
                dfs.append(clean_dataframe(read_csv_from_bytes(z.read(name))))

# -- PROSES LINK --
for u in url_list:
    dfs.extend(load_from_url(u))

if dfs:
    csv_df = pd.concat(dfs, ignore_index=True)
    st.success(f"✅ Total {len(dfs)} file berhasil digabung ({len(csv_df)} baris).")
else:
    st.info("⌛ Unggah file atau masukkan tautan terlebih dahulu.")
    st.stop()

# ----------  SIAP UPLOAD ----------
# 1. Normalisasi tanggal
csv_df = standardize_dates(csv_df)
# 2. Pangkas body (jika ada)
csv_df = limit_body_column(csv_df)

upload_mode = st.radio("Mode upload ke Spreadsheet:",
                       ["Ganti isi lama (Replace)", "Tambahkan di bawah (Append)"])

# -- DETEKSI SHEET TARGET --
if "tier" in csv_df.columns:
    target_worksheet = "RONM"
elif {"original_id", "label"}.issubset(csv_df.columns):
    target_worksheet = "RSOCMED"
    start_idx = csv_df.columns.get_loc("original_id")
    end_idx   = csv_df.columns.get_loc("label")
    csv_df = csv_df.iloc[:, start_idx:end_idx + 1]
else:
    st.error("Kolom 'tier' atau ('original_id' dan 'label') tidak ditemukan.")
    st.stop()

st.info(f"Target sheet: **{target_worksheet}**")
st.write(f"Kolom: {list(csv_df.columns)}")
st.write(f"Jumlah baris: {len(csv_df)}")

# ----------  LINK SPREADSHEET ----------
sheet_link = st.text_input("Tempel link Google Spreadsheet:")

# ----------  AUTENTIKASI ----------
json_auth_option = st.radio(
    "Pilih metode autentikasi:",
    ["Gunakan JSON default di Drive", "Unggah file JSON sendiri"]
)

json_data = None
if json_auth_option == "Gunakan JSON default di Drive":
    default_json_link = "https://drive.google.com/file/d/1VRpKOpI3R918d5voY70wi9CsDRBwDuRl/view?usp=drive_link"
    m = re.search(r"/d/([a-zA-Z0-9-_]+)", default_json_link)
    if not m:
        st.error("Link JSON default tidak valid.")
        st.stop()
    file_id = m.group(1)
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    try:
        r = requests.get(url); r.raise_for_status()
        json_data = json.loads(r.content.decode("utf-8"))
        st.success("✅ JSON default berhasil diambil.")
    except Exception as e:
        st.error(f"❌ Gagal mengambil JSON default\n{e}")
        st.stop()
else:
    uploaded_json = st.file_uploader("Unggah file Service-Account JSON Anda", type="json")
    if uploaded_json:
        json_data = json.loads(uploaded_json.read().decode("utf-8"))
        st.success("✅ File JSON berhasil di-upload.")
    else:
        st.warning("Unggah file JSON untuk lanjut."); st.stop()

# ----------  AKSES & KIRIM ----------
if sheet_link and json_data:
    m = re.search(r"/d/([a-zA-Z0-9-_]+)", sheet_link)
    if not m:
        st.error("Link Spreadsheet tidak valid.")
        st.stop()
    SPREADSHEET_ID = m.group(1)
    try:
        creds = service_account.Credentials.from_service_account_info(
            json_data,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SPREADSHEET_ID)
        try:
            ws = sh.worksheet(target_worksheet)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=target_worksheet, rows="1000", cols="26")

        # Tentukan baris mulai
        next_row = 1
        if upload_mode == "Ganti isi lama (Replace)":
            ws.batch_clear(["A1:AZ"])
        else:
            existing = ws.get_all_values()
            next_row = len(existing) + 1 if existing else 1

        st.write("🚀 Mengunggah…")
        progress = st.progress(0)
        batch_size = 10_000
        total = len(csv_df)

        # chunk pertama (dengan header jika replace)
        set_with_dataframe(ws, csv_df.iloc[:batch_size],
                           include_column_header=(next_row == 1),
                           row=next_row, resize=False)
        progress.progress(min(0.1, 1.0))

        # sisanya
        for start in range(batch_size, total, batch_size):
            end = min(start + batch_size, total)
            set_with_dataframe(ws, csv_df.iloc[start:end],
                               include_column_header=False,
                               row=next_row + start if next_row > 1 else start + 2,
                               resize=False)
            progress.progress(min(end / total, 1.0))

        st.success(f"✅ Sukses upload ke '{target_worksheet}'.")
        st.markdown(f"[📄 Lihat Spreadsheet](https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit)")
    except Exception as e:
        st.error("❌ Gagal mengakses Google Spreadsheet.")
        st.text(traceback.format_exc())
