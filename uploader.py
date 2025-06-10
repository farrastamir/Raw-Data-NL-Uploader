# =====================  IMPORT  =====================
import streamlit as st
import zipfile, io, re, json, traceback, requests
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account


# =====================  FUNGSI BANTU  =====================
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Hilangkan apostrof ('123) yang kadang muncul dari Excel/Sheets."""
    return df.applymap(lambda x: str(x).lstrip("'") if isinstance(x, str) else x)

def detect_delimiter(sample_text: str) -> str:
    """Deteksi delimiter dominan (',' atau ';')."""
    return ";" if sample_text.count(";") > sample_text.count(",") else ","

def limit_body_column(df: pd.DataFrame,
                      column_name="body",
                      max_length=50_000,
                      new_length=30_000) -> pd.DataFrame:
    """Pangkas kolom 'body' yang terlalu panjang."""
    if column_name in df.columns:
        df[column_name] = df[column_name].apply(
            lambda x: x[:new_length] if isinstance(x, str) and len(x) > max_length else x
        )
    return df

def _fix_time_dots(t: str) -> str:
    """Ubah penanda jam 14.26.28 → 14:26:28, 13.00 → 13:00."""
    return re.sub(r"(\d{1,2})\.(\d{2})(?:\.(\d{2}))?",
                  lambda m: f"{m.group(1)}:{m.group(2)}" + (f":{m.group(3)}" if m.group(3) else ""),
                  t)

def _to_full_year(year: int) -> int:
    """Konversi 2-digit year ke 4-digit (≤30 → 20xx, sisanya 19xx)."""
    if year < 100:
        return 2000 + year if year <= 30 else 1900 + year
    return year

def standardize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Ubah kolom date_created / date_published ke 'dd/mm/yyyy hh.mm.ss'."""
    for col in ("date_created", "date_published"):
        if col not in df.columns:
            continue
        def _convert(val):
            if pd.isna(val):
                return val
            s = str(val).strip()
            # pecah tanggal & waktu
            if " " in s:
                date_part, time_part = s.split(" ", 1)
            else:
                date_part, time_part = s, "00:00:00"
            time_part = _fix_time_dots(time_part)
            date_part = date_part.replace("-", "/")

            # lengkapi waktu
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

def load_from_url(url: str) -> list[pd.DataFrame]:
    """Unduh file CSV / ZIP via URL → list DataFrame."""
    dfs: list[pd.DataFrame] = []
    try:
        r = requests.get(url.strip())
        r.raise_for_status()
        content = r.content
        # ZIP?
        if zipfile.is_zipfile(io.BytesIO(content)):
            with zipfile.ZipFile(io.BytesIO(content), "r") as z:
                for name in z.namelist():
                    if name.lower().endswith(".csv"):
                        dfs.append(clean_dataframe(read_csv_from_bytes(z.read(name))))
        else:  # anggap CSV
            dfs.append(clean_dataframe(read_csv_from_bytes(content)))
    except Exception as exc:
        st.error(f"Gagal mengambil {url} → {exc}")
    return dfs


# =====================  UI  =====================
st.title("Upload / Ambil CSV atau ZIP ➜ Google Spreadsheet")

# ----------  1️⃣  PILIH SUMBER DATA  ----------
st.header("1️⃣ Pilih sumber data")

src_choice = st.selectbox(
    "Bagaimana Anda ingin memasukkan data?",
    ("Unggah CSV", "Unggah ZIP", "Masukkan tautan (CSV / ZIP)")
)

dfs = []

if src_choice == "Unggah CSV":
    csv_files = st.file_uploader("Unggah satu / lebih file CSV",
                                 type="csv", accept_multiple_files=True)
    for f in csv_files:
        dfs.append(clean_dataframe(read_csv_from_bytes(f.read())))

elif src_choice == "Unggah ZIP":
    zip_files = st.file_uploader("Unggah satu / lebih file ZIP",
                                 type="zip", accept_multiple_files=True)
    for f in zip_files:
        with zipfile.ZipFile(f, "r") as z:
            for name in z.namelist():
                if name.lower().endswith(".csv"):
                    dfs.append(clean_dataframe(read_csv_from_bytes(z.read(name))))

else:  # Masukkan tautan
    url_text = st.text_area("Tempel satu / lebih tautan (pisahkan dengan baris baru atau koma)")
    url_list = [u.strip() for u in re.split(r"[\n,]+", url_text) if u.strip()]
    for u in url_list:
        dfs.extend(load_from_url(u))

# Gabung dataframe kalau ada
if dfs:
    csv_df = pd.concat(dfs, ignore_index=True)
    st.success(f"✅ Terkumpul {len(dfs)} file  →  {len(csv_df)} baris total.")
else:
    st.info("⌛ Unggah atau masukkan tautan untuk melanjutkan.")
    st.stop()


# ----------  2️⃣  AUTENTIKASI GOOGLE SHEETS  ----------
st.header("2️⃣ Autentikasi Google Sheets")

with st.form("json_auth_form"):
    json_opt = st.radio(
        "Pilih sumber Service-Account JSON:",
        ("Gunakan JSON default di Drive", "Unggah file JSON sendiri")
    )
    uploaded_json = None
    if json_opt == "Unggah file JSON sendiri":
        uploaded_json = st.file_uploader("Unggah file .json", type="json")

    proceed = st.form_submit_button("Proceed")

json_data = None
if not proceed:
    st.stop()

try:
    if json_opt == "Gunakan JSON default di Drive":
        default_link = (
            "https://drive.google.com/file/d/1VRpKOpI3R918d5voY70wi9CsDRBwDuRl/view?usp=drive_link"
        )
        fid = re.search(r"/d/([\w-]+)", default_link).group(1)
        r = requests.get(f"https://drive.google.com/uc?export=download&id={fid}")
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


# ----------  3️⃣  PENGATURAN UPLOAD KE SPREADSHEET  ----------
st.header("3️⃣ Upload ke Spreadsheet")

# normalisasi tanggal + pangkas body
csv_df = limit_body_column(standardize_dates(csv_df))

upload_mode = st.radio(
    "Mode upload:",
    ("Ganti isi lama (Replace)", "Tambahkan di bawah (Append)")
)

# deteksi target sheet
if "tier" in csv_df.columns:
    target_worksheet = "RONM"
elif {"original_id", "label"}.issubset(csv_df.columns):
    target_worksheet = "RSOCMED"
    start_idx = csv_df.columns.get_loc("original_id")
    end_idx   = csv_df.columns.get_loc("label")
    csv_df = csv_df.iloc[:, start_idx:end_idx + 1]  # subset kolom
else:
    st.error("Kolom 'tier' atau ('original_id' + 'label') tidak ditemukan.")
    st.stop()

st.info(f"Target worksheet: **{target_worksheet}**")
st.write(f"Kolom DataFrame: {list(csv_df.columns)}")
st.write(f"Jumlah baris: {len(csv_df)}")

sheet_link = st.text_input("Tempel link Google Spreadsheet:")

if not sheet_link:
    st.stop()

# ----------  EKSEKUSI UPLOAD  ----------
m = re.search(r"/d/([\w-]+)", sheet_link)
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

    # tentukan baris awal
    if upload_mode == "Ganti isi lama (Replace)":
        ws.batch_clear(["A1:AZ"])
        next_row = 1
    else:
        existing = ws.get_all_values()
        next_row = len(existing) + 1 if existing else 1

    st.write("🚀 Mengunggah data…")
    prog = st.progress(0)
    batch_size = 10_000
    total = len(csv_df)

    # chunk pertama (sertakan header jika replace)
    set_with_dataframe(ws, csv_df.iloc[:batch_size],
                       include_column_header=(next_row == 1),
                       row=next_row, resize=False)
    prog.progress(min(0.1, 1.0))

    # sisanya
    for start in range(batch_size, total, batch_size):
        end = min(start + batch_size, total)
        set_with_dataframe(
            ws,
            csv_df.iloc[start:end],
            include_column_header=False,
            row=next_row + start if next_row > 1 else start + 2,
            resize=False
        )
        prog.progress(min(end / total, 1.0))

    st.success(f"✅ Sukses upload ke worksheet '{target_worksheet}'.")
    st.markdown(f"[📄 Buka Spreadsheet](https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit)")
except Exception:
    st.error("❌ Terjadi kesalahan saat mengakses / menulis Spreadsheet.")
    st.text(traceback.format_exc())
