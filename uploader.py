# ===================== IMPORT =====================
import streamlit as st
import zipfile, io, re, json, traceback, requests, time
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account
from typing import List, Any

# ===================== FUNGSI BANTU =====================


def check_data_health(dfs: List[pd.DataFrame]) -> List[dict]:
    results = []

    for idx, df in enumerate(dfs):
        # Gunakan duplikat agar kita tidak memodifikasi data asli
        df_copy = df.copy()
        cols = {str(c).lower() for c in df_copy.columns}

        # Mapping kolom ke lowercase untuk mempermudah pengecekan
        col_mapping = {c: str(c).lower() for c in df_copy.columns}
        df_copy.rename(columns=col_mapping, inplace=True)

        report = {"File Index": idx + 1, "Type": "Unknown", "Issues": [], "Health": 100.0}

        # Check if Socmed
        if {"original_id", "label"}.issubset(cols) or "stream_type" in cols:
            report["Type"] = "Socmed"

            # 1. Stream Type Account and Keyword Check
            if "stream_type" in cols:
                stream_types = set(df_copy['stream_type'].dropna().astype(str).str.lower())
                if "account" not in stream_types or "keyword" not in stream_types:
                    report["Issues"].append("- ⚠️ Data `stream_type` tidak lengkap. Pastikan memiliki 'account' dan 'keyword'. (-10%)")
                    report["Health"] -= 10.0

            # 2. Account comment missing checker
            if {"stream_type", "post_type", "link", "comment", "reply_to_original_id", "parent_link"}.issubset(cols):
                post_made_df = df_copy[(df_copy['post_type'].astype(str).str.lower() == 'post_made') & (df_copy['reply_to_original_id'].isna() | (df_copy['reply_to_original_id'] == ''))]
                total_comments_missing = 0
                for _, row in post_made_df.iterrows():
                    link = row['link']
                    expected_comments = 0
                    try:
                        if pd.notna(row['comment']) and str(row['comment']).strip() != "":
                            expected_comments = int(float(row['comment']))
                    except:
                        pass

                    actual_comments = df_copy[df_copy['parent_link'] == link].shape[0]
                    if actual_comments < expected_comments:
                        total_comments_missing += (expected_comments - actual_comments)

                if total_comments_missing > 0:
                    penalty = min(25.0, (total_comments_missing / 10.0))
                    report["Issues"].append(f"- ⚠️ Terdapat {total_comments_missing} komentar missing (kolom comment vs count parent_link). (-{penalty:.1f}%)")
                    report["Health"] -= penalty

            # 3. Retweet checker
            if {"specific_resource_type", "retweet_from_original_id", "share", "original_id"}.issubset(cols):
                twitter_posts = df_copy[(df_copy['specific_resource_type'].astype(str).str.lower() == 'twitter') & (df_copy['retweet_from_original_id'].isna() | (df_copy['retweet_from_original_id'] == ''))]
                missing_retweets = 0
                for _, row in twitter_posts.iterrows():
                    original_id = row['original_id']
                    expected_shares = 0
                    try:
                        if pd.notna(row['share']) and str(row['share']).strip() != "":
                            expected_shares = int(float(row['share']))
                    except:
                        pass
                    actual_retweets = df_copy[df_copy['retweet_from_original_id'] == original_id].shape[0]
                    if actual_retweets < expected_shares:
                        missing_retweets += (expected_shares - actual_retweets)

                if missing_retweets > 0:
                    penalty = min(20.0, (missing_retweets / 10.0))
                    report["Issues"].append(f"- ⚠️ Terdapat {missing_retweets} retweet missing (kolom share vs count retweet_from_original_id). (-{penalty:.1f}%)")
                    report["Health"] -= penalty

            # 4. Keyword Content checking
            if {"stream_type", "object_tittle", "content"}.issubset(cols):
                keyword_df = df_copy[df_copy['stream_type'].astype(str).str.lower() == 'keyword'].dropna(subset=['object_tittle', 'content'])
                missing_keyword_content = 0
                for _, row in keyword_df.iterrows():
                    title = str(row['object_tittle']).lower()
                    content_text = str(row['content']).lower()
                    if title not in content_text:
                        missing_keyword_content += 1
                if missing_keyword_content > 0:
                    penalty = min(15.0, (missing_keyword_content / 5.0))
                    report["Issues"].append(f"- ⚠️ Terdapat {missing_keyword_content} baris dimana value `object_tittle` tidak ditemukan di dalam `content`. (-{penalty:.1f}%)")
                    report["Health"] -= penalty

            # 5. Duplicate data
            if "original_id" in cols:
                duplicates = df_copy.duplicated(subset=['original_id']).sum()
                if duplicates > 0:
                    penalty = min(30.0, (duplicates / 2.0))
                    report["Issues"].append(f"- ⚠️ Ditemukan {duplicates} data duplikat berdasarkan `original_id`. (-{penalty:.1f}%)")
                    report["Health"] -= penalty

            # 6. Account count as Talk
            if {"from_username", "object_group", "stream_type", "post_type"}.issubset(cols):
                post_made = df_copy[(df_copy['stream_type'].astype(str).str.lower() == 'post_made')].dropna(subset=['from_username', 'object_group'])
                talks = df_copy[df_copy['post_type'].astype(str).str.lower() == 'talk'].dropna(subset=['from_username', 'object_group'])

                false_talks = 0
                for _, row in post_made.iterrows():
                    username = row['from_username']
                    groups = str(row['object_group']).split(',')
                    for g in groups:
                        g = g.strip()
                        match = talks[(talks['from_username'] == username) & (talks['object_group'].astype(str).str.contains(g, na=False, regex=False))]
                        if match.shape[0] > 0:
                            false_talks += 1
                            break

                if false_talks > 0:
                    report["Issues"].append(f"- ℹ️ **Peringatan:** Terdapat {false_talks} indikasi false talk (from_username post_made muncul di talk pada group yang sama). (Tidak mengurangi Health)")

        elif "tier" in cols or "attachment" in cols: # ONM or OFM
            report["Type"] = "ONM" if "tier" in cols else "OFM"

            # 1. Duplicate data
            if "original_id" in cols:
                duplicates = df_copy.duplicated(subset=['original_id']).sum()
                if duplicates > 0:
                    penalty = min(30.0, (duplicates / 2.0))
                    report["Issues"].append(f"- ⚠️ Ditemukan {duplicates} data duplikat berdasarkan `original_id`. (-{penalty:.1f}%)")
                    report["Health"] -= penalty

            # 2. Link Duplicate
            if "link" in cols:
                links = df_copy['link'].dropna().astype(str).tolist()
                cleaned_links = []
                for l in links:
                    cl = l.rstrip('/')
                    cl = cl.replace("?page=all", "")
                    cleaned_links.append(cl)

                df_links = pd.Series(cleaned_links)
                link_dups = df_links.duplicated().sum()
                if link_dups > 0:
                    penalty = min(30.0, (link_dups / 2.0))
                    report["Issues"].append(f"- ⚠️ Ditemukan {link_dups} data duplikat berdasarkan link (setelah normalisasi '/' dan '?page=all'). (-{penalty:.1f}%)")
                    report["Health"] -= penalty

            # 3. Spokeperson Percentage
            if "statement1" in cols:
                total_rows = len(df_copy)
                statement_count = df_copy['statement1'].replace('', pd.NA).dropna().shape[0]
                pct = (statement_count / total_rows) * 100 if total_rows > 0 else 0
                report["Issues"].append(f"- ℹ️ **Info:** Terdapat {pct:.2f}% data memiliki spokeperson (statement1). (Tidak mengurangi Health)")

            # 4. Netral Warning
            if "sentiment" in cols:
                total_sentiment = len(df_copy['sentiment'].dropna())
                netral_sentiment = df_copy['sentiment'].astype(str).str.lower() == 'netral'
                if netral_sentiment.sum() == total_sentiment and total_sentiment > 0:
                    report["Issues"].append("- ⚠️ Peringatan: 100% data memiliki sentiment Netral. Harap dicek kembali. (-10.0%)")
                    report["Health"] -= 10.0

        else:
             report["Type"] = "Follower / Lainnya"
             report["Issues"].append("- Data tidak masuk dalam skema pengecekan utama.")

        # Pastikan tidak < 0
        report["Health"] = max(0.0, min(100.0, report["Health"]))

        if len(report["Issues"]) == 0:
             report["Issues"].append("- ✅ Data dalam kondisi sempurna sesuai pengecekan!")

        results.append(report)

    return results

def get_column_letter(col_index: int) -> str:
    """Mengubah indeks kolom (0-based) menjadi huruf kolom Google Sheets (A, B, ..., AA)."""
    if col_index < 0:
        raise ValueError("Indeks kolom harus non-negatif")
    result = ""
    # Fungsi ini mengonversi indeks berbasis-0 ke notasi kolom berbasis-26
    while col_index >= 0:
        result = chr(col_index % 26 + ord('A')) + result
        col_index = col_index // 26 - 1
    return result

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Membersihkan apostrof di awal string."""
    if hasattr(df, 'map'):
        return df.map(lambda x: str(x).lstrip("'") if isinstance(x, str) else x)
    return df.applymap(lambda x: str(x).lstrip("'") if isinstance(x, str) else x)

def detect_delimiter(sample_text: str) -> str:
    """Mendeteksi delimiter CSV berdasarkan jumlah kemunculan."""
    return ";" if sample_text.count(";") > sample_text.count(",") else ","

def truncate_long_texts(df: pd.DataFrame, max_allowed: int = 50_000, trunc_length: int = 20_000) -> pd.DataFrame:
    """Memotong teks yang terlalu panjang untuk sel Google Sheets."""
    def _trunc(x):
        return x[:trunc_length] if isinstance(x, str) and len(x) > max_allowed else x
    if hasattr(df, 'map'):
        return df.map(_trunc)
    return df.applymap(_trunc)

def _fix_time_dots(t: str) -> str:
    """Mengganti format waktu HH.MM.SS menjadi HH:MM:SS."""
    return re.sub(r"(\d{1,2})\.(\d{2})(?:\.(\d{2}))?", lambda m: f"{m.group(1)}:{m.group(2)}" + (f":{m.group(3)}" if m.group(3) else ""), t)

def _to_full_year(year: int) -> int:
    """Mengonversi tahun 2-digit menjadi 4-digit."""
    if year < 100:
        return 2000 + year if year <= 30 else 1900 + year
    return year

def standardize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Menstandarkan kolom tanggal ke format DD/MM/YYYY HH.MM.SS."""
    for col in ("date_created", "date_published"):
        if col not in df.columns:
            continue
        def _convert(val):
            if pd.isna(val): return val
            s = str(val).strip()
            date_part, time_part = (s.split(" ", 1) + ["00:00:00"])[:2]
            time_part = _fix_time_dots(time_part)
            date_part = date_part.replace("-", "/")
            if time_part.count(":") == 0: time_part += ":00"
            if time_part.count(":") == 1: time_part += ":00"
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
    """Membaca data CSV dari bytes, mencoba beberapa encoding."""
    try:
        sample = b[:2048].decode("utf-8", errors="ignore")
        delim = detect_delimiter(sample)
        return pd.read_csv(io.BytesIO(b), delimiter=delim, encoding='utf-8')
    except UnicodeDecodeError:
        st.warning("⚠️ Gagal membaca dengan UTF-8, mencoba lagi dengan encoding 'latin-1'.")
        sample = b[:2048].decode("latin-1")
        delim = detect_delimiter(sample)
        return pd.read_csv(io.BytesIO(b), delimiter=delim, encoding='latin-1')
    except Exception:
        # Fallback jika semuanya gagal
        return pd.read_csv(io.BytesIO(b), delimiter=';', encoding='latin-1')

def read_excel_from_bytes(b: bytes) -> pd.DataFrame:
    """Membaca data Excel dari bytes."""
    return pd.read_excel(io.BytesIO(b))

def load_from_url(url: str) -> List[pd.DataFrame]:
    """Mengunduh dan membaca file CSV/ZIP dari URL."""
    dfs: List[pd.DataFrame] = []
    try:
        r = requests.get(url.strip())
        r.raise_for_status()
        content = r.content
        if zipfile.is_zipfile(io.BytesIO(content)):
            with zipfile.ZipFile(io.BytesIO(content), "r") as z:
                for name in z.namelist():
                    if name.lower().endswith(".csv") and not name.startswith('__MACOSX'):
                        dfs.append(clean_dataframe(read_csv_from_bytes(z.read(name))))
                    elif name.lower().endswith(".xlsx") and not name.startswith('__MACOSX'):
                        dfs.append(clean_dataframe(read_excel_from_bytes(z.read(name))))
        elif url.lower().endswith(".xlsx"):
               dfs.append(clean_dataframe(read_excel_from_bytes(content)))
        else:
            dfs.append(clean_dataframe(read_csv_from_bytes(content)))
    except Exception as exc:
        st.error(f"Gagal mengambil {url} → {exc}")
    return dfs

def write_dataframe_in_chunks(ws, df: pd.DataFrame, start_row: int, replace_mode: bool, progress_placeholder: Any):
    """Menulis DataFrame ke worksheet dalam batch kecil untuk menghindari API error."""
    rows_per_batch = 10_000
    row_ptr = 0
    total_rows = len(df)
    while row_ptr < total_rows:
        chunk = df.iloc[row_ptr : row_ptr + rows_per_batch]
        start_display = row_ptr + 1
        end_display = min(row_ptr + len(chunk), total_rows)
        progress_placeholder.info(f"⏳ Mengunggah baris {start_display} - {end_display} dari {total_rows}...")
        try:
            set_with_dataframe(
                ws, chunk, include_column_header=(row_ptr == 0 and replace_mode),
                row=start_row + row_ptr, resize=False
            )
            row_ptr += len(chunk)
        except gspread.exceptions.APIError as e:
            if "500" in str(e) and rows_per_batch > 1:
                rows_per_batch = max(1, rows_per_batch // 2)
                st.warning(f"⚠️ 500 error – mengecilkan batch menjadi {rows_per_batch} baris…")
                time.sleep(2)
            else:
                raise
    progress_placeholder.empty()

# ===================== APLIKASI STREAMLIT =====================
st.set_page_config(page_title="Upload CSV/XLSX/ZIP ➜ Google Sheets", page_icon="📄", layout="wide")

col1, col2 = st.columns([3, 1])
with col1:
    st.title("Upload File/Link ➜ Google Spreadsheet")
with col2:
    if st.button("🔄 Reset Aplikasi", use_container_width=True, key="reset_top", help="Mulai ulang seluruh proses dari awal."):
        st.session_state.clear()
        st.rerun()


if 'dfs' not in st.session_state: st.session_state.dfs = []
if 'step' not in st.session_state: st.session_state.step = 1
if 'app_mode' not in st.session_state: st.session_state.app_mode = "normal"

# Password check or mode selection
with st.sidebar:
    st.header("⚙️ Mode Aplikasi")
    mode_input = st.text_input("Password (Opsional):", type="password")
    if mode_input == "datachecking":
        st.session_state.app_mode = "checking_only"
        st.success("Mode Data Checking Aktif!")
    else:
        app_mode_select = st.radio("Pilih Mode:", ("Upload Normal", "Data Checking"))
        if app_mode_select == "Data Checking":
            st.session_state.app_mode = "checking"
        else:
            st.session_state.app_mode = "normal"

# ---------- 1️⃣ PILIH SUMBER DATA ----------

if st.session_state.step == 1:
    st.header("1️⃣ Pilih sumber data")
    src_choice = st.selectbox("Bagaimana Anda ingin memasukkan data?", ("Unggah File (CSV/ZIP)", "Masukkan Tautan"), key="src_choice_key")
    temp_dfs: List[pd.DataFrame] = []
    if src_choice == "Unggah File (CSV/ZIP)":
        uploaded_files = st.file_uploader("Unggah satu / lebih file .CSV, .XLSX atau .ZIP", type=["csv", "xlsx", "zip"], accept_multiple_files=True, key="file_uploader")
        if uploaded_files:
            with st.spinner("Membaca dan memproses file..."):
                for f in uploaded_files:
                    if f.name.lower().endswith('.zip'):
                        with zipfile.ZipFile(f, "r") as z:
                            for name in z.namelist():
                                if name.lower().endswith(".csv") and not name.startswith('__MACOSX'):
                                    temp_dfs.append(clean_dataframe(read_csv_from_bytes(z.read(name))))
                                elif name.lower().endswith(".xlsx") and not name.startswith('__MACOSX'):
                                    temp_dfs.append(clean_dataframe(read_excel_from_bytes(z.read(name))))
                    elif f.name.lower().endswith('.csv'):
                        temp_dfs.append(clean_dataframe(read_csv_from_bytes(f.read())))
                    elif f.name.lower().endswith('.xlsx'):
                        temp_dfs.append(clean_dataframe(read_excel_from_bytes(f.read())))
    else:
        url_text = st.text_area("Tempel satu / lebih tautan (pisahkan dengan baris baru atau koma)", key="url_input")
        if url_text:
            with st.spinner("Mengunduh dan memproses data dari tautan..."):
                url_list = [u.strip() for u in re.split(r"[\n,]+", url_text) if u.strip()]
                for u in url_list:
                    temp_dfs.extend(load_from_url(u))
    if temp_dfs:
        st.session_state.dfs = temp_dfs
        if st.session_state.app_mode in ["checking", "checking_only"]:
            st.session_state.step = 100 # Custom step for data checking
        else:
            st.session_state.step = 2
        st.rerun()
    else:
        st.info("⌛ Unggah file atau masukkan tautan untuk melanjutkan.")
        st.stop()


# ---------- DATA CHECKING ----------
if st.session_state.step == 100:
    st.header("🩺 Data Checking Mode")
    st.success(f"✅ Berhasil mengumpulkan {len(st.session_state.dfs)} file data untuk diperiksa.")

    if st.button("Lakukan Pengecekan Data"):
        with st.spinner("Sedang mengecek kesehatan data..."):
            reports = check_data_health(st.session_state.dfs)
            for report in reports:
                st.subheader(f"📄 Data ke-{report['File Index']} (Tipe: {report['Type']})")
                st.metric(label="Data Health Score", value=f"{report['Health']:.1f}%")

                with st.expander("Detail Pengecekan"):
                    for issue in report['Issues']:
                        st.markdown(issue)
                st.divider()

    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        if st.session_state.app_mode != "checking_only":
            if st.button("Lanjutkan ke Analisis (Mode Normal)", use_container_width=True):
                st.session_state.step = 2
                st.rerun()
    with col_btn2:
        if st.button("Kembali ke Upload Data", use_container_width=True):
            st.session_state.step = 1
            st.session_state.dfs = []
            st.rerun()

# ---------- 2️⃣ PENGATURAN SPREADSHEET ----------

if st.session_state.step == 2:
    st.success(f"✅ Berhasil mengumpulkan {len(st.session_state.dfs)} file data.")
    st.header("2️⃣ Pengaturan Spreadsheet")
    with st.form("sheet_settings_form"):
        sheet_link = st.text_input("Tempel link Google Spreadsheet tujuan:", key="sheet_link_input")
        upload_mode = st.radio("Mode upload:", ("Ganti isi lama (Replace)", "Tambahkan di bawah (Append)"), key="upload_mode_key", horizontal=True)
        confirmed = st.form_submit_button("✅ Konfirmasi & Lanjutkan")
        if confirmed and sheet_link:
            st.session_state.sheet_link = sheet_link
            st.session_state.upload_mode = upload_mode
            st.session_state.step = 3
            st.rerun()
        elif confirmed and not sheet_link:
            st.warning("Harap masukkan link Google Spreadsheet.")
    if not st.session_state.get('sheet_link'):
        st.info("Masukkan link spreadsheet dan klik 'Konfirmasi' untuk melanjutkan.")
        st.stop()
        
# ---------- 3️⃣ AUTENTIKASI GOOGLE SHEETS & PROSES UTAMA ----------
if st.session_state.step == 3:
    st.success(f"✅ Berhasil mengumpulkan {len(st.session_state.dfs)} file data.")
    st.success(f"✅ Link Spreadsheet tujuan: {st.session_state.sheet_link}")
    st.success(f"✅ Mode Unggah: {st.session_state.upload_mode}")
    st.header("3️⃣ Autentikasi & Mulai Proses")
    
    with st.form("json_auth_form"):
        json_opt = st.radio("Pilih sumber Service-Account JSON:", ("Gunakan JSON default di Drive", "Unggah file JSON sendiri"), key="json_opt_key")
        uploaded_json = None
        if json_opt == "Unggah file JSON sendiri":
            uploaded_json = st.file_uploader("Unggah file .json", type="json", key="json_uploader")
        proceed = st.form_submit_button("🚀 Mulai Proses Upload!")
    
    if not proceed:
        st.info("Pilih metode autentikasi dan klik 'Mulai Proses Upload!'")
        st.stop()

    sheet_link = st.session_state.sheet_link
    upload_mode = st.session_state.upload_mode
    dfs = st.session_state.dfs
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
            ronm_dfs, rofm_dfs, rsocmed_dfs, rfollower_dfs, unknown_dfs = [], [], [], [], []
            
            for df in dfs:
                cols = {str(c).lower() for c in df.columns}
                
                # Prioritas 1: Cek untuk ROFM berdasarkan kolom 'attachment'
                if "attachment" in cols:
                    try:
                        clipping_col_name = next((c for c in df.columns if str(c).lower() == 'clipping'), None)
                        if clipping_col_name:
                            clipping_idx = df.columns.get_loc(clipping_col_name)
                            # Data ROFM hanya diambil sampai kolom 'clipping' (inklusi)
                            rofm_df_sliced = df.iloc[:, :clipping_idx + 1] 
                            rofm_dfs.append(rofm_df_sliced)
                        else:
                            st.warning(f"⚠️ File dengan 'attachment' terdeteksi, tetapi kolom 'Clipping' tidak ditemukan. File tidak diunggah.")
                            unknown_dfs.append(df)
                    except Exception as e:
                        st.warning(f"⚠️ Gagal memproses file untuk ROFM: {e}")
                        unknown_dfs.append(df)

                # Prioritas 2: Cek untuk RONM
                elif "tier" in cols:
                    ronm_dfs.append(df)
                
                # Prioritas 3: Cek untuk RSOCMED
                elif {"original_id", "label"}.issubset(cols):
                    start_col = next((c for c in df.columns if str(c).lower() == 'original_id'), None)
                    end_col = next((c for c in df.columns if str(c).lower() == 'label'), None)
                    if start_col and end_col:
                        start_idx, end_idx = df.columns.get_loc(start_col), df.columns.get_loc(end_col)
                        rsocmed_dfs.append(df.iloc[:, start_idx : end_idx + 1])
                
                # Prioritas 4: Cek untuk RFOLLOWER
                elif "social_media" in cols:
                    rfollower_dfs.append(df)
                    
                # Lainnya
                else:
                    unknown_dfs.append(df)

        if not any([ronm_dfs, rofm_dfs, rsocmed_dfs, rfollower_dfs]):
            st.error("❌ Tidak ada data yang cocok dengan skema mana pun. Proses dihentikan.")
            st.stop()

        targets = {
            "RONM": pd.concat(ronm_dfs, ignore_index=True) if ronm_dfs else None,
            "ROFM": pd.concat(rofm_dfs, ignore_index=True) if rofm_dfs else None,
            "RSOCMED": pd.concat(rsocmed_dfs, ignore_index=True) if rsocmed_dfs else None,
            "RFOLLOWER": pd.concat(rfollower_dfs, ignore_index=True) if rfollower_dfs else None,
        }

        creds = service_account.Credentials.from_service_account_info(json_data, scopes=["https://www.googleapis.com/auth/spreadsheets"])
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
                ws = sh.add_worksheet(title=ws_name, rows=1000, cols=len(df.columns) + 5)

            replace = upload_mode.startswith("Ganti")
            
            if ws_name == "RFOLLOWER":
                st.info(f"Mode RFOLLOWER: Menulis ulang data mulai dari baris 2.")
                st.info(f"Membersihkan data lama dari A2:ZZ di sheet '{ws_name}'...")
                ws.batch_clear(['A2:ZZ']) 
                progress_placeholder = st.empty()
                progress_placeholder.info(f"⏳ Mengunggah {len(df)} baris ke {ws_name}...")
                set_with_dataframe(ws, df, row=2, include_column_header=False, resize=False)
                progress_placeholder.empty()
                st.success(f"✅ Selesai! {len(df)} baris berhasil diunggah ke worksheet **{ws_name}**")
                any_upload_success = True
                continue

            if replace:
                if ws_name in ["RONM", "RSOCMED", "ROFM"]:
                    # LOGIKA REVISI UNTUK MENAMPUNG PENAMBAHAN KOLOM
                    num_cols = len(df.columns)
                    last_col_letter = get_column_letter(num_cols - 1)
                    clear_range = f'A:{last_col_letter}'
                else: 
                    clear_range = 'A:ZZ' # Default

                st.info(f"Mode Ganti: Membersihkan kolom {clear_range} di sheet '{ws_name}'...")
                ws.batch_clear([clear_range])
                next_row = 1
                effective_replace_mode = True
            else:
                existing_values = ws.get_all_values()
                next_row = len(existing_values) + 1 if existing_values else 1
                effective_replace_mode = (next_row == 1)
            
            progress_placeholder = st.empty()
            write_dataframe_in_chunks(
                ws, df, start_row=next_row, replace_mode=effective_replace_mode,
                progress_placeholder=progress_placeholder
            )
            st.success(f"✅ Selesai! {len(df)} baris berhasil diunggah ke worksheet **{ws_name}**")
            any_upload_success = True

        st.write("---")
        if any_upload_success:
            st.balloons()
            st.success("🎉 Semua proses unggah telah selesai!")
        if unknown_dfs:
            st.warning(f"⚠️ Ditemukan {len(unknown_dfs)} file yang tidak cocok dengan skema dan tidak diunggah.")
        st.session_state.step = 4

    except Exception:
        st.error("❌ Terjadi kesalahan fatal saat mengakses atau menulis ke Spreadsheet.")
        st.text(traceback.format_exc())
        st.session_state.step = 4

if st.session_state.step == 4:
    st.divider()
    # Peringatan: Variabel SPREADSHEET_ID mungkin tidak ada jika terjadi error di Step 3
    if 'SPREADSHEET_ID' in locals() and 'SPREADSHEET_ID' in st.session_state: 
         st.markdown(f"### [📄 Buka Spreadsheet](https://docs.google.com/spreadsheets/d/{st.session_state.SPREADSHEET_ID}/edit)")
    elif 'SPREADSHEET_ID' in locals():
         # Jika SPREADSHEET_ID tersedia secara lokal (berhasil diambil di Step 3)
         st.markdown(f"### [📄 Buka Spreadsheet](https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit)") 
    
    if st.button("Mulai Lagi (Reset)", use_container_width=True, key="reset_bottom"):
        st.session_state.clear()
        st.rerun()