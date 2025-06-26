# =====================  IMPORT  =====================
import streamlit as st
import zipfile, io, re, json, traceback, requests, time, datetime
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account
from typing import List, Any, Dict

# =====================  KONFIGURASI AWAL  =====================
DEFAULT_PROJECTS = {
    "Visa": "2ab74205-a729-49bb-aa22-4204f852d518",
    "Krakatau Steel": "07a72031-6a9a-4291-81ca-b8b297c63db9"
}
ADD_NEW_PROJECT_OPTION = "➕ Tambah Proyek Baru..."

FINAL_SOCMED_COLUMNS = [
    'Date', 'Original Id', 'From Id', 'From Name', 'Url', 'Content',
    'Sentiment', 'Keyword', 'Post Ownership', 'Social Media', 'Likes',
    'Comments', 'Shares', 'Engagement', 'Reach', 'Impression', 'Labels'
]
FINAL_ONM_COLUMNS = [
    'Date', 'Original Id', 'Media', 'Title', 'Content', 'Url',
    'Sentiment', 'Journalist', 'Tier', 'PR Value', 'Labels'
]

# =====================  Inisialisasi Session State =====================
if 'dfs' not in st.session_state: st.session_state.dfs = []
if 'step' not in st.session_state: st.session_state.step = 1
if 'added_projects' not in st.session_state: st.session_state.added_projects = {}
if 'selected_object_names' not in st.session_state: st.session_state.selected_object_names = []
if 'selected_label_names' not in st.session_state: st.session_state.selected_label_names = []
if 'selected_clipping_names' not in st.session_state: st.session_state.selected_clipping_names = []

# =====================  FUNGSI BANTU =====================
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    return df.applymap(lambda x: str(x).lstrip("'") if isinstance(x, str) else x)
def detect_delimiter(sample_text: str) -> str:
    return ";" if sample_text.count(";") > sample_text.count(",") else ","
def truncate_long_texts(df: pd.DataFrame, max_allowed: int = 50_000, trunc_length: int = 20_000) -> pd.DataFrame:
    def _trunc(x):
        return x[:trunc_length] if isinstance(x, str) and len(x) > max_allowed else x
    return df.applymap(_trunc)
def standardize_dates(df: pd.DataFrame) -> pd.DataFrame:
    # Cek semua kemungkinan nama kolom tanggal
    date_cols_to_check = [col for col in ["Date", "date_created", "date_published"] if col in df.columns]
    for col in date_cols_to_check:
        def _convert(val):
            if pd.isna(val): return val
            try:
                dt_obj = pd.to_datetime(val, errors='coerce')
                if pd.isna(dt_obj):
                     s = str(val).strip()
                     date_part, time_part = (s.split(" ", 1) + ["00:00:00"])[:2]
                     time_part = re.sub(r"(\d{1,2})\.(\d{2})(?:\.(\d{2}))?", lambda m: f"{m.group(1)}:{m.group(2)}" + (f":{m.group(3)}" if m.group(3) else ""), time_part)
                     date_part = date_part.replace("-", "/")
                     if time_part.count(":") == 0: time_part += ":00"
                     if time_part.count(":") == 1: time_part += ":00"
                     s_norm = f"{date_part} {time_part}"
                     dt_obj = pd.to_datetime(s_norm, dayfirst=True, errors="coerce")
                return dt_obj.strftime("%d/%m/%Y %H:%M:%S") if pd.notna(dt_obj) else val
            except Exception:
                return val
        df[col] = df[col].apply(_convert)
    return df
def read_csv_from_bytes(b: bytes) -> pd.DataFrame:
    try:
        sample = b[:2048].decode("utf-8", errors="ignore"); delim = detect_delimiter(sample)
        return pd.read_csv(io.BytesIO(b), delimiter=delim)
    except Exception: return pd.read_csv(io.BytesIO(b), delimiter=';')
def load_from_url(url: str) -> List[pd.DataFrame]:
    dfs: List[pd.DataFrame] = []
    try:
        r = requests.get(url.strip()); r.raise_for_status(); content = r.content
        if zipfile.is_zipfile(io.BytesIO(content)):
            with zipfile.ZipFile(io.BytesIO(content), "r") as z:
                for name in z.namelist():
                    if name.lower().endswith(".csv"): dfs.append(clean_dataframe(read_csv_from_bytes(z.read(name))))
        else: dfs.append(clean_dataframe(read_csv_from_bytes(content)))
    except Exception as exc: st.error(f"Gagal mengambil {url} → {exc}")
    return dfs
def write_dataframe_in_chunks(ws, df: pd.DataFrame, start_row: int, replace_mode: bool, progress_placeholder: Any):
    rows_per_batch = 10_000; row_ptr = 0; total_rows = len(df)
    while row_ptr < total_rows:
        chunk = df.iloc[row_ptr : row_ptr + rows_per_batch]
        start_display = row_ptr + 1; end_display = min(row_ptr + len(chunk), total_rows)
        progress_placeholder.info(f"⏳ Mengunggah baris {start_display} - {end_display} dari {total_rows}...")
        try:
            set_with_dataframe(ws, chunk, include_column_header=(row_ptr == 0 and replace_mode), row=start_row + row_ptr, resize=False)
            row_ptr += len(chunk)
        except gspread.exceptions.APIError as e:
            if "500" in str(e) and rows_per_batch > 1:
                rows_per_batch = max(1, rows_per_batch // 2)
                st.warning(f"⚠️ 500 error – mengecilkan batch menjadi {rows_per_batch} baris…")
                time.sleep(2)
            else: raise
    progress_placeholder.empty()

# ===================== FUNGSI API (Format & Orchestration) =====================
@st.cache_data(ttl=600)
def get_api_list(_api_key: str, endpoint: str) -> List[Dict]:
    headers = {"X-API-KEY": _api_key}; url = f"https://external.backend.dashboard.nolimit.id/v1.0{endpoint}"
    try:
        response = requests.get(url, headers=headers, timeout=20); response.raise_for_status()
        return response.json().get("result", [])
    except requests.exceptions.RequestException as e:
        st.error(f"Gagal mengambil daftar dari {endpoint}: {e}"); return []

def pull_socmed_data_from_api(api_key: str, start_date: datetime.date, end_date: datetime.date, object_ids: List[str], label_ids: List[str]) -> List[pd.DataFrame]:
    all_data = []; page = 1; progress_bar = st.progress(0, "Memulai penarikan data Social Media..."); status_text = st.empty()
    while True:
        payload = {"timestamp_start": f"{start_date} 00:00:00", "timestamp_end": f"{end_date} 23:59:59", "object_ids": object_ids, "label_ids": label_ids, "page": page, "size": 100, "sort_by": "desc"}
        headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}; url = "https://external.backend.dashboard.nolimit.id/v1.0/social-media/stream"
        status_text.info(f"📄 Menarik data Socmed halaman {page}...");
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=60)
            if response.status_code != 200: st.error(f"API Error Socmed: {response.status_code} - {response.text}"); break
            data = response.json(); page_data = data.get("result", {}).get("list", [])
            if not page_data: status_text.success("✅ Semua data Socmed berhasil ditarik!"); progress_bar.progress(1.0); break
            all_data.extend(page_data); progress_bar.progress(min(1.0, len(all_data) / ((page + 1) * 100)), f"Terkumpul {len(all_data)} data..."); page += 1; time.sleep(0.5)
        except requests.exceptions.RequestException as e: st.error(f"Gagal menghubungi API Socmed: {e}"); break
    if not all_data: return []
    df = pd.DataFrame(all_data)
    df['Reach'] = 0; df['Impression'] = 0; df['Labels'] = ''
    df['Keyword'] = df['keyword'].apply(lambda x: x.get('displayName', '') if isinstance(x, dict) else '')
    df.rename(columns={
        'timestamp': 'Date', 'originalId': 'Original Id', 'fromId': 'From Id',
        'fromName': 'From Name', 'link': 'Url', 'content': 'Content',
        'sentiment': 'Sentiment', 'postOwnership': 'Post Ownership',
        'socialMedia': 'Social Media', 'likeCount': 'Likes', 'commentCount': 'Comments',
        'shareCount': 'Shares', 'engagement': 'Engagement'
    }, inplace=True)
    for col in FINAL_SOCMED_COLUMNS:
        if col not in df.columns: df[col] = ''
    df = df[FINAL_SOCMED_COLUMNS]
    return [df]

def _pull_single_onm_clipping(api_key: str, start_date: datetime.date, end_date: datetime.date, clipping_id: str, clipping_name: str, status_placeholder) -> pd.DataFrame:
    all_data = []; page = 1
    while True:
        payload = {"timestamp_start": f"{start_date} 00:00:00", "timestamp_end": f"{end_date} 23:59:59", "clipping_id": clipping_id, "limit": 100, "page": page}
        headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}; url = "https://external.backend.dashboard.nolimit.id/v1.0/online-media/article/get-article"
        status_placeholder.info(f"📄 Menarik halaman {page}...")
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=60)
            if response.status_code != 200: status_placeholder.warning(f"Gagal menarik halaman {page}. Lanjut..."); break
            data = response.json(); page_data = data.get("result", {}).get("list", [])
            if not page_data: break 
            all_data.extend(page_data); page += 1; time.sleep(0.5)
        except requests.exceptions.RequestException: status_placeholder.warning(f"Gagal koneksi saat menarik halaman {page}. Lanjut..."); break
    if not all_data: return pd.DataFrame()
    df = pd.DataFrame(all_data)
    df['Tier'] = ''; df['Labels'] = clipping_name
    df['Journalist'] = df['writer'].apply(lambda x: ', '.join(x) if isinstance(x, list) else '')
    df.rename(columns={
        'datePublished': 'Date', 'originalId': 'Original Id', 'sourceName': 'Media',
        'title': 'Title', 'content': 'Content', 'link': 'Url', 'sentiment': 'Sentiment',
        'prValue': 'PR Value'
    }, inplace=True)
    for col in FINAL_ONM_COLUMNS:
        if col not in df.columns: df[col] = ''
    df = df[FINAL_ONM_COLUMNS]
    return df

def pull_onm_data_for_multiple_clippings(api_key: str, start_date: datetime.date, end_date: datetime.date, clipping_info: Dict[str, str]) -> List[pd.DataFrame]:
    all_dfs = []; total_clippings = len(clipping_info)
    progress_bar = st.progress(0, "Memulai penarikan data Online Media...")
    for i, (clipping_name, clipping_id) in enumerate(clipping_info.items()):
        progress_text = f"Memproses clipping '{clipping_name}' ({i+1} dari {total_clippings})..."
        status_placeholder = st.empty(); status_placeholder.info(progress_text)
        single_clipping_df = _pull_single_onm_clipping(api_key, start_date, end_date, clipping_id, clipping_name, status_placeholder)
        if not single_clipping_df.empty:
            all_dfs.append(single_clipping_df)
            status_placeholder.success(f"✅ Selesai: '{clipping_name}' ({len(single_clipping_df)} artikel).")
        else:
            status_placeholder.warning(f"⚠️ Tidak ada data untuk clipping '{clipping_name}'.")
        progress_bar.progress((i + 1) / total_clippings); time.sleep(1) 
    if not all_dfs: st.warning("Tidak ada data artikel yang berhasil ditarik dari semua clipping yang dipilih."); return []
    final_df = pd.concat(all_dfs, ignore_index=True)
    st.success(f"🎉 Semua data Online Media berhasil digabungkan ({len(final_df)} total artikel).")
    return [final_df]

# =====================  UI =====================
st.set_page_config(page_title="Upload Data ➜ Google Sheets", page_icon="📄", layout="wide")
col1, col2 = st.columns([3, 1])
with col1: st.title("Upload File/Link/API ➜ Google Spreadsheet")
with col2:
    if st.button("🔄 Reset Aplikasi", use_container_width=True, key="reset_top", help="Mulai ulang seluruh proses dari awal."):
        st.session_state.clear(); st.rerun()

if st.session_state.step == 1:
    st.header("1️⃣ Pilih sumber data")
    src_choice = st.radio("Bagaimana Anda ingin memasukkan data?", ("Unggah File (CSV/ZIP)", "Masukkan Tautan", "Tarik Data via API"), key="src_choice_key", horizontal=True)
    temp_dfs: List[pd.DataFrame] = []
    if src_choice == "Tarik Data via API":
        try: API_PASSWORD_FROM_SECRETS = st.secrets["API_PASSWORD"]
        except (FileNotFoundError, KeyError): st.error("Konfigurasi 'API_PASSWORD' tidak ditemukan di Secrets Management. Hubungi developer."); st.stop()
        password = st.text_input("Masukkan Password API", type="password", key="api_password")
        if password == API_PASSWORD_FROM_SECRETS:
            st.success("✅ Password benar. Silakan pilih parameter penarikan data.")
            def clear_filter_selections():
                keys_to_clear = ['selected_object_names', 'selected_label_names', 'selected_clipping_names']
                for key in keys_to_clear:
                    if key in st.session_state: st.session_state[key] = []
            all_projects = {**DEFAULT_PROJECTS, **st.session_state.added_projects}
            project_options = list(all_projects.keys()) + [ADD_NEW_PROJECT_OPTION]
            selected_project_name = st.selectbox("Pilih Proyek:", project_options, key="project_selector", on_change=clear_filter_selections)
            if selected_project_name == ADD_NEW_PROJECT_OPTION:
                st.info("Tambahkan proyek baru untuk digunakan dalam sesi ini.")
                with st.form("add_project_form_main"):
                    new_proj_name = st.text_input("Nama Proyek Baru"); new_proj_key = st.text_input("API Key Baru", type="password")
                    if st.form_submit_button("💾 Simpan Proyek Sementara") and new_proj_name and new_proj_key:
                        st.session_state.added_projects[new_proj_name] = new_proj_key
                        st.success(f"Proyek '{new_proj_name}' berhasil ditambahkan. Silakan pilih dari daftar di atas."); time.sleep(2); st.rerun()
            else:
                api_key = all_projects[selected_project_name]
                data_type_choice = st.radio("Pilih Jenis Data yang Akan Ditarik:", ("Social Media", "Online Media"), horizontal=True, key="data_type_selector", on_change=clear_filter_selections)
                with st.form("api_params_form"):
                    st.subheader(f"Parameter Penarikan Data {data_type_choice}")
                    col_tgl1, col_tgl2 = st.columns(2)
                    with col_tgl1: start_date = st.date_input("Tanggal Mulai", datetime.date.today() - datetime.timedelta(days=7))
                    with col_tgl2: end_date = st.date_input("Tanggal Akhir", datetime.date.today())
                    with st.spinner("Mengambil daftar filter..."):
                        if data_type_choice == "Social Media":
                            object_list = get_api_list(api_key, "/social-media/keyword-list"); label_list = get_api_list(api_key, "/social-media/label-list")
                        else:
                            clipping_list = get_api_list(api_key, "/online-media/clipping-list")
                    if data_type_choice == "Social Media":
                        selected_objects, selected_labels = [], []
                        if object_list:
                            object_options = {f"{item['displayName']} ({item.get('streamType', 'N/A')})": item['id'] for item in object_list}
                            select_all_objects = st.checkbox("Pilih Semua Objek", key="select_all_obj")
                            default_obj = list(object_options.keys()) if select_all_objects else st.session_state.selected_object_names
                            st.session_state.selected_object_names = st.multiselect("Pilih Objek Individual (Opsional):", list(object_options.keys()), default=default_obj)
                            selected_objects = [object_options[name] for name in st.session_state.selected_object_names]
                        if label_list:
                            label_options = {item['displayName']: item['id'] for item in label_list}
                            select_all_labels = st.checkbox("Pilih Semua Label/Grup", key="select_all_lbl")
                            default_lbl = list(label_options.keys()) if select_all_labels else st.session_state.selected_label_names
                            st.session_state.selected_label_names = st.multiselect("Pilih Label/Grup (Opsional):", list(label_options.keys()), default=default_lbl)
                            selected_labels = [label_options[name] for name in st.session_state.selected_label_names]
                    else:
                        selected_clipping_info = {}
                        if clipping_list:
                            clipping_options = {item['displayName']: item['id'] for item in clipping_list}
                            select_all_clippings = st.checkbox("Pilih Semua Clipping", key="select_all_clipping")
                            default_clipping = list(clipping_options.keys()) if select_all_clippings else st.session_state.selected_clipping_names
                            st.session_state.selected_clipping_names = st.multiselect("Pilih Clipping (bisa lebih dari satu):", list(clipping_options.keys()), default=default_clipping)
                            selected_clipping_info = {name: clipping_options[name] for name in st.session_state.selected_clipping_names}
                    submitted = st.form_submit_button("🚀 Tarik Data Sekarang!")
                    if submitted:
                        if start_date > end_date: st.error("Tanggal mulai tidak boleh lebih dari tanggal akhir.")
                        else:
                            with st.container():
                                if data_type_choice == "Social Media":
                                    temp_dfs = pull_socmed_data_from_api(api_key, start_date, end_date, selected_objects, selected_labels)
                                else:
                                    if not selected_clipping_info: st.warning("Harap pilih minimal satu clipping."); st.stop()
                                    temp_dfs = pull_onm_data_for_multiple_clippings(api_key, start_date, end_date, selected_clipping_info)
                                if temp_dfs:
                                    st.session_state.dfs = temp_dfs; st.session_state.step = 2; st.rerun()
                                else: st.warning("Tidak ada data yang berhasil ditarik dari API.")
        elif password: st.error("❌ Password salah. Silakan coba lagi.")
    else: 
        if src_choice == "Unggah File (CSV/ZIP)":
            uploaded_files = st.file_uploader("Unggah satu / lebih file .CSV atau .ZIP", type=["csv", "zip"], accept_multiple_files=True, key="file_uploader")
            if uploaded_files:
                with st.spinner("Membaca dan memproses file..."):
                    for f in uploaded_files:
                        if f.name.lower().endswith('.zip'):
                            with zipfile.ZipFile(f, "r") as z:
                                for name in z.namelist():
                                    if name.lower().endswith(".csv") and not name.startswith('__MACOSX'): temp_dfs.append(clean_dataframe(read_csv_from_bytes(z.read(name))))
                        elif f.name.lower().endswith('.csv'): temp_dfs.append(clean_dataframe(read_csv_from_bytes(f.read())))
        else:
            url_text = st.text_area("Tempel satu / lebih tautan (pisahkan dengan baris baru atau koma)", key="url_input")
            if url_text:
                with st.spinner("Mengunduh dan memproses data dari tautan..."):
                    url_list = [u.strip() for u in re.split(r"[\n,]+", url_text) if u.strip()]
                    for u in url_list: temp_dfs.extend(load_from_url(u))
        if temp_dfs: st.session_state.dfs = temp_dfs; st.session_state.step = 2; st.rerun()

if st.session_state.step == 2:
    st.success(f"✅ Berhasil mengumpulkan {len(st.session_state.dfs)} file data.")
    st.header("2️⃣ Pengaturan Spreadsheet")
    with st.form("sheet_settings_form"):
        sheet_link = st.text_input("Tempel link Google Spreadsheet tujuan:", key="sheet_link_input")
        upload_mode = st.radio("Mode upload:", ("Ganti isi lama (Replace)", "Tambahkan di bawah (Append)"), key="upload_mode_key", horizontal=True)
        confirmed = st.form_submit_button("✅ Konfirmasi & Lanjutkan")
        if confirmed and sheet_link:
            st.session_state.sheet_link = sheet_link; st.session_state.upload_mode = upload_mode; st.session_state.step = 3; st.rerun()
        elif confirmed and not sheet_link:
            st.warning("Harap masukkan link Google Spreadsheet.")
    if not st.session_state.get('sheet_link'): st.info("Masukkan link spreadsheet dan klik 'Konfirmasi' untuk melanjutkan."); st.stop()
if st.session_state.step == 3:
    st.success(f"✅ Berhasil mengumpulkan {len(st.session_state.dfs)} file data.")
    st.success(f"✅ Link Spreadsheet tujuan: {st.session_state.sheet_link}")
    st.success(f"✅ Mode Unggah: {st.session_state.upload_mode}")
    st.header("3️⃣ Autentikasi & Mulai Proses")
    with st.form("json_auth_form"):
        json_opt = st.radio("Pilih sumber Service-Account JSON:", ("Gunakan JSON default di Drive", "Unggah file JSON sendiri"), key="json_opt_key")
        uploaded_json = None
        if json_opt == "Unggah file JSON sendiri": uploaded_json = st.file_uploader("Unggah file .json", type="json", key="json_uploader")
        proceed = st.form_submit_button("🚀 Mulai Proses Upload!")
    if not proceed: st.info("Pilih metode autentikasi dan klik 'Mulai Proses Upload!'"); st.stop()
    sheet_link = st.session_state.sheet_link; upload_mode = st.session_state.upload_mode; dfs = st.session_state.dfs
    m = re.search(r"/d/([\w-]+)", sheet_link)
    if not m: st.error("Link Spreadsheet tidak valid. Pastikan link yang Anda masukkan benar."); st.stop()
    SPREADSHEET_ID = m.group(1)
    try:
        st.info("Mempersiapkan kredensial...")
        if json_opt == "Gunakan JSON default di Drive":
            default_link = "https://drive.google.com/file/d/1VRpKOpI3R918d5voY70wi9CsDRBwDuRl/view?usp=drive_link"
            fid = re.search(r"/d/([\w-]+)", default_link).group(1)
            r = requests.get(f"https://drive.google.com/uc?export=download&id={fid}", timeout=30); r.raise_for_status()
            json_data = json.loads(r.content.decode()); st.success("✅ JSON default berhasil diambil.")
        else:
            if uploaded_json is None: st.error("Silakan unggah file JSON terlebih dahulu."); st.stop()
            json_data = json.loads(uploaded_json.read().decode()); st.success("✅ File JSON berhasil diproses.")
        
        with st.spinner("Mengklasifikasikan data..."):
            ronm_dfs, rsocmed_dfs, rfollower_dfs, unknown_dfs = [], [], [], []
            for df in dfs:
                # --- PERUBAHAN: Logika Klasifikasi yang Diperbaiki ---
                cols = {str(c).lower().replace(' ', ''): c for c in df.columns}
                
                # Prioritas 1: RFOLLOWER (jika hanya ada 'social_media' dan sedikit kolom lain, seperti dari file CSV sederhana)
                # Ini menggunakan logika dari kode lama Anda sebagai prioritas.
                if 'social_media' in cols and 'originalid' not in cols and 'prvalue' not in cols:
                    rfollower_dfs.append(df)
                # Prioritas 2: RONM
                elif 'prvalue' in cols or 'sourcename' in cols or 'media' in cols:
                    ronm_dfs.append(df)
                # Prioritas 3: RSOCMED
                elif 'originalid' in cols or 'fromname' in cols or 'fromid' in cols:
                    rsocmed_dfs.append(df)
                # Fallback jika masih ada 'social_media'
                elif 'social_media' in cols:
                    rfollower_dfs.append(df)
                else:
                    unknown_dfs.append(df)

        if not ronm_dfs and not rsocmed_dfs and not rfollower_dfs: st.error("❌ Tidak ada data yang cocok dengan skema. Proses dihentikan."); st.stop()
        targets = {"RONM": pd.concat(ronm_dfs, ignore_index=True) if ronm_dfs else None, "RSOCMED": pd.concat(rsocmed_dfs, ignore_index=True) if rsocmed_dfs else None, "RFOLLOWER": pd.concat(rfollower_dfs, ignore_index=True) if rfollower_dfs else None}
        creds = service_account.Credentials.from_service_account_info(json_data, scopes=["https://www.googleapis.com/auth/spreadsheets"]); gc = gspread.authorize(creds); sh = gc.open_by_key(SPREADSHEET_ID)
        st.write("---"); st.info("🚀 Memulai proses unggah..."); any_upload_success = False
        
        for ws_name, df in targets.items():
            if df is None or df.empty: continue
            st.subheader(f"Mengunggah ke sheet: `{ws_name}`"); df = truncate_long_texts(standardize_dates(df))
            try: ws = sh.worksheet(ws_name)
            except gspread.exceptions.WorksheetNotFound: st.info(f"Worksheet '{ws_name}' tidak ditemukan, membuat baru..."); ws = sh.add_worksheet(title=ws_name, rows="1000", cols="50")
            
            # --- PERUBAHAN: Logika upload RFOLLOWER yang diperbaiki dan dipisahkan ---
            if ws_name == "RFOLLOWER":
                st.info(f"Mode RFOLLOWER: Menulis ulang data (termasuk header) mulai dari baris 2.")
                st.info(f"Membersihkan data lama dari A2:ZZ di sheet '{ws_name}'...")
                ws.batch_clear(['A2:ZZ']) 
                progress_placeholder = st.empty()
                progress_placeholder.info(f"⏳ Mengunggah {len(df)} baris ke {ws_name}...")
                # Tulis data baru mulai dari baris 2, termasuk header dari DataFrame.
                set_with_dataframe(ws, df, row=2, include_column_header=True, resize=False)
                progress_placeholder.empty(); st.success(f"✅ Selesai! {len(df)} baris berhasil diunggah ke worksheet **{ws_name}**"); any_upload_success = True
                continue # Lanjut ke target berikutnya
            
            # Logika untuk RONM dan RSOCMED
            replace = upload_mode.startswith("Ganti")
            if replace:
                if ws_name == "RONM": clear_range = 'A:AG'
                elif ws_name == "RSOCMED": clear_range = 'A:AZ'
                else: clear_range = 'A:ZZ'
                st.info(f"Mode Ganti: Membersihkan kolom {clear_range} di sheet '{ws_name}'..."); ws.batch_clear([clear_range])
                next_row = 1; effective_replace_mode = True
            else: # Mode Append
                existing_values = ws.get_all_values(); next_row = len(existing_values) + 1 if existing_values else 1; effective_replace_mode = False
            
            progress_placeholder = st.empty()
            write_dataframe_in_chunks(ws, df, start_row=next_row, replace_mode=effective_replace_mode, progress_placeholder=progress_placeholder)
            st.success(f"✅ Selesai! {len(df)} baris berhasil diunggah ke worksheet **{ws_name}**"); any_upload_success = True

        st.write("---")
        if any_upload_success: st.balloons(); st.success("🎉 Semua proses unggah telah selesai!")
        if unknown_dfs: st.warning(f"⚠️ Ditemukan {len(unknown_dfs)} file yang tidak cocok dengan skema dan tidak diunggah.")
        st.session_state.step = 4
    except Exception: st.error("❌ Terjadi kesalahan fatal saat mengakses atau menulis ke Spreadsheet."); st.text(traceback.format_exc()); st.session_state.step = 4
if st.session_state.step == 4:
    st.divider()
    if st.session_state.get('sheet_link'):
        m = re.search(r"/d/([\w-]+)", st.session_state.sheet_link)
        if m: SPREADSHEET_ID = m.group(1); st.markdown(f"### [📄 Buka Spreadsheet](https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit)")
    if st.button("Mulai Lagi (Reset)", use_container_width=True, key="reset_bottom"):
        st.session_state.clear(); st.rerun()
