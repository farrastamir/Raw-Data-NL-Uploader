with st.spinner("Mengklasifikasikan data..."):
            ronm_dfs, rofm_dfs, rsocmed_dfs, rfollower_dfs, unknown_dfs = [], [], [], [], []
            
            for df in dfs:
                cols = {str(c).lower() for c in df.columns}
                
                # ### PERUBAHAN UTAMA: Cek 'attachment' DIPINDAHKAN KE ATAS ###
                # Ini menjadi prioritas pertama. Jika ada 'attachment', pasti masuk ke ROFM.
                if "attachment" in cols:
                    try:
                        clipping_col_name = next((c for c in df.columns if str(c).lower() == 'clipping'), None)
                        if clipping_col_name:
                            clipping_idx = df.columns.get_loc(clipping_col_name)
                            rofm_df_sliced = df.iloc[:, :clipping_idx + 1]
                            rofm_dfs.append(rofm_df_sliced)
                        else:
                            st.warning(f"⚠️ File dengan kolom 'attachment' terdeteksi, tetapi kolom 'Clipping' tidak ditemukan. File ini tidak diunggah.")
                            unknown_dfs.append(df)
                    except Exception as e:
                        st.warning(f"⚠️ Gagal memproses file untuk ROFM: {e}")
                        unknown_dfs.append(df)

                elif "tier" in cols:
                    ronm_dfs.append(df)
                
                elif {"original_id", "label"}.issubset(cols):
                    start_col = next((c for c in df.columns if str(c).lower() == 'original_id'), None)
                    end_col = next((c for c in df.columns if str(c).lower() == 'label'), None)
                    if start_col and end_col:
                        start_idx, end_idx = df.columns.get_loc(start_col), df.columns.get_loc(end_col)
                        rsocmed_dfs.append(df.iloc[:, start_idx : end_idx + 1])
                
                elif "social_media" in cols:
                    rfollower_dfs.append(df)
                    
                else:
                    unknown_dfs.append(df)
