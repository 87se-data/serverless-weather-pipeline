import time
import sys
import os
import urllib.request
import requests
import datetime
import pytz
import numpy as np
from pprint import pprint
import urllib3
import tarfile
import random
import string
import gc
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning)
import grib2_deocde
from google.cloud import datastore
from google.cloud import storage
import json
import io
import zstandard as zstd
import zarr
import gcsfs
import fsspec

from numba import njit

from metpy.units import units
# 💖 比湿（Specific Humidity）を計算するための関数を追加！
from metpy.calc import dewpoint_from_relative_humidity, equivalent_potential_temperature, specific_humidity_from_dewpoint

URL = 'https://database3.rish.kyoto-u.ac.jp/arch/jmadata/data/gpv/original'

GCP_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT', os.environ.get('GCP_PROJECT', 'my-project'))
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME')

datastore_client = datastore.Client()
storage_client = storage.Client()
key_element_name = "store-gcs-msm-pall-db"


@njit
def moist_lapse_rate(p, t):
    # 湿潤断熱減率の計算 (dT/dp)
    Rd = 287.04
    Rv = 461.5
    Cp = 1005.0
    epsilon = Rd / Rv
    Lv = 2.501e6
    
    t_c = t - 273.15
    es = 6.112 * np.exp((17.67 * t_c) / (t_c + 243.5))
    rs = epsilon * es / (p - es)
    
    numerator = Rd * t + Lv * rs
    denominator = p * (Cp + (Lv**2 * rs * epsilon) / (Rd * t**2))
    return numerator / denominator

@njit
def get_parcel_t500(p850, t850_c, td850_c):
    t850_k = t850_c + 273.15
    
    # 850hPaの水蒸気圧 (e)
    e = 6.112 * np.exp((17.67 * td850_c) / (td850_c + 243.5))
    
    # 持ち上げ凝結高度 (LCL) の温度 (Boltonの近似式)
    tlcl_k = 55.0 + 2840.0 / (3.5 * np.log(t850_k) - np.log(e) - 4.805)
    
    # LCL の気圧
    plcl = p850 * (tlcl_k / t850_k)**3.5
    
    if plcl <= 500.0:
        return t850_k * (500.0 / p850)**(287.04/1005.0) - 273.15
        
    # LCLから500hPaまでは湿潤断熱変化
    dp = -5.0
    p = plcl
    t = tlcl_k
    while p > 500.0:
        step = max(dp, 500.0 - p)
        dt_dp = moist_lapse_rate(p, t)
        t += dt_dp * step
        p += step
        
    return t - 273.15

@njit
def calc_ssi_grid(t_850, td_850, t_500):
    ny, nx = t_850.shape
    ssi = np.full((ny, nx), np.nan)
    for y in range(ny):
        for x in range(nx):
            t = t_850[y, x]
            td = td_850[y, x]
            t500_env = t_500[y, x]
            
            if np.isnan(t) or np.isnan(td) or np.isnan(t500_env):
                continue
                
            parcel_t500 = get_parcel_t500(850.0, t, td)
            ssi[y, x] = t500_env - parcel_t500
            
    return ssi

@njit
def get_parcel_profile(p_env, t_env, p0, t0_c, td0_c):
    # p_env: 1D array of environment pressure levels
    # t_env: 1D array of environment temperatures
    pass # To be replaced later with actual logic

@njit
def calc_advanced_thermo_indices_grid(P_1d, T_3d, Td_3d):
    nz, ny, nx = T_3d.shape
    lcl_grid = np.full((ny, nx), np.nan)
    lfc_grid = np.full((ny, nx), np.nan)
    el_grid = np.full((ny, nx), np.nan)
    cape_grid = np.full((ny, nx), np.nan)
    cin_grid = np.full((ny, nx), np.nan)
    gdi_grid = np.full((ny, nx), np.nan)

    for y in range(ny):
        for x in range(nx):
            t0 = T_3d[0, y, x]
            td0 = Td_3d[0, y, x]
            p0 = P_1d[0] # 1000hPa
            if np.isnan(t0) or np.isnan(td0):
                continue
                
            t0_k = t0 + 273.15
            e0 = 6.112 * np.exp((17.67 * td0) / (td0 + 243.5))
            
            # LCL calculation (Bolton)
            tlcl_k = 55.0 + 2840.0 / (3.5 * np.log(t0_k) - np.log(e0) - 4.805)
            plcl = p0 * (tlcl_k / t0_k)**3.5
            lcl_grid[y, x] = plcl
            
            # Simplified Parcel ascent & CAPE/CIN/LFC/EL
            cape = 0.0
            cin = 0.0
            lfc = np.nan
            el = np.nan
            
            dp = -10.0
            p = p0
            t_p = t0_k
            
            # Find closest env temp for interpolation
            Rd = 287.04
            
            # Loop parcel up
            found_lfc = False
            while p > 300.0:
                step = max(dp, 300.0 - p)
                if p > plcl:
                    # Dry adiabatic
                    dt_dp = 287.04 / 1005.0 * t_p / p
                else:
                    # Moist adiabatic
                    dt_dp = moist_lapse_rate(p, t_p)
                
                t_p += dt_dp * step
                p += step
                
                # Interp env temp
                t_env_k = np.nan
                for i in range(nz-1):
                    if P_1d[i] >= p >= P_1d[i+1]:
                        w = (P_1d[i] - p) / (P_1d[i] - P_1d[i+1])
                        t_env_k = T_3d[i, y, x] + w*(T_3d[i+1, y, x] - T_3d[i, y, x]) + 273.15
                        break
                        
                if np.isnan(t_env_k):
                    break
                    
                # Buoyancy
                b = Rd * (t_p - t_env_k) / p * (-step)
                if t_p > t_env_k:
                    cape += b
                    if not found_lfc:
                        lfc = p
                        found_lfc = True
                    el = p
                else:
                    if not found_lfc:
                        cin += b
            
            lfc_grid[y, x] = lfc
            el_grid[y, x] = el
            cape_grid[y, x] = max(0.0, cape)
            cin_grid[y, x] = min(0.0, cin)
            
            # Simplified GDI (Galvez-Davison Index)
            t500 = T_3d[9, y, x]
            t700 = T_3d[7, y, x]
            t850 = T_3d[5, y, x]
            gdi_grid[y, x] = (t850 - t500) + (t850 - t700) # Placeholder simplified GDI
            
    return lcl_grid, lfc_grid, el_grid, cape_grid, cin_grid, gdi_grid

def main():
    print("🚀 [DEBUG] 処理スタートしたよ！")
    
    # --- 新規: 外部設定ファイルの読み込み ---
    export_config = {}
    gcs_config_path = "msm-gpv-grid-store/config/store-gcs-msm-pall_config.json"
    local_config_path = "store-gcs-msm-pall_config.json"
    
    try:
        # ① GCSから取得・パース
        bucket_name = gcs_config_path.split('/')[0]
        blob_path = '/'.join(gcs_config_path.split('/')[1:])
        if storage_client:
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            if blob.exists():
                config_str = blob.download_as_string()
                export_config = json.loads(config_str)
                print("✅ GCSから設定ファイルを読み込みました。")
            else:
                raise FileNotFoundError("GCSに設定ファイルが存在しません。")
        else:
            raise Exception("storage_client が初期化されていません。")
    except Exception as e:
        print(f"⚠️ GCSからの設定ファイル読み込みに失敗しました: {e}")
        # ② 失敗した場合、ローカルから取得
        try:
            with open(local_config_path, 'r') as f:
                export_config = json.load(f)
            print("✅ ローカルから設定ファイルを読み込みました。")
        except Exception as local_e:
            print(f"⚠️ ローカルの設定ファイル読み込みにも失敗しました: {local_e}")

    # ③ 取得した JSON から target_hour_ranges と export_elements を抽出
    target_hour_ranges = export_config.get("target_hour_ranges", ["00-15", "18-33", "36-39"])
    export_elements = export_config.get("export_elements", {})

    ELEMENT_KEYS = []
    for surface, elements in export_elements.items():
        for el in elements:
            ELEMENT_KEYS.append(f"{surface}:{el}")
    # ----------------------------------------

    dtobj_now = datetime.datetime.now(datetime.timezone.utc)
    base_hour = (dtobj_now.hour // 3) * 3
    target_dtobj = dtobj_now.replace(hour=base_hour, minute=0, second=0, microsecond=0)
    
    search_flag = True

    while search_flag:
        target_hour_range = target_hour_ranges
        goal_count = len(target_hour_range)
        for hour_range in target_hour_range:
            target_datetime = target_dtobj.strftime('%Y%m%d%H0000')
            target_file = f"Z__C_RJTD_{target_datetime}_MSM_GPV_Rjp_L-pall_FH{hour_range}_grib2.bin"
            req_url = f"{URL}/{target_dtobj.year:04d}/{target_dtobj.month:02d}/{target_dtobj.day:02d}/{target_file}"
            r = requests.get(req_url, verify=False)
            if r.status_code == 200:
                goal_count -= 1 

        if goal_count == 0:
            search_flag = False 
        else:
            target_dtobj -= datetime.timedelta(hours=3)
            
    datastore_key = datastore_client.key(key_element_name)
    query = datastore_client.query(kind=key_element_name)
    query.order = ["datetime"]
    renewal_flag = False
    
    fetch_list = list(query.fetch())
    
    if len(fetch_list) != 0:
        datastore_dt_obj = fetch_list[-1]["datetime"]
        if target_dtobj > datastore_dt_obj:
            renewal_flag = True
    else:
        renewal_flag = True

    if renewal_flag:
        dat_dir = 'dat'
        if not os.path.exists(dat_dir):
            os.makedirs(dat_dir)

        target_datetime = target_dtobj.strftime('%Y%m%d%H0000')

        # GCSアップロード用のデータ蓄積変数
        gcs_npz_data = {}
        dimension_map_dict = {}
        initial_utc_str = target_dtobj.strftime('%Y%m%d%H0000Z')

        bucket = None
        if GCS_BUCKET_NAME and storage_client:
            bucket = storage_client.bucket(GCS_BUCKET_NAME)

        for hour_range in target_hour_ranges:
            print(f"--- {hour_range} 処理スタート！ ---")
            
            tmp_bin_file = f"{dat_dir}/grib2.bin"

            target_file = f"Z__C_RJTD_{target_datetime}_MSM_GPV_Rjp_L-pall_FH{hour_range}_grib2.bin"
            req_url = f"{URL}/{target_dtobj.year:04d}/{target_dtobj.month:02d}/{target_dtobj.day:02d}/{target_file}"
            r = requests.get(req_url, verify=False)
            
            if r.status_code == 200:
                with open(tmp_bin_file, "wb") as f:
                    f.write(r.content)
                
                grib2_data = grib2.parse_grib2(tmp_bin_file)
                print(f'{hour_range} grib2データ読み込み完了')
                
                if grib2_data['sec1']['status'] == 0 and grib2_data['sec1']['type'] == 1:
                    year, month, day = grib2_data['sec1']['year'], grib2_data['sec1']['month'], grib2_data['sec1']['day']
                    hour, minute, sec = grib2_data['sec1']['hour'], grib2_data['sec1']['min'], grib2_data['sec1']['sec']
                    
                    utc_time = f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{sec:02d}+00:00"
                    utc_dtobj = datetime.datetime.fromisoformat(utc_time)
                    jst_dtobj = utc_dtobj.astimezone(pytz.timezone('Asia/Tokyo'))
                    jst_time = jst_dtobj.strftime("%Y-%m-%dT%H:%M:00+09:00")

                    buffer = {}

                    for cnt in range(0, grib2_data['count']):
                        data = grib2.decode_compr_data(grib2_data, cnt)
                        np.round(data, 3, out=data)
                        
                        if grib2_data['sec1']['type'] in (1, 2):
                            add_sec = 0
                            t_mpl = grib2_data['sec4'][cnt]['template']
                            if t_mpl == 0: add_sec = grib2_data['sec4'][cnt]['forecast_time'] * 3600
                            elif t_mpl == 8: add_sec = (grib2_data['sec4'][cnt]['forecast_time'] + grib2_data['sec4'][cnt]['statistics_time']) * 3600
                            elif t_mpl in (50009, 50008): add_sec = (grib2_data['sec4'][cnt]['forecast_time'] + grib2_data['sec4'][cnt]['statistics_time']) * 60
                            
                            foc_jst_dtobj = jst_dtobj + datetime.timedelta(seconds=add_sec)
                            foc_jst_time = foc_jst_dtobj.strftime("%Y-%m-%dT%H:%M:00+09:00")

                            surface_type = f"{grib2_data['sec4'][cnt]['hPa']}hPa"
                            element_type = f"{grib2_data['sec4'][cnt]['parameter_category']}_{grib2_data['sec4'][cnt]['parameter_number']}"
                            
                            if element_type == '3_0': continue

                            sec3 = grib2_data['sec3'][cnt]

                            # GCS保存用（1000hPa〜600hPaのみ）
                            foc_utc_dtobj = foc_jst_dtobj.astimezone(datetime.timezone.utc)
                            foc_utc_str = foc_utc_dtobj.strftime('%Y%m%d%H0000Z')
                            
                            if surface_type.endswith('hPa'):
                                hpa_val = int(surface_type.replace('hPa', ''))
                                
                                # 💡 高度の縛りを撤廃してすべて保存する！
                                if foc_utc_str not in gcs_npz_data:
                                    gcs_npz_data[foc_utc_str] = {}
                                if surface_type not in gcs_npz_data[foc_utc_str]:
                                    gcs_npz_data[foc_utc_str][surface_type] = {}
                                gcs_npz_data[foc_utc_str][surface_type][element_type] = np.copy(data)

                            # 💖 U風(2_2)とV風(2_3)もフラックス計算のためにバッファに貯めるように変更！
                            if surface_type.endswith('hPa') and element_type in ('0_0', '1_1', '2_2', '2_3'):
                                if foc_jst_time not in buffer:
                                    buffer[foc_jst_time] = {'sec3': sec3, 'jst_time': jst_time, 'data': {}}
                                if surface_type not in buffer[foc_jst_time]['data']:
                                    buffer[foc_jst_time]['data'][surface_type] = {}
                                buffer[foc_jst_time]['data'][surface_type][element_type] = np.copy(data)
                                
                    # --- GRIB2の読み込みループ終了 ---

                    print(f"[{hour_range}] 不安定度＆水蒸気フラックスの事前計算スタート！🔥")
                    target_hPa_list = [1000, 975, 950, 925, 900, 850, 800, 700, 600, 500, 400, 300]
                    P_1d = np.array(target_hPa_list)

                    for foc_jst_time, b_info in buffer.items():
                        sec3 = b_info['sec3']
                        jst_time = b_info['jst_time']
                        b_data = b_info['data']
                        
                        valid = True
                        for p in target_hPa_list:
                            s_key = f"{p}hPa"
                            if s_key not in b_data or '0_0' not in b_data[s_key] or '1_1' not in b_data[s_key]:
                                valid = False; break
                        
                        # 850hPaの風データが存在するかのチェックを追加
                        if '850hPa' not in b_data or '2_2' not in b_data['850hPa'] or '2_3' not in b_data['850hPa']:
                            valid = False
                        
                        if not valid: continue
                        
                        T_3d = np.stack([b_data[f"{p}hPa"]['0_0'] - 273.15 for p in target_hPa_list])
                        rh_3d = np.stack([b_data[f"{p}hPa"]['1_1'] / 100.0 for p in target_hPa_list])
                        P_3d = P_1d.reshape(-1, 1, 1) * np.ones_like(T_3d)
                        
                        ny, nx = sec3['ny'], sec3['nx']

                        with np.errstate(invalid='ignore', divide='ignore'):
                            T_unit = T_3d * units.degC
                            rh_unit = rh_3d * units.dimensionless
                            P_unit = P_3d * units.hPa
                            
                            Td_3d = dewpoint_from_relative_humidity(T_unit, rh_unit).magnitude

                        # KI と TT
                        idx_850, idx_700, idx_500 = target_hPa_list.index(850), target_hPa_list.index(700), target_hPa_list.index(500)
                        ki_grid = (T_3d[idx_850] - T_3d[idx_500]) + Td_3d[idx_850] - (T_3d[idx_700] - Td_3d[idx_700])
                        tt_grid = (T_3d[idx_850] - T_3d[idx_500]) + (Td_3d[idx_850] - T_3d[idx_500])
                        
                        # 850hPa 相当温位 (Theta-e) と 水蒸気フラックス (Water Vapor Flux)
                        with np.errstate(invalid='ignore', divide='ignore'):
                            p_850_unit = 850 * units.hPa
                            t_850_unit = T_3d[idx_850] * units.degC
                            td_850_unit = Td_3d[idx_850] * units.degC
                            theta_e_grid = equivalent_potential_temperature(p_850_unit, t_850_unit, td_850_unit).magnitude
                            
                            # 💖【新規】水蒸気フラックス計算
                            u_850 = b_data['850hPa']['2_2']
                            v_850 = b_data['850hPa']['2_3']
                            wind_speed_850 = np.sqrt(u_850**2 + v_850**2)
                            
                            # 比湿(q)を計算してg/kgに変換
                            q_850 = specific_humidity_from_dewpoint(p_850_unit, td_850_unit).to('g/kg').magnitude
                            
                            # フラックス = 比湿(g/kg) × 風速(m/s)
                            wv_flux_grid = q_850 * wind_speed_850
                        
                        # NumbaでSSIを超高速計算！
                        ssi_grid = calc_ssi_grid(T_3d[idx_850], Td_3d[idx_850], T_3d[idx_500])

                        # Numbaを用いた6つの熱力学指標の一括計算
                        print(f"  👉 [Numba] 6つの熱力学指標(LCL, LFC, EL, CAPE, CIN, GDI)の計算を開始します...")
                        lcl_grid, lfc_grid, el_grid, cape_grid, cin_grid, gdi_grid = calc_advanced_thermo_indices_grid(P_1d, T_3d, Td_3d)

                        def add_index_to_gcs_buffer(index_name, grid_2d, surface_name):
                            grid_2d = np.where(np.isinf(grid_2d), np.nan, grid_2d)
                            
                            # GCS保存用データの追加
                            foc_jst_dtobj = datetime.datetime.fromisoformat(foc_jst_time)
                            foc_utc_dtobj = foc_jst_dtobj.astimezone(datetime.timezone.utc)
                            foc_utc_str = foc_utc_dtobj.strftime('%Y%m%d%H0000Z')
                            
                            if foc_utc_str not in gcs_npz_data:
                                gcs_npz_data[foc_utc_str] = {}
                            if surface_name not in gcs_npz_data[foc_utc_str]:
                                gcs_npz_data[foc_utc_str][surface_name] = {}
                            gcs_npz_data[foc_utc_str][surface_name][index_name] = np.copy(grid_2d)
                            
                        add_index_to_gcs_buffer('ssi', ssi_grid, 'pall')
                        add_index_to_gcs_buffer('ki', ki_grid, 'pall')
                        add_index_to_gcs_buffer('tt', tt_grid, 'pall')
                        add_index_to_gcs_buffer('theta_e', theta_e_grid, '850hPa')
                        add_index_to_gcs_buffer('water_vapor_flux', wv_flux_grid, '850hPa') # 💖 出力追加

                        add_index_to_gcs_buffer('lcl', lcl_grid, 'pall')
                        add_index_to_gcs_buffer('lfc', lfc_grid, 'pall')
                        add_index_to_gcs_buffer('el', el_grid, 'pall')
                        add_index_to_gcs_buffer('cape', cape_grid, 'pall')
                        add_index_to_gcs_buffer('cin', cin_grid, 'pall')
                        add_index_to_gcs_buffer('gdi', gdi_grid, 'pall')

                        print(f"  👉 {foc_jst_time} の計算完了！✨")


            if 'grib2_data' in locals(): del grib2_data
            if 'data' in locals(): del data
            if 'buffer' in locals(): del buffer
            gc.collect()

            for f_path in [tmp_bin_file]:
                if os.path.exists(f_path): os.remove(f_path)

            # 💡 ここでZstdとZarrのアップロードを都度実行！
            if bucket:
                print(f"🚀 [{hour_range}] GCSへの Zstd(npz) / Zarr / npy.zst アップロード開始！")
                for foc_utc_str, surfaces in gcs_npz_data.items():
                    # --- 新規: dimension_map_dict への登録 ---
                    if foc_utc_str not in dimension_map_dict:
                        dimension_map_dict[foc_utc_str] = {}
                        
                        for surface_name, elements in export_elements.items():
                            bbox = {
                                "lat1": float(sec3['lat1']), "lon1": float(sec3['lon1']),
                                "lat2": float(sec3['lat2']), "lon2": float(sec3['lon2'])
                            }
                            grid = {
                                "nlat": float(sec3['nlat']), "nlon": float(sec3['nlon']),
                                "ny": int(sec3['ny']), "nx": int(sec3['nx'])
                            }

                            element_index = {f"{surface_name}:{el}": idx for idx, el in enumerate(elements)}
                            dimension_map_dict[foc_utc_str][surface_name] = {
                                "bbox": bbox,
                                "grid": grid,
                                "element_index": element_index
                            }
                    # ----------------------------------------
                    
                    # --- 新規: 3D配列（npy.zst）の作成とアップロード ---
                    for surface_name, elements in export_elements.items():
                        grid_info = dimension_map_dict[foc_utc_str].get(surface_name, {}).get("grid", {})
                        ny = grid_info.get("ny", 0)
                        nx = grid_info.get("nx", 0)
                        
                        if ny > 0 and nx > 0:
                            layers = []
                            for el in elements:
                                if surface_name in surfaces and el in surfaces[surface_name]:
                                    layers.append(surfaces[surface_name][el])
                                else:
                                    layers.append(np.full((ny, nx), np.nan, dtype=np.float32))
                                    
                            if layers:
                                stacked_data = np.stack(layers).astype(np.float32)
                                
                                buf_npy = io.BytesIO()
                                np.save(buf_npy, stacked_data)
                                uncompressed_npy = buf_npy.getvalue()
                                
                                cctx_npy = zstd.ZstdCompressor(level=3)
                                compressed_npy = cctx_npy.compress(uncompressed_npy)
                                
                                npy_blob_path = f"pall/{initial_utc_str}/{foc_utc_str}/{surface_name}.npy.zst"
                                npy_blob = bucket.blob(npy_blob_path)
                                npy_blob.upload_from_string(compressed_npy, content_type='application/octet-stream')
                    # ----------------------------------------

                    for surface, data_dict in surfaces.items():
                        base_gcs_path = f"{GCS_BUCKET_NAME}/pall/{initial_utc_str}/{foc_utc_str}"
                        
                        # 2. Zarr 形式での保存 (.zarr)
                        # gs:// は付けず、GCSMapを介して確実に同期保存する
                        zarr_path = f"{base_gcs_path}/{surface}.zarr"
                        
                        # 💡 修正ポイント: surfと同じように、ここでプロジェクトIDを指定してfsを毎回初期化！
                        fs_zarr = gcsfs.GCSFileSystem(project=GCP_PROJECT)
                        store = gcsfs.GCSMap(zarr_path, gcs=fs_zarr, check=False)
                        
                        # store を指定して group を作成
                        root_group = zarr.group(store=store, overwrite=True)
                        
                        for element_name, arr in data_dict.items():
                            # shape, dtype, chunks を指定して確実に保存
                            root_group.create_array(
                                element_name, 
                                data=arr, 
                                chunks=(300, 300), 
                                overwrite=True
                            )
                print(f"✅ [{hour_range}] GCSへの Zstd / Zarr アップロード完了！")
            
            # 💡 アップロードが終わったら実データだけメモリから消す！
            gcs_npz_data.clear()
            gc.collect()
            print(f"🧹 [{hour_range}] メモリ解放完了！")

        # --- ループ終了後 ---
        if bucket:
            print("🚀 dimension_map.json のアップロード開始！")
            dimension_map_path = f"{dat_dir}/dimension_map.json"
            try:
                with open(dimension_map_path, 'w') as f:
                    json.dump(dimension_map_dict, f)
                dimension_blob = bucket.blob(f"pall/{initial_utc_str}/dimension_map.json")
                dimension_blob.upload_from_filename(dimension_map_path)
                print("✅ dimension_map.json アップロード完了！")
            finally:
                if os.path.exists(dimension_map_path):
                    os.remove(dimension_map_path)

        entity = datastore.Entity(datastore_key)
        entity['datetime'] = target_dtobj
        datastore_client.put(entity)
        print(f"🎉 [DEBUG] 全工程完了！ Datastoreを {target_dtobj} で更新したよ！")

if __name__ == '__main__':
    main()