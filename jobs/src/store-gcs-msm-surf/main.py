import time
import sys
import io
import os
import gc
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
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning)
import grib2_decode
from google.cloud import datastore
from google.cloud import storage
import json
import zstandard as zstd
import zarr
import gcsfs

URL = 'https://database3.rish.kyoto-u.ac.jp/arch/jmadata/data/gpv/original'

GCP_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT', os.environ.get('GCP_PROJECT', 'my-project'))

datastore_client = datastore.Client()
key_element_name = "store-gcs-msm-surf-db"
storage_client = storage.Client()

# 💖💖💖【爆速一括計算関数】💖💖💖
def calc_wbgt_array(ta_kelvin_arr, rh_arr):
    # 欠損値があっても警告を出さずに計算
    with np.errstate(invalid='ignore', divide='ignore'):
        ta_celsius_arr = ta_kelvin_arr - 273.15
        es_arr = 6.105 * np.exp((17.27 * ta_celsius_arr) / (237.7 + ta_celsius_arr))
        e_arr = (rh_arr / 100.0) * es_arr
        wbgt_arr = 0.567 * ta_celsius_arr + 0.393 * e_arr + 3.94
        return np.round(wbgt_arr, 1)

# 💖【追加】風速（wind_speed）をU風・V風から計算する関数！
def calc_wind_speed_array(u_wind_arr, v_wind_arr):
    with np.errstate(invalid='ignore', divide='ignore'):
        # 三平方の定理で合成風速を計算して、小数第1位で丸めるよ！
        wind_speed_arr = np.sqrt(u_wind_arr**2 + v_wind_arr**2)
        return np.round(wind_speed_arr, 1)

def calc_laundry_index_array(ta_kelvin_arr, rh_arr, u_wind_arr, v_wind_arr, precip_arr, solar_rad_arr=None):
    if solar_rad_arr is None:
        solar_rad_arr = np.zeros_like(ta_kelvin_arr)
    with np.errstate(invalid='ignore', divide='ignore'):
        # 合成風速
        wind_speed_arr = np.sqrt(u_wind_arr**2 + v_wind_arr**2)
        ta_celsius_arr = ta_kelvin_arr - 273.15
        
        es_arr = 6.105 * np.exp((17.27 * ta_celsius_arr) / (237.7 + ta_celsius_arr))
        e_arr = (rh_arr / 100.0) * es_arr
        vpd_arr = es_arr - e_arr
        
        drying_power_arr = vpd_arr * (1.0 + 0.2 * wind_speed_arr)
        solar_bonus_arr = solar_rad_arr * 0.02
        
        index_arr = drying_power_arr * 4.0 + solar_bonus_arr
        index_arr = np.floor(index_arr) # 切り捨て
        index_arr = np.clip(index_arr, 20, 100) # 20〜100に収める
        
        # 降水量が1.0以上のグリッドは強制20
        index_arr = np.where(precip_arr >= 1.0, 20, index_arr)
        return index_arr


def main():
    print("🚀 [DEBUG] 処理スタートしたよ！")
    
    # 💖 設定ファイルの読み込み
    target_hour_ranges = ("00-15", "16-33", "34-39")
    target_elements = ("0_0", "1_1", "2_2", "2_3", "1_8", "4_7", "6_1", "6_3", "6_4", "6_5")
    ELEMENT_KEYS = ['0_0', '1_1', '2_2', '2_3', '1_8', '4_7', '6_1', '6_3', '6_4', '6_5', 'wbgt', 'wind_speed', 'laundry_index']
    
    gcs_bucket_name_for_config = os.environ.get('GCS_BUCKET_NAME')
    config_data = {}
    try:
        if gcs_bucket_name_for_config:
            bucket = storage_client.bucket(gcs_bucket_name_for_config)
            blob = bucket.blob("config/store-gcs-msm-surf_config.json")
            if blob.exists():
                config_str = blob.download_as_string()
                config_data = json.loads(config_str)
                print("☁️ [DEBUG] GCSから設定ファイルを読み込みました。")
            else:
                raise FileNotFoundError("GCSに設定ファイルがありません。")
        else:
            raise ValueError("GCS_BUCKET_NAMEが設定されていません。")
    except Exception as e:
        print(f"⚠️ [WARNING] GCSからの設定ファイル読み込み失敗: {e} -> ローカルから読み込みます。")
        try:
            with open("store-gcs-msm-surf_config.json", "r", encoding="utf-8") as f:
                config_data = json.load(f)
            print("🏠 [DEBUG] ローカルから設定ファイルを読み込みました。")
        except Exception as e2:
            print(f"⚠️ [WARNING] ローカルの設定ファイル読み込みも失敗: {e2} -> デフォルト値を使用します。")
            
    if "target_hour_ranges" in config_data:
        target_hour_ranges = tuple(config_data["target_hour_ranges"])
    if "target_elements" in config_data:
        target_elements = tuple(config_data["target_elements"])
    if "export_elements" in config_data and "surface" in config_data["export_elements"]:
        ELEMENT_KEYS = config_data["export_elements"]["surface"]

    # 今のUTC時間を取得
    dtobj_now = datetime.datetime.now(datetime.timezone.utc)
    
    # 直近の「3の倍数」の時間を計算してスタート地点にする！
    base_hour = (dtobj_now.hour // 3) * 3
    target_dtobj = dtobj_now.replace(hour=base_hour, minute=0, second=0, microsecond=0)
    
    initial_datetime = target_dtobj.strftime('%Y-%m-%dT%H:00:00+00:00')
    search_flag = True
    print(f"🕒 [DEBUG] 最初の検索ターゲット時間: {initial_datetime}")

    while search_flag:
        goal_count = len(target_hour_ranges)
        for hour_range in target_hour_ranges:
            target_datetime = target_dtobj.strftime('%Y%m%d%H0000')
            target_file = f"Z__C_RJTD_{target_datetime}_MSM_GPV_Rjp_Lsurf_FH{hour_range}_grib2.bin"
            req_url = f"{URL}/{target_dtobj.year:04d}/{target_dtobj.month:02d}/{target_dtobj.day:02d}/{target_file}"
            r = requests.get(req_url, verify=False)
            if r.status_code == 200:
                goal_count -= 1 

        print(f"📡 [DEBUG] 検索中... 時間: {target_dtobj}, goal_count残り: {goal_count}")

        if goal_count == 0:
            search_flag = False 
            print("🎯 [DEBUG] 必要なファイルが見つかったよ！")
        else:
            # 見つからなかったら「3時間」巻き戻す！
            target_dtobj -= datetime.timedelta(hours=3)
            initial_datetime = target_dtobj.strftime('%Y-%m-%dT%H:00:00+00:00')          

    initial_datetime = target_dtobj.strftime('%Y-%m-%dT%H:00:00+00:00')
    initial_utc_str = target_dtobj.strftime('%Y%m%d%H0000') + 'Z'
    gcs_bucket_name = os.environ.get('GCS_BUCKET_NAME')

    print("🗄️ [DEBUG] Datastoreのチェック開始！")
    datastore_key = datastore_client.key(key_element_name)
    query = datastore_client.query(kind=key_element_name)
    query.order = ["datetime"]
    renewal_flag = False
    
    fetch_list = list(query.fetch())
    print(f"🗄️ [DEBUG] Datastoreのレコード数: {len(fetch_list)}件")

    if len(fetch_list) != 0:
        datastore_dt_obj = fetch_list[-1]["datetime"]
        print(f"🗄️ [DEBUG] Datastore最新時間: {datastore_dt_obj} / 今回のターゲット: {target_dtobj}")
        if target_dtobj > datastore_dt_obj:
            print("✅ [DEBUG] 新しいデータだから処理するね！")
            renewal_flag = True
        else:
            print("❌ [DEBUG] すでに処理済みだからスキップするよ！（5秒で終わるのはココ！）")
    else:
        print("✅ [DEBUG] Datastoreが空っぽだから新規処理するね！")
        renewal_flag = True

    if renewal_flag:
        print("🔥 [DEBUG] 処理ブロックに突入！")

        dat_dir = 'dat'
        if not os.path.exists(dat_dir):
            os.makedirs(dat_dir)

        target_datetime = target_dtobj.strftime('%Y%m%d%H0000')
        
        dimension_map_dict = {}
        
        for hour_range in target_hour_ranges:
            print(f"--- {hour_range} 処理スタート！ ---")
            target_file = f"Z__C_RJTD_{target_datetime}_MSM_GPV_Rjp_Lsurf_FH{hour_range}_grib2.bin"
            req_url = f"{URL}/{target_dtobj.year:04d}/{target_dtobj.month:02d}/{target_dtobj.day:02d}/{target_file}"
            r = requests.get(req_url, verify=False)
            tmp_bin_file = f"{dat_dir}/grib2.bin"
            
            if r.status_code == 200:
                with open(tmp_bin_file, "wb") as f:
                    f.write(r.content)
                grib2_data = grib2.parse_grib2(tmp_bin_file)
                print('grib2データ読み込み完了')
                
                if grib2_data['sec1']['status'] == 0 and grib2_data['sec1']['type'] == 1:
                    year = grib2_data['sec1']['year']
                    month = grib2_data['sec1']['month']
                    day = grib2_data['sec1']['day']
                    hour = grib2_data['sec1']['hour']
                    minute = grib2_data['sec1']['min']
                    sec = grib2_data['sec1']['sec']
                    utc_time = f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{sec:02d}+00:00"
                    utc_dtobj = datetime.datetime.fromisoformat(utc_time)
                    jst_dtobj = utc_dtobj.astimezone(pytz.timezone('Asia/Tokyo')) 
                    jst_time = jst_dtobj.strftime("%Y-%m-%dT%H:%M:00+09:00") 

                    # 💖【大改造】計算用にNumpy配列をストックするバッファ
                    buffer = {}

                    for cnt in range(0, grib2_data['count']):
                        data = grib2.decode_compr_data(grib2_data, cnt)
                        np.round(data, 3, out=data)

                        if grib2_data['sec1']['type'] == 1 or grib2_data['sec1']['type'] == 2:
                            add_sec = 0
                            if grib2_data['sec4'][cnt]['template'] == 0:
                                add_sec = grib2_data['sec4'][cnt]['forecast_time'] * 60 * 60
                            elif grib2_data['sec4'][cnt]['template'] == 8:
                                add_sec = (grib2_data['sec4'][cnt]['forecast_time'] + grib2_data['sec4'][cnt]['statistics_time']) * 60 * 60
                            elif grib2_data['sec4'][cnt]['template'] == 50009 or grib2_data['sec4'][cnt]['template'] == 50008:
                                add_sec = (grib2_data['sec4'][cnt]['forecast_time'] + grib2_data['sec4'][cnt]['statistics_time']) * 60
                            elif grib2_data['sec4'][cnt]['template'] == 50000:
                                add_sec = 0
                            foc_jst_dtobj = jst_dtobj + datetime.timedelta(seconds=add_sec)
                            foc_jst_time = foc_jst_dtobj.strftime("%Y-%m-%dT%H:%M:00+09:00") 
                            
                            # GCS保存用にUTCの文字列も作成しておく！
                            foc_utc_dtobj = foc_jst_dtobj.astimezone(datetime.timezone.utc)
                            foc_utc_str = foc_utc_dtobj.strftime("%Y%m%d%H%M%S") + "Z"

                            surface_type = 'surface'
                            element_type = f"{grib2_data['sec4'][cnt]['parameter_category']}_{grib2_data['sec4'][cnt]['parameter_number']}"
                            if element_type == '3_0': 
                                continue                            
                            
                            sec3 = grib2_data['sec3'][cnt]

                            # 💖 ここで指数計算とGCSアップに必要な要素をストック！
                            if element_type in target_elements:
                                if foc_jst_time not in buffer:
                                    buffer[foc_jst_time] = {'sec3': sec3, 'jst_time': jst_time, 'data': {}, 'foc_utc_str': foc_utc_str}
                                buffer[foc_jst_time]['data'][element_type] = np.copy(data)

                    # --- GRIB2の読み込み終了 ---

                    # =====================================================================
                    # 💖💖💖【最強の防護壁】データ欠損チェックロジック 💖💖💖
                    # =====================================================================
                    print("🔍 [DEBUG] データの欠損がないかチェックします...")
                    is_complete = True
                    
                    expected_counts = {'00-15': 16, '16-33': 18, '34-39': 6}
                    if len(buffer) != expected_counts.get(hour_range, 0):
                        print(f"⚠️ [ERROR] {hour_range} の時間数が足りません！(期待値:{expected_counts.get(hour_range)}, 実際:{len(buffer)})")
                        is_complete = False

                    required_keys = {'0_0', '1_1', '2_2', '2_3'}
                    for f_time, b_info in buffer.items():
                        keys = set(b_info['data'].keys())
                        if not required_keys.issubset(keys):
                            print(f"⚠️ [ERROR] {f_time} のデータが不完全です！取得できた要素: {keys}")
                            is_complete = False

                    if not is_complete:
                        raise ValueError(f"🚨 RISHサーバーのGRIB2ファイル({hour_range})がまだ書き込み途中で不完全でした！処理を中断します！")
                    # =====================================================================

                    # 💖💖💖【熱中症＆洗濯指数＆風速の事前計算 ＋ GCSアップロードスタート！】💖💖💖
                    print(f"[{hour_range}] 指数の事前計算とGCSアップロードスタート！🔥")
                    
                    # 1. GCSFileSystemをプロジェクトID指定で初期化（ループ外に出してセッション管理を最適化）
                    fs = gcsfs.GCSFileSystem(project=GCP_PROJECT)
                    
                    for foc_jst_time, b_info in buffer.items():
                        b_data = b_info['data']
                        sec3 = b_info['sec3']
                        jst_time = b_info['jst_time']
                        foc_utc_str = b_info['foc_utc_str']
                        
                        # GCSにアップロードするための辞書を作成
                        grids_dict = {}
                        for k, v in b_data.items():
                            grids_dict[k] = v

                        # 熱中症指数 (wbgt)
                        if '0_0' in b_data and '1_1' in b_data:
                            wbgt = calc_wbgt_array(b_data['0_0'], b_data['1_1'])
                            grids_dict['wbgt'] = wbgt
                            
                        # 💖【追加】風速 (wind_speed)
                        if '2_2' in b_data and '2_3' in b_data:
                            wind_speed = calc_wind_speed_array(b_data['2_2'], b_data['2_3'])
                            grids_dict['wind_speed'] = wind_speed
                        
                        # 洗濯指数 (laundry_index)
                        if all(k in b_data for k in ('0_0', '1_1', '2_2', '2_3')):
                            precip = b_data.get('1_8', np.zeros_like(b_data['0_0']))
                            solar = b_data.get('4_7', np.zeros_like(b_data['0_0']))
                            laundry = calc_laundry_index_array(b_data['0_0'], b_data['1_1'], b_data['2_2'], b_data['2_3'], precip, solar)
                            grids_dict['laundry_index'] = laundry

                        # ☁️ ここでGCSにZSTD圧縮NPZとZarrをアップロード！
                        if gcs_bucket_name:
                            # 数値がNoneでないことを確認して安全に取得
                            try:
                                m_lat1 = float(sec3.get("lat1")) if sec3.get("lat1") is not None else 0.0
                                m_lon1 = float(sec3.get("lon1")) if sec3.get("lon1") is not None else 0.0
                                m_lat2 = float(sec3.get("lat2")) if sec3.get("lat2") is not None else 0.0
                                m_lon2 = float(sec3.get("lon2")) if sec3.get("lon2") is not None else 0.0
                                m_nlat = float(sec3.get("nlat")) if sec3.get("nlat") is not None else 0.0
                                m_nlon = float(sec3.get("nlon")) if sec3.get("nlon") is not None else 0.0
                                m_ny   = int(sec3.get("ny")) if sec3.get("ny") is not None else 0
                                m_nx   = int(sec3.get("nx")) if sec3.get("nx") is not None else 0

                                if foc_utc_str not in dimension_map_dict:
                                    dimension_map_dict[foc_utc_str] = {
                                        "surface": {
                                            "bbox": {"lat1": m_lat1, "lon1": m_lon1, "lat2": m_lat2, "lon2": m_lon2},
                                            "grid": {"nlat": m_nlat, "nlon": m_nlon, "ny": m_ny, "nx": m_nx},
                                            "element_index": {key: i for i, key in enumerate(ELEMENT_KEYS)}
                                        }
                                    }
                            except (TypeError, ValueError) as e:
                                print(f"⚠️ [WARNING] Metaデータの数値変換に失敗したためスキップします: {e}")

                            try:
                                bucket = storage_client.bucket(gcs_bucket_name)
                                
                                # =========================================================
                                # 2. Zarr 形式 (.zarr) での保存
                                # =========================================================
                                # 2. GCSMapを使用してZarrのストアを作成
                                zarr_path = f"{gcs_bucket_name}/surf/{initial_utc_str}/{foc_utc_str}/surface.zarr"
                                store = gcsfs.GCSMap(zarr_path, gcs=fs, check=False)

                                # 3. ストアを元にグループを作成（既存データは上書き）
                                root = zarr.group(store=store, overwrite=True)

                                # 4. 各要素をループで確実に書き込む
                                for k, v in grids_dict.items():
                                    # chunksを指定し、1つの配列として一気に書き込み
                                    root.create_array(k, data=v, chunks=(300, 300), overwrite=True)
                                    
                                print(f"☁️ [DEBUG] Zarrアップロード成功: {zarr_path}")

                                # =========================================================
                                # 3. 【新規追加】フラットな3D配列のバイナリ (.npy.zst) での保存
                                # =========================================================
                                try:
                                    # '0_0'は確実に存在するので形状を取得
                                    base_shape = grids_dict['0_0'].shape
                                    ny, nx = base_shape
                                    
                                    arrays_3d_list = []
                                    for key in ELEMENT_KEYS:
                                        if key in grids_dict:
                                            arrays_3d_list.append(grids_dict[key])
                                        else:
                                            # 要素が存在しない場合はNaNで埋める
                                            nan_arr = np.full((ny, nx), np.nan)
                                            arrays_3d_list.append(nan_arr)
                                            
                                    # (要素数, ny, nx) の3D配列を生成
                                    array_3d = np.stack(arrays_3d_list, axis=0)
                                    
                                    npy_zst_blob_name = f"surf/{initial_utc_str}/{foc_utc_str}/surface.npy.zst"
                                    npy_zst_blob = bucket.blob(npy_zst_blob_name)
                                    
                                    buf_npy = io.BytesIO()
                                    np.save(buf_npy, array_3d)
                                    buf_npy.seek(0)
                                    
                                    cctx_npy = zstd.ZstdCompressor()
                                    compressed_npy_data = cctx_npy.compress(buf_npy.read())
                                    
                                    npy_zst_blob.upload_from_string(compressed_npy_data, content_type="application/octet-stream")
                                    print(f"☁️ [DEBUG] Zstd(NPY 3D)アップロード成功: {npy_zst_blob_name}")
                                    
                                except Exception as e:
                                    print(f"⚠️ [ERROR] Zstd(NPY 3D)アップロード失敗: {e}")

                            except Exception as e:
                                print(f"⚠️ [ERROR] GCSアップロード失敗: {e}")

            if 'grib2_data' in locals(): del grib2_data
            if 'data' in locals(): del data
            if 'buffer' in locals(): del buffer
            gc.collect()

            if os.path.exists(tmp_bin_file): os.remove(tmp_bin_file)

        # ----------------------------
        # ☁️ dimension_map.json のアップロード！（新規追加）
        # ----------------------------
        if gcs_bucket_name and dimension_map_dict:
            try:
                bucket = storage_client.bucket(gcs_bucket_name)
                
                dim_blob_name = f"surf/{initial_utc_str}/dimension_map.json"
                dim_blob = bucket.blob(dim_blob_name)
                dim_blob.upload_from_string(json.dumps(dimension_map_dict, indent=2), content_type="application/json")
                print(f"☁️ [DEBUG] GCS dimension_map.json アップロード成功: {dim_blob_name}")
            except Exception as e:
                print(f"⚠️ [ERROR] GCS dimension_map.json アップロード失敗: {e}")

        print("🎉 [DEBUG] fsspecのキャッシュも綺麗にお片付け完了！")
        gcsfs.GCSFileSystem.clear_instance_cache()
        
        # 💖💖 修正ポイント: 全ての処理が成功したここで、Datastoreを更新します！！
        print("🗄️ [DEBUG] 全データ処理成功！Datastoreの時間を更新します！")
        entity = datastore.Entity(datastore_key)
        entity['datetime'] = target_dtobj
        datastore_client.put(entity)

        print("🎉 [DEBUG] すべての処理が完了したよ！！")

if __name__ == '__main__':
    main()
