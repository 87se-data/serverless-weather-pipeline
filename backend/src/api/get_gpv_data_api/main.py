import os
import json
import datetime
import math
import pytz
import concurrent.futures
import io
import numpy as np
import zstandard as zstd
import requests
from google.auth.transport.requests import AuthorizedSession
from google.auth import default
import functions_framework
from google.cloud import datastore
from google.cloud import storage

# =========================================================================
# 🚀 【神改修1】HTTPコネクションの渋滞を解消する「魔法の設定」
# =========================================================================
credentials, _ = default()
session = AuthorizedSession(credentials)
adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=50)
session.mount('https://', adapter)

GCP_PROJECT = os.getenv("GCP_PROJECT")
datastore_client = datastore.Client(project=GCP_PROJECT) if GCP_PROJECT else datastore.Client()

if GCP_PROJECT:
    storage_client = storage.Client(project=GCP_PROJECT, credentials=credentials, _http=session)
else:
    storage_client = storage.Client(credentials=credentials, _http=session)
# =========================================================================

# 🚀 【神改修2】meta.json のグローバルキャッシュ（これで全ユーザーが0.1秒速くなる！）
global_meta_cache = {}
last_initial_time = None

# =========================================================================
# 🚀 【神改修3】ダウンロード〜解凍〜抽出までをスレッド内で一気通貫！
# =========================================================================
def fetch_and_extract(gcs_bucket, initial_time_str, target_time_str, target_keys, ny_idx, nx_idx, element_index):
    blob_path = f"surf/{initial_time_str}/{target_time_str}/surface.npy.zst"
    result_data = {}
    
    try:
        bucket = storage_client.bucket(gcs_bucket)
        blob = bucket.blob(blob_path)
        
        # 1. 渋滞ゼロの爆速ダウンロード！🌊
        buf = blob.download_as_bytes()
        
        # 2. 2vCPUのパワーを使って並列解凍！🔥
        dctx = zstd.ZstdDecompressor()
        decompressed_data = dctx.decompress(buf)
        
        # 3. メモリ展開してピンポイント抽出！🧠
        data = np.load(io.BytesIO(decompressed_data))
        
        element_index = element_index or {}
        keys_to_fetch = target_keys if target_keys else list(element_index.keys())
        for key in keys_to_fetch:
            z_idx = element_index.get(key)
            if z_idx is not None:
                val = data[z_idx, int(ny_idx), int(nx_idx)]
                result_data[key] = None if np.isnan(val) else round(float(val), 2)
            else:
                result_data[key] = None
                    
    except Exception as e:
        print(f"☁️ [DEBUG] Error for {target_time_str}: {e}")
        
    return target_time_str, result_data


def get_initial_datetime_from_datastore():
    query = datastore_client.query(kind='store-gcs-msm-surf-db')
    query.order = ['-datetime']
    results = list(query.fetch(limit=1))
    if results:
        return results[0]['datetime']
    return None


@functions_framework.http
def get_gpv_data(request):
    global global_meta_cache, last_initial_time

    if request.method == "OPTIONS":
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Max-Age": "3600",
        }
        return ("", 204, headers)

    headers = {"Access-Control-Allow-Origin": "*"}
    request_json = request.get_json(silent=True)
    request_args = request.args

    ret_json = {'status': 'NG', 'messages': '', 'result': {}}
    param = {}

    for elem_name in ('latitude','longitude','initial_datetime','gpv_type','response_timezone','surfaces[]','elements[]', 'surfaces', 'elements'):
        if request_json and elem_name in request_json:
            param[elem_name] = request_json[elem_name]
        elif request_args and elem_name in request_args:
            param[elem_name] = request_args[elem_name]

    if param.get('latitude') is None or param.get('longitude') is None:
        ret_json['messages'] = 'Latitude / longitude information does not exist!'
        return (json.dumps(ret_json), 200, headers)
    
    lat = float(param['latitude'])
    lon = float(param['longitude'])
    ret_json['result']['latitude'] = str(lat)
    ret_json['result']['longitude'] = str(lon)

    response_timezone = param.get('response_timezone', 'UTC')
    if response_timezone not in pytz.all_timezones:
        ret_json['messages'] = 'The time zone name is invalid!'
        return (json.dumps(ret_json), 200, headers)

    element_list = []
    if param.get('elements[]'):
        element_list = param['elements[]'].split(',')
    elif param.get('elements'):
        element_list = param['elements'].split(',')
    target_keys = element_list if element_list else []

    initial_dt = None
    if param.get('initial_datetime'):
        initial_dt = datetime.datetime.fromisoformat(param['initial_datetime'])
    else:
        ds_dt = get_initial_datetime_from_datastore()
        if ds_dt:
            initial_dt = ds_dt
        else:
            ret_json['messages'] = 'Target time does not exist in Datastore!'
            return (json.dumps(ret_json), 200, headers)

    initial_time_str = initial_dt.astimezone(pytz.UTC).strftime("%Y%m%d%H%M%SZ")
    ret_json['result']['initial_datetime'] = initial_dt.astimezone(pytz.timezone(response_timezone)).isoformat()

    gcs_bucket = os.getenv('GCS_BUCKET_NAME', 'store') 
    
    # =========================================================================
    # 💡 キャッシュを活用して dimension_map.json を取得！（通信を1回スキップ！）
    # =========================================================================
    if last_initial_time != initial_time_str:
        try:
            bucket = storage_client.bucket(gcs_bucket)
            meta_blob = bucket.blob(f"surf/{initial_time_str}/dimension_map.json")
            global_meta_cache = json.loads(meta_blob.download_as_text())
            last_initial_time = initial_time_str
        except Exception as e:
            ret_json['messages'] = f'dimension_map.json not found: {e}'
            return (json.dumps(ret_json), 200, headers)

    meta_data = global_meta_cache
    # =========================================================================

    target_times = [k for k in meta_data.keys() if k.endswith('Z')]
    if not target_times:
        ret_json['messages'] = 'Invalid dimension_map.json: No target times found'
        return (json.dumps(ret_json), 200, headers)

    first_target = target_times[0]
    bbox = meta_data[first_target]['surface'].get('bbox', {})
    grid = meta_data[first_target]['surface'].get('grid', {})
    lat1 = bbox.get('lat1')
    lon1 = bbox.get('lon1')
    nlat = grid.get('nlat')
    nlon = grid.get('nlon')
    
    if None in (lat1, lon1, nlat, nlon):
        ret_json['messages'] = 'Invalid dimension_map.json format: missing bbox or grid info'
        return (json.dumps(ret_json), 200, headers)

    ny_idx = math.floor((lat1 - lat + (nlat / 2)) / nlat)
    nx_idx = math.floor((lon - lon1 + (nlon / 2)) / nlon)

    # =========================================================================
    # 💡 30並列で「ダウンロード〜解凍〜抽出」をフルスピードでぶん回す！
    # =========================================================================
    ret_json['result']['data'] = []
    results = {}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        futures = [
            executor.submit(fetch_and_extract, gcs_bucket, initial_time_str, t_str, target_keys, ny_idx, nx_idx, meta_data[t_str]['surface'].get('element_index', {}))
            for t_str in target_times
        ]
        for future in concurrent.futures.as_completed(futures):
            t_str, r_data = future.result()
            results[t_str] = r_data

    # 結果をガッチャンコ！🎁
    for target_time_str in sorted(results.keys()):
        try:
            t_dt = datetime.datetime.strptime(target_time_str, "%Y%m%d%H%M%SZ").replace(tzinfo=pytz.UTC)
            formatted_dt = t_dt.astimezone(pytz.timezone(response_timezone)).isoformat()
        except ValueError:
            formatted_dt = target_time_str
            
        contents = [{'surface': 'surface', 'value': results[target_time_str]}]
        ret_json['result']['data'].append({'datetime': formatted_dt, 'contents': contents})

    ret_json['status'] = 'OK'
    ret_json_str = json.dumps(ret_json, ensure_ascii=False).replace('NaN', 'null')

    return (ret_json_str, 200, headers)