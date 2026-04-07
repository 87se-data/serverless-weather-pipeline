# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import datetime
import pytz
from typing import Optional
import math
import gcsfs
import json
import numpy as np
from concurrent import futures
from google.cloud import datastore
import zstandard
import io

app = FastAPI()

# プロジェクトIDを一番上でガッチリ固定！
GCP_PROJECT = os.getenv("GCP_PROJECT", "my-project")

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_latest_datastore_info(gpv: str):
    # 明示的にプロジェクトIDを渡す！
    client = datastore.Client(project=GCP_PROJECT)
    kind = f'store-gcs-{gpv}-db'
    query = client.query(kind=kind)
    query.order = ['-datetime']
    results = list(query.fetch(limit=1))
    
    if not results:
        return None
        
    return results[0]['datetime']

def get_meta_info(gcs_bucket: str, initial_time_str: str, gcsfs_client: gcsfs.GCSFileSystem):
    meta_path = f"gs://{gcs_bucket}/pall/{initial_time_str}/dimension_map.json"
    with gcsfs_client.open(meta_path, 'r') as f:
        meta_data = json.load(f)
    return meta_data

def fetch_npy_data(gcs_bucket, initial_time_str, target_time_str, ny_idx, nx_idx, fs, target_meta):
    path_pall = f"gs://{gcs_bucket}/pall/{initial_time_str}/{target_time_str}/pall.npy.zst"
    path_850 = f"gs://{gcs_bucket}/pall/{initial_time_str}/{target_time_str}/850hPa.npy.zst"
    
    pall_keys = ['ssi', 'lcl', 'lfc', 'el', 'cape', 'cin', 'ki', 'tt', 'gdi']
    hpa850_keys = ['theta_e', 'water_vapor_flux']
    
    pall_values = {k: None for k in pall_keys}
    hpa850_values = {k: None for k in hpa850_keys}
    
    try:
        with fs.open(path_pall, 'rb') as f:
            compressed_data = f.read()
            
        dctx = zstandard.ZstdDecompressor()
        decompressed_data = dctx.decompress(compressed_data)
        data_pall = np.load(io.BytesIO(decompressed_data))
        
        element_index_pall = target_meta.get('pall', {}).get('element_index', {})
        for key in pall_keys:
            z_idx = element_index_pall.get(f"pall:{key}")
            if z_idx is not None:
                val = data_pall[z_idx, int(ny_idx), int(nx_idx)]
                if not np.isnan(val):
                    pall_values[key] = round(float(val), 1)

    except Exception as e:
        print(f"[DEBUG] Failed to fetch or process pall NPY data for {target_time_str}: {e}")

    try:
        with fs.open(path_850, 'rb') as f:
            compressed_data = f.read()
            
        dctx = zstandard.ZstdDecompressor()
        decompressed_data = dctx.decompress(compressed_data)
        data_850 = np.load(io.BytesIO(decompressed_data))
        
        element_index_850 = target_meta.get('850hPa', {}).get('element_index', {})
        for key in hpa850_keys:
            z_idx = element_index_850.get(f"850hPa:{key}")
            if z_idx is not None:
                val = data_850[z_idx, int(ny_idx), int(nx_idx)]
                if not np.isnan(val):
                    hpa850_values[key] = round(float(val), 1)
                    
    except Exception as e:
        print(f"[DEBUG] Failed to fetch or process 850hPa NPY data for {target_time_str}: {e}")
        
    return target_time_str, pall_values, hpa850_values

@app.get("/data/")
def get_instability_points_data_api(
        latitude: Optional[float] = 35.681236,
        longitude: Optional[float] = 139.767125,
        gpv: Optional[str] = 'msm-pall',
        element: Optional[str] = "1_1,0_0,theta_e,water_vapor_flux",
        surface: Optional[str] = '1000hPa,975hPa,950hPa,925hPa,900hPa,850hPa,800hPa,700hPa,600hPa,500hPa,400hPa,300hPa'
):
    try:
        management_info = get_latest_datastore_info(gpv)
    except Exception as e:
        return {"error": f"Datastore connection failed: {str(e)}"}

    ret_json = {
        'status': 'NG',
        'messages': "",
        'result': {
            'data': [],
            'initial_datetime': None,
            'latitude': latitude,
            'longitude': longitude
        }
    }

    initial_dt = management_info
    if not initial_dt:
        ret_json['messages'] = "Management info not found."
        return ret_json

    initial_time_str = initial_dt.astimezone(pytz.UTC).strftime("%Y%m%d%H%M%SZ")
    ret_json['result']['initial_datetime'] = initial_dt.isoformat()

    gcs_bucket = os.getenv("GCS_BUCKET_NAME", "store")

    fs = gcsfs.GCSFileSystem(project=GCP_PROJECT)
    
    try:
        meta_data = get_meta_info(gcs_bucket, initial_time_str, fs)
    except Exception as e:
        ret_json['messages'] = f"Failed to get dimension_map.json: {e}"
        return ret_json

    target_times = [k for k in meta_data.keys() if k.endswith('Z')]
    if not target_times:
        ret_json['messages'] = "No target times found."
        return ret_json

    first_target = target_times[0]
    surface_meta = meta_data[first_target].get('pall', {})
    bbox = surface_meta.get('bbox', {})
    grid = surface_meta.get('grid', {})
    
    lat1, lon1 = bbox.get('lat1'), bbox.get('lon1')
    nlat, nlon = grid.get('nlat'), grid.get('nlon')

    ny_idx = math.floor((lat1 - latitude + (nlat / 2)) / nlat)
    nx_idx = math.floor((longitude - lon1 + (nlon / 2)) / nlon)

    json_data = {}
    
    with futures.ThreadPoolExecutor(max_workers=30) as executor:
        future_to_time = {
            executor.submit(
                fetch_npy_data, gcs_bucket, initial_time_str, t_time, ny_idx, nx_idx, fs, meta_data[t_time]
            ): t_time for t_time in target_times
        }
        for future in futures.as_completed(future_to_time):
            t_time, pall_v, hpa850_v = future.result()
            json_data[t_time] = (pall_v, hpa850_v)

    for target_time in sorted(json_data.keys()):
        dt_utc = datetime.datetime.strptime(target_time, "%Y%m%d%H%M%SZ").replace(tzinfo=pytz.UTC)
        dt_jst = dt_utc.astimezone(pytz.timezone('Asia/Tokyo'))
        
        pall_vals, hpa850_vals = json_data[target_time]
        tmp_json_data = {
            "contents": [
                {"surface": "pall", "value": pall_vals},
                {"surface": "850hPa", "value": hpa850_vals}
            ],
            "datetime": dt_jst.isoformat()
        }
        ret_json['result']['data'].append(tmp_json_data)

    ret_json['status'] = 'OK'
    return ret_json
