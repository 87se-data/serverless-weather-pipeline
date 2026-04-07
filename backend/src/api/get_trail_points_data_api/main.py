# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
from typing import Optional
import datetime
import pytz
from pytz import timezone
import json
import math 
from math import sin, cos, acos, radians
import struct
import time
import io
import urllib.request
import urllib.parse
import urllib.error

import numpy as np
from google.cloud import storage
from google.cloud import datastore
import zstandard as zstd

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

api_key = os.environ.get('API_KEY')
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME', 'gpv-data-bucket')

storage_client = storage.Client()
datastore_client = datastore.Client()

earth_rad = 6378.137

# --- 🏔️ 登山コースタイム設定（ネイスミスの法則ベース） ---
FLAT_SEC_PER_KM = 900.0
ASCENT_SEC_PER_100M = 1200.0
DESCENT_SEC_PER_100M = 180.0

NX = 481
NY = 505
LON_START = 120.0
LAT_START = 47.6
LON_STEP = 0.0625
LAT_STEP = 0.05

def load_topo_data():
    with open("TOPO.MSM_5K", "rb") as f:
        data = f.read(NX * NY * 4)
        elevations = struct.unpack(f">{NX * NY}f", data)
    return elevations

topo_elevations = load_topo_data()

ors_base_url = 'https://api.openrouteservice.org/v2/directions/'

def calc_thermal_indices(ta_kelvin, rh, wind_speed, solar_rad):
        if ta_kelvin is None or rh is None or wind_speed is None:
                return None, None
                
        ta_celsius = ta_kelvin - 273.15
        
        # 飽和水蒸気圧から水蒸気圧(e)を計算 (hPa)
        es = 6.105 * math.exp((17.27 * ta_celsius) / (237.7 + ta_celsius))
        e = (rh / 100.0) * es
        
        # 1. 体感温度 (Apparent Temperature)
        apparent_temp_c = ta_celsius + (0.33 * e) - (0.70 * wind_speed) - 4.0
        apparent_temp_k = apparent_temp_c + 273.15
        
        # 2. WBGT（湿球黒球温度）の近似値 (℃)
        # 環境省の屋外WBGT推定式に準拠（気温、湿度、日射量、風速から算出）
        # 日射量(W/m2)を kW/m2 に変換
        sr_kw = (solar_rad or 0.0) / 1000.0
        
        wbgt_c = (0.735 * ta_celsius) + (0.0374 * rh) + (0.00292 * ta_celsius * rh) + (7.619 * sr_kw) - (4.557 * (sr_kw ** 2)) - (0.0572 * wind_speed)
                
        return apparent_temp_k, wbgt_c

def latlng_to_xyz(lat, lng):
        rlat, rlng = radians(lat), radians(lng)
        coslat = cos(rlat)
        return coslat*cos(rlng), coslat*sin(rlng), sin(rlat)

def dist_on_sphere(pos0, pos1, radious=earth_rad):
        xyz0, xyz1 = latlng_to_xyz(*pos0), latlng_to_xyz(*pos1)
        return acos(sum(x * y for x, y in zip(xyz0, xyz1)))*radious

def get_elevation( target_lat, target_lon):
    if not (LAT_START - (NY-1)*LAT_STEP <= target_lat <= LAT_START):
        return 0.0
    if not (LON_START <= target_lon <= LON_START + (NX-1)*LON_STEP):
        return 0.0

    y = round((LAT_START - target_lat) / LAT_STEP)
    x = round((target_lon - LON_START) / LON_STEP)
    
    y = max(0, min(NY - 1, int(y)))
    x = max(0, min(NX - 1, int(x)))
    
    index = int(y * NX + x)
    return topo_elevations[index]

def get_latest_initial_time():
    try:
        query = datastore_client.query(kind="store-gcs-msm-surf-db")
        query.order = ["-datetime"]
        results = list(query.fetch(limit=1))
        
        if not results:
            return None
            
        dt = results[0]["datetime"]
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=pytz.UTC)
        else:
            dt = dt.astimezone(pytz.UTC)
        return dt.strftime("%Y%m%d%H0000Z")
    except Exception as e:
        print(f"Datastore Error: {e}")
        return None

def fetch_meta_data(bucket, dat_name, ini_time_str, surface):
    surface_dir = surface
    if surface == 'surface':
        surface_dir = 'surf'
    else:
        surface_dir = 'pall'
        
    blob_path = f"{surface_dir}/{ini_time_str}/dimension_map.json"
    blob = bucket.blob(blob_path)
    if not blob.exists():
        print(f"Meta not found: {blob_path}")
        return None
    json_str = blob.download_as_string()
    meta_data = json.loads(json_str)
    
    first_time_key = next(iter(meta_data))
    target_meta = meta_data[first_time_key][surface]
    
    return {
        'lat1': float(target_meta['bbox']['lat1']),
        'lon1': float(target_meta['bbox']['lon1']),
        'lat2': float(target_meta['bbox']['lat2']),
        'lon2': float(target_meta['bbox']['lon2']),
        'ny': int(target_meta['grid']['ny']),
        'nx': int(target_meta['grid']['nx']),
        'nlat': float(target_meta['grid']['nlat']),
        'nlon': float(target_meta['grid']['nlon']),
        'element_index': target_meta.get('element_index', {})
    }

def fetch_npy_data(bucket, dat_name, ini_time_str, surface, target_time_str):
    surface_dir = surface
    if surface == 'surface':
        surface_dir = 'surf'
        
    blob_path = f"{surface_dir}/{ini_time_str}/{target_time_str}/{surface}.npy.zst"
    blob = bucket.blob(blob_path)
    if not blob.exists():
        print(f"NPY not found: {blob_path}")
        return None
    
    dctx = zstd.ZstdDecompressor()
    decompressed_data = dctx.decompress(blob.download_as_bytes())
    buf = io.BytesIO(decompressed_data)
    npy = np.load(buf)
            
    return npy

def apply_correction(element, val, lat, lon, route_ele):
    gpv_ele = get_elevation(lat, lon)
    
    # 気温の減率補正（既存のまま）
    if element == '0_0':
        diff_ele = route_ele - gpv_ele
        return val - (diff_ele * 0.006)
        
    # 風速（U成分・V成分）の補正
    elif element in ['2_2', '2_3']:
        h0 = 10.0  # GPVの基準高度(10m)
        diff_ele = route_ele - gpv_ele
        
        # 💖 1. 緯度から「森林限界」と「樹木限界」を計算
        # 基準(緯度35度で2500m)とし、1度北上するごとに122m下がる。
        # 日本の国土を考慮し、異常値にならないよう500m〜3500mでガード
        timberline = max(500.0, min(3500.0, 2500.0 - (lat - 35.0) * 122.0))
        treeline = timberline + 100.0  # 樹木限界は森林限界の約100m上
        
        # 💖 2. 標高に応じた3段階の「植生（摩擦）」判定
        if route_ele > treeline:
            # 【高山帯】（樹木限界以上）
            # 岩と砂の世界。風を遮るものがなく、風がダイレクトに吹き抜ける
            friction_f = 1.0
            alpha = 0.11
        elif route_ele > timberline:
            # 【移行帯】（森林限界〜樹木限界）
            # ハイマツなどの低木が点在。足元は遮られるが上部は風が通る
            friction_f = 0.85
            alpha = 0.18
        else:
            # 【樹林帯】（森林限界以下）
            # 背の高い木が密生。風の勢いを大きく削ぐ
            friction_f = 0.65
            alpha = 0.25

        # 💖 3. 相対高さ（地形）と摩擦を掛け合わせた風速計算
        if diff_ele > 0:
            # GPV平均標高より高い場所（尾根や山頂）
            height_eff = h0 + diff_ele
            wind_multiplier = (height_eff / h0) ** alpha
            corrected_val = val * wind_multiplier * friction_f
            
            # 安全対策：計算上、元のGPV風速の「4倍」を上限（キャップ）とする
            return max(val, min(corrected_val, val * 4.0))
            
        elif diff_ele < 0:
            # GPV平均標高より低い場所（谷間など）
            # 谷間は風が弱まる傾向があるため、基本係数(0.8)に植生の摩擦をさらに掛ける
            return val * 0.8 * friction_f
            
        else:
            # GPV平均標高と同じ場所
            return val * friction_f

    return val

@app.get("/data/")
def get_trail_points_data_api(
        origin: str, 
        destination: str, 
        means: Optional[str] = "hiking",  
        departure: Optional[int] = None,
        gpv: Optional[str] = 'MSM_GPV_Rjp_Lsurf',
        element: Optional[str] = "1_8,0_0,1_1,2_2,2_3,4_7,ssi,tt,ki,lcl,cape,cin,theta_e,water_vapor_flux",
        surface: Optional[str] = 'surface,pall'
):
        if departure is None:
                departure = int(time.time())
        if not origin or not destination:
                return {"error": "originとdestinationは必須だよ！"}

        request_json = {}
        request_json['origin'] = origin
        request_json['destination'] = destination
        request_json['means'] = means
        request_json['departure_eptime'] = departure
        request_json['gpv'] = gpv
        request_json['element'] = element
        request_json['surface'] = surface

        safe_means = means.lower() if means else "hiking"

        if safe_means in ['driving', 'car', 'd', 'drive']:
                profile = "driving-car"
        elif safe_means in ['walking', 'w', 'walk', 'hiking', 'hike']:
                profile = "foot-hiking"  
        elif safe_means in ['bicycling', 'b', 'bicycle', 'bike']:
                profile = "cycling-mountain"  
        else:
                profile = "foot-hiking"

        def parse_coord(coord_str: str):
                try:
                        lat, lng = map(float, coord_str.split(","))
                        return [lng, lat]  
                except ValueError:
                        return None

        coord_origin = parse_coord(origin)
        coord_dest = parse_coord(destination)

        if not coord_origin or not coord_dest:
                return {"error": "ORSは緯度経度(カンマ区切り)しか受け付けないよ！座標を送ってね😭"}

        body = {
                "coordinates": [coord_origin, coord_dest],
                "language": "ja",
                "elevation": True  
        }

        req_url = ors_base_url + profile + '/geojson'

        headers = {
                'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
                'Authorization': api_key,
                'Content-Type': 'application/json; charset=utf-8'
        }

        req = urllib.request.Request(req_url, data=json.dumps(body).encode('utf-8'), headers=headers, method='POST')
        
        try:
                response = urllib.request.urlopen(req).read()
                directions = json.loads(response)
        except urllib.error.HTTPError as e:
                error_body = e.read().decode('utf-8')
                print(f"ORSエラー発生😱: {error_body}")
                return {"error": f"ORS API側でエラーが起きたよ: {e.code}"}
        except Exception as e:
                print(f"予期せぬエラー😱: {e}")
                return {"error": "リクエスト送信中にエラーが起きたよ😭"}

        if 'features' not in directions or not directions['features']:
                return {"error": "ルートが取得できなかったよ😭 (道がないか遠すぎるかも)"}

        target_dtobj = datetime.datetime.fromtimestamp(departure, tz=timezone('Asia/Tokyo'))
        request_json['Information'] = []
        
        feature = directions['features'][0]
        geometry = feature['geometry']['coordinates'] 
        segments = feature['properties']['segments']

        if not segments:
                return {"error": "ルートのステップ情報がないよ😭"}

        steps = segments[0]['steps']

        for step in steps:
                start_index = step['way_points'][0]
                end_index = step['way_points'][1]
                step_duration = float(step['duration'])
                
                seg_distances = []
                seg_times = [] 
                
                for i in range(start_index, end_index):
                        c1 = geometry[i]
                        c2 = geometry[i+1]
                        d = dist_on_sphere([c1[1], c1[0]], [c2[1], c2[0]])
                        seg_distances.append(d)
                        
                        if profile == "foot-hiking":
                                ele1 = c1[2] if len(c1) > 2 else 0.0
                                ele2 = c2[2] if len(c2) > 2 else 0.0
                                ele_diff = ele2 - ele1
                                
                                base_sec = d * FLAT_SEC_PER_KM
                                extra_sec = 0.0
                                if ele_diff > 0:
                                        extra_sec = (ele_diff / 100.0) * ASCENT_SEC_PER_100M
                                elif ele_diff < 0:
                                        extra_sec = (abs(ele_diff) / 100.0) * DESCENT_SEC_PER_100M
                                        
                                seg_times.append(base_sec + extra_sec)
                        
                total_step_dist = sum(seg_distances)

                for i in range(start_index, end_index):
                        coords = geometry[i]
                        lng, lat = coords[0], coords[1]
                        ele = coords[2] if len(coords) > 2 else 0.0

                        input_dic = {}
                        input_dic['latitude'] = lat
                        input_dic['longitude'] = lng
                        input_dic['elevation'] = ele
                        input_dic['datetime'] = "%s" % target_dtobj.isoformat(timespec='seconds')
                        
                        request_json['Information'].append(input_dic)

                        if profile == "foot-hiking":
                                seg_time = seg_times[i - start_index]
                        else:
                                if total_step_dist > 0:
                                        seg_time = step_duration * (seg_distances[i - start_index] / total_step_dist)
                                else:
                                        seg_time = step_duration / max(1, (end_index - start_index))
                                        
                        target_dtobj += datetime.timedelta(seconds=seg_time)

        if steps:
                last_step = steps[-1]
                end_index = last_step['way_points'][1]
                coords_end = geometry[end_index]
                end_lng, end_lat = coords_end[0], coords_end[1]
                end_ele = coords_end[2] if len(coords_end) > 2 else 0.0

                final_dic = {}
                final_dic['latitude'] = end_lat
                final_dic['longitude'] = end_lng
                final_dic['elevation'] = end_ele
                final_dic['datetime'] = "%s" % target_dtobj.isoformat(timespec='seconds')
                request_json['Information'].append(final_dic)

        return get(request_json)

def get(route_data):
        start = time.time()
        draw_num = 200
        dat_name = route_data['gpv']

        ini_time_str = get_latest_initial_time()
        if not ini_time_str:
            return {"error": "DBから初期時刻を取得できませんでした。"}

        route_info_list = sorted(route_data['Information'], key=lambda x:x['datetime'])
        departure_dtobj = datetime.datetime.fromisoformat(route_info_list[0]['datetime'])
        arrival_dtobj = datetime.datetime.fromisoformat(route_info_list[-1]['datetime'])
        total_eptime = arrival_dtobj.timestamp() - departure_dtobj.timestamp()
        
        target_unit_eptime = total_eptime / draw_num
        target_dtobj = departure_dtobj
        
        points = []

        for _ in range(draw_num + 1):
                current_dis_index = 1
                for dis_index in range(1, len(route_info_list)):
                        point_dtobj = datetime.datetime.fromisoformat(route_info_list[dis_index]['datetime'])
                        current_dis_index = dis_index
                        if target_dtobj <= point_dtobj:
                                break
                
                pos1_lat = float(route_info_list[current_dis_index-1]['latitude'])
                pos1_lon = float(route_info_list[current_dis_index-1]['longitude'])
                pos1_ele = float(route_info_list[current_dis_index-1]['elevation'])
                pos2_lat = float(route_info_list[current_dis_index]['latitude'])
                pos2_lon = float(route_info_list[current_dis_index]['longitude'])
                pos2_ele = float(route_info_list[current_dis_index]['elevation'])
                
                point1_dtobj = datetime.datetime.fromisoformat(route_info_list[current_dis_index-1]['datetime'])
                point2_dtobj = datetime.datetime.fromisoformat(route_info_list[current_dis_index]['datetime'])
                
                seg_total_sec = (point2_dtobj - point1_dtobj).total_seconds()
                elapsed_sec = (target_dtobj - point1_dtobj).total_seconds()
                
                time_ratio = 0.0
                if seg_total_sec > 0:
                        time_ratio = max(0.0, min(1.0, elapsed_sec / seg_total_sec))
                
                write_lat = pos1_lat + (pos2_lat - pos1_lat) * time_ratio
                write_lon = pos1_lon + (pos2_lon - pos1_lon) * time_ratio
                write_ele = pos1_ele + (pos2_ele - pos1_ele) * time_ratio

                points.append({
                    "dt": target_dtobj,
                    "lat": write_lat,
                    "lon": write_lon,
                    "ele": write_ele
                })
                
                target_dtobj = target_dtobj + datetime.timedelta(seconds=target_unit_eptime)

        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        meta_cache = {}
        npy_cache = {}

        def get_meta(surf):
            key = surf
            if key not in meta_cache:
                meta_cache[key] = fetch_meta_data(bucket, dat_name, ini_time_str, surf)
            return meta_cache[key]

        def get_npy(surf, t_str):
            key = (surf, t_str)
            if key not in npy_cache:
                npy_cache[key] = fetch_npy_data(bucket, dat_name, ini_time_str, surf, t_str)
            return npy_cache[key]

        request_json = {}
        request_json['information'] = {}

        elements = route_data['element'].split(',')
        surfaces = route_data['surface'].split(',')

        for pt in points:
            dt_jst = pt['dt']
            dt_utc = dt_jst.astimezone(timezone('UTC'))
            lat = pt['lat']
            lon = pt['lon']
            ele = pt['ele']

            time_str_jst = dt_jst.strftime("%Y-%m-%dT%H:%M:%S+09:00")
            
            if time_str_jst not in request_json['information']:
                request_json['information'][time_str_jst] = {
                    'latitude': format(lat, '.4f'),
                    'longitude': format(lon, '.4f'),
                    'elevation': format(ele, '.1f')
                }

            for surf in surfaces:
                if surf == 'surface':
                    t1_utc = dt_utc.replace(minute=0, second=0, microsecond=0)
                    t2_utc = t1_utc + datetime.timedelta(hours=1)
                    t1_str = t1_utc.strftime("%Y%m%d%H0000Z")
                    t2_str = t2_utc.strftime("%Y%m%d%H0000Z")
                    rate = (dt_utc - t1_utc).total_seconds() / 3600.0
                else:
                    base_hour = (dt_utc.hour // 3) * 3
                    t1_utc = dt_utc.replace(hour=base_hour, minute=0, second=0, microsecond=0)
                    t2_utc = t1_utc + datetime.timedelta(hours=3)
                    t1_str = t1_utc.strftime("%Y%m%d%H0000Z")
                    t2_str = t2_utc.strftime("%Y%m%d%H0000Z")
                    rate = (dt_utc - t1_utc).total_seconds() / 10800.0

                if surf not in request_json['information'][time_str_jst]:
                    request_json['information'][time_str_jst][surf] = {}
                
                meta = get_meta(surf)
                element_index = meta.get('element_index', {}) if meta else {}
                row, col = -1, -1
                in_bounds = False
                
                if meta:
                    lat1 = meta['lat1']
                    lon1 = meta['lon1']
                    lat2 = meta['lat2']
                    lon2 = meta['lon2']
                    nlat = meta['nlat']
                    nlon = meta['nlon']
                    ny = meta['ny']
                    nx = meta['nx']
                    
                    lat_max = max(lat1, lat2)
                    lat_min = min(lat1, lat2)
                    lon_max = max(lon1, lon2)
                    lon_min = min(lon1, lon2)
                    
                    if (lat_min <= lat <= lat_max) and (lon_min <= lon <= lon_max):
                        r = int(abs(lat1 - lat) / abs(nlat))
                        c = int(abs(lon - lon1) / abs(nlon))
                        row = max(0, min(r, ny - 1))
                        col = max(0, min(c, nx - 1))
                        in_bounds = True
                
                if not in_bounds:
                    continue
                
                npy_start = get_npy(surf, t1_str)
                npy_next = get_npy(surf, t2_str)
                
                for elem in elements:
                    z_idx = element_index.get(f"{surf}:{elem}")
                    if z_idx is None:
                        z_idx = element_index.get(elem)

                    if elem == '1_8':
                        val_next = None
                        if npy_next is not None and z_idx is not None:
                            try:
                                v = float(npy_next[z_idx][row][col])
                                if not np.isnan(v):
                                    val_next = v
                            except Exception:
                                pass
                        
                        if val_next is not None:
                            val_next = apply_correction(elem, val_next, lat, lon, ele)
                            request_json['information'][time_str_jst][surf][elem] = format(val_next, '.1f')
                    else:
                        val_start = None
                        val_next = None
                        
                        if npy_start is not None and z_idx is not None:
                            try:
                                v = float(npy_start[z_idx][row][col])
                                if not np.isnan(v):
                                    val_start = v
                            except Exception:
                                pass
                                
                        if npy_next is not None and z_idx is not None:
                            try:
                                v = float(npy_next[z_idx][row][col])
                                if not np.isnan(v):
                                    val_next = v
                            except Exception:
                                pass
                        
                        if val_start is not None:
                            val_start = apply_correction(elem, val_start, lat, lon, ele)
                        if val_next is not None:
                            val_next = apply_correction(elem, val_next, lat, lon, ele)
                        
                        if val_start is not None and val_next is not None:
                            calc_gpv = val_start + (val_next - val_start) * rate
                            request_json['information'][time_str_jst][surf][elem] = format(calc_gpv, '.1f')
                        elif val_next is not None:
                            request_json['information'][time_str_jst][surf][elem] = format(val_next, '.1f')
                        elif val_start is not None:
                            request_json['information'][time_str_jst][surf][elem] = format(val_start, '.1f')

        # 💖 体感温度（apparent_temp）とWBGTをまとめて計算！
        for dt_jst, info_dict in request_json['information'].items():
                for key, val_dict in info_dict.items():
                        if isinstance(val_dict, dict):
                                ta_str = val_dict.get('0_0') # 気温
                                rh_str = val_dict.get('1_1') # 湿度
                                u_str = val_dict.get('2_2')  # 風(U)
                                v_str = val_dict.get('2_3')  # 風(V)
                                sr_str = val_dict.get('4_7') # 🌞 日射量

                                if ta_str and rh_str and u_str and v_str:
                                        ta_kelvin = float(ta_str)
                                        rh = float(rh_str)
                                        u = float(u_str)
                                        v = float(v_str)
                                        solar_rad = float(sr_str) if sr_str else 0.0

                                        wind_speed = math.sqrt(u**2 + v**2)

                                        app_temp, wbgt = calc_thermal_indices(ta_kelvin, rh, wind_speed, solar_rad)
                                        
                                        if app_temp is not None:
                                                val_dict['apparent_temp'] = format(app_temp, '.1f')
                                        if wbgt is not None:
                                                val_dict['wbgt'] = format(wbgt, '.1f')

        elapsed_time = time.time() - start
        return request_json
