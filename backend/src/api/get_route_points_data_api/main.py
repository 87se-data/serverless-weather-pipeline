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
import time
import io
import urllib.request
import urllib.parse
import urllib.error
from google.cloud import storage
from google.cloud import datastore
import zstandard as zstd
import numpy as np

app = FastAPI()

origins = ["*"]  # すべてのオリジンからのアクセスを許可

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# GCSバケット設定
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME', 'my-weather-bucket')

# Routes APIのエンドポイント
endpoint = 'https://routes.googleapis.com/directions/v2:computeRoutes'
api_key = os.environ.get('API_KEY')

storage_client = storage.Client()
datastore_client = datastore.Client()

earth_rad = 6378.137


# 体感温度＆WBGT計算ロジック（日射量対応版）
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

def fetch_meta_data(bucket, ini_time_str, surface):
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

def fetch_npy_data(bucket, ini_time_str, surface, target_time_str):
    if surface == 'surface':
        surface_dir = 'surf'
    else:
        surface_dir = 'pall'
        
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

# 複数のクエリパラメータを受け取るAPIエンドポイント
@app.get("/data/")
def get_route_points_data_api(
        origin: str, 
        destination: str, 
        means: Optional[str] = "driving", 
        departure: Optional[int] = None,
        gpv: Optional[str] = 'MSM_GPV_Rjp_Lsurf',
        element: Optional[str] = "1_8,0_0,1_1,2_2,2_3,4_7,ssi,tt,ki,lcl,cape,cin,theta_e,water_vapor_flux,zero_degree_altitude,pressure_change_3h,6_3",
        surface: Optional[str] = 'surface,pall,850hPa'
):
        if departure is None:
                departure = int(time.time())
        if not origin or not destination:
                return {"error": "origin and destination are required"}

        request_json = {}
        request_json['origin'] = origin
        request_json['destination'] = destination
        request_json['means'] = means
        request_json['departure_eptime'] = departure
        request_json['gpv'] = gpv
        request_json['element'] = element
        request_json['surface'] = surface

        if means in ['driving', 'car', 'd']:
                travel_mode = "DRIVE"
        elif means in ['walking', 'w']:
                travel_mode = "WALK"
        elif means in ['bicycling', 'b']:
                travel_mode = "BICYCLE"
        elif means in ['transit', 't']:
                travel_mode = "TRANSIT"
        else:
                travel_mode = "DRIVE"

        def build_waypoint(point_str: str):
                if "," in point_str:
                        try:
                                lat, lng = map(float, point_str.split(","))
                                return {"location": {"latLng": {"latitude": lat, "longitude": lng}}}
                        except ValueError:
                                pass
                return {"address": point_str}

        current_time = int(time.time())
        api_departure_time = departure if departure > current_time else current_time + 60
        api_dt_utc = datetime.datetime.fromtimestamp(api_departure_time, tz=timezone('UTC'))
        api_departure_time_str = api_dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

        body = {
                "origin": build_waypoint(origin),
                "destination": build_waypoint(destination),
                "travelMode": travel_mode,
                "languageCode": "ja"
        }

        if travel_mode == "DRIVE":
                body["routingPreference"] = "TRAFFIC_AWARE"
                body["departureTime"] = api_departure_time_str
        elif travel_mode == "TRANSIT":
                body["departureTime"] = api_departure_time_str

        # 💖 X-Goog-FieldMaskを修正（steps.durationを除去し、legs全体のdurationとstaticDurationを追加）
        headers = {
                'Content-Type': 'application/json',
                'X-Goog-Api-Key': api_key,
                'X-Goog-FieldMask': 'routes.legs.duration,routes.legs.staticDuration,routes.legs.steps.startLocation,routes.legs.steps.endLocation,routes.legs.steps.staticDuration,routes.legs.steps.transitDetails,routes.legs.steps.polyline.encodedPolyline'
        }

        req = urllib.request.Request(endpoint, data=json.dumps(body).encode('utf-8'), headers=headers, method='POST')
        
        try:
                response = urllib.request.urlopen(req).read()
                directions = json.loads(response)
        except urllib.error.HTTPError as e:
                error_body = e.read().decode('utf-8')
                print(f"APIエラー発生😱: {error_body}")
                return {"error": f"Google API側でエラーが起きたよ: {e.code}"}
        except Exception as e:
                print(f"予期せぬエラー😱: {e}")
                return {"error": "リクエスト送信中にエラーが起きたよ😭"}

        if not directions.get('routes'):
                return {"error": "ルートが取得できなかったよ😭"}

        # 💖 渋滞倍率（Traffic Ratio）の計算ロジック
        leg_data = directions['routes'][0].get('legs', [{}])[0]
        leg_duration_sec = float(leg_data.get('duration', '0s').replace('s', ''))
        leg_static_duration_sec = float(leg_data.get('staticDuration', '0s').replace('s', ''))
        
        # 渋滞でどれくらい遅れているか比率を出す（例：通常10分のところが12分なら1.2倍）
        traffic_ratio = (leg_duration_sec / leg_static_duration_sec) if leg_static_duration_sec > 0 else 1.0

        target_dtobj = datetime.datetime.fromtimestamp(departure, tz=timezone('Asia/Tokyo'))
        request_json['Information'] = []
        
        steps = leg_data.get('steps', [])

        for content_dict in steps:
                # 💖 各ステップの staticDuration に渋滞倍率を掛けて、擬似的に渋滞考慮時間を算出
                static_duration_str = content_dict.get('staticDuration', '0s')
                duration_sec = float(static_duration_str.replace('s', '')) * traffic_ratio
                
                encoded_poly = content_dict.get('polyline', {}).get('encodedPolyline')
                
                if encoded_poly:
                        poly_coords = decode_polyline(encoded_poly)
                        seg_distances = []
                        for i in range(len(poly_coords) - 1):
                                d = dist_on_sphere((poly_coords[i][0], poly_coords[i][1]), (poly_coords[i+1][0], poly_coords[i+1][1]))
                                seg_distances.append(d)
                                
                        total_dist = sum(seg_distances)
                        
                        request_json['Information'].append({
                                'latitude': poly_coords[0][0],
                                'longitude': poly_coords[0][1],
                                'datetime': "%s" % target_dtobj.isoformat(timespec='seconds')
                        })
                        
                        for i in range(len(poly_coords) - 1):
                                time_ratio = (seg_distances[i] / total_dist) if total_dist > 0 else (1.0 / len(seg_distances))
                                added_sec = duration_sec * time_ratio
                                target_dtobj += datetime.timedelta(seconds=added_sec)
                                
                                request_json['Information'].append({
                                        'latitude': poly_coords[i+1][0],
                                        'longitude': poly_coords[i+1][1],
                                        'datetime': "%s" % target_dtobj.isoformat(timespec='seconds')
                                })
                else:
                        start_loc = content_dict.get('startLocation')
                        if not start_loc and 'transitDetails' in content_dict:
                                start_loc = content_dict['transitDetails'].get('stopDetails', {}).get('departureStop', {}).get('location')
                        
                        if start_loc and 'latLng' in start_loc:
                                request_json['Information'].append({
                                        'latitude': start_loc['latLng']['latitude'],
                                        'longitude': start_loc['latLng']['longitude'],
                                        'datetime': "%s" % target_dtobj.isoformat(timespec='seconds')
                                })
                        target_dtobj += datetime.timedelta(seconds=duration_sec)

        if steps:
                last_step = steps[-1]
                end_loc = last_step.get('endLocation')
                if not end_loc and 'transitDetails' in last_step:
                        end_loc = last_step['transitDetails'].get('stopDetails', {}).get('arrivalStop', {}).get('location')

                if end_loc and 'latLng' in end_loc:
                        request_json['Information'].append({
                                'latitude': end_loc['latLng']['latitude'],
                                'longitude': end_loc['latLng']['longitude'],
                                'datetime': "%s" % target_dtobj.isoformat(timespec='seconds')
                        })

        return get(request_json)


def get(route_data):
        start = time.time()
        draw_num = 200

        ini_time_str = get_latest_initial_time()
        if not ini_time_str:
            return {"error": "Datastoreに有効な気象データの初期時刻がありません"}

        route_info_list = sorted(route_data['Information'], key=lambda x:x['datetime'])
        if not route_info_list:
            return {"error": "Route information is empty"}

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
                pos2_lat = float(route_info_list[current_dis_index]['latitude'])
                pos2_lon = float(route_info_list[current_dis_index]['longitude'])
                
                point1_dtobj = datetime.datetime.fromisoformat(route_info_list[current_dis_index-1]['datetime'])
                point2_dtobj = datetime.datetime.fromisoformat(route_info_list[current_dis_index]['datetime'])
                
                seg_total_sec = (point2_dtobj - point1_dtobj).total_seconds()
                elapsed_sec = (target_dtobj - point1_dtobj).total_seconds()
                
                time_ratio = 0.0
                if seg_total_sec > 0:
                        time_ratio = max(0.0, min(1.0, elapsed_sec / seg_total_sec))
                
                write_lat = pos1_lat + (pos2_lat - pos1_lat) * time_ratio
                write_lon = pos1_lon + (pos2_lon - pos1_lon) * time_ratio

                points.append({
                    "dt": target_dtobj,
                    "lat": write_lat,
                    "lon": write_lon
                })
                
                target_dtobj = target_dtobj + datetime.timedelta(seconds=target_unit_eptime)

        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        meta_cache = {}
        npy_cache = {}

        def get_meta(surf):
            key = surf
            if key not in meta_cache:
                meta_cache[key] = fetch_meta_data(bucket, ini_time_str, surf)
            return meta_cache[key]

        def get_npy(surf, t_str):
            key = (surf, t_str)
            if key not in npy_cache:
                npy_cache[key] = fetch_npy_data(bucket, ini_time_str, surf, t_str)
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

            time_str_jst = dt_jst.strftime("%Y-%m-%dT%H:%M:%S+09:00")
            
            if time_str_jst not in request_json['information']:
                request_json['information'][time_str_jst] = {
                    'latitude': format(lat, '.4f'),
                    'longitude': format(lon, '.4f')
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
                    
                    if (min(lat1, lat2) <= lat <= max(lat1, lat2)) and (min(lon1, lon2) <= lon <= max(lon1, lon2)):
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
                        
                        if val_start is not None and val_next is not None:
                            calc_gpv = val_start + (val_next - val_start) * rate
                            request_json['information'][time_str_jst][surf][elem] = format(calc_gpv, '.1f')
                        elif val_next is not None:
                            request_json['information'][time_str_jst][surf][elem] = format(val_next, '.1f')
                        elif val_start is not None:
                            request_json['information'][time_str_jst][surf][elem] = format(val_start, '.1f')

        for dt_jst, info_dict in request_json['information'].items():
                for key, val_dict in info_dict.items():
                        if isinstance(val_dict, dict):
                                ta_str = val_dict.get('0_0')
                                rh_str = val_dict.get('1_1')
                                u_str = val_dict.get('2_2')
                                v_str = val_dict.get('2_3')
                                sr_str = val_dict.get('4_7')

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

        return request_json

def latlng_to_xyz(lat, lng):
        rlat, rlng = radians(lat), radians(lng)
        coslat = cos(rlat)
        return coslat*cos(rlng), coslat*sin(rlng), sin(rlat)

def dist_on_sphere(pos0, pos1, radious=earth_rad):
        xyz0, xyz1 = latlng_to_xyz(*pos0), latlng_to_xyz(*pos1)
        return acos(sum(x * y for x, y in zip(xyz0, xyz1)))*radious

def decode_polyline(polyline_str):
    index, lat, lng = 0, 0, 0
    coordinates = []
    length = len(polyline_str)
    while index < length:
        shift, result = 0, 0
        while True:
            b = ord(polyline_str[index]) - 63
            index += 1
            result |= (b & 0x1f) << shift
            shift += 5
            if b < 0x20:
                break
        dlat = ~(result >> 1) if (result & 1) else (result >> 1)
        lat += dlat
        shift, result = 0, 0
        while True:
            b = ord(polyline_str[index]) - 63
            index += 1
            result |= (b & 0x1f) << shift
            shift += 5
            if b < 0x20:
                break
        dlng = ~(result >> 1) if (result & 1) else (result >> 1)
        lng += dlng
        coordinates.append((lat / 1e5, lng / 1e5))
    return coordinates