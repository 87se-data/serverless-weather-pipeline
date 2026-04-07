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
import urllib.request
import urllib.parse
import urllib.error
from google.cloud import storage
from google.cloud import datastore
import zstandard as zstd
import numpy as np
import io
import concurrent.futures

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


# 複数のクエリパラメータを受け取るAPIエンドポイント
@app.get("/data/")
def get_route_points_data_api(
        origin: str, 
        destination: str, 
        means: Optional[str] = "driving", 
        departure: Optional[int] = None,
        gpv: Optional[str] = 'MSM_GPV_Rjp_Lsurf',
        element: Optional[str] = "1_8,0_0,1_1,2_2,2_3,4_7",
        surface: Optional[str] = 'surface'
):
        """
        HTTPクエリから複数のパラメータを受け取り、GCSからデータを取得する
        """
        if departure is None:
                departure = int(time.time())
        if not origin:
                return {"error": "Query parameter 'origin' is required"}
        if not destination:
                return {"error": "Query parameter 'destination' is required"}

        request_json = {}
        request_json['origin'] = origin
        request_json['destination'] = destination
        request_json['means'] = means
        request_json['departure_eptime'] = departure
        request_json['gpv'] = gpv
        request_json['element'] = element
        request_json['surface'] = surface

        # 移動手段をRoutes APIの形式に変換
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

        # 出発時刻をRoutes API用の形式(RFC3339 UTC)に変換
        dt_utc = datetime.datetime.fromtimestamp(departure, tz=timezone('UTC'))
        departure_time_str = dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

        def build_waypoint(point_str: str):
                # カンマが含まれてたら緯度経度（LatLng）として処理
                if "," in point_str:
                        try:
                                lat, lng = map(float, point_str.split(","))
                                return {
                                        "location": {
                                                "latLng": {
                                                        "latitude": lat,
                                                        "longitude": lng
                                                }
                                        }
                                }
                        except ValueError:
                                pass # もし数字じゃなかったら下のaddressとして処理

                # カンマがない、または数字じゃない場合は住所（address）として処理
                return {"address": point_str}

        # 最低でも現在時刻から1分後(未来)にする
        current_time = int(time.time())
        api_departure_time = departure if departure > current_time else current_time + 60
        
        # APIに送る用の未来時間を文字列にする
        api_dt_utc = datetime.datetime.fromtimestamp(api_departure_time, tz=timezone('UTC'))
        api_departure_time_str = api_dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

        # リクエストボディのベース
        body = {
                "origin": build_waypoint(origin),
                "destination": build_waypoint(destination),
                "travelMode": travel_mode,
                "languageCode": "ja"
        }

        # 移動手段によって設定を追加
        if travel_mode == "DRIVE":
                body["routingPreference"] = "TRAFFIC_AWARE"
                body["departureTime"] = api_departure_time_str
        elif travel_mode == "TRANSIT":
                body["departureTime"] = api_departure_time_str

        headers = {
                'Content-Type': 'application/json',
                'X-Goog-Api-Key': api_key,
                'X-Goog-FieldMask': 'routes.legs.steps.startLocation,routes.legs.steps.endLocation,routes.legs.steps.staticDuration,routes.legs.steps.transitDetails'
        }

        # POSTリクエストでRoutes APIを実行
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

        # ルートが見つからなかった時のクラッシュ防止
        if not directions.get('routes'):
                print("ルート情報が空っぽ！")
                return {"error": "ルートが取得できなかったよ😭 (道がないか遠すぎるかも)"}

        # datetime(JST)の準備
        target_dtobj = datetime.datetime.fromtimestamp(departure, tz=timezone('Asia/Tokyo'))
        request_json['Information'] = []
        
        # stepsのデータを取り出す
        steps = directions['routes'][0].get('legs', [{}])[0].get('steps', [])

        for content_dict in steps:
                input_dic = {}
                
                # 電車(TRANSIT)の時はstartLocationが消えるトラップを回避！
                start_loc = content_dict.get('startLocation')
                if not start_loc and 'transitDetails' in content_dict:
                        stop_details = content_dict['transitDetails'].get('stopDetails', {})
                        start_loc = stop_details.get('departureStop', {}).get('location')
                
                if start_loc and 'latLng' in start_loc:
                        input_dic['latitude'] = start_loc['latLng']['latitude']
                        input_dic['longitude'] = start_loc['latLng']['longitude']
                else:
                        print("座標が見つからなかったよ😭 スキップするね！")
                        continue

                input_dic['datetime'] = "%s" % target_dtobj.isoformat(timespec='seconds')
                request_json['Information'].append(input_dic)

                # 所要時間を足して、次の地点への時間を計算（floatにして小数の秒数に対応）
                duration_str = content_dict.get('duration', content_dict.get('staticDuration', '0s'))
                duration_sec = float(duration_str.replace('s', ''))
                target_dtobj += datetime.timedelta(seconds=duration_sec)

        # ループが終わった後（一番最後）に、最終目的地（endLocation）を追加する！
        if steps: # stepsが空っぽじゃない時だけ
                last_step = steps[-1]
                final_dic = {}
                
                # 最終目的地もtransitDetailsを考慮！
                end_loc = last_step.get('endLocation')
                if not end_loc and 'transitDetails' in last_step:
                        stop_details = last_step['transitDetails'].get('stopDetails', {})
                        end_loc = stop_details.get('arrivalStop', {}).get('location')

                if end_loc and 'latLng' in end_loc:
                        final_dic['latitude'] = end_loc['latLng']['latitude']
                        final_dic['longitude'] = end_loc['latLng']['longitude']
                        final_dic['datetime'] = "%s" % target_dtobj.isoformat(timespec='seconds')
                        request_json['Information'].append(final_dic)
                else:
                        print("最終目的地の座標が見つからなかったよ😭")

        # GCSからのデータ取得処理へ
        return get(request_json)


def get(route_data):
        start = time.time()

        draw_num = 200
        
        # =====================================================================
        # 1. Datastoreから最新の初期時刻を取得する
        # =====================================================================
        try:
                datastore_client = datastore.Client()
                query = datastore_client.query(kind='store-gcs-msm-surf-db')
                query.order = ['-datetime']
                results = list(query.fetch(limit=1))
                
                if not results:
                        return {"error": "Datastoreに有効な気象データの初期時刻がありません"}
                initial_datetime = results[0]['datetime']
        except Exception as e:
                print(f"Datastore Query Error: {e}")
                return {"error": "Datastoreクエリ中にエラーが発生しました"}

        # initial_datetimeをUTCに変換して文字列化
        if initial_datetime.tzinfo is None:
                # タイムゾーン情報がない場合はJSTとして扱う
                initial_datetime = timezone('Asia/Tokyo').localize(initial_datetime)
        
        ini_dt_obj_utc = initial_datetime.astimezone(timezone('UTC'))
        ini_datetime_utc = ini_dt_obj_utc.strftime("%Y%m%d%H0000Z")

        # =====================================================================
        # 2. GCSクライアントとメタデータのロード
        # =====================================================================
        gcs_client = storage.Client()
        bucket = gcs_client.bucket(GCS_BUCKET_NAME)

        meta_blob = bucket.blob(f"surf/{ini_datetime_utc}/dimension_map.json")
        try:
                meta_json_str = meta_blob.download_as_text()
                meta_data = json.loads(meta_json_str)
        except Exception as e:
                print(f"メタデータ取得エラー: {e}")
                return {"error": "メタデータの取得に失敗しました"}
        
        first_time_key = next(iter(meta_data))
        surface_meta = meta_data.get(first_time_key, {}).get('surface', {})
        # ?? element_index は surface の中にあるので、surface_meta から取得する
        element_index = surface_meta.get('element_index', {})
        bbox = surface_meta.get('bbox')
        grid = surface_meta.get('grid')
        
        if not bbox or not grid or len(bbox) < 4 or len(grid) < 4:
                return {"error": "メタデータの形式が不正です"}
        
        if isinstance(bbox, dict):
                lat1 = float(bbox['lat1'])
                lon1 = float(bbox['lon1'])
                lat2 = float(bbox['lat2'])
                lon2 = float(bbox['lon2'])
        else:
                lat1, lon1, lat2, lon2 = map(float, bbox)
                
        if isinstance(grid, dict):
                nlat = float(grid['nlat'])
                nlon = float(grid['nlon'])
                ny = int(grid['ny'])
                nx = int(grid['nx'])
        else:
                nlat = float(grid[0])
                nlon = float(grid[1])
                ny = int(grid[2])
                nx = int(grid[3])

        # ルートを時刻順に整理する
        route_info_list = sorted(route_data['Information'], key=lambda x:x['datetime'])
        if not route_info_list:
                return {"error": "Route information is empty"}

        # 時間と移動経路から、各時刻の緯度経度を求める
        total_distance = 0.0
        distance_list = []      # 区間ごとの距離リスト
        speed_list = [] # 区間ごとの速度リスト(秒速)
        for index in range(len(route_info_list)):
                distance = 0.0
                if index == 0:
                        distance_list.append(0.0)
                        speed_list.append(0.0)
                        continue
                pos1_list = [route_info_list[index-1]['latitude'],route_info_list[index-1]['longitude']]
                pos2_list = [route_info_list[index]['latitude'],route_info_list[index]['longitude']]
                if pos1_list[0] != pos2_list[0] or pos1_list[1] != pos2_list[1]:
                        distance = dist_on_sphere( [float(pos1_list[0]),float(pos1_list[1])],[float(pos2_list[0]),float(pos2_list[1])] )
                distance_list.append(distance)
                time1_dtobj = datetime.datetime.fromisoformat(route_info_list[index-1]['datetime'])
                time2_dtobj = datetime.datetime.fromisoformat(route_info_list[index]['datetime'])
                delta = time2_dtobj - time1_dtobj
                delta_seconds = delta.total_seconds()
                if delta_seconds > 0:
                        speed_list.append( distance / delta_seconds )
                else:
                        speed_list.append( 0.0 )
                total_distance += distance
                
        departure_dtobj = datetime.datetime.fromisoformat(route_info_list[0]['datetime'])
        arrival_dtobj = datetime.datetime.fromisoformat(route_info_list[len(route_info_list)-1]['datetime'])
        total_eptime = arrival_dtobj.timestamp() - departure_dtobj.timestamp()
        
        check_point_dict = {}
        check_point_dict_rain = {}
        target_unit_eptime = total_eptime / draw_num
        target_dtobj = departure_dtobj
        advanced_distance = 0.0
        
        for _ in range(draw_num + 1):
                target_unit_distance = 0.0
                target_dtobj_utc = target_dtobj.astimezone(timezone('UTC'))
                target_time = target_dtobj_utc.strftime("%Y%m%d%H0000Z")
                target_dtobj_utc_rain = target_dtobj_utc + datetime.timedelta(hours=1)
                target_time_rain = target_dtobj_utc_rain.strftime("%Y%m%d%H0000Z")
                if check_point_dict.get(target_time) == None:
                        check_point_dict[target_time] = []
                if check_point_dict_rain.get(target_time_rain) == None:
                        check_point_dict_rain[target_time_rain] = []
                        
                if advanced_distance == 0.0:
                        check_point_dict[target_time].append([target_dtobj.strftime("%Y-%m-%dT%H:%M:%S+09:00"),route_info_list[0]['latitude'],route_info_list[0]['longitude']])
                        check_point_dict_rain[target_time_rain].append([target_dtobj.strftime("%Y-%m-%dT%H:%M:%S+09:00"),route_info_list[0]['latitude'],route_info_list[0]['longitude']])
                        target_unit_distance = speed_list[1] * target_unit_eptime
                else:
                        tmp_distance = 0.0
                        for dis_index in range( len(distance_list) ):
                                tmp_distance += distance_list[dis_index]
                                point_dtobj = datetime.datetime.fromisoformat(route_info_list[dis_index]['datetime'])
                                if tmp_distance >= advanced_distance:
                                        break
                                if target_dtobj <= point_dtobj:
                                        break
                        overflow_distance = advanced_distance - (tmp_distance - distance_list[dis_index])
                        pos1_lat = route_info_list[dis_index-1]['latitude']
                        pos1_lon = route_info_list[dis_index-1]['longitude']
                        pos2_lat = route_info_list[dis_index]['latitude']
                        pos2_lon = route_info_list[dis_index]['longitude']
                        diff_lat = pos2_lat - pos1_lat
                        diff_lon = pos2_lon - pos1_lon
                        overflow_distance_rate = 0.0
                        if distance_list[dis_index] != 0:
                                overflow_distance_rate = overflow_distance / distance_list[dis_index]
                        write_lat = pos1_lat + (diff_lat * overflow_distance_rate)
                        write_lon = pos1_lon + (diff_lon * overflow_distance_rate)

                        check_point_dict[target_time].append([target_dtobj.strftime("%Y-%m-%dT%H:%M:%S+09:00"),write_lat,write_lon])
                        check_point_dict_rain[target_time_rain].append([target_dtobj.strftime("%Y-%m-%dT%H:%M:%S+09:00"),write_lat,write_lon])
                        target_unit_distance = speed_list[dis_index] * target_unit_eptime
                advanced_distance += target_unit_distance
                target_dtobj = target_dtobj + datetime.timedelta(seconds=target_unit_eptime)

        request_json = {}
        request_json['information'] = {}

        max_date_utc = max(check_point_dict.keys())
        min_date_utc = min(check_point_dict.keys())
        max_dtobj_utc = datetime.datetime.strptime(max_date_utc, "%Y%m%d%H%M%SZ").replace(tzinfo=pytz.utc)
        min_dtobj_utc = datetime.datetime.strptime(min_date_utc, "%Y%m%d%H%M%SZ").replace(tzinfo=pytz.utc)
        max_dtobj_utc = max_dtobj_utc + datetime.timedelta(hours=1)
        max_time = max_dtobj_utc.strftime("%Y%m%d%H0000Z")
        check_point_dict[max_time] = []
        min_time = min_dtobj_utc.strftime("%Y%m%d%H0000Z")
        check_point_dict_rain[min_time] = []

        # =====================================================================
        # 3. .npy の並列ダウンロードとキャッシュ
        # =====================================================================
        # 対象時間のリストを取得
        target_times = sorted(list(set(list(check_point_dict.keys()) + list(check_point_dict_rain.keys()))))
        all_required_times = set()
        
        for current_time_str in target_times:
                dt_obj_utc = datetime.datetime.strptime(current_time_str, "%Y%m%d%H%M%SZ").replace(tzinfo=pytz.utc)
                dt_obj_utc_hour_ago = dt_obj_utc - datetime.timedelta(hours=1)
                prev_time_str = dt_obj_utc_hour_ago.strftime("%Y%m%d%H0000Z")
                all_required_times.add(current_time_str)
                all_required_times.add(prev_time_str)

        npy_cache = {}

        def fetch_single_npy(target_time_str):
                npy_blob = bucket.blob(f"surf/{ini_datetime_utc}/{target_time_str}/surface.npy.zst")
                try:
                        buf = npy_blob.download_as_bytes()
                        dctx = zstd.ZstdDecompressor()
                        decompressed_data = dctx.decompress(buf)
                        data = np.load(io.BytesIO(decompressed_data))
                        return data
                except Exception as e:
                        print(f"NPYのダウンロードまたは展開に失敗しました ({target_time_str}): {e}")
                        return None

        # 並列で一気に取得
        with concurrent.futures.ThreadPoolExecutor() as executor:
                future_to_time = {executor.submit(fetch_single_npy, t_str): t_str for t_str in all_required_times}
                for future in concurrent.futures.as_completed(future_to_time):
                        t_str = future_to_time[future]
                        try:
                                npy_cache[t_str] = future.result()
                        except Exception as exc:
                                print(f"{t_str} generated an exception: {exc}")
                                npy_cache[t_str] = None

        # リクエストされている要素
        requested_elements = route_data['element'].split(',')

        # =====================================================================
        # 4. 座標からインデックスへの変換と値の抽出
        # =====================================================================
        for current_time_str in target_times:
                dt_obj_utc = datetime.datetime.strptime(current_time_str, "%Y%m%d%H%M%SZ").replace(tzinfo=pytz.utc)
                dt_obj_utc_hour_ago = dt_obj_utc - datetime.timedelta(hours=1)
                prev_time_str = dt_obj_utc_hour_ago.strftime("%Y%m%d%H0000Z")

                # この時間の npy データを取得
                curr_data = npy_cache.get(current_time_str)
                prev_data = npy_cache.get(prev_time_str)

                # 対象のチェックポイントリスト
                check_points_curr = check_point_dict.get(current_time_str, [])
                check_points_prev = check_point_dict.get(prev_time_str, [])
                check_points_prev_rain = check_point_dict_rain.get(prev_time_str, [])

                # 値を格納するヘルパー関数
                def update_json(dt_jst, lat, lon, element, val, is_interpolation=False):
                        if dt_jst not in request_json['information']:
                                request_json['information'][dt_jst] = {
                                        'latitude': format(lat, '.4f'),
                                        'longitude': format(lon, '.4f'),
                                        'surface': {}
                                }
                        if 'surface' not in request_json['information'][dt_jst]:
                                request_json['information'][dt_jst]['surface'] = {}
                        
                        if is_interpolation:
                                # 時間案分のための初期値をセット
                                if f"{element}_start" not in request_json['information'][dt_jst]['surface']:
                                        request_json['information'][dt_jst]['surface'][f"{element}_start"] = val
                        else:
                                request_json['information'][dt_jst]['surface'][element] = val


                # 前時間の値を取得 (補間用スタート値)
                if prev_data is not None:
                        for point_info in check_points_prev:
                                dt_jst = point_info[0]
                                lat = point_info[1]
                                lon = point_info[2]
                                
                                # 範囲外チェック
                                if lat > lat1 or lat < lat2 or lon < lon1 or lon > lon2:
                                        continue

                                row = int(round((lat1 - lat) / nlat))
                                col = int(round((lon - lon1) / nlon))
                                
                                # 範囲外インデックスチェック
                                if row < 0 or row >= ny or col < 0 or col >= nx:
                                        continue

                                for element in requested_elements:
                                        if element != '1_8' and element in element_index:
                                                z_idx = element_index.get(element)
                                                val = float(prev_data[z_idx, row, col])
                                                if not np.isnan(val):
                                                        update_json(dt_jst, lat, lon, element, val, is_interpolation=True)

                        for point_info in check_points_prev_rain:
                                dt_jst = point_info[0]
                                lat = point_info[1]
                                lon = point_info[2]

                                if lat > lat1 or lat < lat2 or lon < lon1 or lon > lon2:
                                        continue

                                row = int(round((lat1 - lat) / nlat))
                                col = int(round((lon - lon1) / nlon))
                                
                                if row < 0 or row >= ny or col < 0 or col >= nx:
                                        continue

                                if '1_8' in requested_elements and '1_8' in element_index:
                                        z_idx = element_index.get('1_8')
                                        val = float(prev_data[z_idx, row, col])
                                        if not np.isnan(val):
                                                update_json(dt_jst, lat, lon, '1_8', val, is_interpolation=False)

                # 現時間の値を取得 (補間用エンド値、またはそのまま)
                if curr_data is not None:
                        for point_info in check_points_curr:
                                dt_jst = point_info[0]
                                lat = point_info[1]
                                lon = point_info[2]

                                if lat > lat1 or lat < lat2 or lon < lon1 or lon > lon2:
                                        continue

                                row = int(round((lat1 - lat) / nlat))
                                col = int(round((lon - lon1) / nlon))
                                
                                if row < 0 or row >= ny or col < 0 or col >= nx:
                                        continue

                                for element in requested_elements:
                                        if element != '1_8' and element in element_index:
                                                z_idx = element_index.get(element)
                                                val_next = float(curr_data[z_idx, row, col])
                                                if np.isnan(val_next):
                                                        continue
                                                
                                                if dt_jst in request_json['information'] and f"{element}_start" in request_json['information'][dt_jst]['surface']:
                                                        # 線形補間（時間案分）
                                                        val_start = request_json['information'][dt_jst]['surface'][f"{element}_start"]
                                                        target_dtobj = datetime.datetime.fromisoformat(dt_jst)
                                                        hour_ago_dt_utc = dt_obj_utc - datetime.timedelta(hours=1)
                                                        delta = target_dtobj.astimezone(timezone('UTC')) - hour_ago_dt_utc
                                                        rate = delta.total_seconds() / 3600.0
                                                        calc_gpv = val_start + (val_next - val_start) * rate
                                                        update_json(dt_jst, lat, lon, element, format(calc_gpv, '.1f'))
                                                        # 使い終わったスタート値を削除
                                                        del request_json['information'][dt_jst]['surface'][f"{element}_start"]
                                                else:
                                                        update_json(dt_jst, lat, lon, element, format(val_next, '.1f'))

                                        elif element == '1_8' and element in element_index:
                                                pass # 1_8はrain用チェックポイントで取得済み

        # クリーンアップ: 余分な _start キーを削除
        for dt_jst, info_dict in request_json['information'].items():
                if 'surface' in info_dict:
                        keys_to_delete = [k for k in info_dict['surface'].keys() if k.endswith('_start')]
                        for k in keys_to_delete:
                                del info_dict['surface'][k]

        # =====================================================================
        # 5. 体感温度（apparent_temp）とWBGTを計算するロジック
        # =====================================================================
        for dt_jst, info_dict in request_json['information'].items():
                if 'surface' in info_dict:
                        val_dict = info_dict['surface']
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

                                # 体感温度とWBGTをまとめて算出！
                                app_temp, wbgt = calc_thermal_indices(ta_kelvin, rh, wind_speed, solar_rad)
                                
                                if app_temp is not None:
                                        val_dict['apparent_temp'] = format(app_temp, '.1f')
                                if wbgt is not None:
                                        val_dict['wbgt'] = format(wbgt, '.1f')

        elapsed_time = time.time() - start
        # print(f"🚀 GCS Pipeline 処理時間: {elapsed_time:.3f}秒")
        return request_json

# 二点間の緯度経度の距離算出に使用する関数
def latlng_to_xyz(lat, lng):
        rlat, rlng = radians(lat), radians(lng)
        coslat = cos(rlat)
        return coslat*cos(rlng), coslat*sin(rlng), sin(rlat)

def dist_on_sphere(pos0, pos1, radious=earth_rad):
        xyz0, xyz1 = latlng_to_xyz(*pos0), latlng_to_xyz(*pos1)
        return acos(sum(x * y for x, y in zip(xyz0, xyz1)))*radious
