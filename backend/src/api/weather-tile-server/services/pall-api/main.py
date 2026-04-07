import io
import os
import sys
import time
import asyncio
import json
import math
from datetime import datetime
from typing import Optional

import numpy as np
import mercantile
from PIL import Image
from fastapi import FastAPI, Response, HTTPException, Query, Depends, Header
from cachetools import TTLCache
import gcsfs
import zarr

# プロジェクトルートをパスに追加
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from shared.config import ENV_MODE, API_SECRET_KEY

# GCSバケット名
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "weather-tile-data-bucket") # 適宜変更

# --- キャッシュ設定 ---
# TTL キャッシュを使用して数分〜数十分のインメモリキャッシュを保持
cache_dimension_map = TTLCache(maxsize=50, ttl=1800)        # 30分キャッシュ
cache_zarr_data = TTLCache(maxsize=200, ttl=600)        # 10分キャッシュ

# --- 排他制御用ロック ---
# 各キーに対して [asyncio.Lock(), 待機カウント] を保持する
locks_dimension_map = {}
locks_zarr_data = {}

GCP_PROJECT = os.getenv("GCP_PROJECT", "weather-tile-project")

# クライアントの初期化 (プロジェクトIDを明示的に渡して401エラー回避！)
gcs_fs = gcsfs.GCSFileSystem(project=GCP_PROJECT)

app = FastAPI()

async def verify_api_key(x_api_key: Optional[str] = Header(None)):
    """APIキーの検証を行う。ENV_MODE が production の場合のみチェックする。"""
    if ENV_MODE == "production":
        if x_api_key != API_SECRET_KEY:
            raise HTTPException(status_code=403, detail="Forbidden: Invalid API Key")
    return x_api_key

async def get_dimension_map(initial_time: str) -> dict:
    """GCSからdimension_map.jsonを取得する"""
    cache_key = f"pall_{initial_time}"
    
    if cache_key in cache_dimension_map:
        return cache_dimension_map[cache_key]
        
    if cache_key not in locks_dimension_map:
        locks_dimension_map[cache_key] = [asyncio.Lock(), 0]
        
    lock_entry = locks_dimension_map[cache_key]
    lock_entry[1] += 1
    
    try:
        async with lock_entry[0]:
            if cache_key in cache_dimension_map:
                return cache_dimension_map[cache_key]
                
            path = f"gs://{GCS_BUCKET_NAME}/pall/{initial_time}/dimension_map.json"
            try:
                # 同期メソッドを非同期スレッドで実行
                def fetch_json():
                    with gcs_fs.open(path, 'r') as f:
                        return json.load(f)
                
                dimension_map_data = await asyncio.to_thread(fetch_json)
                cache_dimension_map[cache_key] = dimension_map_data
                return dimension_map_data
                
            except FileNotFoundError:
                raise HTTPException(status_code=404, detail="dimension_map.json not found")
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"GCS error reading dimension_map.json: {str(e)}")
    finally:
        lock_entry[1] -= 1
        if lock_entry[1] == 0:
            locks_dimension_map.pop(cache_key, None)

# 💡 level_idx を Optional[int] から Optional[str] に変更するかも検討だが、
# ここでは level_idx という変数名を維持しつつ、パスの構築ロジックを変える
async def get_zarr_data(initial_time: str, foc_time_str: str, element: str, level_str: Optional[str], rmin: int, rmax: int, cmin: int, cmax: int) -> np.ndarray:
    """Zarrから配列データを取得する"""
    cache_key = f"pall_{initial_time}_{foc_time_str}_{element}_{level_str}_{rmin}_{rmax}_{cmin}_{cmax}"
    
    if cache_key in cache_zarr_data:
        return cache_zarr_data[cache_key]
        
    if cache_key not in locks_zarr_data:
        locks_zarr_data[cache_key] = [asyncio.Lock(), 0]
        
    lock_entry = locks_zarr_data[cache_key]
    lock_entry[1] += 1
    
    try:
        async with lock_entry[0]:
            if cache_key in cache_zarr_data:
                return cache_zarr_data[cache_key]
                
            # 💡 Zarrのパス構築を変更
            # もし level_str (例: "850hPa") が指定されていれば、そのディレクトリの .zarr を見に行く
            # 指定されていなければ pall.zarr を見に行く（互換性や2次元データ用など）
            if level_str:
                path = f"gs://{GCS_BUCKET_NAME}/pall/{initial_time}/{foc_time_str}/{level_str}.zarr"
            else:
                path = f"gs://{GCS_BUCKET_NAME}/pall/{initial_time}/{foc_time_str}/pall.zarr"
                
            try:
                def fetch_zarr():
                    # gcsfs を使った Zarr グループの読み込み
                    store = gcsfs.GCSMap(path, gcs=gcs_fs, check=False)
                    # 💡 Zarr V3 形式なら zarr_version=3 を追加！
                    root = zarr.open_group(store, mode="r", zarr_version=3)
                    
                    if element not in root:
                        return None
                        
                    arr = root[element]
                    
                    # 各高度ごとのZarrであれば、基本的にデータは2次元（nlat, nlon）になっているはず
                    return np.array(arr[rmin:rmax+1, cmin:cmax+1])
                    
                data_np = await asyncio.to_thread(fetch_zarr)
                if data_np is None:
                    return np.array([])
                    
                cache_zarr_data[cache_key] = data_np
                return data_np
                
            except Exception as e:
                # Zarrが開けない、またはエラーの場合は空を返す
                print(f"Zarr Error: {e}", file=sys.stderr)
                return np.array([])
    finally:
        lock_entry[1] -= 1
        if lock_entry[1] == 0:
            locks_zarr_data.pop(cache_key, None)

def apply_colormap(data: np.ndarray, element: str) -> Image.Image:
    """データを色付けする。NaNは透明。"""
    mask_nan = np.isnan(data)
    
    # 物理値への変換 (JMA MSMの気温はケルビン単位のため摂氏に変換)
    val = data.copy()
    if element.startswith("TMP"): # TMP_850 などの場合
        val -= 273.15
    elif element == "0_0":
        val -= 273.15
        
    rgba = np.zeros((*data.shape, 4), dtype=np.uint8)

    # --- Androidアプリとの色合わせ（完全一致版） ---
    
    if element == "ssi":
        rgba[val <= 0]   = [255, 235, 59, 255]  # 黄色 (0xFFFFEB3B)
        rgba[val <= -3]  = [255, 152, 0, 255]   # オレンジ (0xFFFF9800)
        rgba[val <= -6]  = [244, 67, 54, 255]   # 赤 (0xFFF44336)
        rgba[val <= -9]  = [156, 39, 176, 255]  # 紫 (0xFF9C27B0)
        
    elif element == "ki":
        rgba[val >= 15]  = [76, 175, 80, 255]   # 緑 (0xFF4CAF50)
        rgba[val >= 26]  = [255, 235, 59, 255]  # 黄色 (0xFFFFEB3B)
        rgba[val >= 31]  = [255, 152, 0, 255]   # オレンジ (0xFFFF9800)
        rgba[val >= 36]  = [244, 67, 54, 255]   # 赤 (0xFFF44336)
        rgba[val >= 40]  = [156, 39, 176, 255]  # 紫 (0xFF9C27B0)
        
    elif element == "theta_e":
        rgba[val >= 330] = [255, 152, 0, 255]   # オレンジ (0xFFFF9800)
        rgba[val >= 340] = [244, 67, 54, 255]   # 赤 (0xFFF44336)
        rgba[val >= 345] = [156, 39, 176, 255]  # 紫 (0xFF9C27B0)
        
    elif element == "water_vapor_flux":
        rgba[val >= 150] = [76, 175, 80, 255]   # 緑 (0xFF4CAF50)
        rgba[val >= 200] = [255, 235, 59, 255]  # 黄 (0xFFFFEB3B)
        rgba[val >= 250] = [255, 152, 0, 255]   # オレンジ (0xFFFF9800)
        rgba[val >= 300] = [244, 67, 54, 255]   # 赤 (0xFFF44336)
        rgba[val >= 350] = [156, 39, 176, 255]  # 紫 (0xFF9C27B0)
        
    elif element == "cape":
        rgba[val > 0]     = [255, 235, 59, 255] # 黄色 (0xFFFFEB3B)
        rgba[val >= 1000] = [255, 152, 0, 255]  # オレンジ (0xFFFF9800)
        rgba[val >= 2500] = [244, 67, 54, 255]  # 赤 (0xFFF44336)
        rgba[val >= 3500] = [156, 39, 176, 255] # 紫 (0xFF9C27B0)
        
    elif element == "tt":
        rgba[val >= 44]  = [76, 175, 80, 255]   # 緑 (0xFF4CAF50)
        rgba[val >= 46]  = [255, 235, 59, 255]  # 黄色 (0xFFFFEB3B)
        rgba[val >= 50]  = [255, 152, 0, 255]   # オレンジ (0xFFFF9800)
        rgba[val >= 52]  = [244, 67, 54, 255]   # 赤 (0xFFF44336)
        rgba[val >= 60]  = [156, 39, 176, 255]  # 紫 (0xFF9C27B0)
    elif element == "cin":
        # CIN: 0で無色、-300に近づくほど青を強くするシームレスなグラデーション
        # 0のとき0.0、-300以下のとき1.0になる「強さ(intensity)」の係数を一括計算
        intensity = np.clip(val / -300.0, 0, 1)
        rgba[..., 0] = 0     # R (赤)
        rgba[..., 1] = 80    # G (緑を少し混ぜると綺麗な空色/シアン寄りになります。純粋な青なら0)
        rgba[..., 2] = 255   # B (青)
        # 透明度(Alpha)を intensity に応じて 0(透明) 〜 200(濃い半透明) の間で変化させる
        # ※最大値を200にしているのは、-300でも下の地図がわずかに透けて見えるようにするためです。
        rgba[..., 3] = (intensity * 200).astype(np.uint8)
        # フタがない（値が0以上）の場所は完全に透明(0)にする
        rgba[val >= 0, 3] = 0
    elif element == "lcl":
        # LCL (高度/m): 500m以下を赤、1500mまでグラデーション、1500mより上は透明
        # ※ val が「高度(メートル)」であることを前提としています。
        # 500m以下で 1.0 (濃い)、1500m以上で 0.0 (透明) になるように逆算
        intensity = np.clip((1500.0 - val) / 1000.0, 0, 1)
        # 色は「危険な赤」に固定
        rgba[..., 0] = 244   # R (赤)
        rgba[..., 1] = 67    # G
        rgba[..., 2] = 54    # B
        # 透明度(Alpha)を intensity に応じて 0(透明) 〜 200(濃い半透明) で変化させる
        rgba[..., 3] = (intensity * 200).astype(np.uint8)
        # 1500mより高い（安全な）場所は完全に透明にする
        rgba[val > 1500, 3] = 0
    elif element == "lfc":
        # LFC (高度/m): 1000m以下を赤、3000mまでグラデーション、3000mより高いところは透明
        # ※ val が「高度(メートル)」であることを前提としています。
        # 1000m以下で 1.0 (濃い)、3000m以上で 0.0 (透明) になるように計算
        intensity = np.clip((3000.0 - val) / 2000.0, 0, 1)
        # 色はLCLと同じ赤系、または少しオレンジを混ぜると区別しやすくなります
        rgba[..., 0] = 255   # R
        rgba[..., 1] = 110   # G (少し明るめのオレンジ赤)
        rgba[..., 2] = 0     # B
        # 透明度を 0 〜 200 で変化
        rgba[..., 3] = (intensity * 200).astype(np.uint8)
        # 3000mより高い場所は透明
        rgba[val > 3000, 3] = 0
    elif element == "el":
        # EL (高度/m): 5,000m以上で色が出始め、12,000m以上で真っ赤になる
        # ※ val が「高度(メートル)」であることを前提としています。
        # 5,000m以下で 0.0 (透明)、12,000m以上で 1.0 (濃い) になるように計算
        intensity = np.clip((val - 5000.0) / 7000.0, 0, 1)
        # 色は「猛烈な発達」を意味する深紅や、少し紫を混ぜた赤にします
        rgba[..., 0] = 200   # R
        rgba[..., 1] = 0     # G
        rgba[..., 2] = 100   # B (少し紫を入れると、成層圏に届くヤバさが出ます)
        # 透明度を 0 〜 200 で変化
        rgba[..., 3] = (intensity * 200).astype(np.uint8)
        # 5,000mより低い（発達しない）場所は完全に透明
        rgba[val < 5000, 3] = 0
    else:
        vmin, vmax = 0.0, 100.0
        if element == "0_0" or element.startswith("TMP"): vmin, vmax = -10.0, 35.0
        
        norm = np.clip((val - vmin) / (vmax - vmin), 0, 1)
        rgba[..., 0] = (norm * 255).astype(np.uint8)
        rgba[..., 1] = ((1 - np.abs(norm - 0.5) * 2) * 255).astype(np.uint8)
        rgba[..., 2] = ((1 - norm) * 255).astype(np.uint8)
        rgba[..., 3] = 160

    rgba[mask_nan, 3] = 0
    return Image.fromarray(rgba, 'RGBA')

@app.post("/clear-cache", dependencies=[Depends(verify_api_key)])
async def clear_cache():
    """全インメモリキャッシュを消去する"""
    cache_dimension_map.clear()
    cache_zarr_data.clear()
    return {"message": "All caches cleared"}

@app.get("/tiles/pall/{z}/{x}/{y}.webp", dependencies=[Depends(verify_api_key)])
async def get_tile(
    z: int, x: int, y: int,
    target_time: datetime = Query(..., alias="target_time"),
    initial_time: datetime = Query(..., alias="initial_time"),
    element: str = Query(..., alias="element"),
    level: Optional[str] = Query(None, alias="level"),
    surface: Optional[str] = Query(None, alias="surface")
):
    # フロントエンドから受け取った initial_time を文字列フォーマットに変換 (末尾Z)
    init_time_str = initial_time.strftime("%Y%m%d%H%M%SZ")
    
    # ターゲット時刻を文字列フォーマットに変換 (末尾Z)
    foc_time_str = target_time.strftime("%Y%m%d%H%M%SZ")

    # dimension_map.jsonを取得し、パラメータを抽出
    dimension_map = await get_dimension_map(init_time_str)
    
    # dimension_map.json内に該当するフォーキャスト時間が存在するかチェック
    if foc_time_str not in dimension_map:
        # 空タイルを返す
        buf = io.BytesIO()
        Image.new("RGBA", (256, 256), (0, 0, 0, 0)).save(buf, format="WEBP")
        return Response(content=buf.getvalue(), media_type="image/webp", headers={"Cache-Control": "no-cache"})

    # pall 特有のメタデータ構造の取得 (必要に応じて階層をたどる)
    time_meta = dimension_map[foc_time_str]
    pall_meta = time_meta.get("pall", time_meta)
    
    # ✅ バッチ側で作られたPall Zarrは3次元（level, nlat, nlon）です。
    # dimension_map.jsonからlevelsリストを取得し、リクエストされたlevel（例: "850"）のインデックスを検索します。
    # dimension_map.json の構造が {"1000hPa": {...}, "850hPa": {...}, "pall": {...}} のようになっている場合、
    # time_meta のキーから levels_list を生成する
    levels_list = pall_meta.get("levels", [])
    if not levels_list:
        # "850hPa" などのキーから数値部分を抽出してソートしたリストを作る（"pall" などを除外）
        extracted_levels = []
        for k in time_meta.keys():
            nums = ''.join(filter(str.isdigit, k))
            if nums:
                extracted_levels.append(int(nums))
        # 通常、Zarrのインデックスは降順(1000->100)または昇順で格納されているはずだが、
        # levelsキーが存在しないということは、このデータセットは2次元の可能性が高いか、
        # levelsの配列が別途どこかにある。
        # 今回のエラーは levels_list が空([])のため、`target_level in levels_list` が False になり
        # level_idx が None のまま進んでしまったが、Zarr側は3次元だったため発生したと考えられる。
        # まずは levels_list を抽出したものにしてみる。Zarrの順番と一致しないと困るが...
        # 実際には2次元データの場合、Zarrから2次元として正しく取れるかが問題。
        levels_list = sorted(extracted_levels, reverse=True) # 1000 -> 100 の順と仮定

    # 💡 level が None の場合は None のままにする（エラーにしない！）
    level_str = None
    req_level = level or surface
    if req_level is not None:
        try:
            # "850hPa" などの文字列から数字だけを抽出
            num_str = ''.join(filter(str.isdigit, str(req_level)))
            if num_str:
                target_level = int(num_str)
                # target_level が 850 のとき、 "850hPa" という文字列を復元、または最初から req_level を使う
                # ここでは dimension_map.json 内のキー(例: "850hPa")に合わせてフォーマットする
                level_str = f"{target_level}hPa"
                
                # levels_list (例: [1000, 975, 850...]) に存在するかチェック
                if target_level not in levels_list:
                    print(f"Warning: target_level {target_level} not found in levels_list {levels_list}", file=sys.stderr)
                    # 2次元データなど、指定した高度が存在しなくてもそのまま進める場合は
                    # level_str を None にして pall.zarr を見に行かせる
                    level_str = None
        except (ValueError, TypeError) as e:
            raise HTTPException(status_code=400, detail=f"Invalid level/surface: {req_level}. Error: {str(e)}")

    # bboxとgridから必要な座標情報を取得
    # 指定された高度のメタデータがあればそれを使う（なければ pall フォールバック）
    target_meta = time_meta.get(level_str, pall_meta) if level_str else pall_meta
    bbox = target_meta.get("bbox", {})
    grid = target_meta.get("grid", {})
    
    try:
        lat1 = bbox["lat1"]
        lon1 = bbox["lon1"]
        lat2 = bbox["lat2"]
        lon2 = bbox["lon2"]
        
        # ✅ 旧 u_lat / u_lon を廃止し、nlat / nlon を取得します
        nlat = grid.get("nlat")
        nlon = grid.get("nlon")
        
        if nlat is None or nlon is None:
            raise HTTPException(status_code=404, detail="Grid metadata (nlat/nlon) missing in dimension_map.json")
            
        # ✅ ny, nx の計算も nlat, nlon に変更します
        ny = math.floor(abs(bbox["lat1"] - bbox["lat2"]) / abs(nlat))
        nx = math.floor(abs(bbox["lon2"] - bbox["lon1"]) / abs(nlon))
    except KeyError as e:
        raise HTTPException(status_code=500, detail=f"Missing metadata parameters: {str(e)}")

    # --- 再投影 ＆ バイリニア補間（今回は最近傍のまま移植） ---
    
    xy_bounds = mercantile.xy_bounds(x, y, z)
    steps = np.linspace(0, 1, 256, endpoint=False) + (1.0 / 512.0)
    x_coords = xy_bounds.left + (xy_bounds.right - xy_bounds.left) * steps
    y_coords = xy_bounds.top + (xy_bounds.bottom - xy_bounds.top) * steps
    x_grid, y_grid = np.meshgrid(x_coords, y_coords)
    
    R = 6378137.0
    lon_grid = np.degrees(x_grid / R)
    lat_grid = np.degrees(2 * np.arctan(np.exp(y_grid / R)) - np.pi / 2)
    
    lon_shifted = (lon_grid - 120.0 + 180.0) % 360.0 - 180.0 + 120.0
    
    # Zarrデータのインデックス算出
    # MSMなどでは北から南へ緯度が振られる場合があるので、nlat の符号等に注意
    row_f = (lat1 - lat_grid) / nlat
    col_f = (lon_shifted - lon1) / nlon
    
    h, w = ny, nx
    
    r0 = np.floor(row_f + 0.5).astype(int)
    c0 = np.floor(col_f + 0.5).astype(int)
    
    valid_mask = (row_f >= -0.5) & (row_f <= (h - 1) + 0.5) & \
                 (col_f >= -0.5) & (col_f <= (w - 1) + 0.5)
    
    r0_s = np.clip(r0, 0, h - 1)
    c0_s = np.clip(c0, 0, w - 1)
    
    rmin, rmax = int(np.min(r0_s)), int(np.max(r0_s))
    cmin, cmax = int(np.min(c0_s)), int(np.max(c0_s))
    
    # Zarrから必要な部分データを取得 (level_strを渡す)
    sub_data = await get_zarr_data(init_time_str, foc_time_str, element, level_str, rmin, rmax, cmin, cmax)
    if sub_data.size == 0:
        buf = io.BytesIO()
        Image.new("RGBA", (256, 256), (0, 0, 0, 0)).save(buf, format="WEBP")
        return Response(content=buf.getvalue(), media_type="image/webp", headers={"Cache-Control": "no-cache"})
    
    sampled_data = sub_data[r0_s - rmin, c0_s - cmin]
    sampled_data[~valid_mask] = np.nan
    
    # 画像化
    img = apply_colormap(sampled_data, element)
    
    buf = io.BytesIO()
    img.save(buf, format="WEBP", lossless=True)
    return Response(
        content=buf.getvalue(), 
        media_type="image/webp",
        headers={"Cache-Control": "public, max-age=31536000, s-maxage=31536000, immutable"}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))