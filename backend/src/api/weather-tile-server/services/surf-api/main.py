import io
import os
import sys
import time
import asyncio
import json
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
locks_dimension_map = {}
locks_zarr_data = {}

# クライアントの初期化
gcs_fs = gcsfs.GCSFileSystem()

app = FastAPI()

async def verify_api_key(x_api_key: Optional[str] = Header(None)):
    """APIキーの検証を行う。ENV_MODE が production の場合のみチェックする。"""
    if ENV_MODE == "production":
        if x_api_key != API_SECRET_KEY:
            raise HTTPException(status_code=403, detail="Forbidden: Invalid API Key")
    return x_api_key

async def get_dimension_map(layer: str, initial_time: str) -> dict:
    """GCSからdimension_map.jsonを取得する"""
    cache_key = f"{layer}_{initial_time}"
    
    if cache_key in cache_dimension_map:
        return cache_dimension_map[cache_key]
        
    if cache_key not in locks_dimension_map:
        locks_dimension_map[cache_key] = asyncio.Lock()
        
    async with locks_dimension_map[cache_key]:
        if cache_key in cache_dimension_map:
            return cache_dimension_map[cache_key]
            
        path = f"gs://{GCS_BUCKET_NAME}/{layer}/{initial_time}/dimension_map.json"
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

async def get_zarr_data(layer: str, initial_time: str, foc_dir_str: str, surface: str, element: str, rmin: int, rmax: int, cmin: int, cmax: int) -> np.ndarray:
    """Zarrから配列データを取得する"""
    cache_key = f"{layer}_{initial_time}_{foc_dir_str}_{surface}_{element}_{rmin}_{rmax}_{cmin}_{cmax}"
    
    if cache_key in cache_zarr_data:
        return cache_zarr_data[cache_key]
        
    if cache_key not in locks_zarr_data:
        locks_zarr_data[cache_key] = asyncio.Lock()
        
    async with locks_zarr_data[cache_key]:
        if cache_key in cache_zarr_data:
            return cache_zarr_data[cache_key]
            
        path = f"gs://{GCS_BUCKET_NAME}/{layer}/{initial_time}/{foc_dir_str}/{surface}.zarr"
        try:
            def fetch_zarr():
                # gcsfs を使った Zarr グループの読み込み
                store = gcsfs.GCSMap(path, gcs=gcs_fs, check=False)
                root = zarr.open_group(store, mode="r")
                if element not in root:
                    return None
                return np.array(root[element][rmin:rmax+1, cmin:cmax+1])
                
            data_np = await asyncio.to_thread(fetch_zarr)
            if data_np is None:
                return np.array([])
                
            cache_zarr_data[cache_key] = data_np
            return data_np
            
        except Exception as e:
            # Zarrが開けない、またはエラーの場合は空を返す
            print(f"Zarr Error: {e}", file=sys.stderr)
            return np.array([])

def apply_colormap(data: np.ndarray, element: str) -> Image.Image:
    """データを色付けする。NaNは透明。"""
    mask_nan = np.isnan(data)
    
    # 物理値への変換 (JMA MSMの気温はケルビン単位のため摂氏に変換)
    val = data.copy()
    if element == "0_0":
        val -= 273.15
        
    rgba = np.zeros((*data.shape, 4), dtype=np.uint8)

    if element == "1_8":
        # 降水量
        rgba[val >= 0.1] = [0x87, 0xCE, 0xEB, 255]
        rgba[val >= 5]   = [0x21, 0x8C, 0xFF, 255]
        rgba[val >= 10]  = [0x00, 0x41, 0xFF, 255]
        rgba[val >= 20]  = [0xFA, 0xF5, 0x00, 255]
        rgba[val >= 30]  = [0xFF, 0x99, 0x00, 255]
        rgba[val >= 50]  = [0xFF, 0x28, 0x00, 255]
        rgba[val >= 80]  = [0xB4, 0x00, 0x68, 255]
    elif element == "wbgt":
        rgba[val >= 25] = [0xFF, 0xEB, 0x3B, 255]
        rgba[val >= 28] = [0xFF, 0x98, 0x00, 255]
        rgba[val >= 31] = [0xFF, 0x00, 0x00, 255]
    elif element == "laundry_index":
        rgba[val >= 60] = [0x4C, 0xAF, 0x50, 255]
        rgba[val >= 80] = [0xFF, 0x40, 0x81, 255]
    elif element == "wind_speed":
        rgba[val >= 0]   = [242, 242, 255, 255]
        rgba[val >= 5]   = [0,   65,  255, 255]
        rgba[val >= 10]  = [250, 245, 0,   255]
        rgba[val >= 15]  = [255, 153, 0,   255]
        rgba[val >= 20]  = [255, 40,  0,   255]
        rgba[val >= 25]  = [180, 0,   104, 255]
    elif element == "0_0":
        rgba[val < -5]   = [0,   32,  1,   255]
        rgba[val >= -5]  = [0,   65,  255, 255]
        rgba[val >= 0]   = [0,   150, 255, 255]
        rgba[val >= 5]   = [185, 235, 255, 255]
        rgba[val >= 10]  = [255, 255, 240, 255]
        rgba[val >= 15]  = [255, 255, 150, 255]
        rgba[val >= 20]  = [250, 245, 0,   255]
        rgba[val >= 25]  = [255, 153, 0,   255]
        rgba[val >= 30]  = [255, 40,  0,   255]
        rgba[val >= 35]  = [180, 0,   104, 255]
    elif element == "1_1":
        norm = np.clip(val / 100.0, 0, 1)
        rgba[..., 0] = (255 - norm * 200).astype(np.uint8)
        rgba[..., 1] = (255 - norm * 150).astype(np.uint8)
        rgba[..., 2] = 255
        rgba[..., 3] = (120 + norm * 100).astype(np.uint8)
    elif element == "4_7":
        norm = np.clip(val / 1000.0, 0, 1)
        rgba[..., 0] = 255
        rgba[..., 1] = (255 - norm * 150).astype(np.uint8)
        rgba[..., 2] = (255 - norm * 255).astype(np.uint8)
        rgba[..., 3] = (norm * 160).astype(np.uint8)
    elif element in ["6_1", "6_3", "6_4", "6_5"]:
        norm = np.clip(val / 100.0, 0, 1)
        gray_val = (255 - norm * 155).astype(np.uint8)
        rgba[..., 0] = gray_val
        rgba[..., 1] = gray_val
        rgba[..., 2] = gray_val
        rgba[..., 3] = (norm * 200).astype(np.uint8)
    else:
        vmin, vmax = 0.0, 100.0
        if element == "0_0": vmin, vmax = -10.0, 35.0
        
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

@app.get("/tiles/{layer}/{z}/{x}/{y}.webp", dependencies=[Depends(verify_api_key)])
async def get_tile(
    layer: str, z: int, x: int, y: int,
    target_time: datetime = Query(..., alias="target_time"),
    initial_time: datetime = Query(..., alias="initial_time"),
    element: str = Query(..., alias="element"),
    surface: str = Query("surface", alias="surface")
):
    # フロントエンドから受け取った initial_time を文字列フォーマットに変換
    init_time_str = initial_time.strftime("%Y%m%d%H%M%SZ")
    
    # ターゲット時刻を文字列フォーマットに変換
    foc_json_key = target_time.strftime("%Y%m%d%H%M%SZ")
    foc_dir_str = target_time.strftime("%Y%m%d%H%M%SZ")

    # dimension_map.jsonを取得し、パラメータを抽出
    dimension_map = await get_dimension_map(layer, init_time_str)
    
    # dimension_map.json内に該当するフォーキャスト時間が存在するかチェック
    if foc_json_key not in dimension_map:
        # 空タイルを返す
        buf = io.BytesIO()
        Image.new("RGBA", (256, 256), (0, 0, 0, 0)).save(buf, format="WEBP")
        return Response(content=buf.getvalue(), media_type="image/webp", headers={"Cache-Control": "no-cache"})

    surface_meta = dimension_map[foc_json_key].get(surface, {})
    
    # bboxとgridから必要な座標情報を取得
    bbox = surface_meta.get("bbox", {})
    grid = surface_meta.get("grid", {})
    
    try:
        lat1 = bbox["lat1"]
        lon1 = bbox["lon1"]
        u_lat = grid.get("nlat")
        u_lon = grid.get("nlon")
        ny = grid["ny"]
        nx = grid["nx"]
    except KeyError as e:
        raise HTTPException(status_code=500, detail=f"Missing metadata parameters: {str(e)}")

    h, w = ny, nx

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
    # MSMなどでは北から南へ緯度が振られる場合があるので、u_lat の符号等に注意
    row_f = (lat1 - lat_grid) / u_lat
    col_f = (lon_shifted - lon1) / u_lon
    
    r0 = np.floor(row_f + 0.5).astype(int)
    c0 = np.floor(col_f + 0.5).astype(int)
    
    valid_mask = (row_f >= -0.5) & (row_f <= (h - 1) + 0.5) & \
                 (col_f >= -0.5) & (col_f <= (w - 1) + 0.5)
    
    r0_s = np.clip(r0, 0, h - 1)
    c0_s = np.clip(c0, 0, w - 1)
    
    rmin, rmax = int(np.min(r0_s)), int(np.max(r0_s))
    cmin, cmax = int(np.min(c0_s)), int(np.max(c0_s))
    
    # Zarrから指定範囲のデータを取得
    sub_data = await get_zarr_data(layer, init_time_str, foc_dir_str, surface, element, rmin, rmax, cmin, cmax)
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
