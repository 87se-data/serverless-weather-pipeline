import os
import json
import math
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Security, Depends
from fastapi.responses import StreamingResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
import vertexai
from vertexai.generative_models import GenerativeModel
import jpholiday
from astral import LocationInfo
from astral.sun import sun, golden_hour

# 環境変数の取得
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
EXPECTED_API_KEY = os.getenv("ADVICE_API_KEY", "your-secret-key-12345")

# Vertex AI の初期化
vertexai.init(project=PROJECT_ID, location=LOCATION)

app = FastAPI(title="Weather Advice API")

api_key_header = APIKeyHeader(name="x-api-key", auto_error=True)

async def verify_api_key(api_key: str = Security(api_key_header)):
    if api_key != EXPECTED_API_KEY:
        raise HTTPException(status_code=403, detail="不正なAPIキーです。アクセスは許可されていません。")
    return api_key

class WeatherDataPoint(BaseModel):
    datetime: str
    ssi: float
    ki: float
    tt: float
    theta_e: float
    water_vapor_flux: float  
    lcl: float      
    lfc: float      
    el: float       
    cape: float     
    cin: float      
    temperature: float       
    humidity: float
    precipitation: float
    wind_speed: float        
    solar_radiation: float
    total_cloud_cover: float
    low_cloud_cover: float
    mid_cloud_cover: float
    altostratus_cloud_cover: float
    laundry_index: float
    wbgt: float

class WeatherRequest(BaseModel):
    latitude: float
    longitude: float
    weather_data: List[WeatherDataPoint]

# 💖 AIに英語を喋らせないための「日本語翻訳辞書」
KEY_MAPPING = {
    'ssi': 'ショワルター安定指数',
    'ki': 'K指数',
    'tt': 'TotalTotals指数',
    'theta_e': '相当温位',
    'water_vapor_flux': '水蒸気フラックス',
    'lcl': '持ち上げ凝結高度',
    'lfc': '自由対流高度',
    'el': '平衡高度',
    'cape': '対流有効位置エネルギー',
    'cin': '対流抑制',
    'temperature': '気温',
    'humidity': '湿度',
    'precipitation': '降水量',
    'wind_speed': '風速',
    'solar_radiation': '日射量',
    'total_cloud_cover': '全雲量',
    'low_cloud_cover': '下層雲量',
    'mid_cloud_cover': '中層雲量',
    'altostratus_cloud_cover': '上層雲量',
    'laundry_index': '洗濯指数',
    'wbgt': '暑さ指数'
}

@app.get("/")
async def root():
    return {"message": "Weather Advice API is running"}

@app.post("/generate-advice")
async def generate_advice(request: WeatherRequest, api_key: str = Depends(verify_api_key)):
    try:
        model = GenerativeModel("gemini-2.5-flash")
        
        jst = timezone(timedelta(hours=9), 'JST')
        now = datetime.now(jst)
        # 💖 現在時刻を「〇日 〇時」とスッキリ表示！
        current_time_str = now.strftime("%d日 %H時")
        
        is_weekend = now.weekday() >= 5
        holiday_name = jpholiday.is_holiday_name(now.date())
        
        day_type = f"休日（{holiday_name}）" if holiday_name else "休日" if is_weekend else "平日"

        loc = LocationInfo("Local", "Japan", "Asia/Tokyo", request.latitude, request.longitude)
        try:
            s = sun(loc.observer, date=now.date(), tzinfo=jst)
            sunrise_str = s['sunrise'].strftime('%H:%M')
            sunset_str = s['sunset'].strftime('%H:%M')
            gh = golden_hour(loc.observer, date=now.date(), tzinfo=jst)
            golden_hour_str = f"{gh[0].strftime('%H:%M')}〜{gh[1].strftime('%H:%M')}"
        except Exception:
            sunrise_str = sunset_str = golden_hour_str = "不明"

        app_rules = """
        【厳格な評価基準（データ駆動型の意思決定支援）】
        1. 🛡️ 予防的ヘルスケア・命を守る基準
           - 暑さ指数: 28以上で「厳重警戒」、31以上で「危険(屋外活動の中止)」。
           - 日射量: 値÷100を「UV指数(0〜11+)」とみなせ。8以上で「非常に強い紫外線」。
        2. 🌱 農作業・屋外作業・防災のリスク管理
           - 気温: 4℃以下で農作物の霜害リスク。
           - 風速: 4m/s超で農薬散布不適、10m/s超で施設補強警告。
           - 降水量: 1.0以上で屋外作業の中断目安。
           - 相当温位 と 水蒸気フラックス: 
             - 相当温位が340以上、かつ 水蒸気フラックスが250以上の場合は「線状降水帯の発生確率が極めて高い大気の川」として、最大級の命を守るアラートを発令せよ。
             - 水蒸気フラックスが300以上の場合は「極めて危険な水蒸気流入」、350以上の場合は「記録的豪雨クラス」として絶対的な警戒を促せ。
           - ゲリラ雷雨と突風: 対流有効位置エネルギーが1000以上の時、対流抑制が0に近づくと「ゲリラ雷雨が爆発的に発生するサイン」。K指数が26以上、またはTotalTotals指数が44以上の場合は落雷リスクを警告。
        3. ☁️ 景観・視界・生活品質(QOL)の最適化
           - 雲量による景観・視界判定: 
             - 「下層雲量」が80以上の場合、山間部や峠道での「濃霧による視界不良」を強く警告せよ。
             - 夜間で「全雲量」が20以下の場合は「星空観測の絶好のチャンス」と伝えよ。
           - 洗濯指数: 80以上=大変よく乾く。
           - 太陽イベントを参照し、日の入りやマジックアワーに基づいた助言を行え。
        """

        system_prompt = (
            "あなたは気象データを用いてユーザーの「データ駆動型の意思決定」を支援する、プロフェッショナルな環境コンサルタントです。\n"
            "挨拶や自己紹介は不要です。結論から書いてください。データに基づく事実を箇条書きで伝えてください。専門用語は極力避け、ユーザーに伝わりやすい言葉を選んでください。\n\n"
            "📌 **結論要約**\n- （1行で）\n\n"
            "⚠️ **環境リスクと注意ポイント**\n- （時間と共に記載）\n\n"
            "💡 **具体的な行動アドバイス**\n"
            f"- （{day_type}の生活や屋外作業に即した支援）\n\n"
            f"※{app_rules}"
        )
        
        formatted_weather_data = []
        for p in request.weather_data:
            p_dict = p.dict()
            try:
                # 1. 時間の処理
                dt_obj = datetime.fromisoformat(p_dict['datetime'])
                if dt_obj.tzinfo is None:
                    dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                dt_jst = dt_obj.astimezone(jst)
                
                # 2. 💖 キーの日本語化＆数値の切り上げ丸め込み
                # 予測タイミングも「〇日 〇時」にスッキリ変更！
                mapped_dict = {'予測タイミング': dt_jst.strftime('%d日 %H時')}
                for key, value in p_dict.items():
                    if key in KEY_MAPPING:
                        # 少数第一位を切り上げて整数にする
                        mapped_dict[KEY_MAPPING[key]] = math.ceil(value) if isinstance(value, (int, float)) else value
                
                formatted_weather_data.append(mapped_dict)

            except (ValueError, KeyError):
                pass
            
        weather_context = json.dumps(formatted_weather_data, ensure_ascii=False)

        user_prompt = (
            f"現在時刻: {current_time_str}\n"
            f"太陽イベント: 日の出 {sunrise_str}, 日の入り {sunset_str}, マジックアワー {golden_hour_str}\n"
            f"地点: 緯度 {request.latitude}, 経度 {request.longitude}\n"
            f"データ: {weather_context}"
        )

        async def advice_stream_generator():
            try:
                # stream=True にして非同期ストリーミングをリクエスト
                responses = await model.generate_content_async(
                    f"{system_prompt}\n\n{user_prompt}",
                    stream=True
                )
                
                # チャンク（断片）が届くたびに随時 yield する
                async for chunk in responses:
                    if chunk.text:
                        # チャンクをJSON化
                        data = json.dumps({"chunk": chunk.text}, ensure_ascii=False)
                        # SSEフォーマットに従って送信（必ず \n\n をつける）
                        yield f"data: {data}\n\n"
                
                # 全ての生成が完了したら DONE サインを送信
                yield "data: [DONE]\n\n"
                
            except Exception as e:
                # ストリーム中のエラーハンドリング
                error_data = json.dumps({"error": f"AI生成中にエラーが発生しました: {str(e)}"}, ensure_ascii=False)
                yield f"data: {error_data}\n\n"
                yield "data: [DONE]\n\n"

        # Content-Type を text/event-stream にしてレスポンスを返す
        return StreamingResponse(
            advice_stream_generator(),
            media_type="text/event-stream"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))