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
from vertexai.generative_models import GenerativeModel, GenerationConfig
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
    ssi: Optional[float] = None
    ki: Optional[float] = None
    tt: Optional[float] = None
    theta_e: Optional[float] = None
    water_vapor_flux: Optional[float] = None
    lcl: Optional[float] = None      
    lfc: Optional[float] = None      
    el: Optional[float] = None       
    cape: Optional[float] = None     
    cin: Optional[float] = None      
    temperature: Optional[float] = None       
    humidity: Optional[float] = None
    precipitation: Optional[float] = None
    wind_speed: Optional[float] = None        
    solar_radiation: Optional[float] = None
    total_cloud_cover: Optional[float] = None
    low_cloud_cover: Optional[float] = None
    mid_cloud_cover: Optional[float] = None
    altostratus_cloud_cover: Optional[float] = None
    laundry_index: Optional[float] = None
    wbgt: Optional[float] = None
    wind_direction: Optional[float] = None
    pressure_change_3h: Optional[float] = None
    zero_degree_altitude: Optional[float] = None
    vertical_wind_shear_deep: Optional[float] = None
    vertical_wind_shear_low: Optional[float] = None

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
    'wbgt': '暑さ指数',
    'wind_direction': '風向',
    'pressure_change_3h': '3時間気圧変化量',
    'zero_degree_altitude': '0℃等温線高度',
    'vertical_wind_shear_deep': '深層シアー',
    'vertical_wind_shear_low': '下層シアー'
}

@app.get("/")
async def root():
    return {"message": "Weather Advice API is running"}

@app.post("/generate-advice")
async def generate_advice(request: WeatherRequest, api_key: str = Depends(verify_api_key)):
    try:
        model = GenerativeModel(os.getenv("GEMINI_MODEL"))
        
        jst = timezone(timedelta(hours=9), 'JST')
        now = datetime.now(jst)
        # 💖 現在時刻を「〇日 〇時」とスッキリ表示！
        current_time_str = now.strftime("%d日 %H時")

        # 🌸🌻🍁❄️ 季節の判定 (3-5月:春, 6-8月:夏, 9-11月:秋, 12-2月:冬)
        current_month = now.month
        if current_month in [3, 4, 5]:
            season = "spring"
        elif current_month in [6, 7, 8]:
            season = "summer"
        elif current_month in [9, 10, 11]:
            season = "autumn"
        else:
            season = "winter"
        
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

        # ---------------------------------------------------------
        # 1. 共通の役割（アイデンティティ）
        # ---------------------------------------------------------
        base_identity = (
            "あなたは、最新の気象力学と長年の実務経験を併せ持つ、日本トップクラスの『気象リスク管理プロフェッショナル（気象予報士・防災士）』です。\n"
            "提供された数値予報データを単なる点の集まりではなく、「大気の立体構造」と「時間的な連続性」として読み解き、ユーザーの命、日常生活（通勤・通学・休日）、そして農作業を守るための意思決定を支援してください。\n"
            "ただし、最大のルールとして【専門用語（CAPE、シアー、0℃高度、Paなど）はAI内部の推論のみで使用し、最終出力には一切出さない】ことを厳守してください。\n"
            "一般のユーザーが直感的に危険を察知できるよう、専門的な現象を「誰にでも即座に理解できる平易かつ断定的な言葉（例：ゲリラ豪雨、路面凍結、急な暴風）」に翻訳し、長文を避けて結論を急いでください。"
        )
        # ---------------------------------------------------------
        # 2. 全季節共通の基本リスク基準（ベースプロンプト）
        # ---------------------------------------------------------
        base_criteria = (
            "【基本評価基準（内部推論用：出力禁止）】\n"
            "1. 対流活動の立体評価（爆発力＋抑制力＋組織化）:\n"
            "   - CAPE（潜在的エネルギー）とCIN（抑制力/フタ）を比較。CINが強い場合は大雨警告を控え、CAPEが高くCINが適度な場合は「Loaded Gun（日射等をきっかけに突発する極端気象）」として警戒。\n"
            "   - 鉛直シアー（深層/下層）を掛け合わせ、積乱雲が単一で終わるか、スーパーセル・線状降水帯へ組織化するかを判定せよ。\n"
            "2. 熱力学的・生理学的リスク:\n"
            "   - ルート標高と0℃等温線高度の差分から、「雨・みぞれ・湿雪・雨氷（ブラックアイスバーン）」の水相変化を正確に判定せよ。\n"
            "   - WBGTと体感気温（風速＋気温）から、熱中症または低体温症の生理学的限界を予測せよ。\n"
            "3. 力学的・総観スケールのリスク:\n"
            "   - 3時間気圧変化量（-300Pa等）から、寒冷前線や急速に発達する低気圧による総観スケールの天候急変を察知せよ。\n"
            "   - 風向と地形（標高）の関係から、吹き抜けやダウンスロープストーム（おろし風）のリスクを推論せよ。"
        )

        # ---------------------------------------------------------
        # 3. 季節特化のプロンプト（シーズナルプロンプト）
        # ---------------------------------------------------------
        seasonal_prompt = ""
        if season == "spring":
            seasonal_prompt = (
                "【🌸春の推論ロジック】\n"
                "- メイストーム（春の嵐）: 気圧急降下を伴う移動性低気圧による、突発的で広範囲な暴風を警戒せよ。\n"
                "- 大気不安定と降雹: 上空の寒気（寒冷渦）の流入と地上の昇温による、急な雷雨と降雹リスクを判定せよ。\n"
                "- 寒暖差・融雪: 周期的な気温変化による自律神経への負担、雪解けによる雪崩リスク、夜間の再凍結を判定せよ。\n\n"
            )
        elif season == "summer":
            seasonal_prompt = (
                "【🌻夏の推論ロジック】\n"
                "- 線状降水帯と極端現象: 850hPa相当温位(340K以上)の大量流入、CAPE、下層シアーの重なりから、局地的な大雨・道路冠水・土砂災害を最大警戒せよ。\n"
                "- 危険な暑さ: WBGTを最優先指標とし、農作業中の熱中症リスクや、登下校・通勤時の危険な暑さによる行動限界を判定せよ。\n"
                "- 熱雷・界雷と突風: 不安定指標から、午後特有の局地的な落雷（開けた農地での危険）とダウンバースト（農業用ハウスへの被害、突発的な強風）を判定せよ。\n"
                "※0℃高度など凍結に関するリスクは、3000m級の高山帯を除き推論から除外せよ。\n\n"
            )
        elif season == "autumn":
            seasonal_prompt = (
                "【🍂秋の推論ロジック】\n"
                "- 遠隔豪雨と秋雨前線: 台風からの湿った空気（水蒸気フラックス）と秋雨前線の相互作用による、長引く極端な大雨を判定せよ。\n"
                "- 放射冷却と濃霧: 移動性高気圧下での全雲量低下に伴う急激な冷え込みと、それに伴う濃霧（視界不良）を判定せよ。\n"
                "- 初雪・初氷: 標高と0℃高度の接近から、晩秋の峠道や山岳での不意の路面凍結・みぞれを判定せよ。\n\n"
            )
        elif season == "winter":
            seasonal_prompt = (
                "【⛄️冬の推論ロジック】\n"
                "- 日本海寒帯気団収束帯(JPCZ)と大雪: 強い冬型の気圧配置下での局地的な豪雪、立ち往生リスクを判定せよ。\n"
                "- 南岸低気圧と雨氷: 気温と0℃高度から、太平洋側での湿雪や、最悪の着氷現象である「雨氷」を最大警戒せよ。\n"
                "- ホワイトアウトと低体温: 下層雲量と風速の掛け合わせによる空間識失調（視界ゼロ）、強風による凍傷・低体温症を判定せよ。\n"
                "※熱中症（WBGT）に関する推論は完全に除外せよ。\n\n"
            )

        # ---------------------------------------------------------
        # 最終プロンプトの組み立て
        # ---------------------------------------------------------
        system_prompt = (
            f"{base_identity}"
            f"{base_criteria}"
            f"{seasonal_prompt}"
            "【出力時の厳守ルール】\n"
            "気温、風速、降水量などの数値を文章に含める際は、AIによる独自の計算や推測による小数点以下の細かい数値（例：8.85℃）は絶対に出力せず、必ず「整数（例：約9℃）」に丸めてわかりやすくユーザーに伝えてください。\n\n"
            "以下の構成で出力してください：\n\n"
            "⚠️ **総合危険度判定：[安全 / 注意 / 警戒 / 危険(中止勧告)]**\n"
            "- （現在の状況と今後の見通しを、プロの視点で1〜2行で総括。急変の可能性があれば必ず触れること）\n\n"
            "🔍 **気象予報士の専門的分析**\n"
            f"- （季節性({season})やCAPE/CIN、上下層シアー、0℃高度、気圧変化量などをどう解釈したか、危険度判定の根拠をプロの視点で解説）\n\n"
            "📍 **日常生活と農作業への想定リスク**\n"
            "- （提示されたデータが引き起こす具体的な影響。例：開けた農地での落雷リスク、通勤・通学時のゲリラ豪雨、朝の路面凍結、熱中症など）\n\n"
            "🛡️ **プロからの安全対策アドバイス**\n"
            f"- （{day_type}の日常生活や農作業に即した支援。登下校・通勤時の注意点、農作業の撤収目安、マジックアワー等の太陽イベントを考慮した指示など）\n"
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
                    if key in KEY_MAPPING and value is not None:
                        # 降水量、風速、気温などの繊細なデータは小数第1位まで残す
                        if key in ['precipitation', 'wind_speed', 'temperature']:
                            mapped_dict[KEY_MAPPING[key]] = round(value, 1)
                        # それ以外（CAPEなど桁が大きいもの）は四捨五入で整数に
                        else:
                            mapped_dict[KEY_MAPPING[key]] = round(value)
                
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
                    generation_config=GenerationConfig(  # 💖 ここに設定を追加！
                        temperature=0.2, 
                        top_p=0.8,
                    ),
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