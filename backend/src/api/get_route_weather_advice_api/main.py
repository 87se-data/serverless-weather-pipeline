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
from astral.moon import phase as moon_phase 

# 環境変数の取得
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
EXPECTED_API_KEY = os.getenv("ADVICE_API_KEY", "your-secret-key-12345")

vertexai.init(project=PROJECT_ID, location=LOCATION)
app = FastAPI(title="Route Weather Advice API")

# APIキーのヘッダー設定（関門）
api_key_header = APIKeyHeader(name="x-api-key", auto_error=True)

async def verify_api_key(api_key: str = Security(api_key_header)):
    if api_key != EXPECTED_API_KEY:
        raise HTTPException(status_code=403, detail="不正なAPIキーです。アクセスは許可されていません。")
    return api_key

class RoutePoint(BaseModel):
    datetime: str
    latitude: float
    longitude: float
    elevation: float
    temp: float
    wind_speed: float
    apparent_temp: float
    precipitation: float
    humidity: float
    solar_radiation: float
    wbgt: Optional[float] = None
    ssi: Optional[float] = None
    tt: Optional[float] = None
    ki: Optional[float] = None

class RouteRequest(BaseModel):
    mode: str
    route_points: List[RoutePoint]

# AIに英語を喋らせないための「日本語翻訳辞書」（ルート天気用）
KEY_MAPPING = {
    'latitude': '緯度',
    'longitude': '経度',
    'elevation': '標高',
    'temp': '気温',
    'wind_speed': '風速',
    'apparent_temp': '体感気温',
    'precipitation': '降水量',
    'humidity': '湿度',
    'solar_radiation': '日射量',
    'wbgt': '暑さ指数',
    'ssi': 'ショワルター安定指数',
    'tt': 'トータルトータルズ指数',
    'ki': 'K-index'
}

@app.post("/generate-route-advice")
async def generate_route_advice(request: RouteRequest, api_key: str = Depends(verify_api_key)):
    try:
        model = GenerativeModel("gemini-2.5-flash")
        
        if request.mode == "mountain":
            system_role = (
                "あなたは山岳遭難を未然に防ぐ、日本トップクラスの「山岳気象の専門家・ベテランガイド」です。\n"
                "挨拶や自己紹介は一切不要です。直ちに結論から出力してください。\n"
                "山の気象は平地とは異なり、小さな天候の崩れが命に関わります。データから「気象遭難」のリスクを先読みし、厳格かつプロフェッショナルな視点でアドバイスをしてください。\n\n"
                "【専門家としての判定基準】\n"
                "- 悪天候と低体温症（ハイポサーミア）：降水量が1以上あり、かつ風速が伴う場合は、夏山であっても「濡れと風による急激な体温低下（疲労凍死）」の危険があると強く警告せよ。早めのレインウェア着用とレイヤリングの調整を指示すること。\n"
                "- 稜線の強風・滑落リスク：風速が10以上の場合、「稜線や岩稜帯でのあおり・滑落リスク」を警告。風速が15以上の場合は「行動不能レベルの暴風」として、稜線への進入禁止や撤退（エスケープ）を強く勧告せよ。\n"
                "- 落雷・突風リスク（大気の不安定度）：ショワルター安定指数(SSI)が0以下の場合は「やや不安定」、-3以下で「中程度」、-6以下で「非常に不安定」、-9以下で「極度に不安定」と判定せよ。また、K指数(KI)が31以上（雷雨確率60%超）やTT指数が50以上（激しい雷雨の可能性）の箇所は極めて危険です。これらの数値をもとに落雷やゲリラ豪雨、突風のリスクを厳格に評価し、稜線や開けた場所での行動に強く警告を出し、安全な場所への避難や計画変更を促すこと。\n"
                "- 紫外線と脱水（暑さ指数/WBGT）：25以上で「警戒（積極的な休憩を）」、28以上で「厳重警戒（こまめな水分・塩分補給を）」、31以上で「危険（外出を控え涼しい環境へ）」レベルです。標高が高いほど紫外線も強烈になります。熱中症・バテ（疲労困憊）による行動不能リスクを指摘し、ペースダウンを指示せよ。\n"
                "- 撤退の判断基準：複数のリスク（雷雨＋強風など）が重なる時間帯・標高帯においては、決して無理をさせず、勇気ある「計画の変更・撤退」を明確に提案せよ。\n"
                "- ネイチャーガイド：提供された「日時（季節）」「緯度」「標高」から、その時期・その場所ならではの自然の魅力（例：秋の標高1500mなら紅葉の見頃、夏山の高山植物、冬の雪景色など）を推測し、登山者を楽しませるワンポイントアドバイスを1つだけ添えること。"
            )
        else:
            system_role = (
                f"あなたは【{request.mode}】のプロフェッショナルな移動アドバイザーです。\n"
                "挨拶や自己紹介は一切不要です。直ちに結論から出力してください。\n"
                "天候による快適さとリスクを判定し、移動手段に応じた具体的かつ実用的な対策を提示してください。\n\n"
                "【判定基準】\n"
                "- 降水量：1以上で、傘・雨具の必要性や、路面状況の悪化（ハイドロプレーニング現象、スリップ、視界不良など）への警戒を促すこと。\n"
                "- 風速：5以上で自転車や徒歩の負担増加・横風を指摘。10以上で車のハンドルを取られるリスクや飛来物への警戒を強く促すこと。\n"
                "- 落雷・突風リスク（大気の不安定度）：ショワルター安定指数(SSI)が0以下の場合は「やや不安定」、-3以下で「中程度」、-6以下で「非常に不安定」、-9以下で「極度に不安定」となります。K指数(KI)が31以上やTT指数が50以上の箇所も雷雨リスクが高いため、屋外での活動や移動に強く注意を促し、安全確保を指示すること。\n"
                "- 気温・体感気温：3度以下の場合は、路面凍結（ブラックアイスバーン）への警戒や、徹底した防寒対策を警告すること。\n"
                "- 暑さ指数(WBGT)：25以上で「警戒」、28以上で「厳重警戒」、31以上で「危険」レベルです。エアコンの適切な使用、日陰での休憩、こまめな水分補給などを具体的に促すこと。\n"
                "- 湿度：80%を超える場合、蒸し暑さによる不快感のほか、車なら「窓の曇りに対する事前対策（デフォッガーの使用）」を指摘せよ。\n"
                "- 景色と楽しみ：リスク情報だけでなく、月齢や季節を考慮して、移動中の景色や星空観測などのポジティブな楽しみ方を1つ提案せよ。"
            )

        jst = timezone(timedelta(hours=9), 'JST')
        now = datetime.now(jst)

        # 月齢を計算し、星空・月明かりのロマンチックな情報を取得
        try:
            m_phase = moon_phase(now.date())
            if m_phase < 2 or m_phase > 26:
                moon_str = "新月（星空観測の絶好のチャンス！）"
            elif 13 <= m_phase <= 16:
                moon_str = "満月付近（夜間は月明かりが美しく、歩きやすいです）"
            else:
                moon_str = "半月"
        except Exception:
            moon_str = "不明"

        formatted_route_points = []
        for p in request.route_points:
            p_dict = p.dict()
            try:
                # 1. 時間の処理
                dt_obj = datetime.fromisoformat(p_dict['datetime'])
                if dt_obj.tzinfo is None:
                    dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                dt_jst = dt_obj.astimezone(jst)
                
                # 時刻を10分刻みに四捨五入して丸める処理
                minute = (dt_jst.minute + 5) // 10 * 10
                if minute == 60:
                    dt_jst += timedelta(hours=1)
                    dt_jst = dt_jst.replace(minute=0)
                else:
                    dt_jst = dt_jst.replace(minute=minute)
                
                # 2. キーの日本語化＆数値の丸め込み
                mapped_dict = {'通過予定': dt_jst.strftime('%d日 %H時%M分')}
                for key, value in p_dict.items():
                    if key in KEY_MAPPING and value is not None:
                        if key == 'elevation' and isinstance(value, (int, float)):
                            # 標高は10m刻みに丸める
                            mapped_dict[KEY_MAPPING[key]] = int(round(value / 10.0)) * 10
                        elif key in ['ssi', 'tt', 'ki'] and isinstance(value, (int, float)):
                            # SSI等の指標は、小数点第1位まで残してAIに渡す
                            mapped_dict[KEY_MAPPING[key]] = round(value, 1)
                        else:
                            # その他は少数第一位を切り上げて整数にする
                            mapped_dict[KEY_MAPPING[key]] = math.ceil(value) if isinstance(value, (int, float)) else value
                
                formatted_route_points.append(mapped_dict)
                
            except (ValueError, KeyError):
                pass 
                
        route_data_summary = json.dumps(formatted_route_points, ensure_ascii=False)
        
        prompt = (
            f"{system_role}\n\n"
            f"【解析対象ルートデータ】\n{route_data_summary}\n\n"
            f"【天文情報】\n"
            f"月齢情報: {moon_str}\n\n"
            "【出力時の厳守ルール】\n"
            "1. 気温、風速、降水量などの数値を文章に含める際は、AIによる独自の計算や推測による小数点以下の細かい数値（例：8.85℃）は絶対に出力せず、必ず「整数（例：約9℃）」に丸めてわかりやすくユーザーに伝えてください。\n"
            "2. ⚠️最重要事項：安全性判定で「中止勧告」を出すような極めて危険な状況の場合、「🌿 ネイチャー＆エンタメ情報」の項目は不謹慎・不適切であるため、項目ごと一切出力しないでください。\n\n"
            "以下のフォーマットで出力してください：\n"
            "⚠️ **安全性判定：[続行可能 / 警戒 / 中止勧告]**\n"
            "- （ルート全体の結論を1行で）\n"
            "📍 **重要ポイント**\n"
            "- （〇〇日 〇〇時〇〇分頃、標高〇〇m付近でのリスクを具体的に指摘）\n"
            "🛡️ **アドバイス**\n"
            "- （具体的な行動指示）\n"
            "🌿 **ネイチャー＆エンタメ情報**\n"
            "- （季節・標高・月齢を考慮した自然の魅力や楽しみ方を1行で。※警戒 / 中止勧告時はこの項目を出力しない）"
        )
        
        async def advice_stream_generator():
            try:
                responses = await model.generate_content_async(prompt, stream=True)
                async for chunk in responses:
                    if chunk.text:
                        data = json.dumps({"chunk": chunk.text}, ensure_ascii=False)
                        yield f"data: {data}\n\n"
                yield "data: [DONE]\n\n"
            except Exception as e:
                error_data = json.dumps({"error": f"AI生成中にエラーが発生しました: {str(e)}"}, ensure_ascii=False)
                yield f"data: {error_data}\n\n"
                yield "data: [DONE]\n\n"

        return StreamingResponse(
            advice_stream_generator(),
            media_type="text/event-stream"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)