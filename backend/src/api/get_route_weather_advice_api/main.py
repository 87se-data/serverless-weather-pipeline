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
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    elevation: Optional[float] = None
    temp: Optional[float] = None
    wind_speed: Optional[float] = None
    apparent_temp: Optional[float] = None
    precipitation: Optional[float] = None
    humidity: Optional[float] = None
    solar_radiation: Optional[float] = None
    wbgt: Optional[float] = None
    ssi: Optional[float] = None
    tt: Optional[float] = None
    ki: Optional[float] = None
    cape: Optional[float] = None
    cin: Optional[float] = None
    theta_e: Optional[float] = None
    water_vapor_flux: Optional[float] = None
    zero_degree_altitude: Optional[float] = None
    pressure_change_3h: Optional[float] = None
    cloud_cover_low: Optional[float] = None
    wind_direction: Optional[float] = None

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
    'ki': 'K-index',
    'cape': '対流有効位置エネルギー(CAPE)',
    'cin': '対流抑制(CIN)',
    'theta_e': '相当温位',
    'water_vapor_flux': '水蒸気フラックス',
    'zero_degree_altitude': '0度高度',
    'pressure_change_3h': '3時間気圧変化',
    'cloud_cover_low': '下層雲量',
    'wind_direction': '風向'
}

@app.post("/generate-route-advice")
async def generate_route_advice(request: RouteRequest, api_key: str = Depends(verify_api_key)):
    try:
        model = GenerativeModel(os.getenv("GEMINI_MODEL"))
        
        jst = timezone(timedelta(hours=9), 'JST')
        now = datetime.now(jst)

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

        # ---------------------------------------------------------
        # 1. 共通の役割（アイデンティティ）
        # ---------------------------------------------------------
        if request.mode == "mountain":
            base_identity = (
                "あなたは、最新の気象力学と長年の実務経験を併せ持つ、日本トップクラスの『山岳気象プロフェッショナル（山岳予報士・ベテランガイド）』です。\n"
                "提供されたルート上の連続的な気象データを「点の集合」ではなく、「標高と時間の推移に伴う大気の立体的な環境変化」として読み解き、登山者の命を守るための意思決定を支援してください。\n"
                "最大のルールとして【専門用語（CAPE、シアー、0度高度、フラックス等）はAI内部の推論のみで使用し、最終出力には一切出さない】ことを厳守し、直感的に危険を察知できる平易かつ断定的な言葉に翻訳して結論を急いでください。\n\n"
            )
        else:
            base_identity = (
                f"あなたは、【{request.mode}】の移動リスク管理を専門とする、最新の気象力学に精通した『気象リスク管理プロフェッショナル』です。\n"
                "提供されたルート上の連続的な気象データが大気の立体構造として「移動の安全性と快適性」にどう影響するかを推論し、具体的な対策を提示してください。\n"
                "最大のルールとして【専門用語（CAPE、シアー、0度高度、フラックス等）はAI内部の推論のみで使用し、最終出力には一切出さない】ことを厳守し、直感的に危険を察知できる平易かつ断定的な言葉に翻訳して結論を急いでください。\n\n"
            )

        # ---------------------------------------------------------
        # 2. 全季節共通の基本リスク基準（ベースプロンプト）
        # ---------------------------------------------------------
        if request.mode == "mountain":
            base_criteria = (
                "【全季節共通の基本リスク評価（内部推論用：出力禁止）】\n"
                "1. 力学的・地形的リスク（滑落・行動不能）:\n"
                "  - 風速・風向とルート標高の推移から、稜線や岩稜帯での「吹き抜け」や「あおり」による滑落リスク、行動不能レベルの暴風（風速15m/s以上）への突入タイミングを判定せよ。\n"
                "2. 対流活動の立体評価（落雷・突風）:\n"
                "  - CAPE（爆発力）とCIN（フタ）、安定度指数(SSI/TT/KI)をクロスチェックし、「Loaded Gun（きっかけ次第で爆発する雷雨）」や「アクティブな危険（即時避難レベル）」への組織化をルート上でいつ迎えるか推論せよ。\n"
                "3. 熱力学的・生理学的リスク（疲労凍死/熱中症）:\n"
                "  - 標高推移と0度高度の差分から「水相変化（雨・雪・氷）」を判定し、降水＋風による「濡れと冷え（急激な低体温症）」リスク、またはWBGTに基づく生理学的限界を予測せよ。\n\n"
            )
        else:
            base_criteria = (
                "【全季節共通の基本リスク評価（内部推論用：出力禁止）】\n"
                "1. 力学的リスク（横転・飛来物）:\n"
                "  - 風速・風向の変化から、移動体に対する「横風の影響」、ハンドルの取られやすさ、または飛来物による物理的脅威をルート上で判定せよ。\n"
                "2. 対流活動と視界・路面評価（ゲリラ豪雨・冠水）:\n"
                "  - CAPEとCINのバランスから局地的な極端降水を推論し、降水量と地形から「アンダーパス等の冠水」「ハイドロプレーニング」「視界不良」の危険度を判定せよ。\n"
                "3. 生理学的・車内環境リスク:\n"
                "  - WBGTによる熱中症限界や、湿度上昇に伴う不快感・車窓の曇り（デフォッガーの必要性）など、移動空間特有の環境悪化を予測せよ。\n\n"
            )

        # ---------------------------------------------------------
        # 3. 季節特化のプロンプト（シーズナルプロンプト）
        # ---------------------------------------------------------
        seasonal_prompt = ""
        if season == "spring":
            seasonal_prompt = (
                "【🌸 春季特有の推論ロジック】\n"
                "- 融雪・雪崩と寒暖差：0度高度の推移と気温上昇による「全層雪崩」「踏み抜き」「沢の増水」、および日没後の急激な気温低下による「再凍結（ブラックアイスバーン）」を判定せよ。\n"
                "- メイストーム（春の嵐）：3時間気圧変化量の急降下から、発達する低気圧に伴う広範囲な暴風・荒天への突入を推論せよ。\n\n"
            )
        elif season == "summer":
            seasonal_prompt = (
                "【🌻 夏季特有の推論ロジック】\n"
                "- 危険な暑さと生理的限界：WBGTを最優先指標とし、標高による紫外線の増幅も加味して、脱水・熱中症による行動不能リスクを判定せよ。\n"
                "- 水蒸気フラックスと線状降水帯：相当温位や水蒸気フラックスの大量流入から、ルート上での継続的な豪雨・土砂災害リスクを最大警戒せよ。\n"
                "※0度高度など凍結に関するリスクは、3000m級の高山帯を除き推論から除外せよ。\n\n"
            )
        elif season == "autumn":
            seasonal_prompt = (
                "【🍁 秋季特有の推論ロジック】\n"
                "- 秋雨前線と台風の複合災害：水蒸気フラックスと気圧変化量（pressure_change_3h）から、長引く極端な大雨・地盤の緩みを推論せよ。\n"
                "- 釣瓶落としの冷え込み：日没の早さとそれに伴う気温（temp）の急落による行動不能・低体温症リスクを判定せよ。\n"
                "- 初雪・初氷：標高と0度高度の接近から、予想外の降雪や峠道での路面凍結を警告せよ。\n\n"
            )
        elif season == "winter":
            seasonal_prompt = (
                "【❄️ 冬季特有の推論ロジック】\n"
                "- ホワイトアウトと空間識失調：降雪と強風、下層雲量（cloud_cover_low）の重なりから、致命的な視界喪失リスクを強く警告せよ。\n"
                "- 重度低体温症と凍傷：氷点下の体感気温（apparent_temp）と強風の組み合わせによる命の危険を判定せよ。\n"
                "- 路面凍結（ブラックアイスバーン）：気温3度以下における積雪・凍結によるスリップ・滑落リスクに最大限警戒せよ。\n"
                "※熱中症（WBGT）に関する推論は完全に除外せよ。\n\n"
            )

        # ---------------------------------------------------------
        # 月齢情報の取得
        # ---------------------------------------------------------
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

        # ---------------------------------------------------------
        # データの丸め処理とJSON化（変更なし）
        # ---------------------------------------------------------
        formatted_route_points = []
        for p in request.route_points:
            p_dict = p.dict()
            try:
                dt_obj = datetime.fromisoformat(p_dict['datetime'])
                if dt_obj.tzinfo is None:
                    dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                dt_jst = dt_obj.astimezone(jst)
                
                minute = (dt_jst.minute + 5) // 10 * 10
                if minute == 60:
                    dt_jst += timedelta(hours=1)
                    dt_jst = dt_jst.replace(minute=0)
                else:
                    dt_jst = dt_jst.replace(minute=minute)
                
                mapped_dict = {'通過予定': dt_jst.strftime('%d日 %H時%M分')}
                for key, value in p_dict.items():
                    if key in KEY_MAPPING and value is not None:
                        if key == 'elevation' and isinstance(value, (int, float)):
                            mapped_dict[KEY_MAPPING[key]] = int(round(value / 10.0)) * 10
                        elif key in ['temp', 'apparent_temp', 'precipitation', 'wind_speed']:
                            mapped_dict[KEY_MAPPING[key]] = round(value, 1)
                        elif key in ['ssi', 'tt', 'ki', 'cape', 'cin', 'theta_e', 'water_vapor_flux', 'zero_degree_altitude', 'pressure_change_3h'] and isinstance(value, (int, float)):
                            mapped_dict[KEY_MAPPING[key]] = round(value, 1)
                        else:
                            mapped_dict[KEY_MAPPING[key]] = round(value) if isinstance(value, (int, float)) else value
                formatted_route_points.append(mapped_dict)
                
            except (ValueError, KeyError):
                pass 
                
        route_data_summary = json.dumps(formatted_route_points, ensure_ascii=False)
        
        # ---------------------------------------------------------
        # 最終プロンプトの組み立て
        # ---------------------------------------------------------
        prompt = (
            f"{base_identity}"
            f"{base_criteria}"
            f"{seasonal_prompt}"
            f"【ネイチャーガイドの視点】\n"
            f"- リスク情報だけでなく、季節（{season}）や月齢を考慮して、移動中の景色や自然の魅力などのポジティブな楽しみ方を1つ提案せよ。\n\n"
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
                responses = await model.generate_content_async(
                    prompt, 
                    generation_config=GenerationConfig(
                        temperature=0.2, 
                        top_p=0.8,
                    ),
                    stream=True
                )
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