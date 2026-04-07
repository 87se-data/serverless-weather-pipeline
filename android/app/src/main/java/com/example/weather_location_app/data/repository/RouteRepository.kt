package com.example.weather_location_app.data.repository

import com.example.weather_location_app.data.api.RetrofitClient
import com.example.weather_location_app.data.api.RouteAdvicePoint
import com.example.weather_location_app.data.api.RouteResponse
import com.example.weather_location_app.data.api.RouteWeatherAdviceRequest
import java.time.OffsetDateTime
import java.time.ZoneOffset
import kotlin.math.sqrt
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.channels.awaitClose
import okhttp3.Request
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.sse.EventSource
import okhttp3.sse.EventSourceListener
import com.google.gson.Gson
import org.json.JSONObject
import android.util.Log

/**
 * ルートデータの取得・加工およびアドバイスAPIとの通信を担当するRepository
 */
class RouteRepository {

    /**
     * ルート天気アドバイスを取得・変換するメソッド
     * 
     * @param mode 移動手段（"walking", "bicycling", "hiking"など）
     * @param routeResponse ルート検索APIからのレスポンス
     * @return アドバイス（テキスト）を含むResult
     */
    suspend fun getRouteWeatherAdvice(mode: String, routeResponse: RouteResponse): Flow<String> = callbackFlow {
        val info = routeResponse.information ?: run {
            close(Exception("ルート情報が見つかりません"))
            return@callbackFlow
        }

        try {
            // 1. 各地点のデータを RouteAdvicePoint に変換
            val routePoints = info.map { (datetime, point) ->
                // apparent_temp (体感気温): surfaceマップ内から取得
                val apparentTemp = point.surface?.get("apparent_temp")?.toDoubleOrNull() ?: 0.0

                // temp (気温): surfaceマップ内の "0_0" を取得。100以上ならケルビンとみなして摂氏に変換
                val rawTemp = point.surface?.get("0_0")?.toDoubleOrNull() ?: 0.0
                val temp = if (rawTemp >= 100.0) rawTemp - 273.15 else rawTemp

                // wind_speed (風速): "2_2"(u成分) と "2_3"(v成分) から計算
                val u = point.surface?.get("2_2")?.toDoubleOrNull() ?: 0.0
                val v = point.surface?.get("2_3")?.toDoubleOrNull() ?: 0.0
                val windSpeed = sqrt(u * u + v * v)

                // humidity (湿度): "1_1" (値がない場合は 0.0)
                val humidity = point.surface?.get("1_1")?.toDoubleOrNull() ?: 0.0

                // solar_radiation (日射量): "4_7" (値がない場合は 0.0)
                val solarRadiation = point.surface?.get("4_7")?.toDoubleOrNull() ?: 0.0

                // precipitation (降水量): "1_8" (値がない場合は 0.0)
                val precipitation = point.surface?.get("1_8")?.toDoubleOrNull() ?: 0.0

                // wbgt (暑さ指数): point.wbgt から取得、なければ計算または null
                val wbgt = point.wbgt?.toDoubleOrNull() ?: 0.0

                // ssi, tt, ki: pallマップ内から取得
                val ssi = point.pall?.get("ssi")?.toDoubleOrNull()
                val tt = point.pall?.get("tt")?.toDoubleOrNull()
                val ki = point.pall?.get("ki")?.toDoubleOrNull()

                RouteAdvicePoint(
                    datetime = datetime,
                    latitude = point.latitude?.toDoubleOrNull() ?: 0.0,
                    longitude = point.longitude?.toDoubleOrNull() ?: 0.0,
                    elevation = point.elevation?.toDoubleOrNull() ?: 0.0,
                    temp = temp,
                    wind_speed = windSpeed,
                    apparent_temp = apparentTemp,
                    humidity = humidity,
                    solar_radiation = solarRadiation,
                    precipitation = precipitation,
                    wbgt = wbgt,
                    ssi = ssi,
                    tt = tt,
                    ki = ki
                )
            }.sortedBy { it.datetime } // datetime の昇順でソート

            // 現在時刻より前のデータを除外（トークン節約のため）
            val now = OffsetDateTime.now(ZoneOffset.ofHours(9))
            val filteredRoutePoints = routePoints.filter { point ->
                try {
                    val dt = parseDateTime(point.datetime)
                    !dt.isBefore(now)
                } catch (e: Exception) {
                    true // パース失敗時は念のため残す
                }
            }

            // 2. データのダウンサンプリング (最大 50 ポイント)
            val maxPoints = 50
            val sampledPoints = if (filteredRoutePoints.size > maxPoints) {
                val step = filteredRoutePoints.size / maxPoints
                val result = mutableListOf<RouteAdvicePoint>()
                
                filteredRoutePoints.forEachIndexed { index, point ->
                    // 最初の要素、最後の要素、またはステップごとの要素を抽出
                    if (index == 0 || index == filteredRoutePoints.size - 1 || index % step == 0) {
                        // 重複を避けるために最後に既に入っているかチェック
                        if (result.isEmpty() || result.last().datetime != point.datetime) {
                            result.add(point)
                        }
                    }
                }
                
                // 最後の要素が確実に入っていることを保証 (念のため)
                if (result.isNotEmpty() && result.last().datetime != filteredRoutePoints.last().datetime) {
                    result.add(filteredRoutePoints.last())
                }
                result
            } else {
                filteredRoutePoints
            }

            // 3. リクエストの構築とAPI呼び出し
            val requestObj = RouteWeatherAdviceRequest(
                mode = mode,
                route_points = sampledPoints
            )

            val url = "https://route-weather-advice-api-dzpj65jtxa-an.a.run.app/generate-route-advice"
            Log.d("RouteAdvice", "Repository: Calling getRouteWeatherAdvice API. URL=\$url")

            val jsonBody = Gson().toJson(requestObj)
            
            val request = Request.Builder()
                .url(url)
                .header("x-api-key", com.example.weather_location_app.BuildConfig.ADVICE_API_KEY)
                .post(jsonBody.toRequestBody("application/json".toMediaType()))
                .build()

            val eventSource = RetrofitClient.sseEventSourceFactory.newEventSource(request, object : EventSourceListener() {
                override fun onEvent(eventSource: EventSource, id: String?, type: String?, data: String) {
                    if (data == "[DONE]") {
                        close()
                        return
                    }
                    try {
                        val json = JSONObject(data)
                        val chunk = json.optString("chunk", "")
                        if (chunk.isNotEmpty()) {
                            trySend(chunk)
                        }
                    } catch (e: Exception) {
                        Log.e("RouteAdvice", "JSON parse error", e)
                    }
                }

                override fun onFailure(eventSource: EventSource, t: Throwable?, response: okhttp3.Response?) {
                    Log.e("RouteAdvice", "SSE Error: \${t?.message}", t)
                    close(t ?: Exception("SSE Connection Failed"))
                }

                override fun onClosed(eventSource: EventSource) {
                    close()
                }
            })

            awaitClose {
                eventSource.cancel()
            }
        } catch (e: Exception) {
            close(e)
        }
    }

    /**
     * 文字列の日時をOffsetDateTimeに変換 (WeatherRepositoryから引用)
     */
    private fun parseDateTime(dtStr: String): OffsetDateTime {
        return try {
            OffsetDateTime.parse(dtStr)
        } catch (e: Exception) {
            try {
                val formatted = dtStr.replace(" ", "T")
                if (!formatted.contains("+") && !formatted.endsWith("Z")) {
                    OffsetDateTime.parse(formatted + "+09:00")
                } else {
                    OffsetDateTime.parse(formatted)
                }
            } catch (e2: Exception) {
                OffsetDateTime.now(ZoneOffset.ofHours(9))
            }
        }
    }
}
