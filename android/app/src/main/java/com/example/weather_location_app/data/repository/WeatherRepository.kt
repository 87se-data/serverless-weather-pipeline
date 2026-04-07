package com.example.weather_location_app.data.repository

import android.content.Context
import android.location.Geocoder
import android.util.Log
import com.example.weather_location_app.WeatherConfig
import com.example.weather_location_app.data.SunTimeRegion
import com.example.weather_location_app.data.api.GpvDataItem
import com.example.weather_location_app.data.api.PointWeatherAdviceRequest
import com.example.weather_location_app.data.api.RetrofitClient
import com.google.android.gms.maps.model.LatLng
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import org.shredzone.commons.suncalc.SunTimes
import java.time.Duration
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZoneOffset

import com.example.weather_location_app.data.api.WeatherDataForAi
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
import kotlin.math.sqrt

/**
 * 気象データ取得・加工の結果を保持するクラス
 */
data class WeatherDataResult(
    val weatherData: List<GpvDataItem>? = null,
    val instabilityData: List<GpvDataItem>? = null,
    val sunTimeRegions: List<SunTimeRegion> = emptyList(),
    val instabilitySunRegions: List<SunTimeRegion> = emptyList(),
    val alerts: Map<String, Float> = emptyMap(),
    val initialTime: String? = null,
    val address: String? = null
)

/**
 * 気象データに関するデータソース（API, Geocoder等）へのアクセスと加工を担当するRepository
 */
class WeatherRepository {

    /**
     * 指定された地点の気象データ、住所、日の出日の入り時間をまとめて取得・加工して返す
     */
    suspend fun getFullWeatherData(context: Context, targetLatLng: LatLng): Result<WeatherDataResult> = withContext(Dispatchers.IO) {
        try {
            coroutineScope {
                // 1. 各種データ取得を並列で実行
                val addressDeferred = async { fetchAddress(context, targetLatLng) }
                val weatherDeferred = async {
                    RetrofitClient.weatherApi.getGpvData(
                        latitude = targetLatLng.latitude,
                        longitude = targetLatLng.longitude,
                        elementsCsv = WeatherConfig.API_ELEMENTS.joinToString(",")
                    )
                }
                val instabilityDeferred = async {
                    RetrofitClient.instabilityApi.getInstabilityData(
                        latitude = targetLatLng.latitude,
                        longitude = targetLatLng.longitude
                    )
                }

                val weatherRes = weatherDeferred.await()
                val instabilityRes = instabilityDeferred.await()
                val address = addressDeferred.await()

                // 2. データの整合性チェック
                if (weatherRes.status != "OK" && instabilityRes.status != "OK") {
                    return@coroutineScope Result.failure(Exception("Weather: ${weatherRes.status}, Instability: ${instabilityRes.status}"))
                }

                // 3. データの加工処理（重い処理をDefaultディスパッチャで行うことも検討できるが、IOでも十分）
                val wData = if (weatherRes.status == "OK") {
                    weatherRes.result?.data?.map { it.copy(lat = targetLatLng.latitude, lon = targetLatLng.longitude) }
                } else null
                
                val iData = if (instabilityRes.status == "OK") {
                    instabilityRes.result?.data?.map { it.copy(lat = targetLatLng.latitude, lon = targetLatLng.longitude) }
                } else null
                
                val jstOffset = ZoneOffset.ofHours(9)
                val now = OffsetDateTime.now(jstOffset)

                val wSunRegions = wData?.let { calculateSunRegions(it, targetLatLng, jstOffset) } ?: emptyList()
                val iSunRegions = iData?.let { calculateSunRegions(it, targetLatLng, jstOffset) } ?: emptyList()

                // 各要素のアラート判定
                val alertMap = mutableMapOf<String, Float>()
                WeatherConfig.WEATHER_ELEMENTS.forEach { element ->
                    val dataList = if (WeatherConfig.INSTABILITY_ELEMENTS.contains(element.key)) iData else wData
                    if (dataList != null) {
                        val futureData = dataList.filter { item ->
                            try { !parseDateTime(item.datetime).isBefore(now) } catch (e: Exception) { true }
                        }
                        val values = futureData.mapNotNull { item -> element.getValue(item)?.toFloat() }
                        if (values.isNotEmpty()) {
                            // SSIは値が低いほど不安定、それ以外は高いほど危険
                            val extremeVal = if (element.key == "ssi") values.minOrNull() else values.maxOrNull()
                            extremeVal?.let {
                                if (element.getLevelLabel(it) != null) {
                                    alertMap[element.key] = it
                                }
                            }
                        }
                    }
                }

                Result.success(WeatherDataResult(
                    weatherData = wData,
                    instabilityData = iData,
                    sunTimeRegions = wSunRegions,
                    instabilitySunRegions = iSunRegions,
                    alerts = alertMap,
                    initialTime = weatherRes.result?.initial_datetime,
                    address = address
                ))
            }
        } catch (e: Exception) {
            Result.failure(e)
        }
    }

    /**
     * 座標から住所を取得
     */
    private fun fetchAddress(context: Context, targetLatLng: LatLng): String? {
        return try {
            val geocoder = Geocoder(context, java.util.Locale.JAPAN)
            val addresses = geocoder.getFromLocation(targetLatLng.latitude, targetLatLng.longitude, 1)
            if (!addresses.isNullOrEmpty()) {
                val addr = addresses[0]
                val fullAddress = addr.getAddressLine(0) ?: ""
                fullAddress
                    .replace("^日本、".toRegex(), "")
                    .replace("^日本".toRegex(), "")
                    .replace("〒\\d{3}-\\d{4}\\s*".toRegex(), "")
                    .trim()
            } else null
        } catch (e: Exception) {
            null
        }
    }

    /**
     * 地点用AIお天気アドバイスを取得
     */
    suspend fun getPointWeatherAdvice(
        latitude: Double,
        longitude: Double,
        weatherData: List<GpvDataItem>?,
        instabilityData: List<GpvDataItem>?
    ): Flow<String> = callbackFlow {
        val url = "https://point-weather-advice-api-dzpj65jtxa-an.a.run.app/generate-advice"
        Log.d("PointAdvice", "Repository: Calling getPointWeatherAdvice API. URL=$url")
        
        // 現在時刻より前のデータを除外
        val now = OffsetDateTime.now(ZoneOffset.ofHours(9))
        
        // weatherData と instabilityData を datetime で紐付けて構造化
        val weatherMap = weatherData?.associateBy { it.datetime } ?: emptyMap()
        val instabilityMap = instabilityData?.associateBy { it.datetime } ?: emptyMap()
        
        // 両方のデータに含まれる全 datetime を取得
        val allDateTimes = (weatherMap.keys + instabilityMap.keys).sorted()
        
        val structuredDataList = allDateTimes.mapNotNull { dt ->
            try {
                val dtObj = parseDateTime(dt)
                if (dtObj.isBefore(now)) return@mapNotNull null
                
                val wItem = weatherMap[dt]
                val iItem = instabilityMap[dt]
                
                // 値の抽出補助関数
                fun getValue(item: GpvDataItem?, key: String): Double {
                    if (item == null) return 0.0
                    // contentsリストを走査し、指定されたkeyを持つ最初のvalueを返す
                    return item.contents.firstNotNullOfOrNull { it.value[key] } ?: 0.0
                }

                // 風速計算 (2_2: U, 2_3: V)
                val u = getValue(wItem, "2_2")
                val v = getValue(wItem, "2_3")
                val windSpeed = sqrt(u * u + v * v)
                
                // ケルビンから摂氏への変換 (0_0: Temperature)
                val tempKelvin = getValue(wItem, "0_0")
                val tempCelsius = if (tempKelvin > 0) tempKelvin - 273.15 else 0.0

                WeatherDataForAi(
                    datetime = dt,
                    // Instability
                    ssi = getValue(iItem, "ssi"),
                    ki = getValue(iItem, "ki"),
                    tt = getValue(iItem, "tt"),
                    theta_e = getValue(iItem, "theta_e"),
                    water_vapor_flux = getValue(iItem, "water_vapor_flux"),
                    lcl = getValue(iItem, "lcl"),
                    lfc = getValue(iItem, "lfc"),
                    el = getValue(iItem, "el"),
                    cape = getValue(iItem, "cape"),
                    cin = getValue(iItem, "cin"),
                    // Weather surface
                    temperature = tempCelsius,
                    humidity = getValue(wItem, "1_1"),
                    precipitation = getValue(wItem, "1_8"),
                    wind_speed = windSpeed,
                    solar_radiation = getValue(wItem, "4_7"),
                    total_cloud_cover = getValue(wItem, "6_1"),
                    low_cloud_cover = getValue(wItem, "6_3"),
                    mid_cloud_cover = getValue(wItem, "6_4"),
                    altostratus_cloud_cover = getValue(wItem, "6_5"),
                    laundry_index = getValue(wItem, "laundry_index"),
                    wbgt = getValue(wItem, "wbgt")
                )
            } catch (e: Exception) {
                null
            }
        }

        if (structuredDataList.isEmpty()) {
            close(Exception("送信可能な気象データがありません。"))
            return@callbackFlow
        }

        val requestObj = PointWeatherAdviceRequest(latitude, longitude, structuredDataList)
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
                    Log.e("PointAdvice", "JSON parse error", e)
                }
            }

            override fun onFailure(eventSource: EventSource, t: Throwable?, response: okhttp3.Response?) {
                Log.e("PointAdvice", "SSE Error: ${t?.message}", t)
                close(t ?: Exception("SSE Connection Failed"))
            }

            override fun onClosed(eventSource: EventSource) {
                close()
            }
        })

        awaitClose {
            eventSource.cancel()
        }
    }

    /**
     * 文字列の日時をOffsetDateTimeに変換
     */
    fun parseDateTime(dtStr: String): OffsetDateTime {
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

    /**
     * 日の出・日の入り領域を計算
     */
    private fun calculateSunRegions(
        dataList: List<GpvDataItem>,
        targetLatLng: LatLng,
        jstOffset: ZoneOffset
    ): List<SunTimeRegion> {
        if (dataList.isEmpty()) return emptyList()
        
        val list = mutableListOf<SunTimeRegion>()
        val firstDt = parseDateTime(dataList.first().datetime).withOffsetSameInstant(jstOffset)
        val lastDt = parseDateTime(dataList.last().datetime).withOffsetSameInstant(jstOffset)
        val totalHours = Duration.between(firstDt, lastDt).toMillis().toFloat() / 3600000f
        
        var currentDay = firstDt.toLocalDate().minusDays(1)
        val endDay = lastDt.toLocalDate().plusDays(1)
        val lat = targetLatLng.latitude
        val lon = targetLatLng.longitude

        while (currentDay <= endDay) {
            val times = SunTimes.compute()
                .at(lat, lon)
                .on(currentDay.year, currentDay.monthValue, currentDay.dayOfMonth)
                .timezone(ZoneId.of("Asia/Tokyo"))
                .execute()
            
            val sunrise = times.rise?.let { OffsetDateTime.ofInstant(it.toInstant(), jstOffset) }
            val sunset = times.set?.let { OffsetDateTime.ofInstant(it.toInstant(), jstOffset) }
            
            val nextDay = currentDay.plusDays(1)
            val sunriseNext = SunTimes.compute()
                .at(lat, lon)
                .on(nextDay.year, nextDay.monthValue, nextDay.dayOfMonth)
                .timezone(ZoneId.of("Asia/Tokyo"))
                .execute()
                .rise?.let { OffsetDateTime.ofInstant(it.toInstant(), jstOffset) }

            if (sunrise != null && sunset != null) {
                val sX = if (sunrise.isBefore(firstDt)) 0f else Duration.between(firstDt, sunrise).toMillis().toFloat() / 3600000f
                val eX = if (sunset.isAfter(lastDt)) totalHours else Duration.between(firstDt, sunset).toMillis().toFloat() / 3600000f
                if (sX < totalHours && eX > 0f) {
                    val iconX = Duration.between(firstDt, sunrise).toMillis().toFloat() / 3600000f
                    list.add(SunTimeRegion(sX, eX, true, iconX, "☀️"))
                }
            }
            
            if (sunset != null && sunriseNext != null) {
                val sX = if (sunset.isBefore(firstDt)) 0f else Duration.between(firstDt, sunset).toMillis().toFloat() / 3600000f
                val eX = if (sunriseNext.isAfter(lastDt)) totalHours else Duration.between(firstDt, sunriseNext).toMillis().toFloat() / 3600000f
                if (sX < totalHours && eX > 0f) {
                    val iconX = Duration.between(firstDt, sunset).toMillis().toFloat() / 3600000f
                    list.add(SunTimeRegion(sX, eX, false, iconX, "🌙"))
                }
            }
            currentDay = currentDay.plusDays(1)
        }
        return list
    }
}
