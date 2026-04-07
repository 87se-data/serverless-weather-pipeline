package com.example.weather_location_app.ui.viewmodels

import androidx.compose.runtime.State
import androidx.compose.runtime.mutableStateOf
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.example.weather_location_app.data.SunTimeRegion
import com.example.weather_location_app.data.api.*
import com.example.weather_location_app.data.repository.RouteRepository
import com.google.android.gms.maps.model.LatLng
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.shredzone.commons.suncalc.SunTimes
import java.time.Duration
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.*

data class RouteUiState(
    val startLatLng: LatLng? = null,
    val endLatLng: LatLng? = null,
    val isSelectingStart: Boolean = true,
    val transportMode: String = "walking",
    val isLoading: Boolean = false,
    val routePoints: List<LatLng> = emptyList(),
    val weatherData: List<GpvDataItem>? = null,
    val sunTimeRegions: List<SunTimeRegion> = emptyList(),
    val selectedRoutePoint: LatLng? = null,
    val selectedRouteIndex: Int? = null, // 【追加】インデックス管理
    val departureTime: Calendar = Calendar.getInstance().apply { 
        set(Calendar.SECOND, 0)
        set(Calendar.MILLISECOND, 0)
    },
    val error: String? = null,
    // AIアドバイス用
    val routeAdviceText: String? = null,
    val isRouteAdviceLoading: Boolean = false,
    val showRouteAdviceDialog: Boolean = false,
    val isRouteAdviceCompleted: Boolean = false,
    val routeAdviceError: String? = null,
    val lastAdviceTimestamp: Long = 0L,
    val lastAdviceRouteData: RouteResponse? = null,
    val currentRouteResponse: RouteResponse? = null, // 現在のルートデータ
    val showPastTimeWarning: Boolean = false
)

class RouteViewModel : ViewModel() {
    companion object {
        const val MAX_DEPARTURE_OFFSET_HOURS = 48 // 24時間制限が厳しすぎたため48時間に拡張
    }

    private val _uiState = mutableStateOf(RouteUiState())
    val uiState: State<RouteUiState> = _uiState
    
    private val routeRepository = RouteRepository()

    fun toggleSelectionMode(isStart: Boolean) {
        _uiState.value = _uiState.value.copy(isSelectingStart = isStart)
    }

    fun dismissPastTimeWarning() {
        _uiState.value = _uiState.value.copy(showPastTimeWarning = false)
    }

    fun setLocation(latLng: LatLng, isStart: Boolean) {
        if (isStart) {
            _uiState.value = _uiState.value.copy(
                startLatLng = latLng,
                isSelectingStart = false,
                weatherData = null,
                routePoints = emptyList()
            )
        } else {
            _uiState.value = _uiState.value.copy(
                endLatLng = latLng,
                isSelectingStart = true, 
                weatherData = null,
                routePoints = emptyList()
            )
        }
    }

    fun onMapClick(latLng: LatLng) {
        setLocation(latLng, _uiState.value.isSelectingStart)
    }

    fun updateTransportMode(mode: String) {
        if (_uiState.value.transportMode == mode) return
        _uiState.value = _uiState.value.copy(
            transportMode = mode,
            weatherData = null,
            routePoints = emptyList(),
            selectedRoutePoint = null,
            selectedRouteIndex = null,
            routeAdviceText = null,
            isRouteAdviceLoading = false,
            isRouteAdviceCompleted = false
        )
    }

    fun setDepartureTime(calendar: Calendar) {
        val now = Calendar.getInstance()
        // 基準を「現在時刻の30分前」に緩和する
        val nowForCompare = (now.clone() as Calendar).apply {
            add(Calendar.MINUTE, -30)
            set(Calendar.SECOND, 0)
            set(Calendar.MILLISECOND, 0)
        }
        
        val maxLimit = Calendar.getInstance().apply { 
            add(Calendar.HOUR_OF_DAY, MAX_DEPARTURE_OFFSET_HOURS) 
        }
        
        var warning = false
        val finalTime = when {
            calendar.before(nowForCompare) -> {
                warning = true
                now // リセット先は現在時刻のままでOK
            }
            calendar.after(maxLimit) -> maxLimit
            else -> calendar
        }
        
        _uiState.value = _uiState.value.copy(
            departureTime = finalTime,
            weatherData = null,
            routePoints = emptyList(),
            showPastTimeWarning = warning,
            routeAdviceText = null,
            isRouteAdviceLoading = false,
            isRouteAdviceCompleted = false
        )
    }

    fun resetDepartureTimeToNow() {
        _uiState.value = _uiState.value.copy(
            departureTime = Calendar.getInstance().apply {
                set(Calendar.SECOND, 0)
                set(Calendar.MILLISECOND, 0)
            },
            weatherData = null,
            routePoints = emptyList(),
            routeAdviceText = null,
            isRouteAdviceLoading = false,
            isRouteAdviceCompleted = false
        )
    }

    // 【最適化】インデックスが変わった時だけ状態を更新する
    fun updateSelectedRoutePoint(index: Int?) {
        if (index == _uiState.value.selectedRouteIndex) return

        val points = _uiState.value.routePoints
        val selectedPoint = if (index != null && index in points.indices) {
            points[index]
        } else {
            null
        }
        
        _uiState.value = _uiState.value.copy(
            selectedRoutePoint = selectedPoint,
            selectedRouteIndex = index
        )
    }

    fun fetchRouteWeatherData() {
        val start = _uiState.value.startLatLng ?: return
        val end = _uiState.value.endLatLng ?: return
        val mode = _uiState.value.transportMode
        
        val nowCal = Calendar.getInstance()
        // 基準を「現在時刻の30分前」に緩和する
        val nowForCompare = (nowCal.clone() as Calendar).apply {
            add(Calendar.MINUTE, -30)
            set(Calendar.SECOND, 0)
            set(Calendar.MILLISECOND, 0)
        }
        
        // 検索実行時に時間が30分以上過去になっていたら、現在時刻にリセットしてダイアログを出し、処理を中断する
        if (_uiState.value.departureTime.before(nowForCompare)) {
            _uiState.value = _uiState.value.copy(
                departureTime = nowCal,
                showPastTimeWarning = true
            )
            return
        }

        // ユーザーが設定した出発時刻をそのまま使用する（30分前までのバリデーションは通過済みのため）
        val departureEpoch = _uiState.value.departureTime.timeInMillis / 1000

        viewModelScope.launch {
            _uiState.value = _uiState.value.copy(isLoading = true, error = null)
            try {
                // 座標の精度を小数点以下6桁に丸める
                val originParam = "%.6f,%.6f".format(Locale.US, start.latitude, start.longitude)
                val destinationParam = "%.6f,%.6f".format(Locale.US, end.latitude, end.longitude)
                
                val response = if (mode == "hiking") {
                    RetrofitClient.trailApi.getTrailData(
                        origin = originParam,
                        destination = destinationParam,
                        means = mode,
                        departure = departureEpoch
                    )
                } else {
                    RetrofitClient.routeApi.getRouteData(
                        origin = originParam,
                        destination = destinationParam,
                        means = mode,
                        departure = departureEpoch
                    )
                }

                if (response.information != null && response.information.isNotEmpty()) {
                    // 【パフォーマンス改善】重いリスト変換処理を Default スレッドで実行
                    val result = withContext(Dispatchers.Default) {
                        // 有効な座標を持つデータのみを抽出してインデックスを同期させる
                        val sortedInfo = response.information.toSortedMap()
                        val validPointsEntries = sortedInfo.filter { entry ->
                            entry.value.latitude?.toDoubleOrNull() != null && 
                            entry.value.longitude?.toDoubleOrNull() != null
                        }

                        if (validPointsEntries.isEmpty()) {
                            return@withContext null
                        }

                        val convertedData = validPointsEntries.map { (timestamp, point) ->
                            val surfaceValues = point.surface?.mapValues { it.value.toDoubleOrNull() ?: 0.0 }?.toMutableMap() ?: mutableMapOf()
                            
                            // 体感気温の処理（ケルビンから摂氏へ変換）
                            point.apparent_temp?.toDoubleOrNull()?.let { kelvin ->
                                surfaceValues["apparent_temp"] = kelvin - 273.15
                            }

                            GpvDataItem(
                                datetime = timestamp,
                                contents = listOf(
                                    GpvContent(
                                        surface = "surface",
                                        value = surfaceValues
                                    )
                                ),
                                lat = point.latitude?.toDoubleOrNull(),
                                lon = point.longitude?.toDoubleOrNull(),
                                elevation = point.elevation
                            )
                        }
                        
                        val detailedPoints = validPointsEntries.values.map { 
                            LatLng(it.latitude!!.toDouble(), it.longitude!!.toDouble())
                        }

                        // 日の出・日の入り計算
                        val sunRegionsList = mutableListOf<SunTimeRegion>()
                        val jstOffset = ZoneOffset.ofHours(9)
                        if (convertedData.isNotEmpty()) {
                            val firstDt = parseDateTime(convertedData.first().datetime).withOffsetSameInstant(jstOffset)
                            val lastDt = parseDateTime(convertedData.last().datetime).withOffsetSameInstant(jstOffset)
                            val totalHours = Duration.between(firstDt, lastDt).toMillis().toFloat() / 3600000f
                            
                            var currentDay = firstDt.toLocalDate().minusDays(1)
                            val endDay = lastDt.toLocalDate().plusDays(1)

                            while (currentDay <= endDay) {
                                // ルートの場合は各日の代表地点（その日の最初のデータポイントなど）の座標を使用
                                val targetPoint = convertedData.find { 
                                    try { parseDateTime(it.datetime).toLocalDate() == currentDay } catch(e: Exception) { false }
                                } ?: convertedData.first()
                                
                                val lat = targetPoint.lat ?: 35.6895
                                val lon = targetPoint.lon ?: 139.6917

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
                                        sunRegionsList.add(SunTimeRegion(sX, eX, true, iconX, "☀️"))
                                    }
                                }
                                
                                if (sunset != null && sunriseNext != null) {
                                    val sX = if (sunset.isBefore(firstDt)) 0f else Duration.between(firstDt, sunset).toMillis().toFloat() / 3600000f
                                    val eX = if (sunriseNext.isAfter(lastDt)) totalHours else Duration.between(firstDt, sunriseNext).toMillis().toFloat() / 3600000f
                                    if (sX < totalHours && eX > 0f) {
                                        val iconX = Duration.between(firstDt, sunset).toMillis().toFloat() / 3600000f
                                        sunRegionsList.add(SunTimeRegion(sX, eX, false, iconX, "🌙"))
                                    }
                                }
                                currentDay = currentDay.plusDays(1)
                            }
                        }
                        
                        Triple(convertedData, detailedPoints, sunRegionsList)
                    }

                    if (result == null) {
                        _uiState.value = _uiState.value.copy(isLoading = false, error = "ルート上に有効な地点が見つかりませんでした")
                        return@launch
                    }

                    _uiState.value = _uiState.value.copy(
                        isLoading = false,
                        weatherData = result.first,
                        routePoints = result.second,
                        sunTimeRegions = result.third,
                        selectedRoutePoint = null,
                        selectedRouteIndex = null,
                        // キャッシュのリセットと現在のデータの保存
                        currentRouteResponse = response,
                        routeAdviceText = null,
                        lastAdviceTimestamp = 0L,
                        lastAdviceRouteData = null,
                        isRouteAdviceCompleted = false
                    )
                } else {
                    val errorMsg = when(mode) {
                        "bicycling" -> "自転車ルートが見つかりませんでした。対象エリア外か、ルートが存在しない可能性があります。"
                        "hiking" -> "登山ルートが見つかりませんでした。対象エリア外か、ルートが存在しない可能性があります。"
                        else -> "ルートデータが見つかりませんでした"
                    }
                    _uiState.value = _uiState.value.copy(isLoading = false, error = errorMsg)
                }
            } catch (e: Exception) {
                _uiState.value = _uiState.value.copy(isLoading = false, error = "通信エラー: ${e.message}")
            }
        }
    }

    /**
     * AIアドバイスダイアログを閉じる
     */
    fun dismissRouteAdviceDialog() {
        _uiState.value = _uiState.value.copy(showRouteAdviceDialog = false)
    }

    /**
     * ルート天気アドバイスを取得（または再表示）
     */
    fun onAiAdviceButtonClicked() {
        if (_uiState.value.isRouteAdviceLoading || _uiState.value.routeAdviceText != null) {
            _uiState.value = _uiState.value.copy(showRouteAdviceDialog = true)
            return
        }

        val routeResponse = _uiState.value.currentRouteResponse ?: return
        val means = _uiState.value.transportMode
        
        // キャッシュ判定: データが同じ かつ 30分以内 かつ 既にテキストがある場合
        val isSameData = _uiState.value.lastAdviceRouteData == routeResponse
        val isRecent = System.currentTimeMillis() - _uiState.value.lastAdviceTimestamp < 1800000
        val hasAdvice = _uiState.value.routeAdviceText != null
        
        if (isSameData && isRecent && hasAdvice) {
            _uiState.value = _uiState.value.copy(showRouteAdviceDialog = true)
            return
        }

        // モード判定: 登山系なら "mountain"、それ以外はそのまま
        val mode = if (means == "hiking" || means == "mountain") "mountain" else means
        
        viewModelScope.launch {
            _uiState.value = _uiState.value.copy(
                isRouteAdviceLoading = true,
                showRouteAdviceDialog = true,
                routeAdviceError = null,
                isRouteAdviceCompleted = false,
                routeAdviceText = ""
            )
            
            try {
                routeRepository.getRouteWeatherAdvice(mode, routeResponse).collect { chunk ->
                    if (_uiState.value.isRouteAdviceLoading) {
                        _uiState.value = _uiState.value.copy(isRouteAdviceLoading = false)
                    }
                    _uiState.value = _uiState.value.copy(
                        routeAdviceText = (_uiState.value.routeAdviceText ?: "") + chunk
                    )
                }
                
                _uiState.value = _uiState.value.copy(
                    isRouteAdviceCompleted = true,
                    lastAdviceTimestamp = System.currentTimeMillis(),
                    lastAdviceRouteData = routeResponse
                )
            } catch (e: Exception) {
                _uiState.value = _uiState.value.copy(
                    routeAdviceError = e.message ?: "アドバイスの取得に失敗しました",
                    isRouteAdviceLoading = false,
                    isRouteAdviceCompleted = true
                )
            }
        }
    }

    private fun parseDateTime(dtStr: String): OffsetDateTime {
        return try {
            OffsetDateTime.parse(dtStr)
        } catch (e: Exception) {
            try {
                // ISO 8601形式（T区切り）に変換を試みる
                val formatted = dtStr.replace(" ", "T")
                if (!formatted.contains("+") && !formatted.endsWith("Z")) {
                    // オフセットがない場合は日本時間(+09:00)と仮定
                    OffsetDateTime.parse(formatted + "+09:00")
                } else {
                    OffsetDateTime.parse(formatted)
                }
            } catch (e2: Exception) {
                // 解析不能な場合は現在時刻を返す（クラッシュ回避）
                OffsetDateTime.now(ZoneOffset.ofHours(9))
            }
        }
    }
}
