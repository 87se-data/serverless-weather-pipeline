package com.example.weather_location_app.ui.viewmodels

import android.content.Context
import android.util.Log
import androidx.compose.runtime.State
import androidx.compose.runtime.mutableStateOf
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.example.weather_location_app.WeatherConfig
import com.example.weather_location_app.data.SunTimeRegion
import com.example.weather_location_app.data.api.GpvDataItem
import com.example.weather_location_app.data.repository.WeatherRepository
import com.google.android.gms.maps.model.LatLng
import kotlinx.coroutines.launch
import java.time.Duration
import java.time.OffsetDateTime
import java.time.ZoneOffset

import kotlinx.coroutines.flow.catch

/**
 * ピンポイント天気画面のUI状態を保持する
 */
data class WeatherUiState(
    val isLoading: Boolean = false,
    val weatherData: List<GpvDataItem>? = null,
    val instabilityData: List<GpvDataItem>? = null,
    val sunTimeRegions: List<SunTimeRegion> = emptyList(),
    val instabilitySunRegions: List<SunTimeRegion> = emptyList(),
    val error: String? = null,
    val lastFetchedLatLng: LatLng? = null,
    val fetchedAddress: String? = null,
    val elementAlerts: Map<String, Float> = emptyMap(),
    val selectedElement: WeatherConfig.WeatherElement = WeatherConfig.WEATHER_ELEMENTS.first { it.key == WeatherConfig.DEFAULT_ELEMENT_KEY },
    val selectedPointIndex: Int? = null,
    val initialTime: String? = null,
    val selectedTargetTime: String? = null,
    val isInteracting: Boolean = false,
    val pointAdviceText: String? = null,
    val isPointAdviceLoading: Boolean = false,
    val showPointAdviceDialog: Boolean = false,
    val isPointAdviceCompleted: Boolean = false,
    val lastPointAdviceLatLng: LatLng? = null,
    val lastPointAdviceTimestamp: Long = 0L
)

/**
 * ピンポイント天気画面のロジックと状態管理を担当するViewModel
 */
class WeatherViewModel : ViewModel() {
    private val repository = WeatherRepository()
    
    private val _uiState = mutableStateOf(WeatherUiState())
    val uiState: State<WeatherUiState> = _uiState

    /**
     * 選択された気象要素を更新する
     */
    fun updateSelectedElement(element: WeatherConfig.WeatherElement) {
        val currentState = _uiState.value
        _uiState.value = currentState.copy(selectedElement = element)
        recalculateSelectedPointIndex()
    }

    /**
     * 現在の選択時刻に基づき、現在のデータセット内での最適なインデックスを再計算する
     */
    private fun recalculateSelectedPointIndex() {
        val currentState = _uiState.value
        val data = getActiveData()
        
        if (!data.isNullOrEmpty()) {
            var targetIndex = 0
            val currentSelectedTime = currentState.selectedTargetTime

            if (currentSelectedTime != null) {
                // 1. 選択時刻がある場合（要素切り替え時など）：時間的に一番近いデータを探す
                val prevTime = repository.parseDateTime(currentSelectedTime)
                var minDiff = Long.MAX_VALUE
                
                data.forEachIndexed { index, item ->
                    val itemTime = repository.parseDateTime(item.datetime)
                    val diff = Math.abs(Duration.between(prevTime, itemTime).toMillis())
                    if (diff < minDiff) {
                        minDiff = diff
                        targetIndex = index
                    }
                }
            } else {
                // 2. 選択時刻がない場合（新規取得直後など）：現在時刻以降の最初のデータを探す
                val now = OffsetDateTime.now(ZoneOffset.ofHours(9))
                var found = false
                
                for (index in data.indices) {
                    val itemTime = repository.parseDateTime(data[index].datetime)
                    if (!itemTime.isBefore(now)) {
                        targetIndex = index
                        found = true
                        break
                    }
                }
                // 未来のデータがない場合は最後のデータ
                if (!found) {
                    targetIndex = data.size - 1
                }
            }
            
            _uiState.value = _uiState.value.copy(
                selectedPointIndex = targetIndex,
                selectedTargetTime = data[targetIndex].datetime,
                isInteracting = false
            )
        }
    }

    /**
     * 選択された地点インデックスを更新する
     */
    fun updateSelectedPointIndex(index: Int?, isInteracting: Boolean = false) {
        val data = getActiveData()
        val targetTime = if (index != null && data != null) {
            data.getOrNull(index)?.datetime
        } else {
            _uiState.value.selectedTargetTime
        }
        _uiState.value = _uiState.value.copy(
            selectedPointIndex = index,
            selectedTargetTime = targetTime,
            isInteracting = isInteracting
        )
    }

    /**
     * 現在選択中の要素に対応するデータを取得
     */
    fun getActiveData(): List<GpvDataItem>? {
        val element = _uiState.value.selectedElement
        return if (WeatherConfig.INSTABILITY_ELEMENTS.contains(element.key)) {
            _uiState.value.instabilityData
        } else {
            _uiState.value.weatherData
        }
    }

    /**
     * 現在選択中の要素に対応する日の出日の入り情報を取得
     */
    fun getActiveSunRegions(): List<SunTimeRegion> {
        val element = _uiState.value.selectedElement
        return if (WeatherConfig.INSTABILITY_ELEMENTS.contains(element.key)) {
            _uiState.value.instabilitySunRegions
        } else {
            _uiState.value.sunTimeRegions
        }
    }

    /**
     * 地点用AIお天気アドバイスを取得する（または再表示）
     */
    fun onAiAdviceButtonClicked() {
        val currentState = _uiState.value
        
        // 既にロード中、または直前の結果が残っている場合は、単にシートを再表示するだけ
        if (currentState.isPointAdviceLoading || currentState.pointAdviceText != null) {
            _uiState.value = currentState.copy(showPointAdviceDialog = true)
            return
        }

        val latLng = currentState.lastFetchedLatLng
        val weatherData = currentState.weatherData
        val instabilityData = currentState.instabilityData

        Log.d("PointAdvice", "fetchPointWeatherAdvice called. latLng=$latLng, weatherData size=${weatherData?.size ?: "null"}")

        if (latLng == null) {
            Log.e("PointAdvice", "Abort: latLng is null.")
            return
        }
        if (weatherData == null) {
            Log.e("PointAdvice", "Abort: weatherData is null.")
            return
        }
        if (instabilityData == null) {
            Log.e("PointAdvice", "Abort: instabilityData is null.")
            return
        }

        // キャッシュ判定: 30分以内、かつ同一地点（緯度経度誤差 0.0001度以内、約11m）
        val lastLatLng = currentState.lastPointAdviceLatLng
        val lastTime = currentState.lastPointAdviceTimestamp
        val now = System.currentTimeMillis()
        val isSameLocation = lastLatLng != null && 
            Math.abs(lastLatLng.latitude - latLng.latitude) < 0.0001 &&
            Math.abs(lastLatLng.longitude - latLng.longitude) < 0.0001
        
        if (isSameLocation && currentState.pointAdviceText != null && (now - lastTime) < 1800000L) {
            Log.d("PointAdvice", "Cache Hit: Showing previous advice. (Location same, time diff=${now - lastTime}ms)")
            _uiState.value = _uiState.value.copy(showPointAdviceDialog = true)
            return
        }

        viewModelScope.launch {
            _uiState.value = _uiState.value.copy(
                isPointAdviceLoading = true,
                error = null,
                showPointAdviceDialog = true,
                pointAdviceText = "",
                isPointAdviceCompleted = false
            )
            Log.d("PointAdvice", "API call started...")
            
            try {
                repository.getPointWeatherAdvice(latLng.latitude, latLng.longitude, weatherData, instabilityData)
                    .catch { error ->
                        Log.e("PointAdvice", "API call failed: ${error.message}", error)
                        _uiState.value = _uiState.value.copy(
                            isPointAdviceLoading = false,
                            error = "AIアドバイスの取得に失敗しました: ${error.localizedMessage ?: "Unknown Error"}"
                        )
                    }
                    .collect { chunk ->
                        val currentState = _uiState.value
                        _uiState.value = currentState.copy(
                            isPointAdviceLoading = false, // 文字が届き始めたら即座にローディングを終了する
                            pointAdviceText = (currentState.pointAdviceText ?: "") + chunk,
                            showPointAdviceDialog = true,
                            lastPointAdviceLatLng = latLng,
                            lastPointAdviceTimestamp = System.currentTimeMillis()
                        )
                    }
                // collectが終了した＝全文受信完了！
                _uiState.value = _uiState.value.copy(isPointAdviceCompleted = true)
            } catch (e: Exception) {
                Log.e("PointAdvice", "API call exception: ${e.message}", e)
                _uiState.value = _uiState.value.copy(
                    isPointAdviceLoading = false,
                    error = "AIアドバイスの取得に失敗しました: ${e.localizedMessage ?: "Unknown Error"}"
                )
            }
        }
    }

    /**
     * アドバイスダイアログを閉じる
     */
    fun dismissPointAdviceDialog() {
        _uiState.value = _uiState.value.copy(showPointAdviceDialog = false)
    }

    /**
     * 指定された座標の気象データを取得する
     */
    fun fetchWeatherData(context: Context, targetLatLng: LatLng) {
        viewModelScope.launch {
            _uiState.value = _uiState.value.copy(isLoading = true, error = null)
            
            repository.getFullWeatherData(context, targetLatLng).onSuccess { result ->
                _uiState.value = _uiState.value.copy(
                    isLoading = false,
                    weatherData = result.weatherData,
                    instabilityData = result.instabilityData,
                    sunTimeRegions = result.sunTimeRegions,
                    instabilitySunRegions = result.instabilitySunRegions,
                    lastFetchedLatLng = targetLatLng,
                    fetchedAddress = result.address,
                    elementAlerts = result.alerts,
                    initialTime = result.initialTime,
                    // 新規取得時は選択状態をリセットし、recalculateSelectedPointIndexで現在時刻基準に再計算させる
                    selectedPointIndex = null,
                    selectedTargetTime = null,
                    // AIアドバイス関連のステートもリセット
                    pointAdviceText = null,
                    showPointAdviceDialog = false,
                    isPointAdviceLoading = false,
                    isPointAdviceCompleted = false,
                    lastPointAdviceTimestamp = 0L // キャッシュを無効化
                )
                recalculateSelectedPointIndex()
            }.onFailure { error ->
                _uiState.value = _uiState.value.copy(
                    isLoading = false,
                    error = error.message ?: "Unknown Error",
                    fetchedAddress = null
                )
            }
        }
    }
}
