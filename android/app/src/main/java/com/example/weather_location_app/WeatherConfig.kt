package com.example.weather_location_app

import android.graphics.Color
import com.example.weather_location_app.data.api.GpvDataItem
import kotlin.math.sqrt

object WeatherConfig {

    data class WeatherElement(
        val name: String,
        val key: String,
        val unit: String,
        val minY: Float? = null,
        val defaultMaxY: Float? = null,
        val isPressureLevel: Boolean = false, // 【追加】気圧面データ（pall）かどうか
        val tileSurface: String = "surface"   // 【追加】タイル取得時のsurfaceパラメータ名
    ) {
        fun getValue(item: GpvDataItem): Double? {
            return when (key) {
                "wind_speed" -> {
                    item.contents.firstNotNullOfOrNull { content ->
                        val u = content.value["2_2"]; val v = content.value["2_3"]
                        if (u != null && v != null) sqrt(u * u + v * v) else null
                    }
                }
                "0_0" -> item.contents.firstNotNullOfOrNull { it.value["0_0"] }?.let { it - 273.15 }
                "apparent_temp" -> item.contents.firstNotNullOfOrNull { it.value["apparent_temp"] }?.let {
                    if (it > 100.0) it - 273.15 else it
                }
                else -> {
                    // surface名を正規化（大文字小文字・hPaの有無）して比較し、目的のコンテンツを探す
                    val normalizedTargetSurface = tileSurface.lowercase().replace("hpa", "")
                    val targetContent = item.contents.find { 
                        val normalizedSurface = it.surface.lowercase().replace("hpa", "")
                        normalizedSurface == normalizedTargetSurface
                    }
                    
                    // 指定された surface 内に目的のキーがあればそれを返す。
                    // 無ければ全コンテンツからキーを探す（以前のSSI等の挙動を維持）。
                    // ※ここで firstOrNull 等の不鮮明なフォールバックは絶対に行わない！
                    targetContent?.value?.get(key) ?: item.contents.firstNotNullOfOrNull { it.value[key] }
                }
            }
        }

        fun getAlertIcon(v: Float): String? {
            return when (key) {
                "1_8" -> {
                    when {
                        v >= 30 -> "⚠️"
                        v >= 0.1 -> "☂️"
                        else -> null
                    }
                }
                "wind_speed" -> {
                    when {
                        v >= 20 -> "⚠️"
                        v >= 5 -> "༄"
                        else -> null
                    }
                }
                "laundry_index" -> {
                    when {
                        v >= 60 -> "👕"
                        else -> null
                    }
                }
                "theta_e" -> {
                    when {
                        v >= 330 -> "⚠️"
                        else -> null
                    }
                }
                else -> {
                    if (getLevelLabel(v) != null) "⚠️" else null
                }
            }
        }

        fun getLevelLabel(v: Float): String? {
            return when (key) {
                "1_8" -> {
                    when {
                        v >= 80 -> "猛烈な雨"
                        v >= 50 -> "非常に激しい雨"
                        v >= 30 -> "激しい雨"
                        v >= 20 -> "強い雨"
                        v >= 10 -> "やや強い雨"
                        v >= 5  -> "雨"
                        v >= 0.1 -> "小雨"
                        else -> null
                    }
                }
                "wind_speed" -> {
                    when {
                        v >= 25 -> "猛烈な風"
                        v >= 20 -> "非常に強い風"
                        v >= 15 -> "強い風"
                        v >= 10 -> "やや強い風"
                        v >= 5  -> "風あり"
                        else -> null
                    }
                }
                "ssi" -> {
                    when {
                        v <= -9 -> "極度に不安定"
                        v <= -6 -> "非常に不安定"
                        v <= -3 -> "中程度に不安定"
                        v <= 0 -> "やや不安定"
                        else -> null
                    }
                }
                "ki" -> {
                    when {
                        v >= 40 -> "雷雨の可能性:100%"
                        v >= 36 -> "雷雨の可能性:80-90%"
                        v >= 31 -> "雷雨の可能性:60-80%"
                        v >= 26 -> "雷雨の可能性:40-60%"
                        v >= 15 -> "雷雨の可能性:20-40%"
                        else -> null
                    }
                }
                "theta_e" -> {
                    when {
                        v >= 345 -> "記録的豪雨（危険）"
                        v >= 340 -> "線状降水帯（警戒）"
                        v >= 330 -> "短時間強雨（注意）"
                        else -> null
                    }
                }
                "water_vapor_flux" -> {
                    when {
                        v >= 350 -> "記録的豪雨クラス"
                        v >= 300 -> "極めて危険な水蒸気流入"
                        v >= 250 -> "線状降水帯の警戒レベル"
                        v >= 200 -> "大雨のポテンシャル"
                        v >= 150 -> "多めの水蒸気"
                        else -> null
                    }
                }
                "cape" -> {
                    when {
                        v >= 3500 -> "極端に不安定"
                        v >= 2500 -> "非常に不安定"
                        v >= 1000 -> "中程度に不安定"
                        v > 0 -> "やや不安定"
                        else -> null
                    }
                }
                "tt" -> {
                    when {
                        v >= 60 -> "広域で並程度の雷雨や散発的で激しい雷雨の可能性"
                        v >= 52 -> "広域で並程度の雷雨の可能性"
                        v >= 50 -> "散発的で激しい雷雨の可能性"
                        v >= 46 -> "散発的で並の程度の雷雨の可能性"
                        v >= 44 -> "孤立した弱い雷雨の可能性"
                        else -> null
                    }
                }
                "wbgt" -> {
                    when {
                        v >= 31 -> "危険: 外出を控え、涼しい環境へ"
                        v >= 28 -> "厳重警戒: こまめな水分・塩分補給を"
                        v >= 25 -> "警戒: 積極的な休憩を"
                        else -> null
                    }
                }
                "laundry_index" -> {
                    when {
                        v >= 80 -> "大変よく乾く: 厚物も乾きやすい"
                        v >= 60 -> "よく乾く: 普段の洗濯物向け"
                        else -> null
                    }
                }
                else -> null
            }
        }

        fun getLevelColor(v: Float): Int {
            return when (key) {
                "1_8" -> {
                    when {
                        v >= 80 -> 0xFFB40068.toInt()
                        v >= 50 -> 0xFFFF2800.toInt()
                        v >= 30 -> 0xFFFF9900.toInt()
                        v >= 20 -> 0xFFFAF500.toInt()
                        v >= 10 -> 0xFF0041FF.toInt()
                        v >= 5  -> 0xFF218CFF.toInt()
                        v >= 0.1 -> 0xFF87CEEB.toInt()
                        else -> 0x00000000
                    }
                }
                "wind_speed" -> {
                    when {
                        v >= 25 -> 0xFFB40068.toInt()
                        v >= 20 -> 0xFFFF2800.toInt()
                        v >= 15 -> 0xFFFF9900.toInt()
                        v >= 10 -> 0xFFFAF500.toInt()
                        v >= 5  -> 0xFF0041FF.toInt()
                        else -> 0x00000000
                    }
                }
                "ssi" -> {
                    when {
                        v <= -9 -> 0xFF9C27B0.toInt() // 紫
                        v <= -6 -> 0xFFF44336.toInt() // 赤
                        v <= -3 -> 0xFFFF9800.toInt() // オレンジ
                        v <= 0 -> 0xFFFFEB3B.toInt() // 黄色
                        else -> 0xFF3F51B5.toInt()
                    }
                }
                "ki" -> {
                    when {
                        v >= 40 -> 0xFF9C27B0.toInt()
                        v >= 36 -> 0xFFF44336.toInt()
                        v >= 31 -> 0xFFFF9800.toInt()
                        v >= 26 -> 0xFFFFEB3B.toInt()
                        v >= 15 -> 0xFF4CAF50.toInt() // 緑
                        else -> 0xFF3F51B5.toInt()
                    }
                }
                "theta_e" -> {
                    when {
                        v >= 345 -> 0xFF9C27B0.toInt() // 紫
                        v >= 340 -> 0xFFF44336.toInt() // 赤
                        v >= 330 -> 0xFFFF9800.toInt() // オレンジ
                        else -> 0xFF3F51B5.toInt()
                    }
                }
                "water_vapor_flux" -> {
                    when {
                        v >= 350 -> 0xFF9C27B0.toInt() // 紫
                        v >= 300 -> 0xFFF44336.toInt() // 赤
                        v >= 250 -> 0xFFFF9800.toInt() // オレンジ
                        v >= 200 -> 0xFFFFEB3B.toInt() // 黄
                        v >= 150 -> 0xFF4CAF50.toInt() // 緑
                        else -> 0xFF3F51B5.toInt()
                    }
                }
                "cape" -> {
                    when {
                        v >= 3500 -> 0xFF9C27B0.toInt()
                        v >= 2500 -> 0xFFF44336.toInt()
                        v >= 1000 -> 0xFFFF9800.toInt()
                        v > 0 -> 0xFFFFEB3B.toInt()
                        else -> 0xFF3F51B5.toInt()
                    }
                }
                "tt" -> {
                    when {
                        v >= 60 -> 0xFF9C27B0.toInt()
                        v >= 52 -> 0xFFF44336.toInt()
                        v >= 50 -> 0xFFFF9800.toInt()
                        v >= 46 -> 0xFFFFEB3B.toInt()
                        v >= 44 -> 0xFF4CAF50.toInt()
                        else -> 0xFF3F51B5.toInt()
                    }
                }
                "wbgt" -> {
                    when {
                        v >= 31 -> 0xFFFF0000.toInt() // 赤
                        v >= 28 -> 0xFFFF9800.toInt() // オレンジ
                        v >= 25 -> 0xFFFFEB3B.toInt() // 黄色
                        else -> 0x00000000 // 透明
                    }
                }
                "laundry_index" -> {
                    when {
                        v >= 80 -> 0xFFFF4081.toInt() // ピンク
                        v >= 60 -> 0xFF4CAF50.toInt() // 緑
                        else -> 0x00000000 // 透明
                    }
                }
                else -> 0xFF3F51B5.toInt() // デフォルト（インディゴ）
            }
        }

        override fun toString(): String = name
    }

    val WEATHER_ELEMENTS = listOf(
        WeatherElement("降水量", "1_8", "mm/h", 0f, 50f),
        WeatherElement("湿度", "1_1", "%", 0f, 100f),
        WeatherElement("風速", "wind_speed", "m/s", 0f, 20f),
        WeatherElement("気温", "0_0", "℃", -20f, 40f),
        WeatherElement("全雲量", "6_1", "%", 0f, 100f),
        WeatherElement("下層雲量", "6_3", "%", 0f, 100f),
        WeatherElement("中層雲量", "6_4", "%", 0f, 100f),
        WeatherElement("上層雲量", "6_5", "%", 0f, 100f),
        WeatherElement("日射量", "4_7", "W/m²"),
        WeatherElement("熱中症指数(WBGT)", "wbgt", "℃"),
        WeatherElement("洗濯指数", "laundry_index", "", 0f, 100f),
        WeatherElement("シュワルター安定指数(SSI)", "ssi", "", -10f, 20f, isPressureLevel = true, tileSurface = "pall"),
        WeatherElement("K指数(KI)", "ki", "", 0f, 50f, isPressureLevel = true, tileSurface = "pall"),
        WeatherElement("トータルトータルズ(TT)", "tt", "", 0f, 60f, isPressureLevel = true, tileSurface = "pall"),
        WeatherElement("相当温位", "theta_e", "K", 270f, 360f, isPressureLevel = true, tileSurface = "850hPa"),
        WeatherElement("水蒸気フラックス", "water_vapor_flux", "g/kg・m/s", 0f, 400f, isPressureLevel = true, tileSurface = "850hPa"),
        WeatherElement("持ち上げ凝結高度(LCL)", "lcl", "m", 500f, 1000f, isPressureLevel = true, tileSurface = "pall"),
        WeatherElement("自由対流高度(LFC)", "lfc", "m", 500f, 1000f, isPressureLevel = true, tileSurface = "pall"),
        WeatherElement("平衡高度(EL)", "el", "m", 100f, 1000f, isPressureLevel = true, tileSurface = "pall"),
        WeatherElement("対流有効位置エネルギー(CAPE)", "cape", "J/kg", 0f, 3000f, isPressureLevel = true, tileSurface = "pall"),
        WeatherElement("対流抑制(CIN)", "cin", "J/kg", -300f, 0f, isPressureLevel = true, tileSurface = "pall")
    )

    val API_ELEMENTS = listOf(
        "1_8", "1_1", "0_0", "2_2", "2_3", "6_1", "6_3", "6_4", "6_5", "4_7", "wbgt", "laundry_index"
    )

    val INDEX_ELEMENTS = listOf(
        "wbgt", "laundry_index"
    )

    val INSTABILITY_ELEMENTS = listOf(
        "ssi", "ki", "tt", "theta_e", "water_vapor_flux", "lcl", "lfc", "el", "cape", "cin"
    )

    const val DEFAULT_ELEMENT_KEY = "1_8"
}
