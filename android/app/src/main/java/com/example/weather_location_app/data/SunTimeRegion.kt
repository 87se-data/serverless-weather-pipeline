package com.example.weather_location_app.data

/**
 * 日の出・日の入り時間帯を保持するモデル
 */
data class SunTimeRegion(
    val startX: Float,
    val endX: Float,
    val isDay: Boolean,
    val iconX: Float? = null,
    val icon: String? = null
)
