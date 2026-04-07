package com.example.weather_location_app.data.api

import com.example.weather_location_app.BuildConfig
import retrofit2.http.Body
import retrofit2.http.GET
import retrofit2.http.Header
import retrofit2.http.POST
import retrofit2.http.Query
import retrofit2.http.Streaming
import retrofit2.http.Url

data class PointWeatherAdviceRequest(
    val latitude: Double,
    val longitude: Double,
    val weather_data: List<WeatherDataForAi>
)

data class WeatherDataForAi(
    val datetime: String,
    // Instability
    val ssi: Double,
    val ki: Double,
    val tt: Double,
    val theta_e: Double,
    val water_vapor_flux: Double,
    val lcl: Double,
    val lfc: Double,
    val el: Double,
    val cape: Double,
    val cin: Double,
    // Weather surface
    val temperature: Double,
    val humidity: Double,
    val precipitation: Double,
    val wind_speed: Double,
    val solar_radiation: Double,
    val total_cloud_cover: Double,
    val low_cloud_cover: Double,
    val mid_cloud_cover: Double,
    val altostratus_cloud_cover: Double,
    val laundry_index: Double,
    val wbgt: Double
)

data class PointWeatherAdviceResponse(
    val advice: String
)

data class GpvResponse(
    val status: String,
    val messages: String? = null,
    val result: GpvResult? = null
)

data class GpvResult(
    val data: List<GpvDataItem>? = null,
    val initial_datetime: String? = null,
    val latitude: String? = null,
    val longitude: String? = null
)

data class GpvDataItem(
    val datetime: String,
    val contents: List<GpvContent>,
    val lat: Double? = null,
    val lon: Double? = null,
    val elevation: String? = null
)

data class GpvContent(
    val surface: String,
    val value: Map<String, Double>
)

interface WeatherApi {
    @GET("get_gpv_data_api")
    suspend fun getGpvData(
        @Query("key") apiKey: String = BuildConfig.GPV_API_KEY,
        @Query("latitude") latitude: Double,
        @Query("longitude") longitude: Double,
        @Query("gpv_type") gpvType: String = "msm-surf",
        @Query("elements[]") elementsCsv: String
    ): GpvResponse

    @GET("data/")
    suspend fun getInstabilityData(
        @Query("key") apiKey: String = BuildConfig.GPV_API_KEY,
        @Query("latitude") latitude: Double,
        @Query("longitude") longitude: Double
    ): GpvResponse

    @Streaming
    @POST
    suspend fun getPointWeatherAdvice(
        @Header("x-api-key") apiKey: String = BuildConfig.ADVICE_API_KEY,
        @Url url: String,
        @Body request: PointWeatherAdviceRequest
    ): okhttp3.ResponseBody
}

data class RouteResponse(
    val information: Map<String, RoutePoint>? = null
)

data class RoutePoint(
    val latitude: String? = null,
    val longitude: String? = null,
    val elevation: String? = null,
    val surface: Map<String, String>? = null,
    val pall: Map<String, String>? = null,
    val apparent_temp: String? = null,
    val wbgt: String? = null
)

data class RouteWeatherAdviceRequest(
    val mode: String,
    val route_points: List<RouteAdvicePoint>
)

data class RouteAdvicePoint(
    val datetime: String,
    val latitude: Double,
    val longitude: Double,
    val elevation: Double,
    val temp: Double,
    val wind_speed: Double,
    val apparent_temp: Double,
    val humidity: Double,
    val solar_radiation: Double,
    val precipitation: Double,
    val wbgt: Double? = null,
    val ssi: Double? = null,
    val tt: Double? = null,
    val ki: Double? = null
)

data class RouteWeatherAdviceResponse(
    val advice: String
)

interface RouteApi {
    @GET("data/")
    suspend fun getRouteData(
        @Query("key") apiKey: String = BuildConfig.ROUTE_API_KEY,
        @Query("origin") origin: String, // "lat,lon"
        @Query("destination") destination: String,
        @Query("means") means: String,
        @Query("departure") departure: Long // Unix timestamp in seconds
    ): RouteResponse

    @POST("https://route-weather-advice-api-dzpj65jtxa-an.a.run.app/generate-route-advice")
    suspend fun getRouteWeatherAdvice(
        @Header("x-api-key") apiKey: String = BuildConfig.ADVICE_API_KEY,
        @Body request: RouteWeatherAdviceRequest
    ): RouteWeatherAdviceResponse
}

interface TrailApi {
    @GET("data/")
    suspend fun getTrailData(
        @Query("key") apiKey: String = BuildConfig.ROUTE_API_KEY, // 暫定的に既存のKeyを使用、必要に応じてBuildConfig追加
        @Query("origin") origin: String,
        @Query("destination") destination: String,
        @Query("means") means: String = "walking",
        @Query("departure") departure: Long
    ): RouteResponse
}
