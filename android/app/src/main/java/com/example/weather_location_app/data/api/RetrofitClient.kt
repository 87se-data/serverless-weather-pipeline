package com.example.weather_location_app.data.api

import android.content.Context
import okhttp3.Cache
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import okhttp3.logging.HttpLoggingInterceptor
import okhttp3.sse.EventSource
import okhttp3.sse.EventSources
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import java.io.File
import java.util.concurrent.TimeUnit

object RetrofitClient {
    private const val BASE_URL = "https://get-gpv-data-gateway-19x3id14.an.gateway.dev/"
    private const val INSTABILITY_BASE_URL = "https://get-instability-points-data-api-99956484472.asia-northeast1.run.app/"
    private const val ROUTE_BASE_URL = "https://get-route-points-data-gateway-19x3id14.an.gateway.dev/"
    private const val TRAIL_BASE_URL = "https://get-trail-points-data-gateway-19x3id14.an.gateway.dev/" // 登山用APIのベースURL

    private const val CACHE_SIZE = 100L * 1024L * 1024L // 100MB

    private val loggingInterceptor = HttpLoggingInterceptor().apply {
        level = HttpLoggingInterceptor.Level.HEADERS
    }

    // キャッシュを強制的に有効にするインターセプター
    private val cacheInterceptor = Interceptor { chain ->
        val response = chain.proceed(chain.request())
        // サーバー側の設定に関わらず、クライアント側で30日間のキャッシュを許可する
        response.newBuilder()
            .header("Cache-Control", "public, max-stale=2592000") // 30 days in seconds
            .build()
    }

    private var client: OkHttpClient? = null

    /**
     * コンテキストを使用してOkHttpClientを初期化する
     */
    fun init(context: Context) {
        if (client == null) {
            val cacheDirectory = File(context.cacheDir, "http_cache")
            val cache = Cache(cacheDirectory, CACHE_SIZE)

            client = OkHttpClient.Builder()
                .cache(cache)
                .addNetworkInterceptor(cacheInterceptor) // ネットワーク層でヘッダーを書き換える
                .addInterceptor(loggingInterceptor)
                .connectTimeout(100, TimeUnit.SECONDS)
                .readTimeout(100, TimeUnit.SECONDS)
                .writeTimeout(100, TimeUnit.SECONDS)
                .build()
        }
    }

    /**
     * 初期化されたOkHttpClientを取得する
     */
    val okHttpClient: OkHttpClient
        get() = client ?: throw IllegalStateException("RetrofitClient must be initialized with context first")

    val sseEventSourceFactory: EventSource.Factory by lazy {
        // ストリーミングを妨げるInterceptorを除外した専用クライアント
        val sseClient = OkHttpClient.Builder()
            .readTimeout(0, java.util.concurrent.TimeUnit.MILLISECONDS) // タイムアウトなし
            .build()
        EventSources.createFactory(sseClient)
    }

    val weatherApi: WeatherApi by lazy {
        Retrofit.Builder()
            .baseUrl(BASE_URL)
            .client(okHttpClient)
            .addConverterFactory(GsonConverterFactory.create())
            .build()
            .create(WeatherApi::class.java)
    }

    val instabilityApi: WeatherApi by lazy {
        Retrofit.Builder()
            .baseUrl(INSTABILITY_BASE_URL)
            .client(okHttpClient)
            .addConverterFactory(GsonConverterFactory.create())
            .build()
            .create(WeatherApi::class.java)
    }

    val routeApi: RouteApi by lazy {
        Retrofit.Builder()
            .baseUrl(ROUTE_BASE_URL)
            .client(okHttpClient)
            .addConverterFactory(GsonConverterFactory.create())
            .build()
            .create(RouteApi::class.java)
    }

    val trailApi: TrailApi by lazy {
        Retrofit.Builder()
            .baseUrl(TRAIL_BASE_URL)
            .client(okHttpClient)
            .addConverterFactory(GsonConverterFactory.create())
            .build()
            .create(TrailApi::class.java)
    }
}
