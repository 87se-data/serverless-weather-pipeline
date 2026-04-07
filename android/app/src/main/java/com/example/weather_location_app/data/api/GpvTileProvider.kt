package com.example.weather_location_app.data.api

import android.util.Log
import com.google.android.gms.maps.model.Tile
import com.google.android.gms.maps.model.TileProvider
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.IOException
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/**
 * GPV気象タイルをカスタムヘッダー付きで取得するためのTileProvider。
 * 標準のUrlTileProviderではヘッダーを付与できないため、OkHttpで自前取得を行う。
 */
class GpvTileProvider(
    private val client: OkHttpClient,
    private val apiKey: String,
    private val isSurface: Boolean,
    private val initialTime: String,
    private val targetTime: String,
    private val element: String,
    private val surface: String
) : TileProvider {

    private val baseUrl = "https://jma-project.web.app/tiles/${if (isSurface) "surf" else "pall"}"

    override fun getTile(x: Int, y: Int, zoom: Int): Tile? {
        // Z/X/Y.webp の形式。クエリパラメータとして予報時刻等を付与。
        // FastAPI側でのバリデーションエラーを避けるため、UTCに変換し末尾を Z に固定する
        val formattedInitialTime = formatToUtcZ(initialTime)
        val formattedTargetTime = formatToUtcZ(targetTime)

        // theta_e の場合は surface を 850hPa に固定
        val finalSurface = if (element == "theta_e") "850hPa" else surface

        val url = "$baseUrl/$zoom/$x/$y.webp?" +
                "initial_time=$formattedInitialTime&" +
                "target_time=$formattedTargetTime&" +
                "element=$element&" +
                "surface=$finalSurface"

        val request = Request.Builder()
            .url(url)
            .addHeader("X-API-KEY", apiKey)
            .build()

        return try {
            client.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    val errorBody = response.body?.string() ?: "No error body"
                    Log.e("GpvTileProvider", "Failed to fetch tile: $url\n" +
                            "Code: ${response.code}\n" +
                            "Error Details (FastAPI): $errorBody")
                    return null
                }
                val bytes = response.body?.bytes() ?: return null
                Tile(256, 256, bytes)
            }
        } catch (e: Exception) {
            Log.e("GpvTileProvider", "Error fetching tile: ${e.message}")
            null
        }
    }

    /**
     * ISO 8601 形式の文字列を UTC (Z) 形式に変換する。
     * 例: 2026-03-29T15:00:00+09:00 -> 2026-03-29T06:00:00Z
     */
    private fun formatToUtcZ(dtStr: String): String {
        return try {
            val odt = OffsetDateTime.parse(dtStr.replace(" ", "T"))
            odt.withOffsetSameInstant(ZoneOffset.UTC)
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))
        } catch (e: Exception) {
            // パースに失敗した場合は、最低限 + を置換して返す
            dtStr.replace("+00:00", "Z").replace("+09:00", "Z") // 暫定的
        }
    }
}
