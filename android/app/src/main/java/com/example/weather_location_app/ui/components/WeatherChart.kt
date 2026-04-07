package com.example.weather_location_app.ui.components

import android.annotation.SuppressLint
import android.content.Context
import android.graphics.Canvas
import android.graphics.Paint
import android.graphics.drawable.GradientDrawable
import android.view.MotionEvent
import android.view.View
import android.widget.TextView
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.material3.MaterialTheme
import androidx.compose.runtime.Composable
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.toArgb
import androidx.compose.ui.viewinterop.AndroidView
import androidx.core.graphics.ColorUtils
import com.example.weather_location_app.R
import com.example.weather_location_app.WeatherConfig
import com.example.weather_location_app.data.SunTimeRegion
import com.example.weather_location_app.data.api.GpvDataItem
import com.github.mikephil.charting.charts.CombinedChart
import com.github.mikephil.charting.components.*
import com.github.mikephil.charting.data.*
import com.github.mikephil.charting.formatter.ValueFormatter
import com.github.mikephil.charting.highlight.Highlight
import com.github.mikephil.charting.listener.OnChartValueSelectedListener
import com.github.mikephil.charting.renderer.XAxisRenderer
import com.github.mikephil.charting.utils.MPPointF
import com.github.mikephil.charting.utils.Transformer
import com.github.mikephil.charting.utils.ViewPortHandler
import java.time.Duration
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import kotlin.math.abs
import kotlin.math.roundToInt

/**
 * チャート描画に関する定数定義
 */
private object ChartDefaults {
    val JstOffset: ZoneOffset = ZoneOffset.ofHours(9)
    const val GradientAlpha = 40
    const val GridAlpha = 30
    const val HighlightAlpha = 100
    const val LegendFormSize = 8f
    const val LegendTextSize = 10f
    const val XAxisLabelRotation = -45f
    const val MaxHighlightDistance = 100000f
    const val HighlightDelayMillis = 100L
    const val LineWidth = 2.0f
    const val CircleRadiusSmall = 1.5f
    const val CircleRadiusLarge = 2.5f
    const val BarWidth = 0.8f
    const val DefaultFillAlpha = 40
    
    // カラーパレット
    val TempColor = android.graphics.Color.parseColor("#F09300")
    val ApparentTempColor = android.graphics.Color.parseColor("#C2185B")
    val WindColor = android.graphics.Color.parseColor("#00897B")
    val ElevationColor = android.graphics.Color.parseColor("#8B4513")
}

// パフォーマンス改善のためのキャッシュ
private val drawableCache = mutableMapOf<Int, GradientDrawable>()
private val timeAxisFormatter = TimeAxisValueFormatter()

/**
 * X軸の時間表示用フォーマッタ
 */
class TimeAxisValueFormatter : ValueFormatter() {
    var firstDt: OffsetDateTime? = null
    private val pattern = DateTimeFormatter.ofPattern("M/d H:mm")

    override fun getFormattedValue(value: Float): String {
        val currentFirstDt = firstDt ?: return ""
        return try {
            val dt = currentFirstDt.plusMinutes((value * 60f).roundToInt().toLong())
            val jst = dt.withOffsetSameInstant(ChartDefaults.JstOffset)
            if (jst.hour % 3 == 0 && jst.minute == 0) jst.format(pattern)
            else ""
        } catch (e: Exception) { "" }
    }
}

/**
 * 透明感のあるグラデーション作成
 */
private fun getGradientDrawable(color: Int): GradientDrawable {
    return drawableCache.getOrPut(color) {
        GradientDrawable(
            GradientDrawable.Orientation.TOP_BOTTOM,
            intArrayOf(
                ColorUtils.setAlphaComponent(color, ChartDefaults.GradientAlpha),
                android.graphics.Color.TRANSPARENT
            )
        )
    }
}

/**
 * チャート描画用データポイント
 */
data class WeatherChartPoint(
    val item: GpvDataItem,
    val odt: OffsetDateTime,
    val x: Float,
    val index: Int
)

/**
 * 降水エリア描画用データ
 */
data class PrecipitationRegion(
    val startX: Float,
    val endX: Float,
    val color: Int
)

/**
 * 気象データを表示する複合チャート（CombinedChart）のComposable
 */
@SuppressLint("ClickableViewAccessibility")
@Composable
fun WeatherChart(
    data: List<GpvDataItem>?,
    sunTimeRegions: List<SunTimeRegion> = emptyList(),
    element: WeatherConfig.WeatherElement? = null,
    modifier: Modifier = Modifier,
    isFixedY: Boolean = false,
    showCurrentTimeLine: Boolean = true,
    selectedPointIndex: Int? = null,
    transportMode: String? = null,
    onPointSelected: (Int?) -> Unit = {}
) {
    val isDarkTheme = isSystemInDarkTheme()
    val onSurfaceColor = MaterialTheme.colorScheme.onSurface.toArgb()
    val onSurfaceVariantColor = MaterialTheme.colorScheme.onSurfaceVariant.toArgb()
    val outlineColor = MaterialTheme.colorScheme.outline.toArgb()
    val primaryColor = MaterialTheme.colorScheme.primary.toArgb()

    // パフォーマンス改善：前回選択されたインデックスを保持
    val lastSelectedIndex = remember { mutableStateOf<Int?>(null) }

    AndroidView(
        modifier = modifier,
        factory = { context ->
            TooltipAlwaysVisibleChart(context).apply {
                setupChartBasicSettings(onSurfaceColor, onSurfaceVariantColor, outlineColor, isDarkTheme)
                
                setOnChartValueSelectedListener(object : OnChartValueSelectedListener {
                    override fun onValueSelected(e: Entry, h: Highlight) { 
                        val index = e.data as? Int
                        if (index != lastSelectedIndex.value) {
                            lastSelectedIndex.value = index
                            onPointSelected(index)
                        }
                    }
                    override fun onNothingSelected() { 
                        if (lastSelectedIndex.value != null) {
                            lastSelectedIndex.value = null
                            onPointSelected(null) 
                        }
                    }
                })
            }
        },
        update = { chart ->
            // テーマ変更時の色更新
            chart.updateColors(onSurfaceVariantColor, isDarkTheme)

            if (data.isNullOrEmpty()) {
                chart.clearChart("データがありません")
                return@AndroidView
            }

            // データのパースとX軸座標計算
            val weatherPoints = data.mapIndexed { index, item ->
                val odt = parseDateTime(item.datetime).withOffsetSameInstant(ChartDefaults.JstOffset)
                WeatherChartPoint(item, odt, 0f, index)
            }
            
            val firstDt = weatherPoints.first().odt
            val lastDt = weatherPoints.last().odt
            val totalHours = Duration.between(firstDt, lastDt).toMillis().toFloat() / 3600000f
            
            val finalPoints = weatherPoints.map { p ->
                p.copy(x = Duration.between(firstDt, p.odt).toMillis().toFloat() / 3600000f)
            }

            // 【パフォーマンス改善】データの同一性チェック
            val currentParams = listOf(data, element, transportMode, isDarkTheme, sunTimeRegions)
            val dataChanged = chart.tag != currentParams
            chart.tag = currentParams

            if (!dataChanged) {
                // 外部スライダー等からの変更の場合のみ、プログラム的に縦線を同期
                if (selectedPointIndex != lastSelectedIndex.value) {
                    chart.updateHighlight(selectedPointIndex, finalPoints, lastSelectedIndex)
                }
                // 💖 タッチ由来・外部由来に関わらず、再描画が走った際は必ず棒グラフの色を更新する！
                chart.updateBarColors(selectedPointIndex, primaryColor)
                return@AndroidView
            }

            // 新しいデータセットの構築
            buildAndSetChartData(
                chart = chart,
                finalPoints = finalPoints,
                element = element,
                sunTimeRegions = sunTimeRegions,
                totalHours = totalHours,
                firstDt = firstDt,
                transportMode = transportMode,
                primaryColor = primaryColor,
                onSurfaceColor = onSurfaceColor,
                onSurfaceVariantColor = onSurfaceVariantColor,
                showCurrentTimeLine = showCurrentTimeLine,
                isFixedY = isFixedY
            )

            // 初回描画またはデータ更新後のハイライト適用
            chart.updateHighlight(selectedPointIndex, finalPoints, lastSelectedIndex)
            chart.updateBarColors(selectedPointIndex, primaryColor)
        }
    )
}

/**
 * チャートの基本設定
 */
private fun CombinedChart.setupChartBasicSettings(
    onSurfaceColor: Int,
    onSurfaceVariantColor: Int,
    outlineColor: Int,
    isDarkTheme: Boolean
) {
    description.isEnabled = false
    setDrawBorders(false)
    setDrawGridBackground(false)
    extraBottomOffset = 15f
    
    setOnTouchListener { v, event ->
        when (event.action) {
            MotionEvent.ACTION_DOWN, MotionEvent.ACTION_MOVE -> v.parent.requestDisallowInterceptTouchEvent(true)
            MotionEvent.ACTION_UP, MotionEvent.ACTION_CANCEL -> v.parent.requestDisallowInterceptTouchEvent(false)
        }
        false
    }

    legend.apply {
        isEnabled = true
        textColor = onSurfaceVariantColor
        verticalAlignment = Legend.LegendVerticalAlignment.BOTTOM
        horizontalAlignment = Legend.LegendHorizontalAlignment.CENTER
        orientation = Legend.LegendOrientation.HORIZONTAL
        setDrawInside(false)
        form = Legend.LegendForm.CIRCLE
        formSize = ChartDefaults.LegendFormSize
        textSize = ChartDefaults.LegendTextSize
    }

    setNoDataText("データを読み込み中...")
    setNoDataTextColor(onSurfaceColor)
    
    marker = CustomMarkerView(context, R.layout.marker_view).apply { chartView = this@setupChartBasicSettings }
    
    xAxis.apply {
        position = XAxis.XAxisPosition.BOTTOM
        textColor = onSurfaceVariantColor
        setDrawGridLines(false)
        setDrawAxisLine(false)
    }

    setXAxisRenderer(CustomXAxisRenderer(viewPortHandler, xAxis, getTransformer(YAxis.AxisDependency.LEFT)).apply {
        this.isDarkTheme = isDarkTheme
    })

    axisLeft.apply {
        textColor = onSurfaceVariantColor
        setDrawGridLines(true)
        gridColor = ColorUtils.setAlphaComponent(outlineColor, ChartDefaults.GridAlpha)
        setDrawAxisLine(false)
        enableGridDashedLine(10f, 10f, 0f)
    }
    axisRight.isEnabled = false
    
    isHighlightPerDragEnabled = true
    isHighlightPerTapEnabled = true
    maxHighlightDistance = ChartDefaults.MaxHighlightDistance
    setTouchEnabled(true)
    isDragEnabled = true
    isScaleXEnabled = true
    isScaleYEnabled = false
}

/**
 * テーマに応じた色の更新
 */
private fun CombinedChart.updateColors(onSurfaceVariantColor: Int, isDarkTheme: Boolean) {
    if (xAxis.textColor != onSurfaceVariantColor) xAxis.textColor = onSurfaceVariantColor
    if (axisLeft.textColor != onSurfaceVariantColor) axisLeft.textColor = onSurfaceVariantColor
    if (legend.textColor != onSurfaceVariantColor) legend.textColor = onSurfaceVariantColor
    (rendererXAxis as? CustomXAxisRenderer)?.let { 
        if (it.isDarkTheme != isDarkTheme) it.isDarkTheme = isDarkTheme
    }
}

/**
 * チャートのクリア
 */
private fun CombinedChart.clearChart(noDataText: String) {
    tag = null
    clear()
    setNoDataText(noDataText)
    invalidate()
}

/**
 * チャートのハイライト状態を更新
 */
private fun CombinedChart.updateHighlight(
    selectedPointIndex: Int?,
    finalPoints: List<WeatherChartPoint>,
    lastSelectedIndex: androidx.compose.runtime.MutableState<Int?>
) {
    postDelayed({
        val currentData = data ?: return@postDelayed
        if (selectedPointIndex != null && selectedPointIndex >= 0) {
            val targetPoint = finalPoints.getOrNull(selectedPointIndex)
            if (targetPoint != null) {
                currentData.notifyDataChanged()
                notifyDataSetChanged()

                var hY = Float.NaN
                var targetDsIndex = -1
                var targetDataIndex = -1

                // LineDataから探す
                currentData.lineData?.let { lData ->
                    for (i in 0 until lData.dataSetCount) {
                        val ds = lData.getDataSetByIndex(i)
                        if (ds.isHighlightEnabled) { // 💖 この条件を追加
                            val entries = ds.getEntriesForXValue(targetPoint.x)
                            if (entries.isNotEmpty()) {
                                hY = entries.first().y
                                targetDsIndex = i
                                targetDataIndex = currentData.allData.indexOf(lData)
                                break
                            }
                        }
                    }
                }

                // 見つからなければBarDataから探す
                if (targetDsIndex == -1) {
                    currentData.barData?.let { bData ->
                        for (i in 0 until bData.dataSetCount) {
                            val entries = bData.getDataSetByIndex(i).getEntriesForXValue(targetPoint.x)
                            if (entries.isNotEmpty()) {
                                hY = entries.first().y
                                targetDsIndex = i
                                targetDataIndex = currentData.allData.indexOf(bData)
                                break
                            }
                        }
                    }
                }

                if (targetDsIndex != -1) {
                    val highlight = Highlight(targetPoint.x, hY, targetDsIndex)
                    highlight.dataIndex = targetDataIndex
                    highlightValue(null, false)
                    highlightValue(highlight, false)
                } else {
                    highlightValue(null, false)
                }
            } else {
                highlightValue(null, false)
            }
        } else {
            highlightValue(null, false)
        }
        lastSelectedIndex.value = selectedPointIndex
        invalidate()
    }, ChartDefaults.HighlightDelayMillis)
}

/**
 * 降水グラフ（BarDataSet）の色をハイライト状態に合わせて更新
 */
private fun CombinedChart.updateBarColors(selectedPointIndex: Int?, primaryColor: Int) {
    val bData = data?.barData ?: return
    var changed = false
    for (i in 0 until bData.dataSetCount) {
        val dataSet = bData.getDataSetByIndex(i) as BarDataSet
        val colors = mutableListOf<Int>()
        // 選択されたバーは100%不透明度のアクセンカラー（今回はオレンジに近い色）
        val selectedColor = android.graphics.Color.parseColor("#FF9800")
        // それ以外はPrimaryColorを30%不透明度にした色
        val unselectedColor = ColorUtils.setAlphaComponent(primaryColor, 150) 
        
        for (j in 0 until dataSet.entryCount) {
            val entry = dataSet.getEntryForIndex(j)
            val index = entry.data as? Int
            if (selectedPointIndex != null && index == selectedPointIndex) {
                colors.add(selectedColor)
            } else {
                colors.add(unselectedColor)
            }
        }
        dataSet.colors = colors
        changed = true
    }
    if (changed) {
        invalidate()
    }
}

/**
 * チャートデータの構築とセット
 */
private fun buildAndSetChartData(
    chart: CombinedChart,
    finalPoints: List<WeatherChartPoint>,
    element: WeatherConfig.WeatherElement?,
    sunTimeRegions: List<SunTimeRegion>,
    totalHours: Float,
    firstDt: OffsetDateTime,
    transportMode: String?,
    primaryColor: Int,
    onSurfaceColor: Int,
    onSurfaceVariantColor: Int,
    showCurrentTimeLine: Boolean,
    isFixedY: Boolean
) {
    // グラフセグメント生成ヘルパー
    fun getSegments(selector: (GpvDataItem) -> Float?): List<List<Entry>> {
        val segments = mutableListOf<List<Entry>>()
        var current = mutableListOf<Entry>()
        var lastIdx = -2
        finalPoints.forEach { p ->
            val v = selector(p.item)
            if (v != null) {
                if (lastIdx != -2 && p.index > lastIdx + 1) {
                    if (current.isNotEmpty()) segments.add(current)
                    current = mutableListOf()
                }
                current.add(Entry(p.x, v, p.index))
                lastIdx = p.index
            }
        }
        if (current.isNotEmpty()) segments.add(current)
        return segments
    }

    // 降水エリア計算
    val precipRegions = mutableListOf<PrecipitationRegion>()
    if (element == null) {
        val precipElement = WeatherConfig.WEATHER_ELEMENTS.find { it.key == "1_8" }
        finalPoints.forEachIndexed { index, p ->
            val precip = precipElement?.getValue(p.item)
            if (precip != null && precip >= 1.0) {
                val x = p.x
                val startX = if (index > 0) (finalPoints[index - 1].x + x) / 2f else if (finalPoints.size > 1) x - (finalPoints[1].x - x) / 2f else x - 0.5f
                val endX = if (index < finalPoints.size - 1) (x + finalPoints[index + 1].x) / 2f else if (index > 0) x + (x - finalPoints[index - 1].x) / 2f else x + 0.5f
                precipRegions.add(PrecipitationRegion(startX, endX, getPrecipitationColor(precip.toFloat())))
            }
        }
    }

    // X軸レンダラーの設定
    (chart.rendererXAxis as? CustomXAxisRenderer)?.apply {
        this.weatherPoints = finalPoints
        this.sunTimeRegions = sunTimeRegions
        this.precipitationRegions = precipRegions
        this.maxHours = totalHours
    }
    
    (chart.marker as? CustomMarkerView)?.setChartData(finalPoints, element, transportMode)

    val combinedData = CombinedData()
    val lineData = LineData()
    val barData = BarData()
    val barEntries = mutableListOf<BarEntry>()

    // 標高データ（ハイキングモード）
    if (transportMode == "hiking") {
        val elevSegments = getSegments { it.elevation?.toDoubleOrNull()?.toFloat() }
        elevSegments.forEach { entries ->
            lineData.addDataSet(LineDataSet(entries, "標高(m)").apply {
                axisDependency = YAxis.AxisDependency.RIGHT
                color = android.graphics.Color.TRANSPARENT
                setDrawCircles(false); setDrawValues(false); setDrawFilled(true)
                fillColor = ChartDefaults.ElevationColor
                fillAlpha = ChartDefaults.DefaultFillAlpha
                mode = LineDataSet.Mode.LINEAR
                isHighlightEnabled = false
            })
        }
        chart.axisRight.apply {
            isEnabled = elevSegments.isNotEmpty()
            textColor = onSurfaceVariantColor
            setDrawGridLines(false); axisMinimum = 0f; spaceTop = 20f
        }
    } else {
        chart.axisRight.isEnabled = false
    }

    if (element == null) {
        // 標準表示：気温、体感気温、風速
        chart.legend.isEnabled = true
        
        // 気温
        getSegments { WeatherConfig.WEATHER_ELEMENTS.find { el -> el.key == "0_0" }?.getValue(it)?.toFloat() }.forEach { entries ->
            lineData.addDataSet(createStandardLineDataSet(entries, "気温(℃)", ChartDefaults.TempColor, onSurfaceColor))
        }

        // 体感気温（ハイキングのみ）
        if (transportMode == "hiking") {
            getSegments { item ->
                item.contents.firstOrNull()?.value?.get("apparent_temp")?.let {
                    if (it > 100.0) (it - 273.15).toFloat() else it.toFloat()
                }
            }.forEach { entries ->
                lineData.addDataSet(createStandardLineDataSet(entries, "体感気温(℃)", ChartDefaults.ApparentTempColor, onSurfaceColor))
            }
        }

        // 風速
        getSegments { WeatherConfig.WEATHER_ELEMENTS.find { el -> el.key == "wind_speed" }?.getValue(it)?.toFloat() }.forEach { entries ->
            lineData.addDataSet(createStandardLineDataSet(entries, "風速(m/s)", ChartDefaults.WindColor, onSurfaceColor))
        }

        // 凡例設定
        val legendEntries = mutableListOf(LegendEntry("気温(℃)", Legend.LegendForm.CIRCLE, 8f, 2f, null, ChartDefaults.TempColor))
        if (transportMode == "hiking") legendEntries.add(LegendEntry("体感気温(℃)", Legend.LegendForm.CIRCLE, 8f, 2f, null, ChartDefaults.ApparentTempColor))
        legendEntries.add(LegendEntry("風速(m/s)", Legend.LegendForm.CIRCLE, 8f, 2f, null, ChartDefaults.WindColor))
        chart.legend.setCustom(legendEntries)
        chart.setDrawOrder(arrayOf(CombinedChart.DrawOrder.BAR, CombinedChart.DrawOrder.LINE))
    } else {
        // 個別要素表示
        chart.legend.isEnabled = false
        if (element.key != "1_8") {
            getSegments { element.getValue(it)?.toFloat() }.forEach { entries ->
                lineData.addDataSet(createDetailedLineDataSet(entries, element, primaryColor, onSurfaceColor))
            }
        } else {
            // 降水量は棒グラフ
            finalPoints.forEach { p -> element.getValue(p.item)?.let { barEntries.add(BarEntry(p.x, it.toFloat(), p.index)) } }
        }

        if (barEntries.isNotEmpty()) {
            barData.addDataSet(BarDataSet(barEntries, element.name).apply { 
                color = primaryColor; setDrawValues(false); isHighlightEnabled = false
                highLightColor = ColorUtils.setAlphaComponent(onSurfaceColor, ChartDefaults.HighlightAlpha)
            })
        }
        chart.legend.resetCustom()
    }

    // --- 全時間をカバーする透明なタッチ専用データセットを追加 ---
    val touchEntries = finalPoints.map { Entry(it.x, 0f, it.index) }
    val touchDataSet = LineDataSet(touchEntries, "TouchTarget").apply {
        color = android.graphics.Color.TRANSPARENT
        setDrawValues(false)
        setDrawCircles(false)
        setDrawFilled(false)
        isHighlightEnabled = true // これだけハイライトを有効にする
        highLightColor = ColorUtils.setAlphaComponent(onSurfaceColor, ChartDefaults.HighlightAlpha)
        highlightLineWidth = 1f
        setDrawHorizontalHighlightIndicator(false)
        setDrawVerticalHighlightIndicator(true)
        enableDashedHighlightLine(10f, 5f, 0f)
    }
    lineData.addDataSet(touchDataSet)
    // ------------------------------------------------

    // 最終的なデータセットとX軸更新
    setupCommonXAxis(chart.xAxis, firstDt, totalHours)
    combinedData.setData(lineData)
    combinedData.setData(barData.apply { barWidth = ChartDefaults.BarWidth })
    chart.data = combinedData

    // 現在時刻線の追加
    if (showCurrentTimeLine) {
        val nowX = Duration.between(firstDt, OffsetDateTime.now(ChartDefaults.JstOffset)).toMillis().toFloat() / 3600000f
        if (nowX in 0f..totalHours) {
            chart.xAxis.addLimitLine(LimitLine(nowX, "現在").apply { 
                lineColor = android.graphics.Color.argb(120, 255, 0, 0)
                lineWidth = 1.5f; enableDashedLine(10f, 10f, 0f)
                textColor = onSurfaceVariantColor; textSize = 9f
                labelPosition = LimitLine.LimitLabelPosition.RIGHT_TOP 
            })
        }
    }

    // 日の出日の入りアイコン
    sunTimeRegions.forEach { region ->
        if (region.iconX != null && region.icon != null && region.iconX in 0f..totalHours) {
            chart.xAxis.addLimitLine(LimitLine(region.iconX, region.icon).apply { 
                lineColor = android.graphics.Color.TRANSPARENT; textColor = onSurfaceVariantColor; textSize = 14f
                labelPosition = LimitLine.LimitLabelPosition.RIGHT_TOP 
            })
        }
    }

    // Y軸の範囲設定
    chart.axisLeft.apply {
        when {
            isFixedY -> { axisMinimum = -20f; axisMaximum = 40f }
            element?.key == "theta_e" -> { axisMinimum = 270f; axisMaximum = 360f }
            element?.key == "water_vapor_flux" -> { axisMinimum = 0f; axisMaximum = 400f }
            else -> {
                val allY = (lineData.dataSets.filter { it.label != "TouchTarget" }.flatMap { ds -> (0 until ds.entryCount).map { ds.getEntryForIndex(it).y } } +
                            barData.dataSets.flatMap { ds -> (0 until ds.entryCount).map { ds.getEntryForIndex(it).y } })
                val min = element?.minY ?: allY.minOrNull() ?: 0f
                val max = maxOf(element?.defaultMaxY ?: 0f, allY.maxOrNull() ?: (min + 1f))
                axisMinimum = min; axisMaximum = if (max > min) max else min + 1f
            }
        }
    }

    // タッチ用データセットのY座標を、実データがあればその位置、なければ中央(midY)に配置する
    val midY = (chart.axisLeft.axisMinimum + chart.axisLeft.axisMaximum) / 2f
    for (i in 0 until touchDataSet.entryCount) {
        val p = finalPoints[i]
        val actualY = if (element == null) {
            WeatherConfig.WEATHER_ELEMENTS.find { el -> el.key == "0_0" }?.getValue(p.item)?.toFloat()
        } else {
            element.getValue(p.item)?.toFloat()
        }
        touchDataSet.getEntryForIndex(i).y = actualY ?: midY
    }

    chart.notifyDataSetChanged()
}

/**
 * 標準的な折れ線グラフデータセットの作成
 */
private fun createStandardLineDataSet(entries: List<Entry>, label: String, color: Int, highlightColor: Int) = LineDataSet(entries, label).apply {
    this.color = color; lineWidth = ChartDefaults.LineWidth; mode = LineDataSet.Mode.LINEAR
    isHighlightEnabled = false; highLightColor = ColorUtils.setAlphaComponent(highlightColor, ChartDefaults.HighlightAlpha)
    highlightLineWidth = 1f; setDrawHorizontalHighlightIndicator(false); setDrawVerticalHighlightIndicator(true); enableDashedHighlightLine(10f, 5f, 0f)
    setDrawValues(false)
    if (entries.size == 1) {
        setDrawCircles(true); setCircleColor(color); circleRadius = ChartDefaults.CircleRadiusSmall; setDrawCircleHole(false); setDrawFilled(false)
    } else {
        setDrawCircles(false); setDrawFilled(true); fillDrawable = getGradientDrawable(color)
    }
}

/**
 * 詳細表示用の折れ線グラフデータセットの作成
 */
private fun createDetailedLineDataSet(entries: List<Entry>, element: WeatherConfig.WeatherElement, primaryColor: Int, highlightColor: Int) = LineDataSet(entries, element.name).apply {
    color = primaryColor; lineWidth = ChartDefaults.LineWidth; mode = LineDataSet.Mode.LINEAR
    isHighlightEnabled = false; highLightColor = ColorUtils.setAlphaComponent(highlightColor, ChartDefaults.HighlightAlpha)
    highlightLineWidth = 1f; setDrawHorizontalHighlightIndicator(false); setDrawVerticalHighlightIndicator(true); enableDashedHighlightLine(10f, 5f, 0f)
    setDrawValues(false)

    val cColors = mutableListOf<Int>()
    var needsCircles = false
    entries.forEach { e ->
        if (element.getLevelLabel(e.y) != null) {
            cColors.add(element.getLevelColor(e.y)); needsCircles = true
        } else {
            if (entries.size == 1) { cColors.add(primaryColor); needsCircles = true }
            else cColors.add(android.graphics.Color.TRANSPARENT)
        }
    }

    if (needsCircles) {
        setDrawCircles(true); circleColors = cColors
        circleRadius = if (entries.size == 1 && cColors.all { it == primaryColor }) ChartDefaults.CircleRadiusSmall else ChartDefaults.CircleRadiusLarge
        setDrawCircleHole(false)
    } else setDrawCircles(false)

    if (entries.size > 1) {
        setDrawFilled(true); fillDrawable = getGradientDrawable(primaryColor)
    } else setDrawFilled(false)
}

private fun setupCommonXAxis(xAxis: XAxis, firstDt: OffsetDateTime?, maxHours: Float) {
    xAxis.apply {
        isGranularityEnabled = true; granularity = 1f; labelRotationAngle = ChartDefaults.XAxisLabelRotation
        axisMinimum = 0f; axisMaximum = maxHours
        timeAxisFormatter.firstDt = firstDt
        valueFormatter = timeAxisFormatter
        setLabelCount((maxHours / 3).toInt() + 1, false)
        removeAllLimitLines()
    }
}

private fun getPrecipitationColor(v: Float): Int {
    val alpha = 60
    return when {
        v < 1f -> android.graphics.Color.TRANSPARENT
        v <= 5f -> android.graphics.Color.argb(alpha, 160, 210, 255)
        v <= 10f -> android.graphics.Color.argb(alpha, 33, 140, 255)
        v <= 20f -> android.graphics.Color.argb(alpha, 0, 65, 255)
        v <= 30f -> android.graphics.Color.argb(alpha, 250, 245, 0)
        v <= 50f -> android.graphics.Color.argb(alpha, 255, 153, 0)
        v <= 80f -> android.graphics.Color.argb(alpha, 255, 40, 0)
        else -> android.graphics.Color.argb(alpha, 180, 0, 104)
    }
}

/**
 * チャート上の選択地点情報を表示するMarkerView
 */
class CustomMarkerView(context: Context, layoutResource: Int) : MarkerView(context, layoutResource) {
    private val tvContent: TextView = findViewById(R.id.text_marker)
    private val tvStatus: TextView = findViewById(R.id.text_marker_status)
    private var singleElement: WeatherConfig.WeatherElement? = null
    private var transportMode: String? = null
    private val decimalFormat = java.text.DecimalFormat("0.0")
    
    private data class MarkerCache(val item: GpvDataItem, val x: Float, val label: String)
    private var cache: List<MarkerCache> = emptyList()

    fun setChartData(points: List<WeatherChartPoint>, element: WeatherConfig.WeatherElement?, mode: String? = null) {
        this.singleElement = element; this.transportMode = mode
        cache = points.map { p ->
            val label = p.odt.format(DateTimeFormatter.ofPattern("M/d HH:mm"))
            MarkerCache(p.item, p.x, label)
        }
    }

    override fun refreshContent(e: Entry, highlight: Highlight) {
        try {
            val cached = cache.minByOrNull { abs(it.x - e.x) } ?: return
            val item = cached.item
            val timeLabel = cached.label

            if (singleElement == null) {
                val temp = WeatherConfig.WEATHER_ELEMENTS.find { it.key == "0_0" }?.getValue(item)
                val apparentTemp = item.contents.firstOrNull()?.value?.get("apparent_temp")?.let {
                    if (it > 100.0) it - 273.15 else it
                }
                val wind = WeatherConfig.WEATHER_ELEMENTS.find { it.key == "wind_speed" }?.getValue(item)
                val precip = WeatherConfig.WEATHER_ELEMENTS.find { it.key == "1_8" }?.getValue(item)
                
                if (temp == null && apparentTemp == null && wind == null && precip == null) {
                    tvContent.text = timeLabel
                    tvStatus.visibility = View.GONE
                } else {
                    val contentBuilder = StringBuilder()
                    contentBuilder.append(timeLabel).append("\n")
                    contentBuilder.append("気温: ${decimalFormat.format(temp ?: 0.0)}℃\n")
                    if (apparentTemp != null) contentBuilder.append("体感気温: ${decimalFormat.format(apparentTemp)}℃\n")
                    contentBuilder.append("風速: ${decimalFormat.format(wind ?: 0.0)} m/s\n")
                    contentBuilder.append("降水: ${decimalFormat.format(precip ?: 0.0)} mm/h")
                    
                    if (transportMode == "hiking" && item.elevation != null) {
                        item.elevation!!.toDoubleOrNull()?.let { contentBuilder.append("\n標高: ${decimalFormat.format(it)}m") }
                    }
                    tvContent.text = contentBuilder.toString()
                    tvStatus.visibility = View.GONE
                }
            } else {
                val value = singleElement!!.getValue(item)
                if (value == null) {
                    tvContent.text = timeLabel
                    tvStatus.visibility = View.GONE
                } else {
                    val displayValue = if (singleElement!!.unit.isNotEmpty()) "${decimalFormat.format(value ?: 0.0)} ${singleElement!!.unit}" else decimalFormat.format(value ?: 0.0)
                    tvContent.text = "$timeLabel\n$displayValue"
                    val floatValue = value.toFloat()
                    val statusText = singleElement!!.getLevelLabel(floatValue)
                    if (statusText != null) {
                        tvStatus.text = statusText; tvStatus.setTextColor(singleElement!!.getLevelColor(floatValue)); tvStatus.visibility = View.VISIBLE
                    } else { tvStatus.visibility = View.GONE }
                }
            }
            
            measure(View.MeasureSpec.makeMeasureSpec(0, View.MeasureSpec.UNSPECIFIED), View.MeasureSpec.makeMeasureSpec(0, View.MeasureSpec.UNSPECIFIED))
            layout(0, 0, measuredWidth, measuredHeight)
            super.refreshContent(e, highlight)
        } catch (ex: Exception) {
            android.util.Log.e("WeatherChart", "Error in refreshContent", ex)
        }
    }

    override fun getOffsetForDrawingAtPoint(posX: Float, posY: Float): MPPointF {
        val offset = getOffset()
        val chart = chartView ?: return offset
        val viewWidth = measuredWidth.toFloat()
        val viewHeight = measuredHeight.toFloat()
        var xOffset = offset.x
        var yOffset = offset.y

        val contentLeft = chart.viewPortHandler.contentLeft()
        val contentRight = chart.viewPortHandler.contentRight()
        val contentTop = chart.viewPortHandler.contentTop()
        val contentBottom = chart.viewPortHandler.contentBottom()

        if (posX + xOffset < contentLeft) xOffset = contentLeft - posX
        if (posX + viewWidth + xOffset > contentRight) xOffset = contentRight - posX - viewWidth

        val paddingTop = 15f
        val paddingBottom = 15f
        val minYOffset = contentTop + paddingTop - posY
        val maxYOffset = contentBottom - paddingBottom - viewHeight - posY

        yOffset = if (minYOffset <= maxYOffset) {
            yOffset.coerceIn(minYOffset, maxYOffset)
        } else {
            minYOffset
        }

        return MPPointF(xOffset, yOffset)
    }

    override fun getOffset(): MPPointF = MPPointF(-(measuredWidth / 2f), -measuredHeight.toFloat() - 10f)
}

/**
 * 日中・夜間の背景色や降水エリアを描画するX軸レンダラー
 */
class CustomXAxisRenderer(viewPortHandler: ViewPortHandler, xAxis: XAxis, trans: Transformer) : XAxisRenderer(viewPortHandler, xAxis, trans) {
    var weatherPoints: List<WeatherChartPoint>? = null
    var sunTimeRegions: List<SunTimeRegion>? = null
    var precipitationRegions: List<PrecipitationRegion>? = null
    var maxHours: Float = 0f
    var isDarkTheme: Boolean = false
    private val nightPaint = Paint().apply { style = Paint.Style.FILL }
    private val dayPaint = Paint().apply { style = Paint.Style.FILL }
    private val precipPaint = Paint().apply { style = Paint.Style.FILL }
    private val regionPts = FloatArray(4)

    override fun renderGridLines(c: Canvas) {
        val points = weatherPoints ?: return
        val regions = sunTimeRegions ?: return
        if (points.isEmpty()) return
        
        val fullTop = mViewPortHandler.contentTop()
        val bottom = mViewPortHandler.contentBottom()
        val sixthHeight = mViewPortHandler.contentHeight() / 4f
        val sixthBottom = fullTop + sixthHeight

        val dayStartColor = if (isDarkTheme) android.graphics.Color.argb(20, 255, 200, 0) else android.graphics.Color.argb(15, 255, 200, 0)
        val nightStartColor = if (isDarkTheme) android.graphics.Color.argb(25, 0, 0, 50) else android.graphics.Color.argb(20, 0, 0, 80)

        dayPaint.shader = android.graphics.LinearGradient(0f, fullTop, 0f, sixthBottom, dayStartColor, android.graphics.Color.TRANSPARENT, android.graphics.Shader.TileMode.CLAMP)
        nightPaint.shader = android.graphics.LinearGradient(0f, fullTop, 0f, sixthBottom, nightStartColor, android.graphics.Color.TRANSPARENT, android.graphics.Shader.TileMode.CLAMP)
        precipPaint.shader = null

        c.save(); c.clipRect(mViewPortHandler.contentRect)
        precipitationRegions?.forEach { region ->
            precipPaint.color = region.color
            drawRegion(c, region.startX, region.endX, fullTop, bottom, precipPaint)
        }
        regions.forEach { region ->
            drawRegion(c, region.startX, region.endX, fullTop, sixthBottom, if (region.isDay) dayPaint else nightPaint)
        }
        c.restore(); super.renderGridLines(c)
    }

    private fun drawRegion(c: Canvas, startX: Float, endX: Float, topY: Float, bottomY: Float, paint: Paint) {
        val sX = if (startX < 0f) 0f else startX
        val eX = if (endX > maxHours) maxHours else endX
        if (sX < eX && sX <= maxHours && eX >= 0f) {
            regionPts[0] = sX; regionPts[1] = 0f; regionPts[2] = eX; regionPts[3] = 0f
            mTrans.pointValuesToPixel(regionPts)
            c.drawRect(regionPts[0], topY, regionPts[2], bottomY, paint)
        }
    }

    override fun drawLabels(c: Canvas, pos: Float, anchor: com.github.mikephil.charting.utils.MPPointF) {
        val positions = FloatArray(mXAxis.mEntryCount * 2)
        for (i in 0 until mXAxis.mEntryCount) { positions[i * 2] = mXAxis.mEntries[i]; positions[i * 2 + 1] = 0f }
        mTrans.pointValuesToPixel(positions)

        for (i in 0 until mXAxis.mEntryCount) {
            val xValue = mXAxis.mEntries[i]
            val xPx = positions[i * 2]
            if (mViewPortHandler.isInBoundsX(xPx)) {
                val label = mXAxis.valueFormatter.getAxisLabel(xValue, mXAxis)
                if (!label.isNullOrEmpty()) {
                    val region = sunTimeRegions?.find { xValue >= it.startX && xValue <= it.endX }
                    val isDay = region?.isDay ?: true
                    mAxisLabelPaint.color = if (isDay) {
                        if (isDarkTheme) android.graphics.Color.parseColor("#FFCC80") else android.graphics.Color.parseColor("#E65100")
                    } else {
                        if (isDarkTheme) android.graphics.Color.parseColor("#90CAF9") else android.graphics.Color.parseColor("#1565C0")
                    }
                    drawLabel(c, label, xPx, pos, anchor, mXAxis.labelRotationAngle)
                }
            }
        }
    }
}

private fun parseDateTime(dtStr: String): OffsetDateTime {
    return try {
        OffsetDateTime.parse(dtStr)
    } catch (e: Exception) {
        try {
            val formatted = dtStr.replace(" ", "T")
            if (!formatted.contains("+") && !formatted.endsWith("Z")) OffsetDateTime.parse(formatted + "+09:00")
            else OffsetDateTime.parse(formatted)
        } catch (e2: Exception) {
            android.util.Log.e("WeatherChart", "Failed to parse datetime: $dtStr", e2)
            OffsetDateTime.now(ChartDefaults.JstOffset)
        }
    }
}

@SuppressLint("ViewConstructor")
private class TooltipAlwaysVisibleChart(context: Context) : CombinedChart(context) {
    override fun drawMarkers(canvas: Canvas?) {
        if (canvas == null || marker == null || !isDrawMarkersEnabled || !valuesToHighlight())
            return

        for (i in highlighted.indices) {
            val highlight = highlighted[i]
            @Suppress("UNCHECKED_CAST")
            val set = data?.getDataSetByIndex(highlight.dataSetIndex) as? com.github.mikephil.charting.interfaces.datasets.IDataSet<Entry> ?: continue
            val e = data?.getEntryForHighlight(highlight) ?: continue

            val entryIndex = set.getEntryIndex(e)
            if (entryIndex.toFloat() > set.entryCount * animator.phaseX) continue

            val pos = getMarkerPosition(highlight)

            // 修正ポイント: Y座標が範囲外でもスキップしない！ X座標(左右)だけ画面内かチェックする
            if (!viewPortHandler.isInBoundsX(pos[0])) continue

            marker.refreshContent(e, highlight)
            marker.draw(canvas, pos[0], pos[1])
        }
    }
}
