package com.example.weather_location_app.ui.components

import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import com.mikepenz.markdown.m3.Markdown
import kotlinx.coroutines.delay

import androidx.compose.ui.graphics.Color
import androidx.compose.material3.MaterialTheme

@Composable
fun SmoothStreamingMarkdown(
    streamedText: String,
    isCompleted: Boolean,
    backgroundColor: Color = MaterialTheme.colorScheme.surface,
    modifier: Modifier = Modifier
) {
    // 開いた瞬間にすでに完了していれば全文をセット、そうでなければ空からスタート
    var displayedText by remember { mutableStateOf(if (isCompleted) streamedText else "") }
    
    // 最新のテキスト状態を常に保持（LaunchedEffectを再起動させないため）
    val targetText by rememberUpdatedState(streamedText)

    LaunchedEffect(Unit) {
        while (true) {
            val targetLen = targetText.length
            val currentLen = displayedText.length

            if (currentLen < targetLen) {
                // 文字の遅れを取り戻すための速度計算
                val diff = targetLen - currentLen
                val delayMs = (50L / diff).coerceIn(5L, 20L)
                
                displayedText += targetText[currentLen]
                delay(delayMs)
            } else {
                // 追いついたら次の文字が来るまで少し待機（約1フレーム）
                delay(16)
            }
        }
    }

    Markdown(
        content = displayedText,
        modifier = modifier
    )
}