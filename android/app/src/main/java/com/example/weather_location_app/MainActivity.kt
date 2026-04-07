package com.example.weather_location_app

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.appcompat.app.AppCompatDelegate
import androidx.core.splashscreen.SplashScreen.Companion.installSplashScreen
import androidx.lifecycle.lifecycleScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.WindowInsets
import androidx.compose.foundation.layout.asPaddingValues
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.navigationBars
import androidx.compose.foundation.layout.windowInsetsPadding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.clickable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.ripple.rememberRipple
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.unit.dp
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.LocationOn
import androidx.compose.material.icons.filled.Route
import androidx.compose.material3.*
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import com.example.weather_location_app.data.api.RetrofitClient
import com.example.weather_location_app.ui.screens.PinpointWeatherScreen
import com.example.weather_location_app.ui.screens.RouteWeatherScreen
import com.example.weather_location_app.ui.theme.WeatherAppTheme

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        // SplashScreenのインストール
        installSplashScreen()
        
        // RetrofitClientの初期化
        RetrofitClient.init(applicationContext)
        
        // 重い初期化処理をバックグラウンドで行う
        lifecycleScope.launch(Dispatchers.IO) {
            // Retrofitクライアントの準備を裏で済ませておく
            val _weather = RetrofitClient.weatherApi
            val _route = RetrofitClient.routeApi
        }

        super.onCreate(savedInstanceState)
        // 画面の端まで描画領域を広げる（モダンなAndroidの基本）
        enableEdgeToEdge()
        
        setContent {
            WeatherAppTheme {
                // Surfaceを追加して背景色を確実に設定
                Surface(
                    modifier = Modifier.fillMaxSize(),
                    color = MaterialTheme.colorScheme.background
                ) {
                    MainScreen()
                }
            }
        }
    }
}

@Composable
fun MainScreen() {
    // 現在選択されているタブの状態を管理
    var selectedTab by remember { mutableStateOf(0) }
    
    // タブの情報定義
    val tabs = listOf("ピンポイント", "ルート")
    val icons = listOf(Icons.Default.LocationOn, Icons.Default.Route)

    Scaffold(
        bottomBar = {
            Surface(
                color = MaterialTheme.colorScheme.surface,
                tonalElevation = 8.dp,
                shadowElevation = 8.dp
            ) {
                Row(
                    modifier = Modifier
                        .fillMaxWidth()
                        .windowInsetsPadding(WindowInsets.navigationBars)
                        .height(80.dp),
                    horizontalArrangement = Arrangement.SpaceEvenly,
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    tabs.forEachIndexed { index, title ->
                        val isSelected = selectedTab == index
                        val iconColor = if (isSelected) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant
                        val textColor = if (isSelected) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant
                        
                        // クリック領域を固定幅（90dp）にし、アイコンとテキストの周囲に限定
                        Column(
                            modifier = Modifier
                                .width(90.dp) // 固定幅に設定
                                .clip(RoundedCornerShape(16.dp))
                                .clickable(
                                    interactionSource = remember { MutableInteractionSource() },
                                    indication = rememberRipple(bounded = true),
                                    onClick = { selectedTab = index }
                                )
                                .padding(vertical = 8.dp), // 横パディングは固定幅のため削除
                            horizontalAlignment = Alignment.CenterHorizontally,
                            verticalArrangement = Arrangement.Center
                        ) {
                            Icon(
                                imageVector = icons[index],
                                contentDescription = title,
                                tint = iconColor,
                                modifier = Modifier.size(24.dp)
                            )
                            Text(
                                text = title,
                                style = MaterialTheme.typography.labelMedium,
                                color = textColor
                            )
                        }
                    }
                }
            }
        }
    ) { innerPadding ->
        // 画面中央の内容部分
        Box(
            modifier = Modifier.fillMaxSize().padding(innerPadding)
        ) {
            when (selectedTab) {
                0 -> PinpointWeatherScreen()
                1 -> RouteWeatherScreen()
            }
        }
    }
}
