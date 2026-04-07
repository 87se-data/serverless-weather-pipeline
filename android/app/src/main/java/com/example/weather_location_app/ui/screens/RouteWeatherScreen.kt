package com.example.weather_location_app.ui.screens

import com.example.weather_location_app.ui.components.shimmerEffect
import android.Manifest
import android.annotation.SuppressLint
import android.content.pm.PackageManager
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.foundation.border
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.DirectionsWalk
import androidx.compose.material.icons.filled.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.core.content.ContextCompat
import androidx.lifecycle.viewmodel.compose.viewModel
import com.example.weather_location_app.WeatherConfig
import com.example.weather_location_app.ui.components.WeatherChart
import com.example.weather_location_app.ui.viewmodels.RouteViewModel
import com.example.weather_location_app.R
import com.google.android.gms.location.LocationServices
import com.google.android.gms.maps.CameraUpdateFactory
import com.google.android.gms.maps.model.*
import com.google.maps.android.compose.*
import com.mikepenz.markdown.m3.Markdown
import kotlinx.coroutines.launch
import java.util.*
import androidx.compose.animation.core.animateDpAsState
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.ui.draw.clip
import androidx.compose.ui.platform.LocalConfiguration
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.animation.slideInVertically
import androidx.compose.animation.slideOutVertically
import androidx.compose.foundation.shape.RoundedCornerShape
import kotlinx.coroutines.delay

@SuppressLint("MissingPermission")
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun RouteWeatherScreen(
    viewModel: RouteViewModel = viewModel()
) {
    val uiState by viewModel.uiState
    val scope = rememberCoroutineScope()
    val context = LocalContext.current
    val snackbarHostState = remember { SnackbarHostState() }
    val configuration = LocalConfiguration.current
    val screenHeight = configuration.screenHeightDp.dp
    
    var isMapExpanded by remember { mutableStateOf(false) }
    val mapHeight by animateDpAsState(
        targetValue = if (isMapExpanded) {
            screenHeight - 100.dp
        } else {
            screenHeight * 0.35f
        },
        label = "MapHeightAnimation"
    )

    var showDatePicker by remember { mutableStateOf(false) }
    var showTimePicker by remember { mutableStateOf(false) }
    var tempCalendar by remember { mutableStateOf(Calendar.getInstance()) }
    
    var showConfirmDialog by remember { mutableStateOf<LatLng?>(null) }
    var hintMessage by remember { mutableStateOf<String?>(null) }
    
    var mapType by remember { mutableStateOf(MapType.NORMAL) }
    var showMapTypeMenu by remember { mutableStateOf(false) }

    // ヒントメッセージの自動消去タイマー
    LaunchedEffect(hintMessage) {
        if (hintMessage != null) {
            delay(3000)
            hintMessage = null
        }
    }

    val transportModes = listOf(
        Triple("徒歩", "walking", Icons.AutoMirrored.Filled.DirectionsWalk),
        Triple("自動車", "driving", Icons.Default.DirectionsCar),
        Triple("自転車", "bicycling", Icons.Default.DirectionsBike),
        Triple("登山", "hiking", Icons.Default.Terrain)
    )

    val initialPos = LatLng(35.6895, 139.6917)
    val cameraPositionState = rememberCameraPositionState {
        position = CameraPosition.fromLatLngZoom(initialPos, 10f)
    }

    val startMarkerState = rememberMarkerState()
    val endMarkerState = rememberMarkerState()

    LaunchedEffect(uiState.startLatLng) {
        uiState.startLatLng?.let { startMarkerState.position = it }
    }
    LaunchedEffect(uiState.endLatLng) {
        uiState.endLatLng?.let { endMarkerState.position = it }
    }

    val fusedLocationClient = remember { LocationServices.getFusedLocationProviderClient(context) }
    val launcher = rememberLauncherForActivityResult(
        ActivityResultContracts.RequestMultiplePermissions()
    ) { _ -> }

    // 初期カメラ位置を現在地に設定
    LaunchedEffect(Unit) {
        if (ContextCompat.checkSelfPermission(context, Manifest.permission.ACCESS_FINE_LOCATION) == PackageManager.PERMISSION_GRANTED) {
            fusedLocationClient.lastLocation.addOnSuccessListener { loc ->
                loc?.let {
                    val currentLatLng = LatLng(it.latitude, it.longitude)
                    scope.launch {
                        cameraPositionState.move(CameraUpdateFactory.newLatLngZoom(currentLatLng, 10f))
                    }
                }
            }
        } else {
            launcher.launch(arrayOf(Manifest.permission.ACCESS_FINE_LOCATION, Manifest.permission.ACCESS_COARSE_LOCATION))
        }
    }

    // データ取得完了時に自動ズーム
    LaunchedEffect(uiState.weatherData) {
        if (uiState.weatherData != null && uiState.startLatLng != null && uiState.endLatLng != null) {
            val bounds = LatLngBounds.builder()
                .include(uiState.startLatLng!!)
                .include(uiState.endLatLng!!)
                .build()
            cameraPositionState.animate(
                CameraUpdateFactory.newLatLngBounds(bounds, 150)
            )
        }
    }

    val selectedMarkerState = rememberMarkerState()
    LaunchedEffect(uiState.selectedRoutePoint) {
        uiState.selectedRoutePoint?.let {
            selectedMarkerState.position = it
        }
    }

    // AIアドバイスのエラー通知
    LaunchedEffect(uiState.routeAdviceError) {
        uiState.routeAdviceError?.let {
            snackbarHostState.showSnackbar(it)
        }
    }

    Box(modifier = Modifier.fillMaxSize()) {
        Column(modifier = Modifier.fillMaxSize().background(MaterialTheme.colorScheme.background)) {
            // 1. 地図エリア
            Card(
                modifier = Modifier
                    .fillMaxWidth()
                    .height(mapHeight)
                    .padding(if (isMapExpanded) 0.dp else 12.dp),
                shape = if (isMapExpanded) RoundedCornerShape(0.dp) else RoundedCornerShape(28.dp),
                colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceVariant)
            ) {
                Box(modifier = Modifier.fillMaxSize()) {
                    GoogleMap(
                        modifier = Modifier.fillMaxSize(),
                        cameraPositionState = cameraPositionState,
                        properties = MapProperties(
                            isMyLocationEnabled = ContextCompat.checkSelfPermission(
                                context, Manifest.permission.ACCESS_FINE_LOCATION
                            ) == PackageManager.PERMISSION_GRANTED,
                            mapType = mapType
                        ),
                        uiSettings = MapUiSettings(
                            zoomControlsEnabled = false,
                            myLocationButtonEnabled = false,
                            compassEnabled = true
                        ),
                        contentPadding = PaddingValues(
                            top = if (hintMessage != null) 72.dp else 16.dp,
                            bottom = 16.dp,
                            start = 16.dp,
                            end = 16.dp
                        ),
                        onMapClick = {
                        val target = if (uiState.isSelectingStart) "出発地" else "到着地"
                        hintMessage = "長押しで${target}を設定"
                        },
                        onMapLongClick = { latLng ->
                            showConfirmDialog = latLng
                        }
                    ) {
                        uiState.startLatLng?.let {
                            Marker(
                                state = startMarkerState,
                                title = "出発地",
                                icon = BitmapDescriptorFactory.defaultMarker(BitmapDescriptorFactory.HUE_RED)
                            )
                        }
                        uiState.endLatLng?.let {
                            Marker(
                                state = endMarkerState,
                                title = "到着地",
                                icon = BitmapDescriptorFactory.defaultMarker(BitmapDescriptorFactory.HUE_BLUE)
                            )
                        }
                        MarkerComposable(
                            keys = arrayOf(uiState.transportMode),
                            state = selectedMarkerState,
                            title = "選択地点",
                            zIndex = 1.0f,
                            visible = uiState.selectedRoutePoint != null
                        ) {
                            Box(
                                modifier = Modifier
                                    .size(36.dp)
                                    .background(MaterialTheme.colorScheme.primary, CircleShape)
                                    .border(2.dp, Color.White, CircleShape),
                                contentAlignment = Alignment.Center
                            ) {
                                val mode = uiState.transportMode
                                val icon = when (mode) {
                                    "walking", "hiking" -> Icons.AutoMirrored.Filled.DirectionsWalk
                                    "bicycling" -> Icons.Default.DirectionsBike
                                    "driving" -> Icons.Default.DirectionsCar
                                    else -> Icons.Default.DirectionsCar
                                }
                                Icon(
                                    imageVector = icon,
                                    contentDescription = "選択地点の移動手段",
                                    tint = Color.White,
                                    modifier = Modifier.size(20.dp)
                                )
                            }
                        }
                        if (uiState.routePoints.size >= 2) {
                            Polyline(
                                points = uiState.routePoints,
                                color = MaterialTheme.colorScheme.primary, // 100%不透明
                                width = 18f
                            )
                        }
                    }

                    // カスタムヒントメッセージ
                    androidx.compose.animation.AnimatedVisibility(
                        visible = hintMessage != null,
                        enter = fadeIn() + slideInVertically(),
                        exit = fadeOut() + slideOutVertically(),
                        modifier = Modifier
                            .align(Alignment.TopCenter)
                            .padding(top = 16.dp)
                    ) {
                        Surface(
                            color = MaterialTheme.colorScheme.inverseSurface.copy(alpha = 0.9f),
                            shape = CircleShape
                        ) {
                            Text(
                                text = hintMessage ?: "",
                                color = MaterialTheme.colorScheme.inverseOnSurface,
                                style = MaterialTheme.typography.bodyMedium,
                                modifier = Modifier.padding(horizontal = 20.dp, vertical = 10.dp)
                            )
                        }
                    }

                    Column(
                        modifier = Modifier.align(Alignment.TopEnd).padding(16.dp),
                        verticalArrangement = Arrangement.spacedBy(12.dp)
                    ) {
                        Box {
                            FloatingActionButton(
                                onClick = { showMapTypeMenu = true },
                                modifier = Modifier.size(48.dp),
                                containerColor = MaterialTheme.colorScheme.surface.copy(alpha = 0.9f),
                                contentColor = MaterialTheme.colorScheme.primary,
                                shape = CircleShape,
                                elevation = FloatingActionButtonDefaults.elevation(0.dp, 0.dp, 0.dp, 0.dp)
                            ) {
                                Icon(
                                    Icons.Default.Layers, 
                                    contentDescription = "マップタイプ切り替え",
                                    modifier = Modifier.size(24.dp)
                                )
                            }
                            DropdownMenu(
                                expanded = showMapTypeMenu,
                                onDismissRequest = { showMapTypeMenu = false }
                            ) {
                                DropdownMenuItem(
                                    text = { Text("標準") },
                                    onClick = { mapType = MapType.NORMAL; showMapTypeMenu = false },
                                    leadingIcon = { RadioButton(selected = mapType == MapType.NORMAL, onClick = null) }
                                )
                                DropdownMenuItem(
                                    text = { Text("地形図") },
                                    onClick = { mapType = MapType.TERRAIN; showMapTypeMenu = false },
                                    leadingIcon = { RadioButton(selected = mapType == MapType.TERRAIN, onClick = null) }
                                )
                                DropdownMenuItem(
                                    text = { Text("航空写真") },
                                    onClick = { mapType = MapType.SATELLITE; showMapTypeMenu = false },
                                    leadingIcon = { RadioButton(selected = mapType == MapType.SATELLITE, onClick = null) }
                                )
                            }
                        }

                        FloatingActionButton(
                            onClick = {
                                if (ContextCompat.checkSelfPermission(context, Manifest.permission.ACCESS_FINE_LOCATION) == PackageManager.PERMISSION_GRANTED) {
                                    fusedLocationClient.lastLocation.addOnSuccessListener { loc ->
                                        loc?.let {
                                            val currentLatLng = LatLng(it.latitude, it.longitude)
                                            scope.launch { cameraPositionState.animate(CameraUpdateFactory.newLatLngZoom(currentLatLng, 15f)) }
                                            showConfirmDialog = currentLatLng
                                        }
                                    }
                                } else {
                                    launcher.launch(arrayOf(Manifest.permission.ACCESS_FINE_LOCATION, Manifest.permission.ACCESS_COARSE_LOCATION))
                                }
                            },
                            modifier = Modifier.size(48.dp),
                            containerColor = MaterialTheme.colorScheme.surface.copy(alpha = 0.9f),
                            contentColor = MaterialTheme.colorScheme.primary,
                            shape = CircleShape,
                            elevation = FloatingActionButtonDefaults.elevation(0.dp, 0.dp, 0.dp, 0.dp)
                        ) {
                            Icon(
                                Icons.Default.MyLocation, 
                                contentDescription = null,
                                modifier = Modifier.size(24.dp)
                            )
                        }
                    }

                    Row(
                        modifier = Modifier.align(Alignment.BottomEnd).padding(bottom = 24.dp, end = 16.dp),
                        horizontalArrangement = Arrangement.spacedBy(12.dp)
                    ) {
                        FloatingActionButton(
                            onClick = { scope.launch { cameraPositionState.animate(CameraUpdateFactory.zoomOut()) } },
                            modifier = Modifier.size(48.dp),
                            containerColor = MaterialTheme.colorScheme.surface.copy(alpha = 0.9f),
                            contentColor = MaterialTheme.colorScheme.primary,
                            shape = CircleShape,
                            elevation = FloatingActionButtonDefaults.elevation(0.dp, 0.dp, 0.dp, 0.dp)
                        ) { 
                            Icon(
                                Icons.Default.Remove, 
                                contentDescription = null,
                                modifier = Modifier.size(24.dp)
                            ) 
                        }
                        FloatingActionButton(
                            onClick = { scope.launch { cameraPositionState.animate(CameraUpdateFactory.zoomIn()) } },
                            modifier = Modifier.size(48.dp),
                            containerColor = MaterialTheme.colorScheme.surface.copy(alpha = 0.9f),
                            contentColor = MaterialTheme.colorScheme.primary,
                            shape = CircleShape,
                            elevation = FloatingActionButtonDefaults.elevation(0.dp, 0.dp, 0.dp, 0.dp)
                        ) { 
                            Icon(
                                Icons.Default.Add, 
                                contentDescription = null,
                                modifier = Modifier.size(24.dp)
                            ) 
                        }
                    }
                }
            }

            // 地図の伸縮ハンドル
            Box(
                modifier = Modifier
                    .fillMaxWidth()
                    .height(32.dp)
                    .clickable { isMapExpanded = !isMapExpanded },
                contentAlignment = Alignment.Center
            ) {
                Box(
                    modifier = Modifier
                        .width(48.dp)
                        .height(6.dp)
                        .clip(CircleShape)
                        .background(MaterialTheme.colorScheme.outlineVariant)
                )
            }

            // 3. スクロールコンテンツ
            LazyColumn(
                modifier = Modifier.fillMaxSize(),
                contentPadding = PaddingValues(horizontal = 20.dp, vertical = 8.dp),
                verticalArrangement = Arrangement.spacedBy(20.dp)
            ) {
                // 2. 始点・終点トグル (LazyColumnの最初の要素へ移動)
                item {
                    SingleChoiceSegmentedButtonRow(
                        modifier = Modifier.fillMaxWidth().padding(vertical = 8.dp)
                    ) {
                        val isDark = isSystemInDarkTheme()
                        SegmentedButton(
                            selected = uiState.isSelectingStart,
                            onClick = { viewModel.toggleSelectionMode(true) },
                            shape = SegmentedButtonDefaults.itemShape(index = 0, count = 2),
                            colors = SegmentedButtonDefaults.colors(
                                activeContainerColor = if (isDark) Color(0xFF442222) else Color(0xFFFFEBEE),
                                activeContentColor = if (isDark) Color(0xFFFF8A80) else Color(0xFFD32F2F)
                            )
                        ) {
                            Row(verticalAlignment = Alignment.CenterVertically) {
                                Icon(Icons.Default.Place, contentDescription = null, modifier = Modifier.size(20.dp))
                                Spacer(Modifier.width(8.dp))
                                Text("出発地")
                            }
                        }
                        SegmentedButton(
                            selected = !uiState.isSelectingStart,
                            onClick = { viewModel.toggleSelectionMode(false) },
                            shape = SegmentedButtonDefaults.itemShape(index = 1, count = 2),
                            colors = SegmentedButtonDefaults.colors(
                                activeContainerColor = if (isDark) Color(0xFF222244) else Color(0xFFE3F2FD),
                                activeContentColor = if (isDark) Color(0xFF82B1FF) else Color(0xFF1976D2)
                            )
                        ) {
                            Row(verticalAlignment = Alignment.CenterVertically) {
                                Icon(Icons.Default.Place, contentDescription = null, modifier = Modifier.size(20.dp))
                                Spacer(Modifier.width(8.dp))
                                Text("到着地")
                            }
                        }
                    }
                }

                item {
                    Card(
                        modifier = Modifier.fillMaxWidth(),
                        shape = RoundedCornerShape(28.dp),
                        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.5f))
                    ) {
                        Column(modifier = Modifier.padding(20.dp), verticalArrangement = Arrangement.spacedBy(16.dp)) {
                            SingleChoiceSegmentedButtonRow(modifier = Modifier.fillMaxWidth()) {
                                transportModes.forEachIndexed { index, (label, mode, icon) ->
                                    SegmentedButton(
                                        selected = uiState.transportMode == mode,
                                        onClick = { viewModel.updateTransportMode(mode) },
                                        shape = SegmentedButtonDefaults.itemShape(index = index, count = transportModes.size),
                                        icon = { SegmentedButtonDefaults.Icon(active = uiState.transportMode == mode) {
                                            Icon(icon, contentDescription = null, modifier = Modifier.size(18.dp))
                                        }}
                                    ) { Text(label, style = MaterialTheme.typography.labelSmall) }
                                }
                            }

                            // 出発日時
                            OutlinedCard(
                                onClick = { showDatePicker = true },
                                modifier = Modifier.fillMaxWidth(),
                                shape = RoundedCornerShape(20.dp),
                                colors = CardDefaults.outlinedCardColors(containerColor = MaterialTheme.colorScheme.surface)
                            ) {
                                Row(
                                    modifier = Modifier.padding(16.dp).fillMaxWidth(),
                                    verticalAlignment = Alignment.CenterVertically
                                ) {
                                    Icon(Icons.Default.CalendarMonth, contentDescription = null, tint = MaterialTheme.colorScheme.primary)
                                    Spacer(Modifier.width(16.dp))
                                    Column {
                                        Text("出発日時", style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.secondary)
                                        val timeFormat = java.text.SimpleDateFormat("yyyy/MM/dd HH:mm", Locale.JAPAN)
                                        Text(timeFormat.format(uiState.departureTime.time), style = MaterialTheme.typography.bodyLarge)
                                    }
                                    Spacer(Modifier.weight(1f))
                                    
                                    TextButton(
                                        onClick = { 
                                            viewModel.resetDepartureTimeToNow()
                                        },
                                        contentPadding = PaddingValues(horizontal = 12.dp)
                                    ) {
                                        Icon(Icons.Default.Restore, contentDescription = null, modifier = Modifier.size(18.dp))
                                        Spacer(Modifier.width(4.dp))
                                        Text("現在", style = MaterialTheme.typography.labelMedium)
                                    }
                                    
                                    VerticalDivider(modifier = Modifier.height(24.dp).padding(horizontal = 8.dp))
                                    
                                    Icon(Icons.Default.Edit, contentDescription = null, modifier = Modifier.size(20.dp), tint = MaterialTheme.colorScheme.primary)
                                }
                            }
                            
                            Row(
                                modifier = Modifier.fillMaxWidth(),
                                verticalAlignment = Alignment.CenterVertically,
                                horizontalArrangement = Arrangement.spacedBy(12.dp)
                            ) {
                                Button(
                                    onClick = { viewModel.fetchRouteWeatherData() },
                                    enabled = uiState.startLatLng != null && uiState.endLatLng != null && !uiState.isLoading && !uiState.isRouteAdviceLoading,
                                    modifier = Modifier.weight(1f).height(64.dp).then(if (uiState.isLoading) Modifier.clip(RoundedCornerShape(28.dp)).shimmerEffect() else Modifier),
                                    shape = RoundedCornerShape(28.dp),
                                    elevation = ButtonDefaults.buttonElevation(0.dp, 0.dp, 0.dp, 0.dp)
                                ) {
                                    if (uiState.isLoading) {
                                        var dotCount by remember { mutableIntStateOf(0) }
                                        LaunchedEffect(Unit) {
                                            while (true) {
                                                delay(500)
                                                dotCount = (dotCount + 1) % 4
                                            }
                                        }
                                        val dots = ".".repeat(dotCount)
                                        Text("取得中$dots", style = MaterialTheme.typography.titleMedium)
                                    } else {
                                        Icon(Icons.Default.CloudDownload, contentDescription = null)
                                        Spacer(Modifier.width(12.dp))
                                        Text("ルート天気を取得", style = MaterialTheme.typography.titleMedium)
                                    }
                                }

                                // AIルートガイドボタン (ピンポイント画面と統一したアイコンボタン形式)
                                FilledTonalIconButton(
                                    onClick = { viewModel.onAiAdviceButtonClicked() },
                                    modifier = Modifier.size(64.dp),
                                    // ルート検索結果がある時のみ有効、かつロード中以外
                                    enabled = uiState.currentRouteResponse != null && !uiState.isLoading,
                                    shape = CircleShape,
                                    colors = IconButtonDefaults.filledTonalIconButtonColors(
                                        containerColor = MaterialTheme.colorScheme.primaryContainer,
                                        contentColor = MaterialTheme.colorScheme.onPrimaryContainer,
                                        disabledContainerColor = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.38f),
                                        disabledContentColor = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.38f)
                                    )
                                ) {
                                    if (uiState.isRouteAdviceLoading) {
                                        CircularProgressIndicator(
                                            modifier = Modifier.size(24.dp),
                                            color = MaterialTheme.colorScheme.primary,
                                            strokeWidth = 2.5.dp
                                        )
                                    } else {
                                        Icon(
                                            imageVector = Icons.Default.AutoAwesome,
                                            contentDescription = "AIルートガイド",
                                            modifier = Modifier.size(32.dp)
                                        )
                                    }
                                }
                            }
                        }
                    }
                }

                item {
                    Card(
                        modifier = Modifier.fillMaxWidth().height(340.dp),
                        shape = RoundedCornerShape(28.dp),
                        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surface)
                    ) {
                        Box(modifier = Modifier.fillMaxSize().padding(12.dp), contentAlignment = Alignment.Center) {
                            if (uiState.weatherData != null) {
                                WeatherChart(
                                    data = uiState.weatherData!!,
                                    sunTimeRegions = uiState.sunTimeRegions,
                                    element = null,
                                    isFixedY = true,
                                    showCurrentTimeLine = false,
                                    selectedPointIndex = uiState.selectedRouteIndex,
                                    transportMode = uiState.transportMode,
                                    modifier = Modifier.fillMaxSize(),
                                    onPointSelected = { index ->
                                        viewModel.updateSelectedRoutePoint(index)
                                    }
                                )
                            } else {
                                Text(
                                    text = if (uiState.error != null) uiState.error!! else "ルートを設定してください",
                                    style = MaterialTheme.typography.bodyMedium,
                                    color = if (uiState.error != null) MaterialTheme.colorScheme.error else MaterialTheme.colorScheme.onSurfaceVariant
                                )
                            }
                        }
                    }
                }
                
                item { Spacer(modifier = Modifier.height(32.dp)) }
            }
        }

        SnackbarHost(
            hostState = snackbarHostState,
            modifier = Modifier
                .align(Alignment.BottomCenter)
                .padding(bottom = 16.dp)
        )
    }

// 過去時刻設定・実行時の警告ダイアログ
if (uiState.showPastTimeWarning) {
    AlertDialog(
        onDismissRequest = { viewModel.dismissPastTimeWarning() },
        title = { Text("出発時刻の確認") },
        text = { Text("出発時刻は現在時刻以降にしてください。現在時刻に再設定しました。") },
        confirmButton = {
            TextButton(onClick = { viewModel.dismissPastTimeWarning() }) {
                Text("OK")
            }
        }
    )
}

// 地点設定確認ダイアログ
if (showConfirmDialog != null) {
    val targetLatLng = showConfirmDialog!!
    AlertDialog(
        onDismissRequest = { showConfirmDialog = null },
        title = { Text(if (uiState.isSelectingStart) "出発地に設定" else "到着地に設定") },
        text = { Text("${if (uiState.isSelectingStart) "出発地" else "到着地"}に設定しますか？") },
        confirmButton = {
            TextButton(onClick = {
                viewModel.setLocation(targetLatLng, uiState.isSelectingStart)
                showConfirmDialog = null
            }) { Text("設定") }
        },
        dismissButton = {
            TextButton(onClick = { showConfirmDialog = null }) { Text("キャンセル") }
        }
    )
}

// 日付選択ダイアログ
if (showDatePicker) {
    val initialDateMillis = remember(uiState.departureTime) {
        val localCal = uiState.departureTime
        val utcCal = Calendar.getInstance(TimeZone.getTimeZone("UTC")).apply {
            set(Calendar.YEAR, localCal.get(Calendar.YEAR))
            set(Calendar.MONTH, localCal.get(Calendar.MONTH))
            set(Calendar.DAY_OF_MONTH, localCal.get(Calendar.DAY_OF_MONTH))
            set(Calendar.HOUR_OF_DAY, 0)
            set(Calendar.MINUTE, 0)
            set(Calendar.SECOND, 0)
            set(Calendar.MILLISECOND, 0)
        }
        utcCal.timeInMillis
    }
    val datePickerState = rememberDatePickerState(
        initialSelectedDateMillis = initialDateMillis
    )
    DatePickerDialog(
        onDismissRequest = { showDatePicker = false },
        confirmButton = {
            TextButton(onClick = {
                datePickerState.selectedDateMillis?.let {
                    val utcCal = Calendar.getInstance(TimeZone.getTimeZone("UTC")).apply {
                        timeInMillis = it
                    }
                    val localCal = Calendar.getInstance().apply {
                        set(Calendar.YEAR, utcCal.get(Calendar.YEAR))
                        set(Calendar.MONTH, utcCal.get(Calendar.MONTH))
                        set(Calendar.DAY_OF_MONTH, utcCal.get(Calendar.DAY_OF_MONTH))
                    }
                    tempCalendar = localCal
                    showDatePicker = false
                    showTimePicker = true
                }
            }) { Text("次へ") }
        },
        dismissButton = {
            TextButton(onClick = { showDatePicker = false }) { Text("キャンセル") }
        }
    ) {
        DatePicker(state = datePickerState)
    }
}

// 時刻選択ダイアログ
if (showTimePicker) {
    val timePickerState = rememberTimePickerState(
        initialHour = uiState.departureTime.get(Calendar.HOUR_OF_DAY),
        initialMinute = uiState.departureTime.get(Calendar.MINUTE)
    )
    
    AlertDialog(
        onDismissRequest = { showTimePicker = false },
        confirmButton = {
            TextButton(onClick = {
                val finalCal = (tempCalendar.clone() as Calendar).apply {
                    set(Calendar.HOUR_OF_DAY, timePickerState.hour)
                    set(Calendar.MINUTE, timePickerState.minute)
                    set(Calendar.SECOND, 0)
                    set(Calendar.MILLISECOND, 0)
                }
                viewModel.setDepartureTime(finalCal)
                showTimePicker = false
            }) { Text("設定完了") }
        },
        dismissButton = {
            TextButton(onClick = { showTimePicker = false }) { Text("戻る") }
        },
        text = {
            Column(horizontalAlignment = Alignment.CenterHorizontally) {
                Text("出発時刻を選択", style = MaterialTheme.typography.labelLarge, modifier = Modifier.padding(bottom = 16.dp))
                TimePicker(state = timePickerState)
            }
        }
    )
}

// AIアドバイス表示ボトムシート
if (uiState.showRouteAdviceDialog) {
    val sheetState = rememberModalBottomSheetState(skipPartiallyExpanded = true)
    val sheetBackgroundColor = MaterialTheme.colorScheme.surface.copy(alpha = 0.98f)
    ModalBottomSheet(
        onDismissRequest = { viewModel.dismissRouteAdviceDialog() },
        sheetState = sheetState,
        dragHandle = { BottomSheetDefaults.DragHandle() },
        shape = RoundedCornerShape(topStart = 28.dp, topEnd = 28.dp),
        containerColor = sheetBackgroundColor
    ) {
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .fillMaxHeight(0.7f)
                .padding(horizontal = 24.dp)
                .padding(bottom = 32.dp)
        ) {
            val isMountain = uiState.transportMode == "hiking" || uiState.transportMode == "mountain"
            Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.padding(bottom = 16.dp)) {
                Icon(
                    if (isMountain) Icons.Default.Terrain else Icons.Default.AutoAwesome,
                    contentDescription = null,
                    tint = MaterialTheme.colorScheme.primary,
                    modifier = Modifier.padding(end = 8.dp)
                )
                Text(if (isMountain) "山岳気象ガイド⚠️" else "ルート天気解説", style = MaterialTheme.typography.titleLarge)
            }

            if (uiState.isRouteAdviceLoading) {
                Box(
                    modifier = Modifier
                        .fillMaxWidth()
                        .height(200.dp),
                    contentAlignment = Alignment.Center
                ) {
                    Column(horizontalAlignment = Alignment.CenterHorizontally) {
                        var dotCount by remember { mutableIntStateOf(0) }
                        LaunchedEffect(Unit) {
                            while (true) {
                                delay(500)
                                dotCount = (dotCount + 1) % 4
                            }
                        }
                        val dots = ".".repeat(dotCount)
                        Text(
                            text = "AIが分析中$dots",
                            style = MaterialTheme.typography.bodyLarge
                        )
                    }
                }
            } else if (uiState.routeAdviceText != null) {
                Box(modifier = Modifier.weight(1f, fill = false)) {
                    val scrollState = rememberScrollState()
                    
                    Box(modifier = Modifier.fillMaxSize()) {
                        CompositionLocalProvider(
                            LocalTextStyle provides LocalTextStyle.current.copy(lineHeight = 28.sp)
                        ) {
                            com.example.weather_location_app.ui.components.SmoothStreamingMarkdown(
                                streamedText = uiState.routeAdviceText ?: "",
                                isCompleted = uiState.isRouteAdviceCompleted,
                                backgroundColor = sheetBackgroundColor,
                                modifier = Modifier
                                    .verticalScroll(scrollState)
                                    .padding(bottom = 24.dp)
                                    .padding(horizontal = 4.dp)
                            )
                        }

                        // 上部のフェードアウト
                        androidx.compose.animation.AnimatedVisibility(
                            visible = scrollState.value > 0,
                            enter = androidx.compose.animation.fadeIn(),
                            exit = androidx.compose.animation.fadeOut(),
                            modifier = Modifier.align(Alignment.TopCenter)
                        ) {
                            Box(
                                modifier = Modifier
                                    .fillMaxWidth()
                                    .height(32.dp)
                                    .background(
                                        androidx.compose.ui.graphics.Brush.verticalGradient(
                                            colors = listOf(
                                                sheetBackgroundColor,
                                                Color.Transparent
                                            )
                                        )
                                    )
                            )
                        }

                        // 下部のフェードアウト
                        androidx.compose.animation.AnimatedVisibility(
                            visible = scrollState.value < scrollState.maxValue,
                            enter = androidx.compose.animation.fadeIn(),
                            exit = androidx.compose.animation.fadeOut(),
                            modifier = Modifier.align(Alignment.BottomCenter)
                        ) {
                            Box(
                                modifier = Modifier
                                    .fillMaxWidth()
                                    .height(32.dp)
                                    .background(
                                        androidx.compose.ui.graphics.Brush.verticalGradient(
                                            colors = listOf(
                                                Color.Transparent,
                                                sheetBackgroundColor
                                            )
                                        )
                                    )
                            )
                        }

                        // 上スクロール矢印
                        androidx.compose.animation.AnimatedVisibility(
                            visible = scrollState.value > 0,
                            enter = androidx.compose.animation.fadeIn(),
                            exit = androidx.compose.animation.fadeOut(),
                            modifier = Modifier.align(Alignment.TopCenter).padding(top = 4.dp)
                        ) {
                            Surface(
                                shape = CircleShape,
                                color = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.6f),
                                modifier = Modifier.size(40.dp)
                            ) {
                                Icon(
                                    imageVector = Icons.Default.KeyboardArrowUp,
                                    contentDescription = "上へスクロール",
                                    modifier = Modifier.padding(4.dp).size(32.dp),
                                    tint = MaterialTheme.colorScheme.onSurfaceVariant.copy(alpha = 0.8f)
                                )
                            }
                        }

                        // 下スクロール矢印
                        androidx.compose.animation.AnimatedVisibility(
                            visible = scrollState.value < scrollState.maxValue,
                            enter = androidx.compose.animation.fadeIn(),
                            exit = androidx.compose.animation.fadeOut(),
                            modifier = Modifier.align(Alignment.BottomCenter).padding(bottom = 4.dp)
                        ) {
                            Surface(
                                shape = CircleShape,
                                color = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.6f),
                                modifier = Modifier.size(40.dp)
                            ) {
                                Icon(
                                    imageVector = Icons.Default.KeyboardArrowDown,
                                    contentDescription = "下へスクロール",
                                    modifier = Modifier.padding(4.dp).size(32.dp),
                                    tint = MaterialTheme.colorScheme.onSurfaceVariant.copy(alpha = 0.8f)
                                )
                            }
                        }
                    }
                }
                
                // 閉じるボタン
                Box(modifier = Modifier.fillMaxWidth(), contentAlignment = Alignment.CenterEnd) {
                    TextButton(onClick = { viewModel.dismissRouteAdviceDialog() }) {
                        Text("閉じる")
                    }
                }
            }
        }
    }
}
}
