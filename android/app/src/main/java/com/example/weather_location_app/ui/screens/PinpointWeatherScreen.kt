package com.example.weather_location_app.ui.screens

import android.Manifest
import android.annotation.SuppressLint
import android.content.Context
import android.content.pm.PackageManager
import android.location.Geocoder
import android.widget.Toast
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.animation.animateContentSize
import androidx.compose.animation.core.animateDpAsState
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.BasicTextField
import androidx.compose.foundation.text.KeyboardActions
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.alpha
import androidx.compose.ui.draw.clip
import androidx.compose.ui.focus.FocusManager
import androidx.compose.ui.focus.FocusRequester
import androidx.compose.ui.focus.focusRequester
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalConfiguration
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalFocusManager
import androidx.compose.ui.platform.LocalSoftwareKeyboardController
import androidx.compose.ui.platform.SoftwareKeyboardController
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.core.content.ContextCompat
import androidx.lifecycle.viewmodel.compose.viewModel
import com.example.weather_location_app.BuildConfig
import com.example.weather_location_app.WeatherConfig
import com.example.weather_location_app.data.api.GpvTileProvider
import com.example.weather_location_app.data.api.RetrofitClient
import com.example.weather_location_app.ui.components.WeatherChart
import com.example.weather_location_app.ui.components.shimmerEffect
import kotlinx.coroutines.delay
import com.example.weather_location_app.ui.viewmodels.WeatherViewModel
import com.google.android.gms.location.FusedLocationProviderClient
import com.google.android.gms.location.LocationServices
import com.google.android.gms.maps.CameraUpdateFactory
import com.google.android.gms.maps.model.CameraPosition
import com.google.android.gms.maps.model.LatLng
import com.google.maps.android.compose.*
import com.mikepenz.markdown.m3.Markdown
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

/**
 * デザイン定数
 */
private object PinpointScreenDefaults {
    val MapCollapsedHeightRatio = 0.35f
    val MapExpandedOffset = 100.dp
    val MapCornerRadius = 28.dp
    val MapPadding = 12.dp
    val IconSizeSmall = 24.dp
    val IconSizeMedium = 32.dp
    val ButtonHeight = 64.dp
    val CardCornerRadius = 24.dp
    val ChartHeight = 320.dp
    val StandardPadding = 16.dp
    val LargePadding = 20.dp
    val SpacingMedium = 12.dp
    val SpacingLarge = 20.dp
    val FabSize = 48.dp
    val SearchBarHeight = 48.dp
}

/**
 * ピンポイント天気画面のメインコンポーネント
 */
@SuppressLint("MissingPermission")
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun PinpointWeatherScreen(
    viewModel: WeatherViewModel = viewModel()
) {
    val uiState by viewModel.uiState
    val scope = rememberCoroutineScope()
    val context = LocalContext.current
    val configuration = LocalConfiguration.current
    val screenHeight = configuration.screenHeightDp.dp
    
    var expanded by remember { mutableStateOf(false) }
    var isMapExpanded by remember { mutableStateOf(false) }
    var mapType by remember { mutableStateOf(MapType.NORMAL) }
    var showMapTypeMenu by remember { mutableStateOf(false) }
    
    // 検索バーの状態管理
    var isSearchBarVisible by remember { mutableStateOf(false) }
    var searchQuery by remember { mutableStateOf("") }
    
    // タイル表示用に「少し遅延（Debounce）させて更新される」内部State
    var targetTimeForTile by remember { mutableStateOf<String?>(null) }
    
    // デバッグログ：Stateの変更を監視
    LaunchedEffect(uiState.selectedPointIndex, uiState.selectedElement.key) {
        android.util.Log.d("PinpointScreen", "State changed: index=${uiState.selectedPointIndex}, element=${uiState.selectedElement.key}")
    }

    // パフォーマンス対策：選択時刻が頻繁に変わっても、300ms静止した時だけタイルプロバイダーを更新
    LaunchedEffect(Unit) {
        snapshotFlow { uiState.selectedTargetTime }
            .debounce(300L)
            .collectLatest { time ->
                targetTimeForTile = time
            }
    }
    
    val focusRequester = remember { FocusRequester() }
    val keyboardController = LocalSoftwareKeyboardController.current
    val focusManager = LocalFocusManager.current
    
    val mapHeight by animateDpAsState(
        targetValue = if (isMapExpanded) {
            screenHeight - PinpointScreenDefaults.MapExpandedOffset
        } else {
            screenHeight * PinpointScreenDefaults.MapCollapsedHeightRatio
        },
        label = "MapHeightAnimation"
    )
    
    val initialPos = LatLng(35.6895, 139.6917)
    val cameraPositionState = rememberCameraPositionState {
        position = CameraPosition.fromLatLngZoom(initialPos, 8f)
    }

    val fetchedMarkerState = rememberMarkerState()
    LaunchedEffect(uiState.lastFetchedLatLng) {
        uiState.lastFetchedLatLng?.let { fetchedMarkerState.position = it }
    }

    val fusedLocationClient = remember { LocationServices.getFusedLocationProviderClient(context) }
    val launcher = rememberLauncherForActivityResult(
        ActivityResultContracts.RequestMultiplePermissions()
    ) { permissions ->
        val granted = permissions.values.all { it }
        if (granted) {
            fusedLocationClient.lastLocation.addOnSuccessListener { location ->
                val target = if (location != null) LatLng(location.latitude, location.longitude) else initialPos
                scope.launch {
                    cameraPositionState.move(CameraUpdateFactory.newLatLngZoom(target, 8f))
                }
            }
        }
    }

    // 初回起動時の処理（位置情報権限チェックと初期位置への移動）
    LaunchedEffect(Unit) {
        val hasFine = ContextCompat.checkSelfPermission(context, Manifest.permission.ACCESS_FINE_LOCATION) == PackageManager.PERMISSION_GRANTED
        val hasCoarse = ContextCompat.checkSelfPermission(context, Manifest.permission.ACCESS_COARSE_LOCATION) == PackageManager.PERMISSION_GRANTED
        
        if (hasFine && hasCoarse) {
            fusedLocationClient.lastLocation.addOnSuccessListener { location ->
                val target = if (location != null) LatLng(location.latitude, location.longitude) else initialPos
                scope.launch {
                    cameraPositionState.move(CameraUpdateFactory.newLatLngZoom(target, 12f))
                }
            }
        } else {
            launcher.launch(arrayOf(Manifest.permission.ACCESS_FINE_LOCATION, Manifest.permission.ACCESS_COARSE_LOCATION))
        }
    }

    Column(modifier = Modifier.fillMaxSize().background(MaterialTheme.colorScheme.background)) {
        
        // 1. 地図エリア
        MapArea(
            mapHeight = mapHeight,
            isMapExpanded = isMapExpanded,
            cameraPositionState = cameraPositionState,
            mapType = mapType,
            uiState = uiState,
            fetchedMarkerState = fetchedMarkerState,
            targetTimeForTile = targetTimeForTile,
            isSearchBarVisible = isSearchBarVisible,
            searchQuery = searchQuery,
            onSearchQueryChange = { searchQuery = it },
            onSearchBarToggle = { isSearchBarVisible = it },
            onSearchExecute = {
                executeSearch(
                    query = searchQuery,
                    scope = scope,
                    context = context,
                    cameraPositionState = cameraPositionState,
                    keyboardController = keyboardController,
                    focusManager = focusManager,
                    onSuccess = { isSearchBarVisible = false }
                )
            },
            showMapTypeMenu = showMapTypeMenu,
            onMapTypeMenuToggle = { showMapTypeMenu = it },
            onMapTypeSelect = { mapType = it },
            onCurrentLocationClick = {
                getCurrentLocation(context, fusedLocationClient, launcher, scope, cameraPositionState)
            },
            focusRequester = focusRequester,
            keyboardController = keyboardController,
            focusManager = focusManager
        )

        // 地図の伸縮ハンドル
        MapResizeHandle(onClick = { isMapExpanded = !isMapExpanded })

        // 2. スクロール可能なコンテンツエリア
        LazyColumn(
            modifier = Modifier.fillMaxSize(),
            contentPadding = PaddingValues(
                horizontal = PinpointScreenDefaults.LargePadding,
                vertical = 8.dp
            ),
            verticalArrangement = Arrangement.spacedBy(PinpointScreenDefaults.SpacingLarge)
        ) {
            // 取得地点の住所表示
            uiState.fetchedAddress?.let { address ->
                item {
                    AddressInfoCard(address = address)
                }
            }

            // データ取得ボタン
            item {
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    verticalAlignment = Alignment.CenterVertically,
                    horizontalArrangement = Arrangement.spacedBy(PinpointScreenDefaults.SpacingMedium)
                ) {
                    FetchWeatherButton(
                        modifier = Modifier.weight(1f),
                        isLoading = uiState.isLoading,
                        isAdviceLoading = uiState.isPointAdviceLoading,
                        onClick = { viewModel.fetchWeatherData(context, cameraPositionState.position.target) }
                    )
                    
                    // AIコンシェルジュボタン
                    AdviceButton(
                        isLoading = uiState.isPointAdviceLoading,
                        enabled = uiState.weatherData != null && uiState.instabilityData != null && !uiState.isLoading,
                        onClick = { viewModel.onAiAdviceButtonClicked() }
                    )
                }
            }

            // 気象要素選択UI
            item {
                WeatherElementSelector(
                    selectedElement = uiState.selectedElement,
                    elementAlerts = uiState.elementAlerts,
                    expanded = expanded,
                    onExpandedChange = { expanded = it },
                    onElementSelect = { 
                        viewModel.updateSelectedElement(it)
                        expanded = false
                    }
                )
            }

            // チャート表示
            item {
                WeatherChartCard(
                    activeData = viewModel.getActiveData(),
                    activeSunRegions = viewModel.getActiveSunRegions(),
                    selectedElement = uiState.selectedElement,
                    selectedPointIndex = uiState.selectedPointIndex,
                    error = uiState.error,
                    onPointSelected = { index ->
                        viewModel.updateSelectedPointIndex(index, index != null)
                    }
                )
            }
            
            item { Spacer(modifier = Modifier.height(32.dp)) }
        }

        // AIアドバイスボトムシート
        if (uiState.showPointAdviceDialog) {
            val sheetState = rememberModalBottomSheetState(skipPartiallyExpanded = true)
            val sheetBackgroundColor = MaterialTheme.colorScheme.surface.copy(alpha = 0.98f)
            ModalBottomSheet(
                onDismissRequest = { viewModel.dismissPointAdviceDialog() },
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
                    Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.padding(bottom = 16.dp)) {
                        Icon(Icons.Default.AutoAwesome, contentDescription = null, tint = MaterialTheme.colorScheme.primary)
                        Spacer(Modifier.width(8.dp))
                        Text("AI天気解説", style = MaterialTheme.typography.titleLarge)
                    }

                    if (uiState.isPointAdviceLoading) {
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
                    } else if (uiState.pointAdviceText != null) {
                        Box(modifier = Modifier.weight(1f, fill = false)) {
                            val scrollState = rememberScrollState()
                            
                            Box(modifier = Modifier.fillMaxSize()) {
                                // MarkdownのテキストスタイルのlineHeightを広げる対応は、Markdownコンポーネントの機能またはModifier等で対応
                                // ただしm3.Markdownはデフォルトのタイポグラフィを使うので、CompositionLocalProviderで上書きするのもあり
                                CompositionLocalProvider(
                                    LocalTextStyle provides LocalTextStyle.current.copy(lineHeight = 28.sp)
                                ) {
                                    com.example.weather_location_app.ui.components.SmoothStreamingMarkdown(
                                        streamedText = uiState.pointAdviceText!!,
                                        isCompleted = uiState.isPointAdviceCompleted,
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
                            TextButton(onClick = { viewModel.dismissPointAdviceDialog() }) {
                                Text("閉じる")
                            }
                        }
                    }
                }
            }
        }
    }
}

/**
 * 地図エリアを表示するコンポーネント
 */
@Composable
private fun MapArea(
    mapHeight: Dp,
    isMapExpanded: Boolean,
    cameraPositionState: CameraPositionState,
    mapType: MapType,
    uiState: com.example.weather_location_app.ui.viewmodels.WeatherUiState,
    fetchedMarkerState: MarkerState,
    targetTimeForTile: String?,
    isSearchBarVisible: Boolean,
    searchQuery: String,
    onSearchQueryChange: (String) -> Unit,
    onSearchBarToggle: (Boolean) -> Unit,
    onSearchExecute: () -> Unit,
    showMapTypeMenu: Boolean,
    onMapTypeMenuToggle: (Boolean) -> Unit,
    onMapTypeSelect: (MapType) -> Unit,
    onCurrentLocationClick: () -> Unit,
    focusRequester: FocusRequester,
    keyboardController: SoftwareKeyboardController?,
    focusManager: FocusManager
) {
    val context = LocalContext.current

    Card(
        modifier = Modifier
            .fillMaxWidth()
            .height(mapHeight)
            .padding(if (isMapExpanded) 0.dp else PinpointScreenDefaults.MapPadding),
        shape = if (isMapExpanded) RoundedCornerShape(0.dp) else RoundedCornerShape(PinpointScreenDefaults.MapCornerRadius),
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
                onMapClick = {
                    if (isSearchBarVisible) {
                        onSearchBarToggle(false)
                        keyboardController?.hide()
                        focusManager.clearFocus()
                    }
                },
                contentPadding = PaddingValues(PinpointScreenDefaults.StandardPadding)
            ) {
                // データ取得地点にピンを表示
                uiState.lastFetchedLatLng?.let {
                    Marker(
                        state = fetchedMarkerState,
                        title = "データ取得地点",
                        snippet = uiState.fetchedAddress ?: ""
                    )
                }

                // 気象タイルのオーバーレイ表示
                WeatherTileOverlay(uiState, targetTimeForTile)
            }
            
            // 十字マーク
            Icon(
                imageVector = Icons.Default.Add,
                contentDescription = null,
                modifier = Modifier
                    .align(Alignment.Center)
                    .size(PinpointScreenDefaults.IconSizeMedium),
                tint = Color.Black.copy(alpha = 0.5f)
            )

            // 検索バー
            MapSearchBar(
                isVisible = isSearchBarVisible,
                query = searchQuery,
                onQueryChange = onSearchQueryChange,
                onToggle = onSearchBarToggle,
                onExecute = onSearchExecute,
                focusRequester = focusRequester,
                keyboardController = keyboardController,
                focusManager = focusManager
            )

            // 右上の操作ボタン群
            MapActionButtons(
                modifier = Modifier.align(Alignment.TopEnd),
                showMapTypeMenu = showMapTypeMenu,
                onMapTypeMenuToggle = onMapTypeMenuToggle,
                mapType = mapType,
                onMapTypeSelect = onMapTypeSelect,
                onCurrentLocationClick = onCurrentLocationClick
            )

            // 右下の操作ボタンと時刻表示
            MapBottomControls(
                modifier = Modifier.align(Alignment.BottomEnd),
                selectedTargetTime = uiState.selectedTargetTime,
                cameraPositionState = cameraPositionState
            )
        }
    }
}

/**
 * 気象タイルのオーバーレイ表示
 */
@Composable
private fun WeatherTileOverlay(
    uiState: com.example.weather_location_app.ui.viewmodels.WeatherUiState,
    targetTimeForTile: String?
) {
    if (targetTimeForTile != null && uiState.initialTime != null) {
        val selectedElement = uiState.selectedElement
        val isSurface = !selectedElement.isPressureLevel
        val surfaceValue = selectedElement.tileSurface

        val tileProvider = remember(targetTimeForTile, selectedElement) {
            GpvTileProvider(
                client = RetrofitClient.okHttpClient,
                apiKey = BuildConfig.TILE_API_KEY,
                isSurface = isSurface,
                initialTime = uiState.initialTime!!,
                targetTime = targetTimeForTile!!,
                element = selectedElement.key,
                surface = surfaceValue
            )
        }
        TileOverlay(
            tileProvider = tileProvider,
            fadeIn = false,
            transparency = 0.3f
        )
    }
}

/**
 * 地図上の検索バー
 */
@Composable
private fun MapSearchBar(
    isVisible: Boolean,
    query: String,
    onQueryChange: (String) -> Unit,
    onToggle: (Boolean) -> Unit,
    onExecute: () -> Unit,
    focusRequester: FocusRequester,
    keyboardController: SoftwareKeyboardController?,
    focusManager: FocusManager
) {
    Surface(
        modifier = Modifier
            .padding(top = PinpointScreenDefaults.StandardPadding, start = 72.dp, end = 72.dp)
            .height(PinpointScreenDefaults.SearchBarHeight)
            .then(
                if (isVisible) Modifier.fillMaxWidth()
                else Modifier.width(PinpointScreenDefaults.SearchBarHeight)
            )
            .animateContentSize(),
        shape = CircleShape,
        color = MaterialTheme.colorScheme.surfaceVariant,
        tonalElevation = 4.dp
    ) {
        if (!isVisible) {
            Box(
                modifier = Modifier
                    .fillMaxSize()
                    .clickable { onToggle(true) },
                contentAlignment = Alignment.Center
            ) {
                Icon(
                    imageVector = Icons.Default.Search,
                    contentDescription = "検索を開く",
                    tint = MaterialTheme.colorScheme.primary
                )
            }
        } else {
            Row(
                modifier = Modifier
                    .fillMaxSize()
                    .padding(horizontal = 8.dp),
                verticalAlignment = Alignment.CenterVertically
            ) {
                IconButton(onClick = onExecute) {
                    Icon(
                        imageVector = Icons.Default.Search,
                        contentDescription = "検索実行",
                        tint = MaterialTheme.colorScheme.primary
                    )
                }
                Box(
                    modifier = Modifier.weight(1f),
                    contentAlignment = Alignment.CenterStart
                ) {
                    if (query.isEmpty()) {
                        Text(
                            text = "場所を検索",
                            style = MaterialTheme.typography.bodyLarge,
                            color = MaterialTheme.colorScheme.onSurfaceVariant.copy(alpha = 0.6f)
                        )
                    }
                    BasicTextField(
                        value = query,
                        onValueChange = onQueryChange,
                        modifier = Modifier
                            .fillMaxWidth()
                            .focusRequester(focusRequester),
                        singleLine = true,
                        textStyle = MaterialTheme.typography.bodyLarge.copy(
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        ),
                        keyboardOptions = KeyboardOptions(imeAction = ImeAction.Search),
                        keyboardActions = KeyboardActions(
                            onSearch = { onExecute() }
                        )
                    )
                }
                IconButton(onClick = {
                    if (query.isNotEmpty()) {
                        onQueryChange("")
                    } else {
                        onToggle(false)
                        keyboardController?.hide()
                        focusManager.clearFocus()
                    }
                }) {
                    Icon(
                        imageVector = Icons.Default.Close,
                        contentDescription = "閉じる",
                        tint = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
            }
        }
    }
}

/**
 * 地図右上の操作ボタン（レイヤー、現在地）
 */
@Composable
private fun MapActionButtons(
    modifier: Modifier = Modifier,
    showMapTypeMenu: Boolean,
    onMapTypeMenuToggle: (Boolean) -> Unit,
    mapType: MapType,
    onMapTypeSelect: (MapType) -> Unit,
    onCurrentLocationClick: () -> Unit
) {
    Column(
        modifier = modifier.padding(PinpointScreenDefaults.StandardPadding),
        verticalArrangement = Arrangement.spacedBy(PinpointScreenDefaults.SpacingMedium)
    ) {
        Box {
            FloatingActionButton(
                onClick = { onMapTypeMenuToggle(true) },
                modifier = Modifier.size(PinpointScreenDefaults.FabSize),
                containerColor = MaterialTheme.colorScheme.surface.copy(alpha = 0.9f),
                contentColor = MaterialTheme.colorScheme.primary,
                shape = CircleShape,
                elevation = FloatingActionButtonDefaults.elevation(0.dp, 0.dp, 0.dp, 0.dp)
            ) {
                Icon(
                    Icons.Default.Layers, 
                    contentDescription = "マップタイプ切り替え",
                    modifier = Modifier.size(PinpointScreenDefaults.IconSizeSmall)
                )
            }
            DropdownMenu(
                expanded = showMapTypeMenu,
                onDismissRequest = { onMapTypeMenuToggle(false) }
            ) {
                MapTypeMenuItem("標準", mapType == MapType.NORMAL) {
                    onMapTypeSelect(MapType.NORMAL)
                    onMapTypeMenuToggle(false)
                }
                MapTypeMenuItem("地形図", mapType == MapType.TERRAIN) {
                    onMapTypeSelect(MapType.TERRAIN)
                    onMapTypeMenuToggle(false)
                }
                MapTypeMenuItem("航空写真", mapType == MapType.SATELLITE) {
                    onMapTypeSelect(MapType.SATELLITE)
                    onMapTypeMenuToggle(false)
                }
            }
        }

        FloatingActionButton(
            onClick = onCurrentLocationClick,
            modifier = Modifier.size(PinpointScreenDefaults.FabSize),
            containerColor = MaterialTheme.colorScheme.surface.copy(alpha = 0.9f),
            contentColor = MaterialTheme.colorScheme.primary,
            shape = CircleShape,
            elevation = FloatingActionButtonDefaults.elevation(0.dp, 0.dp, 0.dp, 0.dp)
        ) {
            Icon(
                Icons.Default.MyLocation, 
                contentDescription = "現在地",
                modifier = Modifier.size(PinpointScreenDefaults.IconSizeSmall)
            )
        }
    }
}

@Composable
private fun MapTypeMenuItem(
    text: String,
    isSelected: Boolean,
    onClick: () -> Unit
) {
    DropdownMenuItem(
        text = { Text(text) },
        onClick = onClick,
        leadingIcon = { RadioButton(selected = isSelected, onClick = null) }
    )
}

/**
 * 地図右下の操作ボタンと時刻表示
 */
@Composable
private fun MapBottomControls(
    modifier: Modifier = Modifier,
    selectedTargetTime: String?,
    cameraPositionState: CameraPositionState
) {
    val scope = rememberCoroutineScope()

    Column(
        modifier = modifier.padding(bottom = 24.dp, end = PinpointScreenDefaults.StandardPadding),
        horizontalAlignment = Alignment.End,
        verticalArrangement = Arrangement.spacedBy(PinpointScreenDefaults.SpacingMedium)
    ) {
        // 時刻表示 (MM/DD HH:mm)
        selectedTargetTime?.let { timeStr ->
            FormattedTimeLabel(timeStr)
        }

        Row(horizontalArrangement = Arrangement.spacedBy(PinpointScreenDefaults.SpacingMedium)) {
            MapZoomButton(
                icon = Icons.Default.Remove,
                contentDescription = "ズームアウト",
                onClick = { scope.launch { cameraPositionState.animate(CameraUpdateFactory.zoomOut()) } }
            )
            MapZoomButton(
                icon = Icons.Default.Add,
                contentDescription = "ズームイン",
                onClick = { scope.launch { cameraPositionState.animate(CameraUpdateFactory.zoomIn()) } }
            )
        }
    }
}

@Composable
private fun FormattedTimeLabel(timeStr: String) {
    val dt = try {
        OffsetDateTime.parse(timeStr.replace(" ", "T").let { 
            if (!it.contains("+") && !it.endsWith("Z")) it + "+09:00" else it 
        }).withOffsetSameInstant(java.time.ZoneOffset.ofHours(9))
    } catch (e: Exception) {
        null
    }
    
    dt?.let {
        val formatter = DateTimeFormatter.ofPattern("MM/dd HH:mm")
        Surface(
            color = MaterialTheme.colorScheme.surface.copy(alpha = 0.7f),
            shape = RoundedCornerShape(12.dp),
            modifier = Modifier.padding(bottom = 4.dp)
        ) {
            Text(
                text = it.format(formatter),
                style = MaterialTheme.typography.labelMedium,
                color = MaterialTheme.colorScheme.onSurface,
                modifier = Modifier.padding(horizontal = 8.dp, vertical = 4.dp)
            )
        }
    }
}

@Composable
private fun MapZoomButton(
    icon: androidx.compose.ui.graphics.vector.ImageVector,
    contentDescription: String,
    onClick: () -> Unit
) {
    FloatingActionButton(
        onClick = onClick,
        modifier = Modifier.size(PinpointScreenDefaults.FabSize),
        containerColor = MaterialTheme.colorScheme.surface.copy(alpha = 0.9f),
        contentColor = MaterialTheme.colorScheme.primary,
        shape = CircleShape,
        elevation = FloatingActionButtonDefaults.elevation(0.dp, 0.dp, 0.dp, 0.dp)
    ) {
        Icon(
            icon, 
            contentDescription = contentDescription,
            modifier = Modifier.size(PinpointScreenDefaults.IconSizeSmall)
        )
    }
}

/**
 * 地図のサイズを変更するためのハンドル
 */
@Composable
private fun MapResizeHandle(onClick: () -> Unit) {
    Box(
        modifier = Modifier
            .fillMaxWidth()
            .height(32.dp)
            .clickable { onClick() },
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
}

/**
 * 住所情報を表示するカード
 */
@Composable
private fun AddressInfoCard(address: String) {
    Card(
        modifier = Modifier.fillMaxWidth(),
        shape = RoundedCornerShape(PinpointScreenDefaults.CardCornerRadius),
        colors = CardDefaults.cardColors(
            containerColor = MaterialTheme.colorScheme.secondaryContainer.copy(alpha = 0.7f)
        )
    ) {
        Row(
            modifier = Modifier.padding(PinpointScreenDefaults.StandardPadding),
            verticalAlignment = Alignment.CenterVertically
        ) {
            Icon(
                imageVector = Icons.Default.Place,
                contentDescription = null,
                tint = MaterialTheme.colorScheme.secondary
            )
            Spacer(Modifier.width(PinpointScreenDefaults.SpacingMedium))
            Text(
                text = address,
                style = MaterialTheme.typography.bodyLarge,
                color = MaterialTheme.colorScheme.onSecondaryContainer
            )
        }
    }
}

/**
 * 天気データ取得ボタン
 */
@Composable
private fun FetchWeatherButton(
    modifier: Modifier = Modifier,
    isLoading: Boolean,
    isAdviceLoading: Boolean = false,
    onClick: () -> Unit
) {
    Button(
        onClick = onClick,
        modifier = modifier.height(PinpointScreenDefaults.ButtonHeight).then(if (isLoading) Modifier.clip(RoundedCornerShape(28.dp)).shimmerEffect() else Modifier),
        enabled = !isLoading && !isAdviceLoading,
        shape = RoundedCornerShape(28.dp),
        elevation = ButtonDefaults.buttonElevation(0.dp, 0.dp, 0.dp, 0.dp)
    ) {
        if (isLoading) {
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
            Spacer(Modifier.width(PinpointScreenDefaults.SpacingMedium))
            Text("天気を取得", style = MaterialTheme.typography.titleMedium)
        }
    }
}

/**
 * AIアドバイス取得ボタン
 */
@Composable
private fun AdviceButton(
    isLoading: Boolean,
    enabled: Boolean,
    onClick: () -> Unit
) {
    FilledTonalIconButton(
        onClick = onClick,
        modifier = Modifier.size(PinpointScreenDefaults.ButtonHeight),
        enabled = enabled,
        shape = CircleShape,
        colors = IconButtonDefaults.filledTonalIconButtonColors(
            containerColor = MaterialTheme.colorScheme.primaryContainer,
            contentColor = MaterialTheme.colorScheme.onPrimaryContainer,
            disabledContainerColor = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.38f),
            disabledContentColor = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.38f)
        )
    ) {
        if (isLoading) {
            CircularProgressIndicator(
                modifier = Modifier.size(24.dp),
                color = MaterialTheme.colorScheme.primary,
                strokeWidth = 2.5.dp
            )
        } else {
            Icon(
                imageVector = Icons.Default.AutoAwesome,
                contentDescription = "AIアドバイス",
                modifier = Modifier.size(PinpointScreenDefaults.IconSizeMedium)
            )
        }
    }
}

/**
 * 気象要素を選択するドロップダウン
 */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun WeatherElementSelector(
    selectedElement: WeatherConfig.WeatherElement,
    elementAlerts: Map<String, Float>,
    expanded: Boolean,
    onExpandedChange: (Boolean) -> Unit,
    onElementSelect: (WeatherConfig.WeatherElement) -> Unit
) {
    ExposedDropdownMenuBox(
        expanded = expanded,
        onExpandedChange = onExpandedChange,
        modifier = Modifier.fillMaxWidth()
    ) {
        OutlinedTextField(
            value = selectedElement.name,
            onValueChange = {},
            readOnly = true,
            label = { Text("気象要素") },
            trailingIcon = { ExposedDropdownMenuDefaults.TrailingIcon(expanded = expanded) },
            modifier = Modifier.menuAnchor().fillMaxWidth(),
            textStyle = MaterialTheme.typography.bodyLarge,
            shape = RoundedCornerShape(PinpointScreenDefaults.CardCornerRadius),
            colors = OutlinedTextFieldDefaults.colors(
                unfocusedContainerColor = MaterialTheme.colorScheme.surface,
                focusedContainerColor = MaterialTheme.colorScheme.surface,
                unfocusedBorderColor = MaterialTheme.colorScheme.outlineVariant,
                focusedBorderColor = MaterialTheme.colorScheme.primary
            )
        )
        ExposedDropdownMenu(
            expanded = expanded,
            onDismissRequest = { onExpandedChange(false) },
            modifier = Modifier.background(MaterialTheme.colorScheme.surface)
        ) {
            val weatherDataElements = WeatherConfig.WEATHER_ELEMENTS.filter { 
                !WeatherConfig.INSTABILITY_ELEMENTS.contains(it.key) && 
                !WeatherConfig.INDEX_ELEMENTS.contains(it.key) 
            }
            val indexElements = WeatherConfig.WEATHER_ELEMENTS.filter { 
                WeatherConfig.INDEX_ELEMENTS.contains(it.key) 
            }
            val instabilityElements = WeatherConfig.WEATHER_ELEMENTS.filter { 
                WeatherConfig.INSTABILITY_ELEMENTS.contains(it.key) 
            }

            if (weatherDataElements.isNotEmpty()) {
                DropdownSectionHeader("気象データ")
                weatherDataElements.forEach { element ->
                    WeatherElementItem(
                        element = element,
                        alertValue = elementAlerts[element.key],
                        isSelected = selectedElement.key == element.key,
                        onClick = { onElementSelect(element) }
                    )
                }
            }

            if (indexElements.isNotEmpty()) {
                HorizontalDivider(modifier = Modifier.padding(vertical = 4.dp))
                DropdownSectionHeader("生活指数")
                indexElements.forEach { element ->
                    WeatherElementItem(
                        element = element,
                        alertValue = elementAlerts[element.key],
                        isSelected = selectedElement.key == element.key,
                        onClick = { onElementSelect(element) }
                    )
                }
            }

            if (instabilityElements.isNotEmpty()) {
                HorizontalDivider(modifier = Modifier.padding(vertical = 4.dp))
                DropdownSectionHeader("大気不安定")
                instabilityElements.forEach { element ->
                    WeatherElementItem(
                        element = element,
                        alertValue = elementAlerts[element.key],
                        isSelected = selectedElement.key == element.key,
                        onClick = { onElementSelect(element) }
                    )
                }
            }
        }
    }
}

@Composable
private fun DropdownSectionHeader(text: String) {
    DropdownMenuItem(
        text = { Text(text, style = MaterialTheme.typography.labelLarge, color = MaterialTheme.colorScheme.primary) },
        onClick = { },
        enabled = false
    )
}

@Composable
private fun WeatherElementItem(
    element: WeatherConfig.WeatherElement,
    alertValue: Float?,
    isSelected: Boolean,
    onClick: () -> Unit
) {
    DropdownMenuItem(
        text = { 
            Row(verticalAlignment = Alignment.CenterVertically) {
                if (isSelected) {
                    Box(
                        modifier = Modifier
                            .width(4.dp)
                            .height(24.dp)
                            .clip(RoundedCornerShape(2.dp))
                            .background(MaterialTheme.colorScheme.primary)
                    )
                    Spacer(Modifier.width(8.dp))
                }
                Text(element.name, modifier = Modifier.weight(1f))
                if (alertValue != null) {
                    val iconStr = element.getAlertIcon(alertValue)
                    if (iconStr == "⚠️") {
                        Icon(
                            imageVector = Icons.Default.Warning,
                            contentDescription = "アラート",
                            tint = Color(element.getLevelColor(alertValue)),
                            modifier = Modifier.size(18.dp)
                        )
                    } else if (iconStr != null) {
                        Text(iconStr, modifier = Modifier.size(18.dp))
                    }
                }
            }
        },
        onClick = onClick,
        modifier = (if (alertValue != null) {
            Modifier.background(Color(element.getLevelColor(alertValue)).copy(alpha = 0.15f))
        } else Modifier).then(
            if (!isSelected) Modifier.alpha(0.5f) else Modifier
        )
    )
}

/**
 * 気象チャートを表示するカード
 */
@Composable
private fun WeatherChartCard(
    activeData: List<com.example.weather_location_app.data.api.GpvDataItem>?,
    activeSunRegions: List<com.example.weather_location_app.data.SunTimeRegion>,
    selectedElement: WeatherConfig.WeatherElement,
    selectedPointIndex: Int?,
    error: String?,
    onPointSelected: (Int?) -> Unit
) {
    Card(
        modifier = Modifier
            .fillMaxWidth()
            .height(PinpointScreenDefaults.ChartHeight),
        shape = RoundedCornerShape(28.dp),
        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surface)
    ) {
        Box(
            modifier = Modifier.fillMaxSize().padding(12.dp),
            contentAlignment = Alignment.Center
        ) {
            if (activeData != null) {
                WeatherChart(
                    data = activeData,
                    sunTimeRegions = activeSunRegions,
                    element = selectedElement,
                    selectedPointIndex = selectedPointIndex,
                    modifier = Modifier.fillMaxSize(),
                    onPointSelected = onPointSelected
                )
            } else {
                Text(
                    text = if (error != null) error else "地点を選択してください",
                    style = MaterialTheme.typography.bodyMedium,
                    color = if (error != null) MaterialTheme.colorScheme.error else MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
    }
}

/**
 * 検索を実行し、地図を移動させる
 */
private fun executeSearch(
    query: String,
    scope: CoroutineScope,
    context: Context,
    cameraPositionState: CameraPositionState,
    keyboardController: SoftwareKeyboardController?,
    focusManager: FocusManager,
    onSuccess: () -> Unit
) {
    if (query.isNotBlank()) {
        scope.launch {
            try {
                val addresses = withContext(Dispatchers.IO) {
                    Geocoder(context).getFromLocationName(query, 1)
                }
                if (!addresses.isNullOrEmpty()) {
                    val address = addresses[0]
                    val target = LatLng(address.latitude, address.longitude)
                    
                    cameraPositionState.animate(
                        CameraUpdateFactory.newLatLngZoom(target, 14f)
                    )
                    
                    onSuccess()
                    keyboardController?.hide()
                    focusManager.clearFocus()
                } else {
                    Toast.makeText(context, "場所が見つかりませんでした", Toast.LENGTH_SHORT).show()
                }
            } catch (e: Exception) {
                Toast.makeText(context, "検索中にエラーが発生しました", Toast.LENGTH_SHORT).show()
            }
        }
    }
}

/**
 * 現在地を取得してカメラを移動させる
 */
private fun getCurrentLocation(
    context: Context,
    fusedLocationClient: FusedLocationProviderClient,
    launcher: androidx.activity.result.ActivityResultLauncher<Array<String>>,
    scope: CoroutineScope,
    cameraPositionState: CameraPositionState
) {
    if (ContextCompat.checkSelfPermission(
            context, Manifest.permission.ACCESS_FINE_LOCATION
        ) == PackageManager.PERMISSION_GRANTED) {
        fusedLocationClient.lastLocation.addOnSuccessListener { location ->
            location?.let {
                scope.launch {
                    cameraPositionState.animate(
                        CameraUpdateFactory.newLatLngZoom(LatLng(it.latitude, it.longitude), 14f)
                    )
                }
            }
        }
    } else {
        launcher.launch(arrayOf(
            Manifest.permission.ACCESS_FINE_LOCATION,
            Manifest.permission.ACCESS_COARSE_LOCATION
        ))
    }
}
