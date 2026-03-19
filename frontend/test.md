<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Urban Morphology Observatory - Interactive Analysis</title>

    <!-- Leaflet CSS -->
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css" />
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css" />
    <link rel="stylesheet" href="https://unpkg.com/leaflet-draw@1.0.4/dist/leaflet.draw.css" />

    <!-- Dark Academia Typography -->
    <link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600;700&family=IBM+Plex+Sans:wght@300;400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet"/>

    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        :root {
            --bg: #16140F;
            --bg2: #1E1B14;
            --surface: #26221A;
            --surface2: #302C22;
            --ink: #F0EBE0;
            --ink2: #B8B0A4;
            --ink3: #6E6860;
            --accent: #D97706;
            --accent2: #F59E0B;
            --border: #302C22;
        }

        body {
            font-family: 'IBM Plex Sans', sans-serif;
            background: var(--bg);
            color: var(--ink);
            overflow: hidden;
        }

        #map {
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            z-index: 0;
        }

        /* Smooth Leaflet path transitions */
        .leaflet-zoom-animated {
            will-change: transform;
        }

        .leaflet-interactive {
            transition: stroke-opacity 0.2s ease, fill-opacity 0.2s ease;
        }

        /* Optimize circle rendering */
        svg.leaflet-zoom-animated {
            will-change: transform;
        }

        /* GPU acceleration for map panning */
        .leaflet-tile-container,
        .leaflet-marker-pane,
        .leaflet-overlay-pane {
            will-change: transform;
        }

        /* Header */
        .header {
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 60px;
            background: var(--bg);
            backdrop-filter: blur(10px);
            border-bottom: 1px solid var(--border);
            z-index: 1000;
            display: flex;
            align-items: center;
            padding: 0 20px;
        }

        .header h1 {
            font-family: 'Playfair Display', serif;
            font-size: 18px;
            font-weight: 700;
            color: var(--ink);
            margin-right: auto;
        }

        .header-subtitle {
            font-size: 10px;
            color: var(--ink3);
            text-transform: uppercase;
            letter-spacing: 0.1em;
            margin-left: 15px;
            font-weight: 600;
        }

        .draw-tools {
            display: flex;
            gap: 6px;
            margin-left: 20px;
            padding-left: 20px;
            border-left: 1px solid var(--border);
        }

        .tool-btn {
            padding: 7px 14px;
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 2px;
            color: var(--ink3);
            cursor: pointer;
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            transition: all 0.15s ease;
        }

        .tool-btn:hover {
            background: var(--surface2);
            border-color: var(--ink3);
            color: var(--ink);
        }

        .tool-btn.active {
            background: var(--accent);
            border-color: var(--accent);
            color: #fff;
        }

        /* City Selector */
        .city-selector {
            display: flex;
            gap: 6px;
            margin-left: 30px;
        }

        .city-btn {
            padding: 7px 14px;
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 2px;
            color: var(--ink3);
            cursor: pointer;
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            transition: all 0.15s ease;
        }

        .city-btn:hover {
            background: var(--surface2);
            border-color: var(--ink3);
            color: var(--ink);
        }

        .city-btn.active {
            background: var(--accent);
            border-color: var(--accent);
            color: #fff;
        }

        /* Metrics Panel */
        .metrics-panel {
            position: absolute;
            top: 80px;
            left: 20px;
            width: auto;
            min-width: 380px;
            max-width: calc(100vw - 400px);
            max-height: calc(100vh - 100px);
            background: var(--bg);
            backdrop-filter: blur(10px);
            border: 1px solid var(--border);
            border-radius: 2px;
            z-index: 999;
            overflow: hidden;
            opacity: 0;
            visibility: hidden;
            transform: translateX(-20px);
            transition: opacity 0.3s ease, transform 0.3s ease, visibility 0s linear 0.3s;
        }

        .metrics-panel.visible {
            opacity: 1;
            visibility: visible;
            transform: translateX(0);
            transition: opacity 0.3s ease, transform 0.3s ease, visibility 0s linear 0s;
        }

        .circles-comparison {
            display: flex;
            gap: 16px;
            flex-wrap: nowrap;
            overflow-x: auto;
        }

        .circle-metrics-card {
            min-width: 340px;
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 4px;
            padding: 0;
            position: relative;
            overflow: hidden;
            transition: all 0.3s ease;
        }

        .circle-metrics-card:hover {
            border-color: var(--accent2);
            box-shadow: 0 4px 12px rgba(217, 119, 6, 0.15);
            transform: translateY(-2px);
        }

        .circle-metrics-card.circle-1 {
            border-top: 3px solid #D97706;
        }
        .circle-metrics-card.circle-1 .card-color-bar { background: linear-gradient(135deg, #D97706 0%, #B45309 100%); }

        .circle-metrics-card.circle-2 {
            border-top: 3px solid #F59E0B;
        }
        .circle-metrics-card.circle-2 .card-color-bar { background: linear-gradient(135deg, #F59E0B 0%, #D97706 100%); }

        .circle-metrics-card.circle-3 {
            border-top: 3px solid #FBBF24;
        }
        .circle-metrics-card.circle-3 .card-color-bar { background: linear-gradient(135deg, #FBBF24 0%, #F59E0B 100%); }

        .circle-metrics-card.circle-4 {
            border-top: 3px solid #FCD34D;
        }
        .circle-metrics-card.circle-4 .card-color-bar { background: linear-gradient(135deg, #FCD34D 0%, #FBBF24 100%); }

        .card-color-bar {
            height: 4px;
            width: 100%;
        }

        .circle-card-header {
            padding: 12px 14px 10px 14px;
            background: var(--surface2);
            border-bottom: 1px solid var(--border);
        }

        .circle-header-top {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 6px;
        }

        .circle-card-title {
            font-family: 'IBM Plex Mono', monospace;
            font-weight: 600;
            font-size: 10px;
            letter-spacing: 0.1em;
            text-transform: uppercase;
            color: var(--accent2);
        }

        .circle-header-actions {
            display: flex;
            gap: 6px;
        }

        .circle-action-btn {
            width: 22px;
            height: 22px;
            display: flex;
            align-items: center;
            justify-content: center;
            cursor: pointer;
            background: var(--surface);
            border-radius: 2px;
            font-size: 14px;
            color: var(--ink2);
            transition: all 0.2s;
            border: 1px solid var(--border);
        }

        .circle-action-btn:hover {
            background: var(--accent);
            color: #fff;
            border-color: var(--accent);
        }

        .circle-location {
            font-family: 'IBM Plex Sans', sans-serif;
            font-size: 11px;
            color: var(--ink2);
            line-height: 1.4;
            display: flex;
            align-items: flex-start;
            gap: 6px;
        }

        .circle-location .location-icon {
            color: var(--accent2);
            font-size: 12px;
            margin-top: 1px;
            flex-shrink: 0;
        }

        .circle-location .location-text {
            flex: 1;
            overflow: hidden;
            text-overflow: ellipsis;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
        }

        .circle-location .loading-location {
            color: var(--ink3);
            font-style: italic;
        }

        .card-metrics-body {
            padding: 14px;
        }

        /* Loading skeleton animations */
        @keyframes pulse {
            0%, 100% {
                opacity: 1;
            }
            50% {
                opacity: 0.5;
            }
        }

        @keyframes shimmer {
            0% {
                background-position: -1000px 0;
            }
            100% {
                background-position: 1000px 0;
            }
        }

        .skeleton {
            background: linear-gradient(
                90deg,
                var(--surface2) 0%,
                var(--border) 50%,
                var(--surface2) 100%
            );
            background-size: 1000px 100%;
            animation: shimmer 2s infinite linear;
            border-radius: 2px;
        }

        .skeleton-text {
            height: 12px;
            margin: 4px 0;
        }

        .skeleton-title {
            height: 16px;
            width: 60%;
            margin-bottom: 8px;
        }

        .card-calculating {
            opacity: 0.7;
        }

        .card-calculating .card-metrics-body {
            pointer-events: none;
        }

        .remove-circle-btn {
            cursor: pointer;
            width: 22px;
            height: 22px;
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 2px;
            display: flex;
            align-items: center;
            justify-content: center;
            color: var(--ink3);
            font-size: 16px;
            transition: all 0.15s ease;
        }

        .remove-circle-btn:hover {
            background: #991b1b;
            border-color: #991b1b;
            color: #fff;
        }

        .metrics-panel.visible {
            display: block;
        }

        .panel-header {
            padding: 14px 16px;
            background: var(--surface);
            border-bottom: 1px solid var(--border);
            font-weight: 600;
            font-size: 11px;
            color: var(--ink);
            display: flex;
            justify-content: space-between;
            align-items: center;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }

        .close-panel {
            cursor: pointer;
            width: 22px;
            height: 22px;
            background: var(--surface2);
            border: 1px solid var(--border);
            border-radius: 2px;
            display: flex;
            align-items: center;
            justify-content: center;
            color: var(--ink3);
            font-size: 18px;
            line-height: 1;
            transition: all 0.15s ease;
        }

        .close-panel:hover {
            background: var(--accent);
            border-color: var(--accent);
            color: #fff;
        }

        .panel-content {
            max-height: calc(100vh - 200px);
            overflow-y: auto;
            padding: 16px;
        }

        .panel-content::-webkit-scrollbar {
            width: 3px;
        }

        .panel-content::-webkit-scrollbar-track {
            background: var(--surface);
        }

        .panel-content::-webkit-scrollbar-thumb {
            background: var(--border);
            border-radius: 2px;
        }

        .metric-section {
            margin-bottom: 20px;
        }

        .section-title {
            font-size: 10px;
            font-weight: 700;
            color: var(--accent);
            text-transform: uppercase;
            letter-spacing: 0.1em;
            margin-bottom: 10px;
        }

        .metric-row {
            display: flex;
            justify-content: space-between;
            padding: 8px 12px;
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 1px;
            margin-bottom: 4px;
        }

        .metric-label {
            font-size: 11px;
            color: var(--ink3);
            text-transform: uppercase;
            letter-spacing: 0.06em;
            font-weight: 600;
        }

        .metric-value {
            font-family: 'IBM Plex Mono', monospace;
            font-size: 13px;
            font-weight: 500;
            color: var(--accent2);
        }

        .metric-value.good {
            color: #51cf66;
        }

        .metric-value.medium {
            color: #ffd43b;
        }

        .metric-value.low {
            color: #ff6b6b;
        }

        .comparison-note {
            font-size: 10px;
            color: var(--ink3);
            line-height: 1.5;
            margin-top: 8px;
            padding: 10px;
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 1px;
        }

        .comparison-note code {
            font-family: 'IBM Plex Mono', monospace;
            color: var(--accent2);
            background: var(--surface2);
            padding: 2px 4px;
            border-radius: 2px;
        }

        /* Control Panel */
        .control-panel {
            position: absolute;
            top: 80px;
            right: 20px;
            width: 320px;
            max-height: calc(100vh - 100px);
            background: var(--bg);
            backdrop-filter: blur(10px);
            border: 1px solid var(--border);
            border-radius: 2px;
            z-index: 999;
            overflow: hidden;
        }

        .layer-group {
            margin-bottom: 12px;
        }

        .layer-group-title {
            font-family: 'Playfair Display', serif;
            font-size: 12px;
            font-weight: 700;
            color: var(--ink);
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin-bottom: 8px;
            padding: 4px 0;
        }

        .layer-item {
            display: flex;
            align-items: center;
            padding: 9px 12px;
            margin-bottom: 3px;
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 1px;
            transition: all 0.15s ease;
            cursor: pointer;
        }

        .layer-item:hover {
            background: var(--surface2);
            border-color: var(--ink3);
        }

        .layer-item input[type="checkbox"] {
            margin-right: 12px;
            cursor: pointer;
            width: 16px;
            height: 16px;
            accent-color: var(--accent);
        }

        .layer-name {
            flex: 1;
            font-size: 11px;
            color: var(--ink2);
            text-transform: uppercase;
            letter-spacing: 0.05em;
            font-weight: 500;
        }

        /* Basemap Selector */
        .basemap-selector {
            padding: 14px 16px;
            background: var(--surface);
            border-bottom: 1px solid var(--border);
        }

        .basemap-selector label {
            font-size: 10px;
            color: var(--ink3);
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-weight: 600;
            margin-bottom: 8px;
            display: block;
        }

        .basemap-selector select {
            width: 100%;
            padding: 8px 12px;
            background: var(--surface2);
            border: 1px solid var(--border);
            border-radius: 2px;
            color: var(--ink);
            font-size: 11px;
            font-family: 'IBM Plex Sans', sans-serif;
            cursor: pointer;
            transition: all 0.15s ease;
        }

        .basemap-selector select:hover {
            border-color: var(--ink3);
        }

        .basemap-selector select:focus {
            outline: none;
            border-color: var(--accent);
        }

        /* Loading indicator */
        .loading {
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            background: var(--bg);
            padding: 24px 36px;
            border-radius: 2px;
            border: 1px solid var(--border);
            z-index: 10000;
            opacity: 0;
            visibility: hidden;
            transition: opacity 0.2s ease, visibility 0s linear 0.2s;
            font-size: 12px;
            color: var(--ink2);
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-weight: 600;
        }

        .loading.visible {
            opacity: 1;
            visibility: visible;
            transition: opacity 0.2s ease, visibility 0s linear 0s;
        }

        .spinner {
            border: 2px solid var(--border);
            border-top: 2px solid var(--accent);
            border-radius: 50%;
            width: 20px;
            height: 20px;
            animation: spin 0.8s linear infinite;
            display: inline-block;
            margin-right: 12px;
            vertical-align: middle;
            will-change: transform;
        }

        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }

        /* Leaflet custom styles */
        .leaflet-draw-toolbar {
            display: none !important;
        }

        .leaflet-draw-draw-polygon {
            background-color: var(--accent) !important;
        }

        .leaflet-control-attribution {
            font-size: 9px !important;
            background: rgba(22, 20, 15, 0.85) !important;
            color: var(--ink3) !important;
        }

        .leaflet-control-attribution a {
            color: var(--accent) !important;
        }

        .leaflet-control-zoom {
            border: 1px solid var(--border) !important;
            border-radius: 2px !important;
        }

        .leaflet-control-zoom a {
            background: var(--bg) !important;
            color: var(--ink2) !important;
            border-bottom: 1px solid var(--border) !important;
        }

        .leaflet-control-zoom a:hover {
            background: var(--surface) !important;
            color: var(--accent2) !important;
        }

        .leaflet-control-scale-line {
            background: rgba(22, 20, 15, 0.8) !important;
            border: 1px solid var(--border) !important;
            color: var(--ink3) !important;
            font-size: 9px !important;
            font-family: 'IBM Plex Mono', monospace !important;
        }

        /* Circle tool cursor */
        #map.circle-tool-active {
            cursor: crosshair !important;
        }

        #map.circle-tool-active * {
            cursor: crosshair !important;
        }

        /* Magnifying glass lens */
        #magnify-lens {
            position: absolute;
            border: 3px solid #D97706;
            border-radius: 50%;
            overflow: hidden;
            pointer-events: none;
            z-index: 1000;
            box-shadow: 0 0 0 2px rgba(217, 119, 6, 0.3),
                        0 8px 24px rgba(0, 0, 0, 0.6);
            display: none;
            background: #000;
        }

        #magnify-lens.active {
            display: block;
        }

        #magnify-lens::before {
            content: '';
            position: absolute;
            top: -3px;
            left: -3px;
            right: -3px;
            bottom: -3px;
            border-radius: 50%;
            background: linear-gradient(135deg,
                rgba(255,255,255,0.3) 0%,
                transparent 50%,
                rgba(0,0,0,0.2) 100%);
            pointer-events: none;
            z-index: 10;
        }

        #magnify-map-container {
            width: 100%;
            height: 100%;
            position: relative;
        }

        /* Hover preview tooltip */
        .hover-preview {
            position: absolute;
            background: var(--bg);
            border: 2px solid var(--accent);
            border-radius: 2px;
            padding: 10px 14px;
            pointer-events: none;
            z-index: 10001;
            font-size: 11px;
            color: var(--ink);
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.5);
            opacity: 0;
            visibility: hidden;
            transform: translateY(5px);
            transition: opacity 0.15s ease, transform 0.15s ease, visibility 0s linear 0.15s;
        }

        .hover-preview.visible {
            opacity: 1;
            visibility: visible;
            transform: translateY(0);
            transition: opacity 0.15s ease, transform 0.15s ease, visibility 0s linear 0s;
        }

        .hover-preview-title {
            font-family: 'Playfair Display', serif;
            font-size: 12px;
            font-weight: 700;
            color: var(--accent2);
            margin-bottom: 6px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }

        .hover-preview-metric {
            display: flex;
            justify-content: space-between;
            gap: 12px;
            margin-bottom: 3px;
        }

        .hover-preview-label {
            color: var(--ink3);
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .hover-preview-value {
            font-family: 'IBM Plex Mono', monospace;
            color: var(--accent2);
            font-weight: 600;
        }

        /* Magnifying glass lens effect */
        .magnifying-lens {
            border: 3px solid var(--accent) !important;
            box-shadow: 0 0 0 2px rgba(217, 119, 6, 0.3),
                        inset 0 0 20px rgba(217, 119, 6, 0.15),
                        0 0 30px rgba(217, 119, 6, 0.4) !important;
        }

        /* Magnification effect pane */
        .magnify-effect {
            filter: brightness(1.25) contrast(1.15) saturate(1.1);
            transform-origin: center;
        }
    </style>
</head>
<body>
    <!-- Header -->
    <div class="header">
        <h1>Urban Morphology Observatory</h1>
        <span class="header-subtitle">Click on map to place analysis circles • Compare up to 4 locations</span>

        <div class="draw-tools">
            <button class="tool-btn active" id="circle-tool-btn">🔍 Circle Tool</button>
            <button class="tool-btn" id="clear-circles-btn">🗑️ Clear All</button>
            <div style="display: flex; align-items: center; margin-left: 12px; padding-left: 12px; border-left: 1px solid var(--border);">
                <label style="font-size: 10px; color: var(--ink3); text-transform: uppercase; letter-spacing: 0.05em; margin-right: 8px;">Radius:</label>
                <select id="radius-select" style="padding: 5px 10px; background: var(--surface); border: 1px solid var(--border); border-radius: 2px; color: var(--ink); font-size: 11px; cursor: pointer;">
                    <option value="250">250m</option>
                    <option value="500" selected>500m</option>
                    <option value="1000">1 km</option>
                    <option value="2000">2 km</option>
                </select>
            </div>
        </div>

        <div class="city-selector">
            <button class="city-btn" data-city="barcelona">Barcelona</button>
            <button class="city-btn" data-city="tokyo">Tokyo</button>
            <button class="city-btn" data-city="paris">Paris</button>
            <button class="city-btn" data-city="singapore">Singapore</button>
            <button class="city-btn" data-city="nairobi">Nairobi</button>
            <button class="city-btn" data-city="cairo">Cairo</button>
            <button class="city-btn" data-city="bogota">Bogotá</button>
        </div>
    </div>

    <!-- Map Container -->
    <div id="map"></div>

    <!-- Metrics Panel -->
    <div class="metrics-panel" id="metrics-panel">
        <div class="panel-header">
            <span>📊 Circle Analysis</span>
            <div class="close-panel" onclick="closeMetricsPanel()">×</div>
        </div>
        <div class="panel-content" id="metrics-content">
            <!-- Dynamically populated with circle metrics -->
        </div>
    </div>

    <!-- Control Panel -->
    <div class="control-panel">
        <div class="panel-header">Layer Controls</div>

        <div class="basemap-selector">
            <label>Base Map</label>
            <select id="basemap-select">
                <option value="osm" selected>OpenStreetMap</option>
                <option value="dark">Dark Mode</option>
                <option value="satellite">Satellite</option>
                <option value="terrain">Terrain</option>
                <option value="cartodb">CartoDB Positron</option>
            </select>
        </div>

        <div class="panel-content" id="layer-controls"></div>
    </div>

    <!-- Loading Indicator -->
    <div class="loading" id="loading">
        <div class="spinner"></div>
        Calculating metrics...
    </div>

    <!-- Hover Preview Tooltip -->
    <div class="hover-preview" id="hover-preview">
        <div class="hover-preview-title">🔍 Quick Preview</div>
        <div id="hover-preview-content"></div>
    </div>

    <!-- Magnifying lens -->
    <div id="magnify-lens">
        <div id="magnify-map-container"></div>
    </div>

    <!-- Leaflet JS -->
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
    <script src="https://unpkg.com/leaflet-draw@1.0.4/dist/leaflet.draw.js"></script>
    <script src="https://unpkg.com/@turf/turf@6.5.0/turf.min.js"></script>

    <script>
        // Initialize map
        const map = L.map('map', {
            center: [35.6762, 139.6503],
            zoom: 12,
            zoomControl: true,
            preferCanvas: true
        });

        // Base layers
        const baseLayers = {
            osm: L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                attribution: '© OpenStreetMap contributors',
                maxZoom: 19
            }),
            dark: L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
                attribution: '© OpenStreetMap, © CartoDB',
                maxZoom: 19
            }),
            satellite: L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
                attribution: 'Esri, Maxar',
                maxZoom: 19
            }),
            terrain: L.tileLayer('https://stamen-tiles-{s}.a.ssl.fastly.net/terrain/{z}/{x}/{y}{r}.png', {
                attribution: 'Stamen Design',
                maxZoom: 18
            }),
            cartodb: L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
                attribution: '© OpenStreetMap, © CartoDB',
                maxZoom: 19
            })
        };

        baseLayers.osm.addTo(map);

        // Circle tool state
        let circleToolActive = true;
        let currentRadius = 500; // meters
        let hoverCircle = null;
        let spotlightOverlay = null;
        let lockedCircles = [];
        let circleMetrics = [];
        const MAX_CIRCLES = 4;
        let hoverPreviewTimeout = null;
        let cachedGridData = null; // Cache pre-computed grid data
        let magnifyLayer = null; // Magnified tile layer
        let lastHoverPosition = null; // Track last hover position
        let hoverElements = null; // Cache hover elements for reuse
        let hoverAnimationFrame = null; // Animation frame for smooth updates
        const HOVER_MOVE_THRESHOLD = 3; // pixels - only update if moved more than this
        let magnifyLens = null; // Magnification lens element
        let magnifyMap = null; // Separate map instance for magnification
        const MAGNIFY_ZOOM_OFFSET = 3; // How many zoom levels to magnify

        const circleColors = ['#D97706', '#F59E0B', '#FBBF24', '#FCD34D'];
        const circleLabels = ['A', 'B', 'C', 'D'];

        // Performance optimization utilities
        function throttle(func, delay) {
            let lastCall = 0;
            let timeoutId = null;
            return function(...args) {
                const now = Date.now();
                const timeSinceLastCall = now - lastCall;

                if (timeSinceLastCall >= delay) {
                    lastCall = now;
                    func.apply(this, args);
                } else {
                    // Schedule a call at the end of the delay period
                    clearTimeout(timeoutId);
                    timeoutId = setTimeout(() => {
                        lastCall = Date.now();
                        func.apply(this, args);
                    }, delay - timeSinceLastCall);
                }
            };
        }

        function debounce(func, delay) {
            let timeoutId;
            return function(...args) {
                clearTimeout(timeoutId);
                timeoutId = setTimeout(() => func.apply(this, args), delay);
            };
        }

        // City configurations
        const cities = {
            barcelona: {
                name: 'Barcelona',
                center: [41.3851, 2.1734],
                zoom: 12,
                layers: {
                    'Neighbourhoods': 'geodata/barcelona/barris_neighbourhoods.geojson',
                    'Districts': 'geodata/barcelona/districtes.geojson',
                    'Major Roads': 'geodata/barcelona/roads/roads_major.geojson',
                    'All Roads': 'geodata/barcelona/roads/roads_all.geojson'
                }
            },
            tokyo: {
                name: 'Tokyo',
                center: [35.6762, 139.6503],
                zoom: 11,
                layers: {
                    'Special Wards (23)': 'geodata/tokyo/special_wards_23.geojson',
                    'Chome Level 9': 'geodata/tokyo/chome_level9.geojson',
                    'Major Roads': 'geodata/tokyo/roads/roads_major.geojson',
                    'All Roads': 'geodata/tokyo/roads/roads_all.geojson',
                    'Transit Stops': 'geodata/tokyo/roads/transit_stops.geojson'
                }
            },
            paris: {
                name: 'Paris',
                center: [48.8566, 2.3522],
                zoom: 12,
                layers: {
                    'Arrondissements': 'geodata/paris/arrondissements.geojson',
                    'Major Roads': 'geodata/paris/roads/roads_major.geojson',
                    'All Roads': 'geodata/paris/roads/roads_all.geojson',
                    'Transit Stops': 'geodata/paris/roads/transit_stops.geojson'
                }
            },
            singapore: {
                name: 'Singapore',
                center: [1.3521, 103.8198],
                zoom: 11,
                layers: {
                    'Planning Areas (55)': 'geodata/singapore/master_plan_2019_planning_areas.geojson',
                    'Subzones (332)': 'geodata/singapore/master_plan_2019_subzones.geojson',
                    'Major Roads': 'geodata/singapore/roads/roads_major.geojson',
                    'All Roads': 'geodata/singapore/roads/roads_all.geojson',
                    'Transit Stops': 'geodata/singapore/roads/transit_stops.geojson'
                }
            },
            nairobi: {
                name: 'Nairobi',
                center: [-1.2921, 36.8219],
                zoom: 11,
                layers: {
                    'Wards': 'geodata/nairobi/nairobi_wards.geojson',
                    'Major Roads': 'geodata/nairobi/roads/roads_major.geojson',
                    'All Roads': 'geodata/nairobi/roads/roads_all.geojson'
                }
            },
            cairo: {
                name: 'Cairo',
                center: [30.0444, 31.2357],
                zoom: 11,
                layers: {
                    'Major Roads': 'geodata/cairo/roads/roads_major.geojson',
                    'All Roads': 'geodata/cairo/roads/roads_all.geojson'
                }
            },
            bogota: {
                name: 'Bogotá',
                center: [4.7110, -74.0721],
                zoom: 11,
                layers: {
                    'Major Roads': 'geodata/bogota/roads/roads_major.geojson',
                    'All Roads': 'geodata/bogota/roads/roads_all.geojson',
                    'Transit Stops': 'geodata/bogota/roads/transit_stops.geojson'
                }
            }
        };

        // Layer storage
        let loadedLayers = {};
        let currentCity = null;
        let loadedRoadsData = null;
        let precomputedGridData = null;

        // Show/hide loading
        function showLoading(message = 'Calculating metrics...') {
            const loading = document.getElementById('loading');
            if (!loading) {
                console.error('Loading element not found');
                return;
            }
            const spinner = loading.querySelector('.spinner');
            if (spinner && spinner.nextSibling) {
                spinner.nextSibling.textContent = message;
            }
            loading.classList.add('visible');
        }

        function hideLoading() {
            const loading = document.getElementById('loading');
            if (loading) {
                loading.classList.remove('visible');
            }
        }

        // Cache for geocoding results to avoid redundant API calls
        const geocodingCache = new Map();
        const MAX_GEOCODING_CACHE_SIZE = 100; // Limit cache size to prevent memory leak

        // Reverse geocoding to get location name/address (with caching)
        async function reverseGeocode(lat, lng) {
            const cacheKey = `${lat.toFixed(4)},${lng.toFixed(4)}`;

            // Check cache first
            if (geocodingCache.has(cacheKey)) {
                return geocodingCache.get(cacheKey);
            }

            // Implement LRU cache - remove oldest entry if cache is full
            if (geocodingCache.size >= MAX_GEOCODING_CACHE_SIZE) {
                const firstKey = geocodingCache.keys().next().value;
                geocodingCache.delete(firstKey);
            }

            try {
                const response = await fetch(
                    `https://nominatim.openstreetmap.org/reverse?lat=${lat}&lon=${lng}&format=json&addressdetails=1&accept-language=en`,
                    {
                        headers: {
                            'User-Agent': 'UrbanMorphologyObservatory/1.0'
                        }
                    }
                );

                if (!response.ok) {
                    console.warn('Geocoding failed:', response.status);
                    return null;
                }

                const data = await response.json();

                // Extract meaningful location components
                const addr = data.address || {};
                let locationName = '';

                // Priority: neighborhood > suburb > city_district > district > city
                if (addr.neighbourhood) {
                    locationName = addr.neighbourhood;
                } else if (addr.suburb) {
                    locationName = addr.suburb;
                } else if (addr.city_district) {
                    locationName = addr.city_district;
                } else if (addr.district) {
                    locationName = addr.district;
                } else if (addr.quarter) {
                    locationName = addr.quarter;
                } else if (addr.city || addr.town || addr.village) {
                    locationName = addr.city || addr.town || addr.village;
                }

                // Add city context if available and different
                const cityName = addr.city || addr.town || addr.village;
                if (cityName && locationName && locationName !== cityName) {
                    locationName += `, ${cityName}`;
                }

                const result = {
                    name: locationName || data.display_name?.split(',')[0] || 'Unknown Location',
                    fullAddress: data.display_name,
                    coords: `${lat.toFixed(5)}, ${lng.toFixed(5)}`
                };

                // Cache the result
                geocodingCache.set(cacheKey, result);

                return result;
            } catch (error) {
                console.error('Geocoding error:', error);
                return null;
            }
        }

        // Load GeoJSON layer with timeout
        async function loadLayer(url, layerName) {
            try {
                // Add timeout for large files
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), 60000); // 60 second timeout

                const response = await fetch(url, { signal: controller.signal });
                clearTimeout(timeoutId);

                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }

                const data = await response.json();

                // Store roads data for analysis
                if (layerName.includes('Road') && layerName.includes('All')) {
                    loadedRoadsData = data;
                    console.log(`✓ Loaded ${data.features.length} road segments for analysis`);
                }

                let layer;

                if (data.features[0]?.geometry?.type === 'Point') {
                    const markers = L.markerClusterGroup({
                        iconCreateFunction: function(cluster) {
                            return L.divIcon({
                                html: '<div style="background: #D97706; color: #fff; border-radius: 50%; width: 28px; height: 28px; display: flex; align-items: center; justify-content: center; font-size: 11px; font-weight: 600; font-family: \'IBM Plex Mono\', monospace; border: 1px solid #F59E0B;">' + cluster.getChildCount() + '</div>',
                                className: 'custom-cluster-icon',
                                iconSize: L.point(28, 28)
                            });
                        }
                    });

                    data.features.forEach(feature => {
                        const marker = L.circleMarker([
                            feature.geometry.coordinates[1],
                            feature.geometry.coordinates[0]
                        ], {
                            radius: 4,
                            fillColor: '#D97706',
                            color: '#F59E0B',
                            weight: 1,
                            opacity: 1,
                            fillOpacity: 0.8
                        });
                        marker.feature = feature;
                        markers.addLayer(marker);
                    });

                    layer = markers;
                } else {
                    layer = L.geoJSON(data, {
                        style: function(feature) {
                            if (layerName.includes('Major')) {
                                return { color: '#D97706', weight: 2.5, opacity: 0.8 };
                            } else if (layerName.includes('Road')) {
                                return { color: '#6E6860', weight: 1, opacity: 0.5 };
                            } else {
                                return {
                                    fillColor: '#D97706',
                                    fillOpacity: 0.12,
                                    color: '#D97706',
                                    weight: 1.5,
                                    opacity: 0.7
                                };
                            }
                        }
                    });
                }

                return layer;
            } catch (error) {
                console.error(`Error loading ${layerName}:`, error);

                // Show user-friendly error message
                if (error.name === 'AbortError') {
                    alert(`⏱️ ${layerName} took too long to load (>60s). This layer contains a very large dataset. Try refreshing or use "Major Roads" instead.`);
                } else {
                    alert(`❌ Failed to load ${layerName}: ${error.message}`);
                }

                return null;
            }
        }

        // Fetch building footprints from OpenStreetMap Overpass API
        async function fetchBuildingData(polygonGeoJSON) {
            try {
                // Convert polygon to bbox for Overpass query
                const bbox = turf.bbox(polygonGeoJSON);
                const [minLon, minLat, maxLon, maxLat] = bbox;

                // Overpass query to get building footprints
                const query = `
                    [out:json][timeout:25];
                    (
                        way["building"](${minLat},${minLon},${maxLat},${maxLon});
                        relation["building"](${minLat},${minLon},${maxLat},${maxLon});
                    );
                    out geom;
                `;

                const overpassUrl = 'https://overpass-api.de/api/interpreter';
                const response = await fetch(overpassUrl, {
                    method: 'POST',
                    body: query
                });

                if (!response.ok) {
                    throw new Error(`Overpass API error: ${response.status}`);
                }

                const data = await response.json();
                console.log(`Fetched ${data.elements.length} buildings from OSM`);

                // Calculate total built area
                let totalBuiltArea = 0;
                let buildingCount = 0;

                data.elements.forEach(element => {
                    if (element.type === 'way' && element.geometry) {
                        // Convert OSM way to GeoJSON polygon
                        const coords = element.geometry.map(node => [node.lon, node.lat]);

                        // Close the polygon if not already closed
                        if (coords[0][0] !== coords[coords.length-1][0] ||
                            coords[0][1] !== coords[coords.length-1][1]) {
                            coords.push(coords[0]);
                        }

                        try {
                            const building = turf.polygon([coords]);

                            // Check if building intersects with our polygon
                            if (turf.booleanIntersects(building, polygonGeoJSON)) {
                                const buildingArea = turf.area(building);
                                totalBuiltArea += buildingArea;
                                buildingCount++;
                            }
                        } catch (e) {
                            // Skip invalid geometries
                            console.warn('Invalid building geometry:', e);
                        }
                    }
                });

                return {
                    totalBuiltArea,
                    buildingCount
                };

            } catch (error) {
                console.error('Error fetching building data:', error);
                return null;
            }
        }

        // Calculate metrics for drawn area (FIXED ALGORITHM)
        async function calculateMetrics(polygon) {
            showLoading('Analyzing selected area...');

            try {
                // Handle both Leaflet layer and GeoJSON
                const polygonGeoJSON = polygon.toGeoJSON ? polygon.toGeoJSON() : polygon;

                console.log('Calculating metrics for polygon:', polygonGeoJSON);

                const areaM2 = turf.area(polygonGeoJSON);
                console.log('Area:', areaM2, 'm²');

                const metrics = {
                    area_km2: (areaM2 / 1000000).toFixed(3),
                    road_length_km: 0,
                    intersection_count: 0,
                    cnr: 0,
                    intersection_density: 0,
                    road_density: 0,
                    open_ratio: 0
                };

                if (!loadedRoadsData) {
                    console.warn('No road data loaded. Please enable "All Roads" layer.');
                    metrics.error = 'No road data loaded. Enable "All Roads" layer for accurate metrics.';
                    return metrics;
                }

                // Filter roads within polygon (check if ANY point is inside)
                console.log('Total road features available:', loadedRoadsData.features.length);

                const roadsWithin = loadedRoadsData.features.filter(road => {
                    if (road.geometry.type !== 'LineString') return false;

                    // Check if road intersects polygon
                    try {
                        return turf.booleanIntersects(road, polygonGeoJSON);
                    } catch (e) {
                        console.warn('Error checking road intersection:', e);
                        return false;
                    }
                });

                console.log('Roads found within area:', roadsWithin.length);

                if (roadsWithin.length === 0) {
                    metrics.error = 'No roads found in selected area. Try a larger area or different location.';
                    return metrics;
                }

                // STEP 1: Build coordinate-to-roads mapping
                // Round coordinates to 6 decimal places (~0.1m precision) to merge nearby nodes
                const coordToRoads = new Map();

                function roundCoord(coord) {
                    return `${coord[0].toFixed(6)},${coord[1].toFixed(6)}`;
                }

                roadsWithin.forEach((road, roadIdx) => {
                    if (road.geometry.type === 'LineString') {
                        const coords = road.geometry.coordinates;

                        // Add ALL coordinates from this road
                        coords.forEach(coord => {
                            const key = roundCoord(coord);
                            if (!coordToRoads.has(key)) {
                                coordToRoads.set(key, {
                                    coord: coord,
                                    roads: new Set()
                                });
                            }
                            coordToRoads.get(key).roads.add(roadIdx);
                        });
                    }
                });

                // STEP 2: Identify nodes (intersections + endpoints)
                const nodes = new Map();

                roadsWithin.forEach((road, roadIdx) => {
                    if (road.geometry.type === 'LineString') {
                        const coords = road.geometry.coordinates;

                        // Always add first and last points as nodes
                        const startKey = roundCoord(coords[0]);
                        const endKey = roundCoord(coords[coords.length - 1]);

                        if (!nodes.has(startKey)) {
                            nodes.set(startKey, {
                                coord: coords[0],
                                degree: 0,
                                roads: new Set()
                            });
                        }

                        if (!nodes.has(endKey)) {
                            nodes.set(endKey, {
                                coord: coords[coords.length - 1],
                                degree: 0,
                                roads: new Set()
                            });
                        }

                        // Add intermediate points if they're shared by multiple roads (intersections)
                        for (let i = 1; i < coords.length - 1; i++) {
                            const key = roundCoord(coords[i]);
                            const coordInfo = coordToRoads.get(key);

                            if (coordInfo && coordInfo.roads.size > 1) {
                                // This coordinate is used by multiple roads = intersection
                                if (!nodes.has(key)) {
                                    nodes.set(key, {
                                        coord: coords[i],
                                        degree: 0,
                                        roads: new Set()
                                    });
                                }
                            }
                        }
                    }
                });

                // STEP 3: Count edges at each node (degree calculation)
                roadsWithin.forEach((road, roadIdx) => {
                    if (road.geometry.type === 'LineString') {
                        const coords = road.geometry.coordinates;

                        // Find which coordinates in this road are actual nodes
                        const roadNodes = [];
                        coords.forEach(coord => {
                            const key = roundCoord(coord);
                            if (nodes.has(key)) {
                                roadNodes.push(key);
                            }
                        });

                        // Connect consecutive nodes - each connection adds degree
                        for (let i = 0; i < roadNodes.length - 1; i++) {
                            const node1 = nodes.get(roadNodes[i]);
                            const node2 = nodes.get(roadNodes[i + 1]);

                            // Each edge contributes 1 to degree at both endpoints
                            node1.degree++;
                            node2.degree++;

                            node1.roads.add(roadIdx);
                            node2.roads.add(roadIdx);
                        }
                    }
                });

                // STEP 4: Calculate road length
                let totalLength = 0;
                roadsWithin.forEach(road => {
                    if (road.geometry.type === 'LineString') {
                        try {
                            const line = turf.lineString(road.geometry.coordinates);
                            totalLength += turf.length(line, {units: 'kilometers'});
                        } catch (e) {
                            console.warn('Error calculating road length:', e);
                        }
                    }
                });

                metrics.road_length_km = totalLength.toFixed(2);
                metrics.road_density = (totalLength / metrics.area_km2).toFixed(2);

                // STEP 5: Calculate CNR and intersection metrics
                const totalNodes = nodes.size;
                const connectedNodes = Array.from(nodes.values()).filter(n => n.degree >= 3).length;

                metrics.cnr = totalNodes > 0 ? (connectedNodes / totalNodes).toFixed(3) : '0.000';
                metrics.intersection_count = connectedNodes;
                metrics.intersection_density = (connectedNodes / metrics.area_km2).toFixed(1);

                // STEP 6: Calculate degree distribution
                const degreeDistribution = {};
                nodes.forEach(node => {
                    degreeDistribution[node.degree] = (degreeDistribution[node.degree] || 0) + 1;
                });

                metrics.degree_distribution = degreeDistribution;
                metrics.total_nodes = totalNodes;
                metrics.dead_ends = degreeDistribution[1] || 0;
                metrics.three_way = degreeDistribution[3] || 0;
                metrics.four_way = degreeDistribution[4] || 0;

                // STEP 7: Calculate open space ratios
                // Fetch building footprints from OpenStreetMap
                showLoading('Fetching building footprints from OpenStreetMap...');
                const buildingData = await fetchBuildingData(polygonGeoJSON);

                if (buildingData && buildingData.totalBuiltArea >= 0) {
                    const builtRatio = buildingData.totalBuiltArea / areaM2;

                    // Calculate road area from road length (approximate)
                    // Assume average road width of 8 meters
                    const roadAreaM2 = totalLength * 1000 * 8; // km to m, times width
                    const roadRatio = roadAreaM2 / areaM2;

                    // Two metrics:
                    // 1. Open space (excluding only buildings) - simple version
                    metrics.open_ratio_buildings = Math.max(0, 1 - builtRatio).toFixed(3);

                    // 2. Open space (excluding buildings AND roads) - matches grid data
                    metrics.open_ratio_total = Math.max(0, 1 - builtRatio - roadRatio).toFixed(3);

                    metrics.building_count = buildingData.buildingCount;
                    metrics.built_area_km2 = (buildingData.totalBuiltArea / 1000000).toFixed(3);
                    metrics.road_area_km2 = (roadAreaM2 / 1000000).toFixed(3);
                } else {
                    // Fallback: mark as unavailable
                    metrics.open_ratio_buildings = 'N/A';
                    metrics.open_ratio_total = 'N/A';
                    metrics.building_count = 0;
                }

                // Try to load pre-computed grid data
                await loadPrecomputedGrid(polygonGeoJSON);

                return metrics;

            } catch (error) {
                console.error('Error calculating metrics:', error);
                return {
                    error: `Calculation failed: ${error.message}`,
                    area_km2: 0,
                    cnr: 0
                };
            } finally {
                hideLoading();
            }
        }

        // Load pre-computed grid data
        async function loadPrecomputedGrid(polygon) {
            try {
                const gridFile = `morphology_results/${currentCity}_morphology_grid.geojson`;
                const response = await fetch(gridFile);

                if (response.ok) {
                    const gridData = await response.json();

                    // Filter grid cells that intersect with polygon
                    const intersectingCells = gridData.features.filter(cell => {
                        try {
                            return turf.booleanIntersects(cell, polygon);
                        } catch {
                            return false;
                        }
                    });

                    if (intersectingCells.length > 0) {
                        // Calculate averages
                        const avgMetrics = {
                            cnr: 0,
                            intersection_density: 0,
                            block_size: 0
                        };

                        let validCells = 0;
                        intersectingCells.forEach(cell => {
                            const props = cell.properties;
                            if (props.intersection_density_km2) {
                                avgMetrics.intersection_density += props.intersection_density_km2;
                                validCells++;
                            }
                            if (props.median_block_size_m2) {
                                avgMetrics.block_size += props.median_block_size_m2;
                            }
                        });

                        if (validCells > 0) {
                            avgMetrics.intersection_density = (avgMetrics.intersection_density / validCells).toFixed(1);
                            avgMetrics.block_size = (avgMetrics.block_size / validCells).toFixed(0);

                            precomputedGridData = avgMetrics;
                            return avgMetrics;
                        }
                    }
                }
            } catch (error) {
                console.log('No pre-computed grid data available');
            }

            precomputedGridData = null;
            return null;
        }

        // Calculate metrics for a circle
        async function calculateCircleMetrics(circle, index) {
            try {
                // Convert Leaflet circle to proper GeoJSON polygon using turf
                const center = circle.getLatLng();
                const radiusKm = circle.getRadius() / 1000; // Convert meters to km

                // Create a proper polygon using turf.circle
                const circlePolygon = turf.circle([center.lng, center.lat], radiusKm, {
                    steps: 64,
                    units: 'kilometers'
                });

                console.log('Circle polygon created:', circlePolygon);

                const metrics = await calculateMetrics(circlePolygon);

                circleMetrics[index] = metrics;
                updateMetricsDisplay();
                hideLoading();

            } catch (error) {
                console.error('Error calculating circle metrics:', error);
                hideLoading();
            }
        }

        // Update metrics display for all circles
        function updateMetricsDisplay() {
            try {
                const content = document.getElementById('metrics-content');

                if (!content) {
                    console.error('Metrics content element not found');
                    return;
                }

                if (circleMetrics.length === 0) {
                    closeMetricsPanel();
                    return;
                }

                let html = '<div class="circles-comparison">';

            circleMetrics.forEach((metrics, index) => {
                const circleNum = index + 1;
                const color = circleColors[index];
                const circle = lockedCircles[index];
                const label = circle?.label || circleLabels[index];
                const location = circle?.location;
                const radius = circle?.radius || 0;

                // Format radius for display
                const radiusDisplay = radius >= 1000 ? `${(radius/1000).toFixed(1)} km` : `${radius} m`;

                // Get location text
                let locationHTML = '<span class="loading-location">Fetching location...</span>';
                if (location) {
                    locationHTML = `
                        <span class="location-icon">📍</span>
                        <span class="location-text" title="${location.fullAddress || location.coords}">${location.name}</span>
                    `;
                } else if (circle) {
                    locationHTML = `
                        <span class="location-icon">📍</span>
                        <span class="location-text">${circle.latlng.lat.toFixed(4)}, ${circle.latlng.lng.toFixed(4)}</span>
                    `;
                }

                // Check for errors
                if (metrics.error) {
                    html += `
                        <div class="circle-metrics-card circle-${circleNum}">
                            <div class="card-color-bar"></div>
                            <div class="circle-card-header">
                                <div class="circle-header-top">
                                    <span class="circle-card-title">● Location ${label}</span>
                                    <div class="circle-header-actions">
                                        <div class="remove-circle-btn" onclick="removeCircle(${index})" title="Remove">×</div>
                                    </div>
                                </div>
                                <div class="circle-location">${locationHTML}</div>
                            </div>
                            <div style="padding: 14px; background: var(--surface2); border: 1px solid var(--accent); border-radius: 2px; color: var(--accent2); font-size: 11px;">
                                <strong style="text-transform: uppercase; letter-spacing: 0.08em; font-weight: 700;">⚠ ${metrics.error}</strong>
                            </div>
                        </div>
                    `;
                    return;
                }

                // Calculate classes for styling
                let cnrClass = 'low';
                const cnrValue = parseFloat(metrics.cnr);
                if (cnrValue > 0.4) cnrClass = 'good';
                else if (cnrValue > 0.25) cnrClass = 'medium';

                // Use the total open ratio (excluding roads+buildings) for comparison with grid
                let openClass = 'medium';
                let openDisplayValue = 'N/A';
                let openSimpleValue = 'N/A';

                if (metrics.open_ratio_total && metrics.open_ratio_total !== 'N/A') {
                    const openValue = parseFloat(metrics.open_ratio_total);
                    openDisplayValue = `${(openValue * 100).toFixed(1)}%`;
                    if (openValue > 0.3) openClass = 'good';
                    else if (openValue < 0.1) openClass = 'low';
                } else if (metrics.open_ratio_buildings && metrics.open_ratio_buildings !== 'N/A') {
                    // Fallback to buildings-only if total not available
                    const openValue = parseFloat(metrics.open_ratio_buildings);
                    openDisplayValue = `${(openValue * 100).toFixed(1)}%`;
                }

                if (metrics.open_ratio_buildings && metrics.open_ratio_buildings !== 'N/A') {
                    openSimpleValue = `${(parseFloat(metrics.open_ratio_buildings) * 100).toFixed(1)}%`;
                }

                html += `
                    <div class="circle-metrics-card circle-${circleNum}">
                        <div class="card-color-bar"></div>
                        <div class="circle-card-header">
                            <div class="circle-header-top">
                                <span class="circle-card-title">● Location ${label} · ${radiusDisplay}</span>
                                <div class="circle-header-actions">
                                    <div class="circle-action-btn" onclick="jumpToCircle(${index})" title="Jump to location">🎯</div>
                                    <div class="remove-circle-btn" onclick="removeCircle(${index})" title="Remove">×</div>
                                </div>
                            </div>
                            <div class="circle-location">${locationHTML}</div>
                        </div>

                        <div class="card-metrics-body">
                            <div class="metric-row">
                                <span class="metric-label">Area</span>
                                <span class="metric-value">${metrics.area_km2} km²</span>
                            </div>
                            <div class="metric-row">
                                <span class="metric-label">CNR</span>
                                <span class="metric-value ${cnrClass}">${metrics.cnr}</span>
                            </div>
                            <div class="metric-row">
                                <span class="metric-label">Road Density</span>
                                <span class="metric-value">${metrics.road_density || 0} km/km²</span>
                            </div>
                            <div class="metric-row">
                                <span class="metric-label">Intersections</span>
                                <span class="metric-value">${metrics.intersection_count}</span>
                            </div>
                            <div class="metric-row">
                                <span class="metric-label">Int. Density</span>
                                <span class="metric-value">${metrics.intersection_density}/km²</span>
                            </div>
                            ${metrics.building_count !== undefined ? `
                            <div class="metric-row">
                                <span class="metric-label">Buildings</span>
                                <span class="metric-value">${metrics.building_count}</span>
                            </div>
                            <div class="metric-row">
                                <span class="metric-label">Built Area</span>
                                <span class="metric-value">${metrics.built_area_km2 || 0} km²</span>
                            </div>
                            ` : ''}
                            <div class="metric-row">
                                <span class="metric-label">Open (no rd/bld)</span>
                                <span class="metric-value ${openClass}">${openDisplayValue}</span>
                            </div>
                            ${openSimpleValue !== 'N/A' ? `
                            <div class="metric-row">
                                <span class="metric-label">Open (no bldgs)</span>
                                <span class="metric-value">${openSimpleValue}</span>
                            </div>
                            ` : ''}
                            <div class="metric-row">
                                <span class="metric-label">Dead Ends</span>
                                <span class="metric-value">${metrics.dead_ends || 0}</span>
                            </div>
                            <div class="metric-row">
                                <span class="metric-label">4-way Int.</span>
                                <span class="metric-value">${metrics.four_way || 0}</span>
                            </div>
                        </div>
                    </div>
                `;
            });

            html += '</div>';

                content.innerHTML = html;
                document.getElementById('metrics-panel').classList.add('visible');
            } catch (error) {
                console.error('Error updating metrics display:', error);
                // Gracefully fail - don't crash the UI
            }
        }

        function closeMetricsPanel() {
            document.getElementById('metrics-panel').classList.remove('visible');
        }

        // Circle tool functions
        function createHoverCircle(latlng, radius) {
            // Optimize: Only recreate if radius changed, otherwise just update position
            if (hoverElements && hoverElements.radius === radius) {
                // Just update positions of existing elements
                updateHoverCirclePosition(latlng, radius);
                return;
            }

            // Remove old hover circle if exists
            if (hoverCircle) {
                map.removeLayer(hoverCircle);
                hoverCircle = null;
                hoverElements = null; // CRITICAL: Clear cache when recreating
            }

            // Create spotlight effect - dark overlay with circle cutout
            const bounds = map.getBounds();
            const padding = 0.1;
            const outerRing = [
                [bounds.getSouth() - padding, bounds.getWest() - padding],
                [bounds.getSouth() - padding, bounds.getEast() + padding],
                [bounds.getNorth() + padding, bounds.getEast() + padding],
                [bounds.getNorth() + padding, bounds.getWest() - padding],
                [bounds.getSouth() - padding, bounds.getWest() - padding]
            ];

            // Create circle hole using turf - REVERSE winding order for hole
            const radiusKm = radius / 1000;
            const innerCircle = turf.circle([latlng.lng, latlng.lat], radiusKm, {
                steps: 32, // Reduced from 64 for better performance
                units: 'kilometers'
            });

            // Reverse the inner circle coordinates for proper hole rendering
            const innerRing = innerCircle.geometry.coordinates[0].slice().reverse();

            // Create polygon with hole - darken outside, leave inside bright
            const spotlightPolygon = {
                type: 'Feature',
                geometry: {
                    type: 'Polygon',
                    coordinates: [outerRing, innerRing]
                }
            };

            const spotlight = L.geoJSON(spotlightPolygon, {
                style: {
                    fillColor: '#000',
                    fillOpacity: 0.6,
                    color: 'transparent',
                    weight: 0
                },
                interactive: false,
                pane: 'overlayPane'
            });

            // Create bright inner circle (slight brightness boost)
            const brightCircle = L.circle(latlng, {
                radius: radius,
                fillColor: '#FFF',
                fillOpacity: 0.08,
                color: 'transparent',
                weight: 0,
                interactive: false
            });

            // Create magnifying lens circle border
            const lensCircle = L.circle(latlng, {
                radius: radius,
                color: '#D97706',
                fillColor: 'transparent',
                fillOpacity: 0,
                weight: 3,
                opacity: 1,
                className: 'magnifying-lens'
            });

            // Add center crosshair
            const crosshairSize = 15;
            const crosshair1 = L.polyline([
                [latlng.lat, latlng.lng - crosshairSize / 111320],
                [latlng.lat, latlng.lng + crosshairSize / 111320]
            ], {
                color: '#D97706',
                weight: 2,
                opacity: 0.8
            });

            const crosshair2 = L.polyline([
                [latlng.lat - crosshairSize / 110540, latlng.lng],
                [latlng.lat + crosshairSize / 110540, latlng.lng]
            ], {
                color: '#D97706',
                weight: 2,
                opacity: 0.8
            });

            // Cache elements for reuse
            hoverElements = {
                spotlight,
                brightCircle,
                lensCircle,
                crosshair1,
                crosshair2,
                radius
            };

            // Group ALL hover elements together
            hoverCircle = L.layerGroup([spotlight, brightCircle, lensCircle, crosshair1, crosshair2]);

            // Add the complete group to the map
            hoverCircle.addTo(map);

            // Store last position
            lastHoverPosition = latlng;

            // Create magnifying lens effect
            createMagnification(latlng, radius);

            // Calculate and show quick preview metrics
            showHoverPreview(latlng, radius);
        }

        // Optimized function to just update positions without recreating
        function updateHoverCirclePosition(latlng, radius) {
            if (!hoverElements || !hoverCircle) {
                return;
            }

            // Check if moved enough to warrant update
            if (lastHoverPosition) {
                const distance = map.latLngToContainerPoint(latlng).distanceTo(
                    map.latLngToContainerPoint(lastHoverPosition)
                );
                if (distance < HOVER_MOVE_THRESHOLD) {
                    return; // Skip update if moved less than threshold
                }
            }

            // Use RAF for smooth updates
            if (hoverAnimationFrame) {
                cancelAnimationFrame(hoverAnimationFrame);
            }

            hoverAnimationFrame = requestAnimationFrame(() => {
                // Update circle positions
                hoverElements.brightCircle.setLatLng(latlng);
                hoverElements.lensCircle.setLatLng(latlng);

                // Update crosshairs
                const crosshairSize = 15;
                hoverElements.crosshair1.setLatLngs([
                    [latlng.lat, latlng.lng - crosshairSize / 111320],
                    [latlng.lat, latlng.lng + crosshairSize / 111320]
                ]);
                hoverElements.crosshair2.setLatLngs([
                    [latlng.lat - crosshairSize / 110540, latlng.lng],
                    [latlng.lat + crosshairSize / 110540, latlng.lng]
                ]);

                // Update spotlight - recreate only if needed (less frequent)
                const bounds = map.getBounds();
                const padding = 0.1;
                const outerRing = [
                    [bounds.getSouth() - padding, bounds.getWest() - padding],
                    [bounds.getSouth() - padding, bounds.getEast() + padding],
                    [bounds.getNorth() + padding, bounds.getEast() + padding],
                    [bounds.getNorth() + padding, bounds.getWest() - padding],
                    [bounds.getSouth() - padding, bounds.getWest() - padding]
                ];

                const radiusKm = radius / 1000;
                const innerCircle = turf.circle([latlng.lng, latlng.lat], radiusKm, {
                    steps: 32,
                    units: 'kilometers'
                });
                const innerRing = innerCircle.geometry.coordinates[0].slice().reverse();

                const spotlightPolygon = {
                    type: 'Feature',
                    geometry: {
                        type: 'Polygon',
                        coordinates: [outerRing, innerRing]
                    }
                };

                // Remove old spotlight and add new one
                hoverCircle.removeLayer(hoverElements.spotlight);
                hoverElements.spotlight = L.geoJSON(spotlightPolygon, {
                    style: {
                        fillColor: '#000',
                        fillOpacity: 0.6,
                        color: 'transparent',
                        weight: 0
                    },
                    interactive: false,
                    pane: 'overlayPane'
                });
                hoverCircle.addLayer(hoverElements.spotlight);

                lastHoverPosition = latlng;

                // Update magnifying lens position
                createMagnification(latlng, radius);

                // Update hover preview with new position
                showHoverPreview(latlng, radius);
            });
        }

        // Create magnification effect inside circle
        // Create magnifying lens effect
        function createMagnification(latlng, radius) {
            try {
                const lens = document.getElementById('magnify-lens');
                if (!lens) {
                    console.warn('Magnify lens element not found');
                    return;
                }

                // Calculate lens size and position
                const pixelPos = map.latLngToContainerPoint(latlng);
                const lensSize = Math.min(radius * 0.4, 200); // Lens diameter in pixels (smaller than circle)

                // Position and size the lens
                lens.style.width = lensSize + 'px';
                lens.style.height = lensSize + 'px';
                lens.style.left = (pixelPos.x - lensSize / 2) + 'px';
                lens.style.top = (pixelPos.y - lensSize / 2) + 'px';
                lens.classList.add('active');

                // Initialize magnify map if not exists
                if (!magnifyMap) {
                    const container = document.getElementById('magnify-map-container');
                    if (!container) {
                        console.warn('Magnify map container not found');
                        return;
                    }

                magnifyMap = L.map(container, {
                    zoomControl: false,
                    attributionControl: false,
                    dragging: false,
                    touchZoom: false,
                    scrollWheelZoom: false,
                    doubleClickZoom: false,
                    boxZoom: false,
                    keyboard: false,
                    tap: false
                });

                // Add the same basemap as main map
                const basemapSelect = document.getElementById('basemap-select');
                const basemapType = basemapSelect ? basemapSelect.value : 'osm';

                let tileLayer;
                if (basemapType === 'dark') {
                    tileLayer = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
                        maxZoom: 19,
                        subdomains: 'abcd'
                    });
                } else if (basemapType === 'satellite') {
                    tileLayer = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
                        maxZoom: 19
                    });
                } else if (basemapType === 'cartodb') {
                    tileLayer = L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
                        maxZoom: 19,
                        subdomains: 'abcd'
                    });
                } else if (basemapType === 'terrain') {
                    tileLayer = L.tileLayer('https://stamen-tiles-{s}.a.ssl.fastly.net/terrain/{z}/{x}/{y}{r}.png', {
                        maxZoom: 18,
                        subdomains: 'abcd'
                    });
                } else {
                    tileLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                        maxZoom: 19,
                        attribution: ''
                    });
                }

                tileLayer.addTo(magnifyMap);
            }

            // Synchronize magnify map with main map
            const mainZoom = map.getZoom();
            const magnifyZoom = Math.min(mainZoom + MAGNIFY_ZOOM_OFFSET, 19);

            magnifyMap.setView(latlng, magnifyZoom, { animate: false });
            } catch (error) {
                console.error('Error creating magnification:', error);
                // Fail gracefully - don't break hover functionality
            }
        }

        // Remove magnification lens
        function removeMagnification() {
            const lens = document.getElementById('magnify-lens');
            if (lens) {
                lens.classList.remove('active');
            }
            // Note: We keep magnifyMap alive for reuse (performance optimization)
        }

        // Load cached grid data for quick preview
        async function loadCachedGridData() {
            if (cachedGridData) {
                console.log('Using already cached grid data:', cachedGridData.features.length, 'cells');
                return cachedGridData;
            }

            try {
                const gridFile = `morphology_results/${currentCity}_morphology_grid.geojson`;
                console.log('Attempting to load grid data from:', gridFile);

                const response = await fetch(gridFile);

                if (response.ok) {
                    cachedGridData = await response.json();
                    console.log(`✅ Cached grid data loaded successfully: ${cachedGridData.features.length} cells for ${currentCity}`);
                    return cachedGridData;
                } else {
                    console.log(`❌ Grid data file not found (HTTP ${response.status}):`, gridFile);
                }
            } catch (error) {
                console.log('❌ Error loading grid data for', currentCity, ':', error.message);
            }

            cachedGridData = null;
            return null;
        }

        // Get metrics from pre-computed grid
        function getGridMetricsForCircle(latlng, radius) {
            if (!cachedGridData) {
                console.log('⚠️ No cached grid data available for hover preview');
                return null;
            }

            console.log('🔍 Searching grid data for circle at', latlng, 'radius:', radius);

            const radiusKm = radius / 1000;
            const circlePolygon = turf.circle([latlng.lng, latlng.lat], radiusKm, {
                steps: 32,
                units: 'kilometers'
            });

            // Find intersecting grid cells
            const intersectingCells = cachedGridData.features.filter(cell => {
                try {
                    return turf.booleanIntersects(cell, circlePolygon);
                } catch (e) {
                    console.warn('Error checking cell intersection:', e);
                    return false;
                }
            });

            console.log(`Found ${intersectingCells.length} intersecting grid cells`);

            if (intersectingCells.length === 0) {
                console.log('⚠️ No grid cells intersect with this circle');
                return null;
            }

            // Calculate averages from available properties
            let totalIntersections = 0;
            let avgIntDensity = 0;
            let avgBlockSize = 0;
            let avgOpenRatio = 0;
            let validCells = 0;
            let blockSizeCells = 0;

            intersectingCells.forEach((cell, idx) => {
                const props = cell.properties;
                if (idx === 0) console.log(`Sample cell properties:`, props);

                if (props.intersection_density_km2 !== undefined && props.intersection_density_km2 !== null) {
                    avgIntDensity += props.intersection_density_km2;
                    validCells++;
                }

                if (props.intersections !== undefined && props.intersections !== null) {
                    totalIntersections += props.intersections;
                }

                if (props.median_block_size_m2 !== undefined && props.median_block_size_m2 !== null) {
                    avgBlockSize += props.median_block_size_m2;
                    blockSizeCells++;
                }

                if (props.open_ratio !== undefined && props.open_ratio !== null) {
                    avgOpenRatio += props.open_ratio;
                }
            });

            console.log(`Valid cells: ${validCells}`);
            console.log(`  avgIntDensity: ${avgIntDensity}, total: ${(avgIntDensity / validCells).toFixed(2)}`);
            console.log(`  avgOpenRatio: ${avgOpenRatio}, avg: ${(avgOpenRatio / validCells).toFixed(3)}`);
            console.log(`  total intersections: ${totalIntersections}`);

            if (validCells === 0) {
                console.log('⚠️ No valid cells with data');
                return null;
            }

            // Estimate CNR from intersection density (higher density = better connectivity)
            // Rough heuristic: CNR ~= intersection_density / 150 (capped at 0.8)
            const avgDensity = avgIntDensity / validCells;
            const estimatedCNR = Math.min(avgDensity / 150, 0.8);

            const result = {
                cnr: estimatedCNR.toFixed(3),
                intersection_density: (avgIntDensity / validCells).toFixed(1),
                block_size: blockSizeCells > 0 ? (avgBlockSize / blockSizeCells).toFixed(0) : 'N/A',
                open_ratio: ((avgOpenRatio / validCells) * 100).toFixed(1) + '%',
                intersections: totalIntersections,
                cells: intersectingCells.length
            };

            console.log('✅ Grid metrics calculated:', result);
            return result;
        }

        // Show hover preview with quick metrics
        function showHoverPreview(latlng, radius) {
            const preview = document.getElementById('hover-preview');
            const content = document.getElementById('hover-preview-content');

            if (!preview || !content) {
                console.error('Preview elements not found - DOM may not be ready');
                return;
            }

            // Calculate area immediately
            const areaM2 = Math.PI * radius * radius;
            const areaKm2 = (areaM2 / 1000000).toFixed(3);

            // Try to get pre-computed grid metrics first
            const gridMetrics = getGridMetricsForCircle(latlng, radius);

            if (gridMetrics) {
                // Show pre-computed metrics immediately!
                content.innerHTML = `
                    <div class="hover-preview-metric">
                        <span class="hover-preview-label">Area</span>
                        <span class="hover-preview-value">${areaKm2} km²</span>
                    </div>
                    <div class="hover-preview-metric">
                        <span class="hover-preview-label">CNR (Est.)</span>
                        <span class="hover-preview-value">${gridMetrics.cnr}</span>
                    </div>
                    <div class="hover-preview-metric">
                        <span class="hover-preview-label">Int. Density</span>
                        <span class="hover-preview-value">${gridMetrics.intersection_density}/km²</span>
                    </div>
                    <div class="hover-preview-metric">
                        <span class="hover-preview-label">Intersections</span>
                        <span class="hover-preview-value">${gridMetrics.intersections}</span>
                    </div>
                    <div class="hover-preview-metric">
                        <span class="hover-preview-label">Open (no roads/bldgs)</span>
                        <span class="hover-preview-value">${gridMetrics.open_ratio}</span>
                    </div>
                    <div style="font-size: 9px; color: var(--ink3); margin-top: 4px; text-align: center;">
                        📊 Pre-computed from ${gridMetrics.cells} grid cells<br>
                        <span style="color: var(--accent2);">Click to lock for exact calculation</span>
                    </div>
                `;
            } else {
                // Fallback to basic info
                content.innerHTML = `
                    <div class="hover-preview-metric">
                        <span class="hover-preview-label">Radius</span>
                        <span class="hover-preview-value">${radius >= 1000 ? (radius/1000).toFixed(1) + ' km' : radius + ' m'}</span>
                    </div>
                    <div class="hover-preview-metric">
                        <span class="hover-preview-label">Area</span>
                        <span class="hover-preview-value">${areaKm2} km²</span>
                    </div>
                    <div class="hover-preview-metric">
                        <span class="hover-preview-label">CNR</span>
                        <span class="hover-preview-value">⏳</span>
                    </div>
                    <div class="hover-preview-metric">
                        <span class="hover-preview-label">Intersections</span>
                        <span class="hover-preview-value">⏳</span>
                    </div>
                    <div style="font-size: 9px; color: var(--ink3); margin-top: 4px; text-align: center;">
                        Calculating from roads...
                    </div>
                `;

                // Clear existing timeout
                if (hoverPreviewTimeout) {
                    clearTimeout(hoverPreviewTimeout);
                }

                // Calculate from roads with debounce
                hoverPreviewTimeout = setTimeout(() => {
                    let roadCount = 'N/A';
                    let estimatedIntersections = 'N/A';

                    if (loadedRoadsData) {
                        const radiusKm = radius / 1000;
                        const circlePolygon = turf.circle([latlng.lng, latlng.lat], radiusKm, {
                            steps: 32,
                            units: 'kilometers'
                        });

                        const quickRoadCount = loadedRoadsData.features.filter(road => {
                            if (road.geometry.type !== 'LineString') return false;
                            try {
                                return turf.booleanIntersects(road, circlePolygon);
                            } catch {
                                return false;
                            }
                        }).length;

                        roadCount = quickRoadCount;
                        estimatedIntersections = Math.round(quickRoadCount * 0.15);
                    }

                    content.innerHTML = `
                        <div class="hover-preview-metric">
                            <span class="hover-preview-label">Area</span>
                            <span class="hover-preview-value">${areaKm2} km²</span>
                        </div>
                        <div class="hover-preview-metric">
                            <span class="hover-preview-label">Roads</span>
                            <span class="hover-preview-value">${roadCount}</span>
                        </div>
                        <div class="hover-preview-metric">
                            <span class="hover-preview-label">Est. Intersections</span>
                            <span class="hover-preview-value">~${estimatedIntersections}</span>
                        </div>
                        <div style="font-size: 9px; color: var(--ink3); margin-top: 4px; text-align: center;">
                            Quick estimate
                        </div>
                    `;
                }, 200);
            }

            preview.classList.add('visible');
        }

        function hideHoverPreview() {
            if (hoverPreviewTimeout) {
                clearTimeout(hoverPreviewTimeout);
                hoverPreviewTimeout = null;
            }
            if (hoverAnimationFrame) {
                cancelAnimationFrame(hoverAnimationFrame);
                hoverAnimationFrame = null;
            }
            document.getElementById('hover-preview').classList.remove('visible');
        }

        async function lockCircle(latlng, radius) {
            if (lockedCircles.length >= MAX_CIRCLES) {
                alert(`Maximum ${MAX_CIRCLES} circles reached. Remove a circle to add a new one.`);
                return;
            }

            const colorIndex = lockedCircles.length;
            const color = circleColors[colorIndex];
            const label = circleLabels[colorIndex];

            const circle = L.circle(latlng, {
                radius: radius,
                color: color,
                fillColor: color,
                fillOpacity: 0.15,
                weight: 2.5,
                opacity: 0.9
            }).addTo(map);

            // Add center marker
            const centerMarker = L.circleMarker(latlng, {
                radius: 6,
                fillColor: color,
                color: '#fff',
                weight: 2,
                opacity: 1,
                fillOpacity: 1
            }).addTo(map);

            // Add location label with letter identifier
            const radiusLabel = L.marker(latlng, {
                icon: L.divIcon({
                    className: 'circle-label',
                    html: `<div style="background: ${color}; color: #fff; padding: 6px 10px; border-radius: 3px; font-size: 11px; font-weight: 700; font-family: 'IBM Plex Mono', monospace; white-space: nowrap; box-shadow: 0 3px 8px rgba(0,0,0,0.4); border: 2px solid #fff;">
                        <span style="font-size: 13px; margin-right: 4px;">●</span> LOCATION ${label}
                    </div>`,
                    iconAnchor: [0, -radius / 2 - 15]
                })
            }).addTo(map);

            const circleGroup = L.layerGroup([circle, centerMarker, radiusLabel]);

            const circleData = {
                group: circleGroup,
                circle: circle,
                latlng: latlng,
                radius: radius,
                index: lockedCircles.length,
                label: label,
                location: null  // Will be populated by reverse geocoding
            };

            lockedCircles.push(circleData);

            // Fetch location info asynchronously
            reverseGeocode(latlng.lat, latlng.lng).then(locationData => {
                if (locationData) {
                    circleData.location = locationData;
                    // Update the display with location info
                    updateMetricsDisplay();
                }
            });

            // Calculate metrics for this circle
            calculateCircleMetrics(circle, lockedCircles.length - 1);
        }

        function jumpToCircle(index) {
            if (lockedCircles[index]) {
                const circle = lockedCircles[index];
                map.setView(circle.latlng, 15, { animate: true, duration: 0.5 });
            }
        }

        function removeCircle(index) {
            if (lockedCircles[index]) {
                map.removeLayer(lockedCircles[index].group);
                lockedCircles.splice(index, 1);
                circleMetrics.splice(index, 1);

                // Rebuild display with updated indices
                updateMetricsDisplay();

                if (lockedCircles.length === 0) {
                    closeMetricsPanel();
                }
            }
        }

        function clearAllCircles() {
            // Remove all locked circles from map
            lockedCircles.forEach(item => {
                if (item.group && map.hasLayer(item.group)) {
                    map.removeLayer(item.group);
                }
            });
            lockedCircles = [];
            circleMetrics = [];

            // Clear any pending calculations
            if (hoverPreviewTimeout) {
                clearTimeout(hoverPreviewTimeout);
                hoverPreviewTimeout = null;
            }

            closeMetricsPanel();
        }

        // Map event handlers for circle tool
        // Throttled mousemove handler for better performance
        const handleMouseMove = throttle(function(e) {
            if (circleToolActive && lockedCircles.length < MAX_CIRCLES) {
                createHoverCircle(e.latlng, currentRadius);

                // Position hover preview tooltip
                const preview = document.getElementById('hover-preview');
                preview.style.left = (e.originalEvent.pageX + 20) + 'px';
                preview.style.top = (e.originalEvent.pageY + 20) + 'px';
            }
        }, 50); // Update at most every 50ms (20fps)

        map.on('mousemove', handleMouseMove);

        map.on('mouseout', function() {
            if (hoverCircle) {
                map.removeLayer(hoverCircle);
                hoverCircle = null;
                hoverElements = null; // CRITICAL: Clear cache
                spotlightOverlay = null; // Reset reference
                lastHoverPosition = null;
            }
            removeMagnification();
            hideHoverPreview();
        });

        map.on('click', function(e) {
            if (circleToolActive) {
                // Hide hover preview and remove hover circle (includes spotlight)
                hideHoverPreview();
                removeMagnification();
                if (hoverCircle) {
                    map.removeLayer(hoverCircle);
                    hoverCircle = null;
                    hoverElements = null; // Clear cached elements
                    spotlightOverlay = null;
                    lastHoverPosition = null;
                }

                // Lock the circle and calculate full metrics
                lockCircle(e.latlng, currentRadius);
            }
        });

        // UI controls
        document.getElementById('circle-tool-btn').addEventListener('click', function() {
            circleToolActive = !circleToolActive;
            this.classList.toggle('active');

            if (circleToolActive) {
                document.getElementById('map').classList.add('circle-tool-active');
            } else {
                document.getElementById('map').classList.remove('circle-tool-active');
                if (hoverCircle) {
                    map.removeLayer(hoverCircle);
                    hoverCircle = null;
                    hoverElements = null; // Clear cache
                    spotlightOverlay = null;
                    lastHoverPosition = null;
                }
                removeMagnification();
                hideHoverPreview();
            }
        });

        document.getElementById('clear-circles-btn').addEventListener('click', function() {
            clearAllCircles();
        });

        document.getElementById('radius-select').addEventListener('change', function() {
            currentRadius = parseInt(this.value);

            // Clear cached hover elements to force recreation with new radius
            if (hoverCircle) {
                map.removeLayer(hoverCircle);
                hoverCircle = null;
            }
            hoverElements = null;
            lastHoverPosition = null;
            removeMagnification();
        });

        // Load city function
        async function loadCity(cityKey) {
            showLoading('Loading city data...');

            // Clear existing layers
            Object.values(loadedLayers).forEach(layer => {
                if (layer && map.hasLayer(layer)) {
                    map.removeLayer(layer);
                }
            });
            loadedLayers = {};
            loadedRoadsData = null;
            cachedGridData = null; // Reset cached grid data

            // Clear hover state when switching cities
            if (hoverCircle) {
                map.removeLayer(hoverCircle);
                hoverCircle = null;
            }
            hoverElements = null;
            lastHoverPosition = null;
            if (hoverAnimationFrame) {
                cancelAnimationFrame(hoverAnimationFrame);
                hoverAnimationFrame = null;
            }
            removeMagnification();
            hideHoverPreview();

            // Update active city button
            document.querySelectorAll('.city-btn').forEach(btn => {
                btn.classList.remove('active');
            });
            document.querySelector(`[data-city="${cityKey}"]`).classList.add('active');

            const city = cities[cityKey];
            currentCity = cityKey;

            // Fly to city
            map.flyTo(city.center, city.zoom, { duration: 1.5 });

            // Build layer controls
            const layerControls = document.getElementById('layer-controls');
            layerControls.innerHTML = '';

            const layerGroup = document.createElement('div');
            layerGroup.className = 'layer-group';

            const groupTitle = document.createElement('div');
            groupTitle.className = 'layer-group-title';
            groupTitle.textContent = city.name;
            layerGroup.appendChild(groupTitle);

            // Create layer items
            for (const [layerName, layerUrl] of Object.entries(city.layers)) {
                const layerItem = document.createElement('div');
                layerItem.className = 'layer-item';

                const checkbox = document.createElement('input');
                checkbox.type = 'checkbox';
                checkbox.id = `layer-${layerName.replace(/\s/g, '-')}`;
                checkbox.dataset.layer = layerName;
                checkbox.dataset.url = layerUrl;

                // Auto-load major roads
                if (layerName.includes('Major') || layerName.includes('Ward') ||
                    layerName.includes('Arrond') || layerName.includes('Planning')) {
                    checkbox.checked = true;
                }

                checkbox.addEventListener('change', async function(e) {
                    if (e.target.checked) {
                        // Show file size warning for large datasets
                        const fileSizes = {
                            'tokyo': '120MB',
                            'singapore': '87MB',
                            'paris': '62MB',
                            'bogota': '48MB',
                            'cairo': '47MB',
                            'barcelona': '44MB',
                            'nairobi': '18MB'
                        };

                        if (layerName.includes('All Roads')) {
                            const size = fileSizes[currentCity] || 'large';
                            showLoading(`Loading ${size} road network... This may take 10-30 seconds`);
                        } else {
                            showLoading('Loading layer...');
                        }

                        if (!loadedLayers[layerName]) {
                            loadedLayers[layerName] = await loadLayer(layerUrl, layerName);
                        }

                        if (loadedLayers[layerName]) {
                            loadedLayers[layerName].addTo(map);
                        } else {
                            // Failed to load - uncheck the box
                            e.target.checked = false;
                        }

                        hideLoading();
                    } else {
                        if (loadedLayers[layerName]) {
                            map.removeLayer(loadedLayers[layerName]);
                        }

                        // Clear road data if All Roads was unchecked
                        if (layerName.includes('All Roads')) {
                            loadedRoadsData = null;
                        }
                    }
                });

                const label = document.createElement('span');
                label.className = 'layer-name';
                label.textContent = layerName;

                // Add file size indicator for All Roads
                if (layerName.includes('All Roads')) {
                    const fileSizes = {
                        'tokyo': '120MB',
                        'singapore': '87MB',
                        'paris': '62MB',
                        'bogota': '48MB',
                        'cairo': '47MB',
                        'barcelona': '44MB',
                        'nairobi': '18MB'
                    };
                    const sizeSpan = document.createElement('span');
                    sizeSpan.style.fontSize = '9px';
                    sizeSpan.style.color = 'var(--ink3)';
                    sizeSpan.style.marginLeft = '6px';
                    sizeSpan.textContent = `(${fileSizes[currentCity] || '~50MB'})`;
                    label.appendChild(sizeSpan);
                }

                layerItem.appendChild(checkbox);
                layerItem.appendChild(label);
                layerGroup.appendChild(layerItem);
            }

            layerControls.appendChild(layerGroup);

            // Load default layers (skip All Roads by default - too large)
            for (const [layerName, layerUrl] of Object.entries(city.layers)) {
                const checkbox = document.getElementById(`layer-${layerName.replace(/\s/g, '-')}`);
                if (checkbox && checkbox.checked) {
                    // Skip All Roads on initial load - let user manually enable it
                    if (layerName.includes('All Roads')) {
                        console.log(`⚠️  Skipping auto-load of ${layerName} (large file). Check the box to load manually.`);
                        checkbox.checked = false;
                        continue;
                    }

                    loadedLayers[layerName] = await loadLayer(layerUrl, layerName);
                    if (loadedLayers[layerName]) {
                        loadedLayers[layerName].addTo(map);
                    }
                }
            }

            // Load pre-computed grid data for quick previews (await to ensure it's ready)
            console.log('Loading grid data for', cityKey);
            await loadCachedGridData();

            hideLoading();
        }

        // City selector event listeners
        document.querySelectorAll('.city-btn').forEach(btn => {
            btn.addEventListener('click', function() {
                loadCity(this.dataset.city);
            });
        });

        // Basemap selector
        document.getElementById('basemap-select').addEventListener('change', function(e) {
            Object.values(baseLayers).forEach(layer => map.removeLayer(layer));
            baseLayers[e.target.value].addTo(map);
        });

        // Initialize circle tool cursor
        document.getElementById('map').classList.add('circle-tool-active');

        // Make functions globally accessible for onclick handlers
        window.removeCircle = removeCircle;
        window.jumpToCircle = jumpToCircle;

        // Load Tokyo by default
        loadCity('tokyo');

        // Add scale control
        L.control.scale({
            imperial: false,
            position: 'bottomleft'
        }).addTo(map);
    </script>
</body>
</html>
