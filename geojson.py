from typing import Dict, Any, List

import numpy as np
from pyproj import CRS
from shapely.geometry import box, Point, mapping
from sqlalchemy.orm import Session

import geopandas as gpd

from models import ConcentrationInfo, PointSource, CadastreSource
from processing import wgs84_point_to_crs, crs_point_to_wgs84
from util import MSK_48_CRS


def _choose_project_crs_for_lonlat(lon: float, lat: float) -> CRS:
    """
    Рекомендуемый простой выбор проекции: локальный UTM (точнее для метрических размеров).
    Возвращает pyproj.CRS объекта UTM зоны для данной lon/lat.
    """
    zone = int((lon + 180) / 6) + 1
    is_northern = lat >= 0
    crs_utm = CRS.from_dict({'proj': 'utm', 'zone': zone, 'south': not is_northern})
    return crs_utm

def _swap_coords_geom(geom: Dict[str, Any]) -> Dict[str, Any]:
    """
    Swap coordinates [lon, lat] -> [lat, lon] for Polygon/MultiPolygon GeoJSON geometry dict.
    Returns a new geometry dict.
    """
    gtype = geom.get("type")
    coords = geom.get("coordinates")
    if gtype == "Polygon":
        new_coords = []
        for ring in coords:
            new_ring = [[c[1], c[0]] for c in ring]
            new_coords.append(new_ring)
        return {"type": "Polygon", "coordinates": new_coords}
    elif gtype == "MultiPolygon":
        new_mp = []
        for poly in coords:
            new_poly = []
            for ring in poly:
                new_ring = [[c[1], c[0]] for c in ring]
                new_poly.append(new_ring)
            new_mp.append(new_poly)
        return {"type": "MultiPolygon", "coordinates": new_mp}
    elif gtype == "Point":
        x, y = coords
        return {"type": "Point", "coordinates": [y, x]}
    else:
        # generic recursive swap for other types (LineString etc.)
        def swap_any(c):
            if not c:
                return c
            if isinstance(c[0], list):
                return [swap_any(cc) for cc in c]
            return [[cc[1], cc[0]] for cc in c]
        return {"type": gtype, "coordinates": swap_any(coords)}


def generate_geojson_for_map_timestamp(
        db_session: Session,
        map_id: int,
        timestamp: str,
        include_point_sources: bool = True,
        include_cadastre_sources: bool = True,
        cell_size_m: float = 200.0,
        use_utm: bool = True,
        drop_zero: bool = False,
        swap_coords: bool = True,
        left_bottom: tuple[float, float] | None = None
) -> Dict[str, Any]:
    """
    Генерирует GeoJSON FeatureCollection:
      - Для каждой записи ConcentrationInfo строит квадрат cell_size_m x cell_size_m (в метрах) вокруг точки.
      - Добавляет point_source и cadastre_source как точки.
    Параметры:
      - db_session: SQLAlchemy session
      - map_id, timestamp: фильтры
      - cell_size_m: размер клетки в метрах (по умолчанию 200)
      - use_utm: если True — используется локальная UTM-проекция по центру данных (более точна)
      - drop_zero: если True — не включает ячейки с value == 0
      - swap_coords: если True — меняет порядок координат в итоговом geojson на [lat, lon]
    """
    conc_rows = (
        db_session.query(ConcentrationInfo)
        .filter(ConcentrationInfo.map_id == map_id, ConcentrationInfo.timestamp == timestamp)
        .all()
    )
    if not conc_rows:
        return {"type": "FeatureCollection", "features": []}

    lons = []
    lats = []
    props_list = []
    for r in conc_rows:
        lx = float(r.x)
        ly = float(r.y)
        lons.append(lx)
        lats.append(ly)
        props_list.append({
            "value": float(r.value),
            "map_id": int(r.map_id),
            "timestamp": str(r.timestamp),
            "info_id": int(r.info_id) if hasattr(r, "info_id") else None,
        })

    # Создаем GeoDataFrame точек в WGS84
    gdf_pts = gpd.GeoDataFrame(props_list, geometry=[Point(xy) for xy in zip(lons, lats)], crs="EPSG:4326")

    # Проекция для метрических операций
    if use_utm:
        center_lon = (min(lons) + max(lons)) / 2.0
        center_lat = (min(lats) + max(lats)) / 2.0
        proj_crs = _choose_project_crs_for_lonlat(center_lon, center_lat)
    else:
        proj_crs = CRS.from_epsg(3857)

    gdf_m = gdf_pts.to_crs(proj_crs.to_string())

    half = cell_size_m / 2.0
    boxes = []
    for geom in gdf_m.geometry:
        cx, cy = geom.x, geom.y
        b = box(cx - half, cy - half, cx + half, cy + half)
        boxes.append(b)
    gdf_m["geometry"] = boxes

    # Обратно в EPSG:4326
    gdf_out = gdf_m.to_crs("EPSG:4326")

    features: List[Dict[str, Any]] = []
    for _, row in gdf_out.iterrows():
        if drop_zero and float(row["value"]) == 0.0:
            continue
        geom = mapping(row.geometry)  # GeoJSON-like dict
        if swap_coords:
            geom = _swap_coords_geom(geom)
        prop = {
            "type": "concentration_cell",
            "value": row["value"],
            "map_id": row["map_id"],
            "timestamp": row["timestamp"],
            "info_id": row.get("info_id"),
        }
        features.append({"type": "Feature", "geometry": geom, "properties": prop})

    xllcorner = 0
    yllcorner = 0
    if left_bottom is not None:
        xllcorner, yllcorner = wgs84_point_to_crs(left_bottom, MSK_48_CRS)

    # 4) Добавляем point sources
    if include_point_sources:
        psrc_rows = db_session.query(PointSource).filter(PointSource.map_id == map_id).all()
        for p in psrc_rows:
            geom = crs_point_to_wgs84(Point(float(xllcorner + p.x * 200), float(yllcorner + p.y * 200)), MSK_48_CRS)
            geom = Point(geom.y, geom.x)
            prop = {
                "type": "point_source",
                "id": int(p.id),
                "h2s_kg_h": None if p.h2s_kg_h is None else float(p.h2s_kg_h),
                "source_group": int(p.source_group) if p.source_group is not None else None,
            }
            features.append({"type": "Feature", "geometry": mapping(geom), "properties": prop})

    if include_cadastre_sources:
        c_rows = db_session.query(CadastreSource).filter(CadastreSource.map_id == map_id).all()
        for c in c_rows:
            geom = crs_point_to_wgs84(Point(float(xllcorner + c.x * 200), float(yllcorner + c.y * 200)), MSK_48_CRS)
            geom = Point(geom.y, geom.x)
            prop = {
                "type": "cadastre_source",
                "id": int(c.id),
                "h2s_kg_h": None if c.h2s_kg_h is None else float(c.h2s_kg_h),
                "source_group": int(c.source_group) if c.source_group is not None else None,
            }
            features.append({"type": "Feature", "geometry": mapping(geom), "properties": prop})

    fc = {"type": "FeatureCollection", "features": features}
    return fc
