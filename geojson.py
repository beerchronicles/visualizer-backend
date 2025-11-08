from typing import Dict, Any, List

import numpy as np
from shapely.geometry import box, Point, mapping
from sqlalchemy.orm import Session

from models import ConcentrationInfo, PointSource, CadastreSource


def _median_step(sorted_vals: np.ndarray) -> float:
    """Вычислить типичный шаг между соседними координатами.
       Если данных мало, вернуть маленькое значение (1.0) как fallback."""
    if len(sorted_vals) < 2:
        return 1.0
    diffs = np.diff(sorted_vals)
    # отбросим нули и NaN
    diffs = diffs[np.isfinite(diffs) & (diffs > 0)]
    if diffs.size == 0:
        return 1.0
    return float(np.median(diffs))

def generate_geojson_for_map_timestamp(
    db_session: Session,
    map_id: int,
    timestamp: str,
    include_point_sources: bool = True,
    include_cadastre_sources: bool = True,
) -> Dict[str, Any]:
    """
    Возвращает GeoJSON FeatureCollection (как dict) для одного map_id и timestamp.
    - Прямоугольники (cells) вокруг каждой точки concentration с полем 'value'.
    - Точечные источники как отдельные Feature с полями.
    """
    # 1) Получаем концентрации
    conc_rows = (
        db_session.query(ConcentrationInfo)
        .filter(ConcentrationInfo.map_id == map_id, ConcentrationInfo.timestamp == timestamp)
        .all()
    )

    if not conc_rows:
        # Возвращаем пустую FeatureCollection (фронт может отобразить сообщение)
        return {"type": "FeatureCollection", "features": []}

    xs = np.array([float(r.x) for r in conc_rows])
    ys = np.array([float(r.y) for r in conc_rows])
    vals = np.array([float(r.value) for r in conc_rows])

    # 2) вычисляем шаги по x,y (для ширины/высоты ячеек)
    unique_x = np.unique(np.sort(xs))
    unique_y = np.unique(np.sort(ys))
    dx = _median_step(unique_x)
    dy = _median_step(unique_y)

    # если точки задают пересечение сетки (nodes), то половина шага вокруг точки
    half_dx = dx / 2.0
    half_dy = dy / 2.0

    # 3) Создаём список Feature (фигур)
    features: List[Dict[str, Any]] = []

    # создать полигональные ячейки для каждой точки концентрации
    for r in conc_rows:
        cx = float(r.x)
        cy = float(r.y)
        v = float(r.value)

        # границы ячейки
        minx = cx - half_dx
        maxx = cx + half_dx
        miny = cy - half_dy
        maxy = cy + half_dy

        poly = box(minx, miny, maxx, maxy)
        prop = {
            "type": "concentration_cell",
            "value": v,
            "map_id": int(r.map_id),
            "timestamp": str(r.timestamp),
            # при желании можно положить info_id и другие поля:
            "info_id": int(r.info_id) if hasattr(r, "info_id") else None,
        }
        features.append({"type": "Feature", "geometry": mapping(poly), "properties": prop})

    # 4) Добавляем point sources
    if include_point_sources:
        psrc_rows = db_session.query(PointSource).filter(PointSource.map_id == map_id).all()
        for p in psrc_rows:
            geom = Point(float(p.x), float(p.y))
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
            geom = Point(float(c.x), float(c.y))
            prop = {
                "type": "cadastre_source",
                "id": int(c.id),
                "h2s_kg_h": None if c.h2s_kg_h is None else float(c.h2s_kg_h),
                "source_group": int(c.source_group) if c.source_group is not None else None,
            }
            features.append({"type": "Feature", "geometry": mapping(geom), "properties": prop})

    fc = {"type": "FeatureCollection", "features": features}
    return fc
