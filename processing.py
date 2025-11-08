import geopandas
import pyproj
from typing import cast
from shapely import Point

def wgs84_point_to_crs(point: tuple[float, float], crs: str) -> tuple[float, float]:
    """
    Проецирует точку из WGS84 в указанную CRS

    @param point: Точка с координатами X и Y
    @param crs: Целевая система координат (crs)
    @return: Точка с координатами, спроецированными в указанную систему
    """
    return cast(tuple[float, float],
                pyproj.Transformer.from_crs('EPSG:4326', crs, always_xy=True, ).transform(*point, errcheck=True))


def crs_point_to_wgs84(point: Point, crs: str) -> Point:
    tup = pyproj.Transformer.from_crs(crs, 'EPSG:4326', always_xy=True).transform(point.x, point.y, errcheck=True)
    return Point(tup[0], tup[1])

def read_grid_to_geodataframe(path: str,
                              target_crs: str,
                              left_bottom: tuple[float, float] | None = None) -> tuple[geopandas.GeoDataFrame, dict]:
    """
    Считывание подготовленного файла с сеткой и полями данных, перевод в GeoDataFrame со столбцами [value, coordinates]:
        value - Значение в точке сетки
        coordinates - shapely.Point с координатами X, Y в указанной системе координат (target_crs) относительно левого
    нижнего угла сетки файла
    Расстояние между точками рассчитывается с помощью шага cellsize из файла

    Parameters
    ----------
    path - Путь к файлу сетки
    target_crs - Координаты точек переводятся из WGS84 в указанную систему координат
    left_bottom - Координаты левого нижнего угла сетки в системе WGS84


    Returns
    -------
    GeoDataFrame с координатами точек сетки и их значениями, словарь с метаданными файла сетки
    """
    with open(path, 'r', encoding='utf-8') as file:
        ncols = int(file.readline().split(" ")[-1])
        nrows = int(file.readline().split(" ")[-1])

        # Координаты X, Y левого нижнего угла в рамках сетки модели
        # Нужны для получения координат источников выбросов
        model_xllcorner = float(file.readline().split(" ")[-1])
        model_yllcorner = float(file.readline().split(" ")[-1])

        # Координаты X, Y левого нижнего угла в указанной координатной системе
        # При наличии координат, относительно них будут построены точки выходного файла границ
        xllcorner = 0
        yllcorner = 0
        if left_bottom is not None:
            xllcorner, yllcorner = wgs84_point_to_crs(left_bottom, target_crs)

        cellsize = int(file.readline().split(" ")[-1])

        NODATA_value = file.readline()
        unit = None

        if "Unit:" in NODATA_value:
            temp = NODATA_value.replace("\t", "").split(" ")
            while '' in temp:
                temp.remove('')
            NODATA_value = temp[1]
            unit = temp[-1][5:-1]
        else:
            NODATA_value = int(NODATA_value.split(" ")[-1])

        height_temp_list = []

        # !!! Для корректного вычисления смещения, координаты должны быть в местной системе координат,
        # где смещение на 1км = +1.0 к координате (например MSK-48)

        # Индексы строк в порядке относительно левого нижнего угла в правый верхний угол
        for row in range(nrows - 1, -1, -1):
            row_values = file.readline().split(" ")
            for column in range(ncols):
                height_temp_list.append({
                    'value': float(row_values[column]),
                    'coordinates': Point(
                        xllcorner + column * cellsize,
                        yllcorner + row * cellsize
                    )
                })

        geo_df = geopandas.GeoDataFrame(height_temp_list, geometry='coordinates', crs=target_crs)

        metadata = {
            "ncols": ncols,
            "nrows": nrows,
            "model_xllcorner": model_xllcorner,
            "model_yllcorner": model_yllcorner,
            "xllcorner_crs": xllcorner,
            "yllcorner_crs": yllcorner,
            "target_crs": target_crs,
            "cellsize": cellsize,
            "NODATA_value": NODATA_value,
            "unit": unit
        }
        return geo_df, metadata
