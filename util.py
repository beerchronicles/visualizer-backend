import re
from typing import Final

import requests
from pandas import DataFrame

cadastre_original_headers = [
    "x","y","z","dx","dy","dz","H2S[kg/h]","--","--","--",
    "source group","deposition parameters F2.5","F10","DiaMax","Density",
    "VDep2.5","VDep10","VDepMax","Dep_Conc"
]

cadastre_order = [
    "x","y","z","dx","dy","dz","h2s_kg_h",
    "unknown_1","unknown_2","unknown_3",
    "source_group","dep_f2_5","dep_f10","dep_diamax","dep_density",
    "dep_vdep2_5","dep_vdep10","dep_vdepmax","dep_conc"
]

point_original_headers = [
    "x","y","z","H2S[kg/h]","--","--","--",
    "exit vel.[m/s]","diameter[m]","Temp.[K]","Source group",
    "deposition parameters F2.5","F10","DiaMax","Density",
    "VDep2.5","VDep10","VDepMax","Dep_Conc"
]

point_order = [
    "x","y","z","h2s_kg_h",
    "unknown_1","unknown_2","unknown_3",
    "exit_vel_m_s","diameter_m","temp_k","source_group",
    "dep_f2_5","dep_f10","dep_diamax","dep_density",
    "dep_vdep2_5","dep_vdep10","dep_vdepmax","dep_conc"
]

timestamp_regex = re.compile(r'soft/Project/Computation/\d{5}-\d+\.txt')

MSK_48_CRS: Final[
    str] = '+proj=tmerc +lat_0=0 +lon_0=38.48333333333 +k=1 +x_0=1250000 +y_0=-5412900.566 +ellps=krass +towgs84=23.57,-140.95,-79.8,0,0.35,0.79,-0.22 +units=m +no_defs'

def download_file(url, f):
    r = requests.get(url, stream=True)
    for chunk in r.iter_content(chunk_size=16 * 1024):
        f.write(chunk)

def normalize_columns(df: DataFrame):
    """Привести названия колонок CSV к именам полей моделей"""
    mapping = {
        "x": "x",
        "y": "y",
        "z": "z",
        "dx": "dx",
        "dy": "dy",
        "dz": "dz",
        "H2S[kg/h]": "h2s_kg_h",
        "exit vel.[m/s]": "exit_vel_m_s",
        "diameter[m]": "diameter_m",
        "Temp.[K]": "temp_k",
        "source group": "source_group",
        "Source group": "source_group",
        "deposition parameters F2.5": "dep_f2_5",
        "F10": "dep_f10",
        "DiaMax": "dep_diamax",
        "Density": "dep_density",
        "VDep2.5": "dep_vdep2_5",
        "VDep10": "dep_vdep10",
        "VDepMax": "dep_vdepmax",
        "Dep_Conc": "dep_conc",
    }
    df = df.drop('--', axis=1)
    df = df.drop('--.1', axis=1)
    df = df.drop('--.2', axis=1)
    df = df.rename(columns=mapping)
    return df

def df_to_objects(df, model_cls):
    """Преобразовать DataFrame в список ORM-объектов"""
    records = df.to_dict(orient="records")
    return [model_cls(**rec) for rec in records]

def cadastre_to_dict(o):
    return {
        "x": o.x,
        "y": o.y,
        "z": o.z,
        "dx": o.dx,
        "dy": o.dy,
        "dz": o.dz,
        "h2s_kg_h": o.h2s_kg_h,
        "unknown_1": 0,
        "unknown_2": 0,
        "unknown_3": 0,
        "source_group": o.source_group,
        "dep_f2_5": o.dep_f2_5,
        "dep_f10": o.dep_f10,
        "dep_diamax": o.dep_diamax,
        "dep_density": o.dep_density,
        "dep_vdep2_5": o.dep_vdep2_5,
        "dep_vdep10": o.dep_vdep10,
        "dep_vdepmax": o.dep_vdepmax,
        "dep_conc": o.dep_conc,
    }

def point_to_dict(o):
    return {
        "x": o.x,
        "y": o.y,
        "z": o.z,
        "h2s_kg_h": o.h2s_kg_h,
        "unknown_1": 0,
        "unknown_2": 0,
        "unknown_3": 0,
        "exit_vel_m_s": o.exit_vel_m_s,
        "diameter_m": o.diameter_m,
        "temp_k": o.temp_k,
        "source_group": o.source_group,
        "dep_f2_5": o.dep_f2_5,
        "dep_f10": o.dep_f10,
        "dep_diamax": o.dep_diamax,
        "dep_density": o.dep_density,
        "dep_vdep2_5": o.dep_vdep2_5,
        "dep_vdep10": o.dep_vdep10,
        "dep_vdepmax": o.dep_vdepmax,
        "dep_conc": o.dep_conc,
    }
