import os.path
import re
from io import StringIO
from tempfile import TemporaryFile, TemporaryDirectory
from typing import Annotated, List
from zipfile import ZipFile

import pandas
import sqlalchemy
from fastapi import FastAPI, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from geopandas import GeoDataFrame
from sqlalchemy import select, Sequence, delete
from sqlalchemy.orm import Session

from config import postgres_url, gral_path, gral_base_url
from geojson import generate_geojson_for_map_timestamp
from models import Base, PointSource, CadastreSource, Map, ConcentrationInfo
from processing import read_grid_to_geodataframe
from util import df_to_objects, point_to_dict, cadastre_to_dict, cadastre_order, point_order, cadastre_original_headers, \
    point_original_headers, download_file, timestamp_regex, normalize_columns, MSK_48_CRS

app = FastAPI()
engine = sqlalchemy.create_engine(postgres_url)
app.processing = False

Base.metadata.create_all(engine)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def root():
    return {"status": 200}


@app.post("/new_map")
async def new_map(lbx: float, lby: float):
    map_ = Map(lbx=lbx, lby=lby)
    with Session(engine) as session:
        session.add(map_)
        session.commit()
        session.refresh(map_)
    return map_

@app.get("/all_maps")
async def all_maps():
    with Session(engine) as session:
        return {"maps": [m.map_id for m in session.query(Map.map_id).all()]}


@app.post("/upload_point")
async def upload_point(map_id: int, file: Annotated[bytes, File()]):
    with Session(engine) as session:
        session.execute(delete(PointSource).where(PointSource.map_id == map_id))

        point_csv = normalize_columns(pandas.read_csv(StringIO(file.decode('utf-8'))))
        point_models: List[PointSource] = df_to_objects(point_csv, PointSource)

        for model in point_models:
            model.map_id = map_id

        session.add_all(point_models)
        session.commit()

    return {"status": 200}


@app.post("/upload_cadastre")
async def upload_cadastre(map_id: int, file: Annotated[bytes, File()]):
    with Session(engine) as session:
        session.execute(delete(CadastreSource).where(CadastreSource.map_id == map_id))

        cadastre_csv = normalize_columns(pandas.read_csv(StringIO(file.decode('utf-8'))))
        cadastre_models: List[CadastreSource] = df_to_objects(cadastre_csv, CadastreSource)

        for model in cadastre_models:
            model.map_id = map_id

        session.add_all(cadastre_models)
        session.commit()

    return {"status": 200}


@app.get("/process")
async def process(map_id: int):
    if app.processing:
        raise HTTPException(status_code=410, detail="Process is currently running")

    try:
        app.processing = True

        with Session(engine) as session:
            session.execute(delete(ConcentrationInfo).where(ConcentrationInfo.map_id == map_id))

            statement = select(PointSource).where(PointSource.map_id == map_id)
            point_models: Sequence[PointSource] = session.scalars(statement).all()

            statement = select(CadastreSource).where(CadastreSource.map_id == map_id)
            cadastre_models: Sequence[CadastreSource] = session.scalars(statement).all()

            statement = select(Map).where(Map.map_id == map_id)
            map: Map = session.scalars(statement).one()

            point_records = [point_to_dict(o) for o in point_models]
            cadastre_records = [cadastre_to_dict(o) for o in cadastre_models]

            df_cad = pandas.DataFrame(cadastre_records, columns=cadastre_order)
            df_point = pandas.DataFrame(point_records, columns=point_order)

            df_cad.to_csv(f"{gral_path}/proj/Computation/cadastre.dat", index=False, header=cadastre_original_headers)
            df_point.to_csv(f"{gral_path}/proj/Computation/point.dat", index=False, header=point_original_headers)

            resulting_frames: List[tuple[str, GeoDataFrame]] = []

            with TemporaryDirectory() as tmpdir:
                with TemporaryFile() as tmpfile:
                    download_file(f"{gral_base_url}/gralfile", tmpfile)
                    zf = ZipFile(tmpfile)
                    zf.extractall(tmpdir)

                    for file in zf.infolist():
                        if re.match(timestamp_regex, file.filename):
                            name = os.path.basename(file.filename)
                            filepath = os.path.join(tmpdir, file.filename)
                            zf.extract(file.filename, tmpdir)
                            resulting_frames.append((name,
                                                     read_grid_to_geodataframe(filepath, MSK_48_CRS,
                                                                               (map.lbx, map.lby))[0].to_crs(4326)))

            infos = []
            for (filename, gdf) in resulting_frames:
                timestamp = filename[:5]
                for index, row in gdf.iterrows():
                    conc_info = ConcentrationInfo()
                    point = row['coordinates']
                    value = row['value']
                    conc_info.timestamp = timestamp
                    conc_info.map_id = map_id
                    conc_info.x = point.x
                    conc_info.y = point.y
                    conc_info.value = value
                    infos.append(conc_info)

            session.add_all(infos)
            session.commit()
    finally:
        app.processing = False

    return {"status": 200}


@app.get("/available_timestamps")
async def get_available_timestamps(map_id: int):
    timestamps = []

    with Session(engine) as session:
        timestamps = [r[0] for r in session.query(ConcentrationInfo.timestamp) \
            .filter(ConcentrationInfo.map_id == map_id) \
            .distinct() \
            .order_by(ConcentrationInfo.timestamp) \
            .all()]

    return {'timestamps': timestamps}

@app.get("/generate_geojson_timestamp")
async def generate_geojson_timestamp(map_id: int, timestamp: str):
    with Session(engine) as session:
        return generate_geojson_for_map_timestamp(session, map_id, timestamp)
