import re
from io import StringIO
from tempfile import TemporaryFile
from typing import Annotated, List
from zipfile import ZipFile

import pandas
import sqlalchemy
from fastapi import FastAPI, File, HTTPException
from geopandas import GeoDataFrame
from sqlalchemy import select, Sequence, delete
from sqlalchemy.orm import Session

from config import postgres_url, gral_path, gral_base_url
from models import Base, PointSource, CadastreSource, Map, ConcentrationInfo
from processing import read_grid_to_geodataframe
from util import df_to_objects, point_to_dict, cadastre_to_dict, cadastre_order, point_order, cadastre_original_headers, \
    point_original_headers, download_file, timestamp_regex, normalize_columns

app = FastAPI()
engine = sqlalchemy.create_engine(postgres_url)
app.processing = False

Base.metadata.create_all(engine)

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

            with TemporaryFile() as tmpfile:
                download_file(f"{gral_base_url}/gralfile", tmpfile)
                zf = ZipFile(tmpfile)

                for file in zf.infolist():
                    if re.match(timestamp_regex, file.filename):
                        zf.extract(file.filename, file.filename)
                        resulting_frames.append((file.filename, read_grid_to_geodataframe(file.filename, 'EPSG:4326', (map.lbx, map.lby))[0]))

            for (filename, gdf) in resulting_frames:
                print(f'frame {filename} with content {gdf}')

            # TODO: Сделать перегон GeoDataFrame'ов в бд
    finally:
        app.processing = False

    return {"status": 200}
