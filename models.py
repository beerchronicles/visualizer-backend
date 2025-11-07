from typing import Optional

from sqlalchemy import ForeignKey, Column, Integer, Float
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass

class Map(Base):
    __tablename__ = "maps"

    map_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    lbx: Mapped[float]
    lby: Mapped[float]

class ConcentrationInfo(Base):
    __tablename__ = "concentration_infos"

    info_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    map_id: Mapped[int] = mapped_column(ForeignKey("maps.map_id"))
    gridfile: Mapped[str]
    x: Mapped[float]
    y: Mapped[float]
    value: Mapped[float]

class CadastreSource(Base):
    __tablename__ = "cadastre_sources"

    id = Column(Integer, primary_key=True, autoincrement=True)
    map_id: Mapped[int] = mapped_column(ForeignKey("maps.map_id"))

    x = Column(Float, nullable=False, info={"orig": "x"})
    y = Column(Float, nullable=False, info={"orig": "y"})
    z = Column(Float, nullable=False, info={"orig": "z"})

    dx = Column(Float, nullable=True, info={"orig": "dx"})
    dy = Column(Float, nullable=True, info={"orig": "dy"})
    dz = Column(Float, nullable=True, info={"orig": "dz"})

    h2s_kg_h = Column(Float, nullable=True, info={"orig": "H2S[kg/h]"})

    source_group = Column(Integer, nullable=True, info={"orig": "source group"})

    dep_f2_5 = Column(Float, nullable=True, info={"orig": "deposition parameters F2.5"})
    dep_f10 = Column(Float, nullable=True, info={"orig": "F10"})
    dep_diamax = Column(Float, nullable=True, info={"orig": "DiaMax"})
    dep_density = Column(Float, nullable=True, info={"orig": "Density"})
    dep_vdep2_5 = Column(Float, nullable=True, info={"orig": "VDep2.5"})
    dep_vdep10 = Column(Float, nullable=True, info={"orig": "VDep10"})
    dep_vdepmax = Column(Float, nullable=True, info={"orig": "VDepMax"})
    dep_conc = Column(Float, nullable=True, info={"orig": "Dep_Conc"})

    def __repr__(self):
        return f"<CadastreSource(id={self.id}, x={self.x}, y={self.y}, z={self.z}, h2s={self.h2s_kg_h})>"

class PointSource(Base):
    __tablename__ = "point_sources"

    id = Column(Integer, primary_key=True, autoincrement=True)
    map_id: Mapped[int] = mapped_column(ForeignKey("maps.map_id"))

    x = Column(Float, nullable=False, info={"orig": "x"})
    y = Column(Float, nullable=False, info={"orig": "y"})
    z = Column(Float, nullable=False, info={"orig": "z"})

    h2s_kg_h = Column(Float, nullable=True, info={"orig": "H2S[kg/h]"})

    exit_vel_m_s = Column(Float, nullable=True, info={"orig": "exit vel.[m/s]"})
    diameter_m = Column(Float, nullable=True, info={"orig": "diameter[m]"})
    temp_k = Column(Float, nullable=True, info={"orig": "Temp.[K]"})

    source_group = Column(Integer, nullable=True, info={"orig": "Source group"})

    dep_f2_5 = Column(Float, nullable=True, info={"orig": "deposition parameters F2.5"})
    dep_f10 = Column(Float, nullable=True, info={"orig": "F10"})
    dep_diamax = Column(Float, nullable=True, info={"orig": "DiaMax"})
    dep_density = Column(Float, nullable=True, info={"orig": "Density"})
    dep_vdep2_5 = Column(Float, nullable=True, info={"orig": "VDep2.5"})
    dep_vdep10 = Column(Float, nullable=True, info={"orig": "VDep10"})
    dep_vdepmax = Column(Float, nullable=True, info={"orig": "VDepMax"})
    dep_conc = Column(Float, nullable=True, info={"orig": "Dep_Conc"})

    def __repr__(self):
        return f"<PointSource(id={self.id}, x={self.x}, y={self.y}, z={self.z}, h2s={self.h2s_kg_h})>"