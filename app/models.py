# app/models.py
from sqlalchemy import Column, Integer, Float, DateTime, String
from sqlalchemy.orm import declarative_base
from datetime import datetime

Base = declarative_base()

class Measurement(Base):
    __tablename__ = "measurements"
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, index=True)   # timestamp of sample (UTC)
    power_consumption_w = Column(Float, default=0.0)   # instantaneous power (W) - main
    power_generation_w  = Column(Float, default=0.0)   # instantaneous solar power (W)
    power_battery_w     = Column(Float, default=0.0)   # battery instantaneous (W)
    total_consumption_w = Column(Float, default=0.0)   # total consumption instantaneous (W)
    # energy integrals computed for the interval preceding this timestamp (kWh):
    energy_consumed_kwh = Column(Float, default=0.0)
    energy_generated_kwh = Column(Float, default=0.0)
    energy_exported_kwh = Column(Float, default=0.0)
    # price applied to that timestamp (€/kWh)
    price_eur_per_kwh = Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
