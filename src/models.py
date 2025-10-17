# src/models.py
from sqlalchemy import Integer, String, Column, Text, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime
from sqlalchemy import Numeric, JSON, DateTime

Base = declarative_base()

class Branch(Base):
    __tablename__ = "branches"
    id = Column(Integer, primary_key=True)
    name = Column(String(256), nullable=False)
    address = Column(String(512))
    place_id = Column(String(256), unique=True, index=True)  # unique identifier if available
    phone = Column(String(64))
    lat = Column(String(64))
    lng = Column(String(64))
    url = Column(String(1024))
    scraped_at = Column(DateTime, default=datetime.utcnow)

    reviews = relationship("Review", back_populates="branch", cascade="all, delete-orphan")

class Review(Base):
    __tablename__ = "reviews"
    review_id = Column(Integer, primary_key=True)
    branch_id = Column(Integer, ForeignKey("branches.id"))
    author = Column(String(256))
    rating = Column(Integer)  # 1-5
    text = Column(Text)
    review_date = Column(String(64))  # textual human date, you can parse to datetime later
    scraped_at = Column(DateTime, default=datetime.utcnow)

    branch = relationship("Branch", back_populates="reviews")

# =========================
# Insights Tables
# =========================


class InsightMeta(Base):
    __tablename__ = "insights_meta"
    
    meta_id = Column(Integer, primary_key=True)
    branch_id = Column(Integer, nullable=False)
    branch_name = Column(String(255), nullable=False)
    analysis_date = Column(DateTime, default=datetime.utcnow)
    number_of_reviews_processed = Column(Integer, nullable=False)
    number_of_topics = Column(Integer, nullable=False)
    bertopic_parameters = Column(JSON)


class Insight(Base):
    __tablename__ = "insights"
    
    insight_id = Column(Integer, primary_key=True)
    meta_id = Column(Integer, ForeignKey("insights_meta.meta_id"), nullable=False)
    topic_id = Column(Integer, nullable=False)
    percentage = Column(Numeric(5, 2), nullable=False)
    top_keywords = Column(Text)
    gemini_aspect = Column(String(255))
    gemini_sentiment = Column(String(50))
    gemini_summary = Column(Text)
    gemini_recommendation = Column(Text)
