#!/usr/bin/env python3
"""
Pazarlama/Satış ekipleri için Google Places sonuçlarından normalize edilmiş lead modelleri.
"""

from typing import List, Optional
from pydantic import BaseModel, Field


class Lead(BaseModel):
    """Tekil işletme/lead kaydı."""
    name: str = Field(description="İşletme adı")
    formatted_address: Optional[str] = Field(default=None, description="Adres (Google formatlı)")
    latitude: Optional[float] = Field(default=None, description="Enlem")
    longitude: Optional[float] = Field(default=None, description="Boylam")
    place_id: str = Field(description="Google Place ID")
    types: List[str] = Field(default_factory=list, description="Google kategori etiketleri")
    rating: Optional[float] = Field(default=None, description="Puan")
    user_ratings_total: Optional[int] = Field(default=None, description="Yorum sayısı")
    business_status: Optional[str] = Field(default=None, description="İş durumu")
    phone: Optional[str] = Field(default=None, description="Telefon (formatlı)")
    phone_intl: Optional[str] = Field(default=None, description="Uluslararası telefon")
    website: Optional[str] = Field(default=None, description="Web sitesi")


class LeadSearchResponse(BaseModel):
    """Lead araması yanıtı."""
    leads: List[Lead]
    total: int
    query: dict
    location: dict
    note: Optional[str] = None



