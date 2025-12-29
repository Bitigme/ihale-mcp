#!/usr/bin/env python3
"""
Lightweight Google Places (Web Service) async client built on httpx.
Supports:
- Geocoding an address/location text to lat/lng
- Text Search for places with keyword + location + radius
- Place Details (to enrich leads with phone/website/opening hours)

Notes
- Uses legacy Places Web Service endpoints for simplicity and broad support.
- For production, consider migrating to Places API (New, v1) endpoints with field masks.
"""

from __future__ import annotations

import os
import asyncio
from typing import Any, Dict, List, Optional, Tuple

import httpx


GOOGLE_GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
GOOGLE_TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
GOOGLE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"


class GooglePlacesClient:
    """Minimal async client around Google Geocoding + Places Text Search + Details APIs."""

    def __init__(self, api_key: Optional[str] = None, *, timeout_seconds: int = 20) -> None:
        self.api_key = api_key or os.environ.get("GOOGLE_MAPS_API_KEY")
        if not self.api_key:
            raise RuntimeError(
                "GOOGLE_MAPS_API_KEY bulunamadı. Lütfen ortam değişkenini ayarlayın."
            )
        self._client = httpx.AsyncClient(timeout=timeout_seconds)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def geocode(self, location_text: str, *, language: str = "tr") -> Optional[Tuple[float, float]]:
        """Geocode a free-form address/location to (lat, lng). Returns None if not found."""
        # Eğer sadece şehir adı verilmişse, "Türkiye" ekle
        location_query = location_text.strip()
        if location_query and "türkiye" not in location_query.lower() and "turkey" not in location_query.lower():
            # Şehir adı gibi görünüyorsa (virgül yok, uzun değil) Türkiye ekle
            if "," not in location_query and len(location_query.split()) <= 3:
                location_query = f"{location_query}, Türkiye"
        
        params = {
            "address": location_query,
            "key": self.api_key,
            "language": language,
            "region": "tr",  # Türkiye sonuçlarına öncelik ver
        }
        resp = await self._client.get(GOOGLE_GEOCODE_URL, params=params)
        data = resp.json()
        status = data.get("status")
        if status != "OK" or not data.get("results"):
            # Hata durumunda log için status'u döndür (debug için)
            return None
        loc = data["results"][0]["geometry"]["location"]
        return float(loc["lat"]), float(loc["lng"])

    async def text_search(
        self,
        query: str,
        *,
        location: Optional[Tuple[float, float]] = None,
        radius_meters: Optional[int] = None,
        pagetoken: Optional[str] = None,
        language: str = "tr",
    ) -> Dict[str, Any]:
        """Call Places Text Search.

        Returns raw JSON: {results: [...], next_page_token: str?, status: str}
        """
        params: Dict[str, Any] = {
            "query": query,
            "key": self.api_key,
            "language": language,
            "region": "tr",  # Türkiye sonuçlarına öncelik ver
        }
        if location is not None:
            lat, lng = location
            params["location"] = f"{lat:.7f},{lng:.7f}"
        if radius_meters is not None:
            params["radius"] = int(radius_meters)
        if pagetoken:
            params["pagetoken"] = pagetoken

        resp = await self._client.get(GOOGLE_TEXT_SEARCH_URL, params=params)
        data = resp.json()
        return data

    async def place_details(
        self, place_id: str, *, language: str = "tr"
    ) -> Dict[str, Any]:
        """Get place details to enrich with phone / website / opening hours."""
        params = {
            "place_id": place_id,
            "key": self.api_key,
            "language": language,
            # fields kept modest to reduce quota usage
            "fields": "formatted_phone_number,international_phone_number,website,opening_hours"
        }
        resp = await self._client.get(GOOGLE_DETAILS_URL, params=params)
        return resp.json()

    async def search_leads(
        self,
        *,
        keyword: str,
        location_text: str,
        radius_meters: int = 5000,
        limit: int = 50,
        include_details: bool = True,
        language: str = "tr",
    ) -> Dict[str, Any]:
        """High-level helper: keyword + location_text -> paginated Text Search -> optional details.

        Returns dict with keys: leads, total, query, location, note
        """
        geocoded = await self.geocode(location_text, language=language)
        if geocoded is None:
            return {
                "error": True,
                "message": f"Konum geocode edilemedi: '{location_text}'. Lütfen daha spesifik bir konum girin (örn: 'Samsun, Türkiye' veya 'Samsun İlkadım').",
                "location_input": location_text,
            }

        lat, lng = geocoded
        collected: List[Dict[str, Any]] = []
        next_token: Optional[str] = None

        # Google en az 2-3 saniye gecikme ile next_page_token'ı etkinleştirir.
        # Bu nedenle döngü içinde gerektiğinde bekleme uygulanır.
        while len(collected) < limit:
            data = await self.text_search(
                query=keyword, location=(lat, lng), radius_meters=radius_meters,
                pagetoken=next_token, language=language
            )
            status = data.get("status")
            if status not in ("OK", "ZERO_RESULTS"):
                return {"error": True, "message": f"Places API hatası: {status}", "raw": data}

            for item in data.get("results", []):
                if len(collected) >= limit:
                    break
                collected.append(item)

            next_token = data.get("next_page_token")
            if not next_token or len(collected) >= limit or status == "ZERO_RESULTS":
                break
            await asyncio.sleep(2.2)

        # Optional details enrichment (best-effort, do not fail hard)
        if include_details and collected:
            # Bound parallelism to avoid throttling
            semaphore = asyncio.Semaphore(5)

            async def enrich(item: Dict[str, Any]) -> None:
                place_id = item.get("place_id")
                if not place_id:
                    return
                async with semaphore:
                    try:
                        details = await self.place_details(place_id, language=language)
                        if details.get("status") == "OK":
                            result = details.get("result", {})
                            item["details"] = {
                                "formatted_phone_number": result.get("formatted_phone_number"),
                                "international_phone_number": result.get("international_phone_number"),
                                "website": result.get("website"),
                                "opening_hours": result.get("opening_hours"),
                            }
                    except Exception:
                        # Swallow errors to keep lead collection robust
                        pass

            await asyncio.gather(*(enrich(it) for it in collected))

        return {
            "leads_raw": collected,
            "total": len(collected),
            "query": {
                "keyword": keyword,
                "location_text": location_text,
                "radius_meters": radius_meters,
                "include_details": include_details,
            },
            "location": {"lat": lat, "lng": lng},
            "note": "Kaynak: Google Places Text Search"
        }


