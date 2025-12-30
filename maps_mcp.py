#!/usr/bin/env python3
"""
MCP Server for Google Maps/Places Lead Generation

Provides a single tool `find_business_leads` to search businesses by
keyword + location and return normalized lead data, with optional
filters and CSV output.
"""

import os
import json
from typing import Optional, List, Literal, Dict, Any
from fastmcp import FastMCP
from google_places_client import GooglePlacesClient
from google_sheets_writer import GoogleSheetsAppender, leads_to_sheet_rows, _extract_il_from_location_text, _parse_il_ilce, _normalize_il_name, _parse_il_from_address_only


app = FastMCP(
    name="maps-mcp",
    instructions=(
        "Use find_business_leads tool to search Google Maps/Places for businesses "
        "using a keyword (e.g., 'tarım makinaları bayileri', 'ilaç bayileri') and a location text (e.g., 'Sinop', 'Samsun', 'İstanbul'). "
        "The tool automatically filters results to only include businesses from the specified location. "
        "Results are automatically exported to Google Sheets if GOOGLE_SHEETS_AUTO_EXPORT is enabled. "
        "ALWAYS use this tool when the user asks to search for businesses, dealers, or companies in a specific location."
    ),
)

@app.tool()
async def find_business_leads(
    keyword: str,
    location_text: str,
    radius_meters: int = 5000,
    limit: int = 50,
    include_details: bool = True,
    # Filters
    min_rating: Optional[float] = None,
    min_user_ratings_total: Optional[int] = None,
    types_include: Optional[List[str]] = None,
    types_exclude: Optional[List[str]] = None,
    require_phone_or_website: bool = False,
    only_open_now: Optional[bool] = None,
    business_status_in: Optional[List[str]] = None,
    # Output
    output_format: Literal["json", "csv"] = "json",
    csv_columns: Optional[List[str]] = None,
    dedupe_by: Literal["place_id", "name_address"] = "place_id",
    # Google Sheets export
    export_to_google_sheets: bool = False,
    google_sheets_spreadsheet_id: Optional[str] = None,
    google_sheets_sheet_name: Optional[str] = None,
    google_sheets_include_raw_json: bool = False,
) -> Dict[str, Any]:
    """
    Google Maps/Places Text Search ile işletme arayıp satış lead listesine dönüştürür.
    
    Bu tool, belirli bir lokasyonda (il, ilçe, şehir) belirli bir anahtar kelime ile
    işletme araması yapar ve sonuçları Google Sheets'e otomatik olarak aktarır.
    
    Örnek kullanım:
    - keyword: "tarım makinaları bayileri"
    - location_text: "Sinop" veya "Samsun" veya "İstanbul"
    
    Sonuçlar otomatik olarak Google Sheets'e yazılır (GOOGLE_SHEETS_AUTO_EXPORT=true ise).
    
    ÖNEMLİ: Bu tool'u kullanarak herhangi bir il/şehir için işletme araması yapabilirsiniz.
    Tool, sonuçları filtreler ve sadece belirtilen lokasyona ait sonuçları döndürür.
    """

    if limit < 1:
        limit = 1
    if limit > 120:
        limit = 120

    client = GooglePlacesClient()
    raw = await client.search_leads(
        keyword=keyword,
        location_text=location_text,
        radius_meters=radius_meters,
        limit=limit,
        include_details=include_details,
        language="tr",
    )

    if raw.get("error"):
        return raw

    leads_out = []
    for r in raw.get("leads_raw", []):
        geom = (r.get("geometry") or {}).get("location") or {}
        det = r.get("details") or {}
        leads_out.append({
            "name": r.get("name"),
            "formatted_address": r.get("formatted_address"),
            "latitude": geom.get("lat"),
            "longitude": geom.get("lng"),
            "place_id": r.get("place_id"),
            "types": r.get("types") or [],
            "rating": r.get("rating"),
            "user_ratings_total": r.get("user_ratings_total"),
            "business_status": r.get("business_status"),
            "phone": det.get("formatted_phone_number"),
            "phone_intl": det.get("international_phone_number"),
            "website": det.get("website"),
            "open_now": (det.get("opening_hours") or {}).get("open_now"),
        })

    # Extract il from location_text for filtering
    location_il = _extract_il_from_location_text(location_text) if location_text else None
    
    # Import postal code functions for strict filtering
    from google_sheets_writer import _extract_postal_code_from_address, _get_il_from_postal_code
    
    # Filters
    def pass_filters(item: Dict[str, Any]) -> bool:
        # KRİTİK: İl filtresi - location_text'teki il ile formatted_address'teki il eşleşmeli
        if location_il:
            address = item.get("formatted_address")
            if address:
                loc_il_norm = _normalize_il_name(location_il)
                
                # 1. POSTA KODU KONTROLÜ (en güvenilir yöntem - ÖNCELİKLİ)
                postal_code = _extract_postal_code_from_address(address)
                if postal_code:
                    postal_il = _get_il_from_postal_code(postal_code)
                    if postal_il:
                        postal_il_norm = _normalize_il_name(postal_il)
                        # Posta kodu il'i location_text ile eşleşmiyorsa → FİLTRELE
                        if loc_il_norm != postal_il_norm:
                            return False
                    else:
                        # Posta kodu var ama il tespit edilemedi → FİLTRELE (güvenlik için)
                        return False
                else:
                    # Posta kodu yok → adres içinden parse et ve kontrol et
                    # 2. ADRES İÇİNDEN İL PARSE ET (location_text olmadan)
                    parsed_il_raw = _parse_il_from_address_only(address)
                    if parsed_il_raw:
                        parsed_il_norm = _normalize_il_name(parsed_il_raw)
                        # Parse edilen il location_text ile eşleşmiyorsa → FİLTRELE
                        if loc_il_norm != parsed_il_norm:
                            return False
                    else:
                        # İl parse edilemedi → adres içinde string kontrolü yap
                        address_lower = address.lower()
                        # Normalize edilmiş il adını adres içinde ara
                        if loc_il_norm not in address_lower:
                            # İl adı adres içinde yoksa → FİLTRELE
                            return False
        
        if min_rating is not None:
            r = item.get("rating")
            if r is None or r < min_rating:  # type: ignore[operator]
                return False
        if min_user_ratings_total is not None:
            ur = item.get("user_ratings_total")
            if ur is None or ur < min_user_ratings_total:  # type: ignore[operator]
                return False
        if types_include:
            itypes = set((item.get("types") or []))
            if not itypes.intersection({t.lower() for t in types_include}):
                return False
        if types_exclude:
            itypes = set((item.get("types") or []))
            if itypes.intersection({t.lower() for t in types_exclude}):
                return False
        if require_phone_or_website and not (item.get("phone") or item.get("website")):
            return False
        if only_open_now is True and item.get("open_now") is not True:
            return False
        if business_status_in:
            bs = (item.get("business_status") or "").upper()
            if bs not in {s.upper() for s in business_status_in}:
                return False
        return True

    filtered = [x for x in leads_out if pass_filters(x)]

    # Dedupe
    seen: set = set()
    unique: List[Dict[str, Any]] = []
    for x in filtered:
        if dedupe_by == "name_address":
            key = (str(x.get("name") or "").strip().lower(), str(x.get("formatted_address") or "").strip().lower())
        else:
            key = x.get("place_id")
        if key in seen:
            continue
        seen.add(key)
        unique.append(x)

    meta = {
        "query": raw.get("query", {}),
        "location": raw.get("location", {}),
        "note": raw.get("note"),
        "filters": {
            "min_rating": min_rating,
            "min_user_ratings_total": min_user_ratings_total,
            "types_include": types_include,
            "types_exclude": types_exclude,
            "require_phone_or_website": require_phone_or_website,
            "only_open_now": only_open_now,
            "business_status_in": business_status_in,
            "dedupe_by": dedupe_by,
        },
    }

    if output_format == "csv":
        import io, csv
        default_cols = [
            "name",
            "formatted_address",
            "latitude",
            "longitude",
            "place_id",
            "types",
            "rating",
            "user_ratings_total",
            "business_status",
            "phone",
            "phone_intl",
            "website",
        ]
        cols = [c for c in (csv_columns or default_cols) if c in default_cols]
        if not cols:
            cols = default_cols

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(cols)
        for x in unique:
            row = []
            for c in cols:
                val = x.get(c)
                if c == "types":
                    val = ";".join(x.get("types") or [])
                row.append(val)
            writer.writerow(row)
        csv_text = buf.getvalue()
        return {
            "total": len(unique),
            "columns": cols,
            "csv": csv_text,
            **meta,
            "content_type": "text/csv; charset=utf-8",
        }

    out: Dict[str, Any] = {
        "leads": unique,
        "total": len(unique),
        **meta,
    }

    # Optional: export to Google Sheets (best-effort)
    auto_export = os.environ.get("GOOGLE_SHEETS_AUTO_EXPORT", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if export_to_google_sheets or auto_export:
        try:
            # TEK SAYFA: Her zaman aynı sayfaya yaz (keyword'e göre ayrı sayfa yok)
            # sheet_name parametresi varsa onu kullan, yoksa env'den al, yoksa "Leads" default
            sheet_name = google_sheets_sheet_name or os.environ.get("GOOGLE_SHEETS_SHEET_NAME") or "Leads"
            appender = GoogleSheetsAppender(
                spreadsheet_id=google_sheets_spreadsheet_id,
                sheet_name=sheet_name,
            )
            header, rows = leads_to_sheet_rows(
                leads=unique,
                meta=meta,
                keyword=keyword,
                include_raw_json=google_sheets_include_raw_json,
            )
            appender.ensure_header(header)
            write_res = appender.append_rows(rows)
            out["google_sheets"] = {
                "ok": True,
                "auto_export": auto_export,
                "spreadsheet_id": write_res.spreadsheet_id,
                "sheet_name": write_res.sheet_name,
                "updated_range": write_res.updated_range,
                "updated_rows": write_res.updated_rows,
                "rows_sent": len(rows),
            }
        except Exception as e:
            out["google_sheets"] = {
                "ok": False,
                "auto_export": auto_export,
                "error": str(e),
            }

    return out


def main() -> None:
    app.run()


if __name__ == "__main__":
    main()




