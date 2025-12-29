import httpx
import json
import os
import logging
from typing import List, Optional, Dict, Any, Union
from datetime import date, datetime, timedelta
from ted_models import TedTender, TedSearchResponse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [API] - %(message)s')

TED_API_BASE_URL = "https://api.ted.europa.eu/v3/notices/search"

def _first_text(val: Union[str, dict, list], lang_keys=("eng", "en", "EN")) -> str:
    if isinstance(val, str):
        return val.strip()
    if isinstance(val, dict):
        for lk in lang_keys:
            if lk in val:
                v = val[lk]
                if isinstance(v, list) and v:
                    return str(v[0]).strip()
                if isinstance(v, str):
                    return v.strip()
        for v in val.values():
            if isinstance(v, list) and v:
                return str(v[0]).strip()
            if isinstance(v, str):
                return v.strip()
    if isinstance(val, list) and val:
        return _first_text(val[0], lang_keys)
    return ""

def _parse_iso_date(d: Optional[str]) -> Optional[date]:
    if not d:
        return None
    s = str(d).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        try:
            return date.fromisoformat(s[:10])
        except Exception:
            return None

# Yeni: karma yapılardan tarih bulucu (deadline vb. için)
def _find_first_date(obj: Any) -> Optional[date]:
    if obj is None:
        return None
    if isinstance(obj, str):
        return _parse_iso_date(obj)
    if isinstance(obj, (int, float)):
        # epoch vb. beklemiyoruz; desteklemiyoruz
        return None
    if isinstance(obj, list):
        for item in obj:
            dt = _find_first_date(item)
            if dt:
                return dt
        return None
    if isinstance(obj, dict):
        # Yaygın anahtar isimlerini önce dene
        for key in [
            "deadline", "deadline-date", "deadlineDate",
            "time-limit", "time-limit-receipt-tenders", "timeLimitReceiptTenders",
            "date", "value"
        ]:
            if key in obj:
                dt = _find_first_date(obj.get(key))
                if dt:
                    return dt
        # Aksi halde tüm değerleri dene
        for v in obj.values():
            dt = _find_first_date(v)
            if dt:
                return dt
        return None
    return None

def _pick_country_code(place_of_perf: Union[str, list, dict]) -> str:
    vals: List[str] = []
    if isinstance(place_of_perf, str):
        vals = [place_of_perf]
    elif isinstance(place_of_perf, list):
        vals = [str(x) for x in place_of_perf if isinstance(x, (str, int))]
    elif isinstance(place_of_perf, dict):
        for v in place_of_perf.values():
            if isinstance(v, str):
                vals.append(v)
            elif isinstance(v, list):
                vals.extend([str(x) for x in v if isinstance(x, (str, int))])
    for v in vals:
        v = v.strip()
        if len(v) == 3 and v.isalpha():
            return v.upper()
    return (vals[0].upper() if vals else "N/A")

def _expand_terms(search_text: str) -> List[str]:
    """Add synonyms only for UAV-like queries; otherwise return the raw term/phrase."""
    s = (search_text or "").strip()
    if not s:
        return []
    terms = [s]
    low = s.lower()
    if any(k in low for k in ["drone", "uav", "uas", "rpas", "unmanned"]):
        terms.extend(["drone", "UAV", "UAS", "RPAS", "unmanned"])
    # unique while keeping order
    seen = set()
    out = []
    for t in terms:
        if t.lower() not in seen:
            seen.add(t.lower())
            out.append(t)
    return out

def _ft_or_clause(terms: List[str]) -> Optional[str]:
    if not terms:
        return None
    parts = []
    for t in terms:
        # Quote phrases (contains whitespace or special chars)
        quoted = f"\"{t}\"" if any(ch.isspace() for ch in t) else t
        parts.append(f'FT~({quoted})')
    if len(parts) == 1:
        return parts[0]
    return "(" + " OR ".join(parts) + ")"

class TEDApiClient:
    def __init__(self, timeout: int = 30):
        headers = {
            "User-Agent": "Public Procurement Watcher / 1.0",
            "Content-Type": "application/json",
        }
        api_key = os.environ.get("TED_API_KEY")
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        self.client = httpx.AsyncClient(timeout=timeout, headers=headers)

    async def search_tenders(
        self,
        search_text: str = "",
        country_codes: Optional[List[str]] = None,
        limit: int = 10,
        page: int = 1,
        days_back: int = 30,
        scope: str = "ACTIVE",
    ) -> Dict[str, Any]:
        today = date.today()
        from_date = (today - timedelta(days=days_back)).strftime("%Y%m%d")

        query_parts: List[str] = []

        # Full-text (OR) clause
        ft = _ft_or_clause(_expand_terms(search_text))
        if ft:
            query_parts.append(ft)

        # place-of-performance filter (space-separated list inside IN())
        if country_codes:
            code_map = {
                "DE":"DEU","FR":"FRA","IT":"ITA","ES":"ESP","PL":"POL","RO":"ROU","NL":"NLD","BE":"BEL",
                "GR":"GRC","CZ":"CZE","PT":"PRT","HU":"HUN","SE":"SWE","AT":"AUT","BG":"BGR","DK":"DNK",
                "FI":"FIN","SK":"SVK","IE":"IRL","HR":"HRV","LT":"LTU","SI":"SVN","LV":"LVA","EE":"EST",
                "CY":"CYP","LU":"LUX","MT":"MLT"
            }
            iso3 = [code_map.get(c.upper(), c.upper()) for c in country_codes]
            query_parts.append(f'(place-of-performance IN ({" ".join(iso3)}))')

        # Date filter (publication date)
        query_parts.append(f'(PD>={from_date})')

        # Final expert query with sorting
        final_query = " AND ".join(query_parts) + " SORT BY publication-date DESC"

        payload = {
            "query": final_query,
            "fields": [
                "publication-number",
                "notice-title",
                "publication-date",
                "place-of-performance",
                "buyer-name",
                # Desteklenen deadline alanları (lot/part seviyeleri)
                "deadline-receipt-tender-date-lot",
                "deadline-date-lot",
                "deadline-date-part",
                "deadline-time-lot",
                "deadline-time-part",
                "public-opening-date-lot"
            ],
            "page": page,
            "limit": max(1, min(int(limit), 250)),
            "scope": scope,                       # ACTIVE | LATEST | ALL
            "checkQuerySyntax": False,
            "paginationMode": "PAGE_NUMBER"
        }

        logging.info(f"Sending TED API Request - Body: {json.dumps(payload, indent=2)}")
        try:
            resp = await self.client.post(TED_API_BASE_URL, json=payload)
            logging.info(f"Received TED API Response - Status: {resp.status_code}")
            logging.debug(f"Response Body: {resp.text}")
            resp.raise_for_status()
            data = resp.json()

            items = data.get("notices", []) or data.get("items", [])
            tenders: List[TedTender] = []

            for n in items:
                pub_no = n.get("publication-number") or n.get("ND")
                if not pub_no:
                    continue
                title_raw = n.get("notice-title") or n.get("TI")
                title = _first_text(title_raw) or "No Title Found"
                pd_raw = n.get("publication-date") or n.get("PD")
                pub_date = _parse_iso_date(pd_raw) or today
                buyer_raw = n.get("buyer-name")
                buyer_name = _first_text(buyer_raw) if buyer_raw else "Not specified"
                pop = n.get("place-of-performance") or n.get("CY") or []
                country_code = _pick_country_code(pop)
                url = f"https://ted.europa.eu/en/notice/-/detail/{pub_no}"

                # Yeni: deadline çıkarımı
                deadline_val = (
                    n.get("deadline-receipt-tender-date-lot")
                    or n.get("deadline-date-lot")
                    or n.get("deadline-date-part")
                    or n.get("deadline-time-lot")
                    or n.get("deadline-time-part")
                    or n.get("public-opening-date-lot")
                )
                deadline_date = _find_first_date(deadline_val)

                tenders.append(TedTender(
                    id=str(pub_no),
                    title=title,
                    publication_date=pub_date,
                    country_code=country_code,
                    buyer_name=buyer_name,
                    deadline=deadline_date,
                    cpv_codes=[],
                    url=url
                ))

            total = data.get("totalNoticeCount") or data.get("total") or len(tenders)
            return TedSearchResponse(
                total_found=int(total),
                tenders=tenders,
                page=page
            ).model_dump()

        except httpx.HTTPStatusError as e:
            err = f"HTTP error: {e.response.status_code}. Response: {e.response.text}"
            logging.error(err)
            return {"error": err}
        except Exception as e:
            logging.exception("Unexpected error")
            return {"error": f"An unexpected error occurred: {str(e)}"}
