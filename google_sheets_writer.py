#!/usr/bin/env python3
"""
Google Sheets append helper (service account).

Env options:
- GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON: service account JSON content (string)
- GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE: path to service account JSON file
- GOOGLE_SHEETS_SPREADSHEET_ID: default spreadsheet id
- GOOGLE_SHEETS_SHEET_NAME: default sheet/tab name
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _load_service_account() -> Dict[str, Any]:
    raw = os.environ.get("GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON")
    path = os.environ.get("GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE")

    if raw:
        try:
            return json.loads(raw)
        except Exception as e:
            raise RuntimeError("GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON geçerli JSON değil") from e

    if path:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError as e:
            raise RuntimeError("GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE bulunamadı") from e
        except Exception as e:
            raise RuntimeError("GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE okunamadı") from e

    raise RuntimeError(
        "Google Sheets için kimlik bulunamadı. "
        "GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON veya GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE ayarlayın."
    )


@dataclass(frozen=True)
class GoogleSheetsWriteResult:
    spreadsheet_id: str
    sheet_name: str
    updated_range: Optional[str]
    updated_rows: Optional[int]


class GoogleSheetsAppender:
    """Append rows to a Google Sheet, ensuring a header exists."""

    def __init__(
        self,
        *,
        spreadsheet_id: Optional[str] = None,
        sheet_name: Optional[str] = None,
    ) -> None:
        self.spreadsheet_id = spreadsheet_id or os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID")
        # HER ZAMAN "Leads" sayfasına yaz - yeni sayfa oluşturma
        self.sheet_name = "Leads"
        if not self.spreadsheet_id:
            raise RuntimeError("spreadsheet_id verilmedi (GOOGLE_SHEETS_SPREADSHEET_ID).")

        # Lazy imports so Maps usage doesn't require sheets deps unless used
        from google.oauth2.service_account import Credentials  # type: ignore
        from googleapiclient.discovery import build  # type: ignore

        sa_info = _load_service_account()
        creds = Credentials.from_service_account_info(
            sa_info,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        self._service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    def _get_first_row(self) -> List[Any]:
        rng = f"{self.sheet_name}!1:1"
        resp = (
            self._service.spreadsheets()
            .values()
            .get(spreadsheetId=self.spreadsheet_id, range=rng)
            .execute()
        )
        values = resp.get("values") or []
        if not values:
            return []
        return values[0] or []

    def _check_sheet_exists(self) -> bool:
        """Check if sheet tab exists. Returns True if exists, False otherwise."""
        meta = (
            self._service.spreadsheets()
            .get(spreadsheetId=self.spreadsheet_id, fields="sheets(properties(title))")
            .execute()
        )
        existing = {
            (s.get("properties") or {}).get("title")
            for s in (meta.get("sheets") or [])
        }
        return self.sheet_name in existing

    def ensure_header(self, header: Sequence[str]) -> None:
        """Ensure header exists. Raises RuntimeError if sheet does not exist."""
        if not self._check_sheet_exists():
            raise RuntimeError(
                f'Google Sheets sayfası "{self.sheet_name}" bulunamadı. '
                f'Lütfen önce "{self.sheet_name}" adında bir sayfa oluşturun.'
            )
        existing = [str(x) for x in self._get_first_row() if x is not None]
        if existing:
            return
        rng = f"{self.sheet_name}!A1"
        (
            self._service.spreadsheets()
            .values()
            .update(
                spreadsheetId=self.spreadsheet_id,
                range=rng,
                valueInputOption="RAW",
                body={"values": [list(header)]},
            )
            .execute()
        )

    def append_rows(self, rows: Sequence[Sequence[Any]]) -> GoogleSheetsWriteResult:
        """Append rows to sheet. Raises RuntimeError if sheet does not exist."""
        if not self._check_sheet_exists():
            raise RuntimeError(
                f'Google Sheets sayfası "{self.sheet_name}" bulunamadı. '
                f'Lütfen önce "{self.sheet_name}" adında bir sayfa oluşturun.'
            )
        rng = f"{self.sheet_name}!A1"
        resp = (
            self._service.spreadsheets()
            .values()
            .append(
                spreadsheetId=self.spreadsheet_id,
                range=rng,
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": [list(r) for r in rows]},
            )
            .execute()
        )
        upd = (resp.get("updates") or {})
        return GoogleSheetsWriteResult(
            spreadsheet_id=self.spreadsheet_id,
            sheet_name=self.sheet_name,
            updated_range=upd.get("updatedRange"),
            updated_rows=upd.get("updatedRows"),
        )


# Türkiye posta kodu -> il eşleşmeleri (ilk 2 hane)
# Posta kodları il bazlıdır (örn: 55xxx = Samsun, 54xxx = Sakarya)
POSTAL_CODE_TO_IL: Dict[str, str] = {
    "01": "adana", "02": "adiyaman", "03": "afyonkarahisar", "04": "agri", "05": "amasya",
    "06": "ankara", "07": "antalya", "08": "artvin", "09": "aydin", "10": "balikesir",
    "11": "bilecik", "12": "bingol", "13": "bitlis", "14": "bolu", "15": "burdur",
    "16": "bursa", "17": "canakkale", "18": "cankiri", "19": "corum", "20": "denizli",
    "21": "diyarbakir", "22": "edirne", "23": "elazig", "24": "erzincan", "25": "erzurum",
    "26": "eskisehir", "27": "gaziantep", "28": "giresun", "29": "gumushane", "30": "hakkari",
    "31": "hatay", "32": "isparta", "33": "mersin", "34": "istanbul", "35": "izmir",
    "36": "kars", "37": "kastamonu", "38": "kayseri", "39": "kirklareli", "40": "kirsehir",
    "41": "kocaeli", "42": "konya", "43": "kutahya", "44": "malatya", "45": "manisa",
    "46": "kahramanmaras", "47": "mardin", "48": "mugla", "49": "mus", "50": "nevsehir",
    "51": "nigde", "52": "ordu", "53": "rize", "54": "sakarya", "55": "samsun",
    "56": "siirt", "57": "sinop", "58": "sivas", "59": "tekirdag", "60": "tokat",
    "61": "trabzon", "62": "tunceli", "63": "sanliurfa", "64": "usak", "65": "van",
    "66": "yozgat", "67": "zonguldak", "68": "aksaray", "69": "bayburt", "70": "karaman",
    "71": "kirikkale", "72": "batman", "73": "sirnak", "74": "bartin", "75": "ardahan",
    "76": "igdir", "77": "yalova", "78": "karabuk", "79": "kilis", "80": "osmaniye",
    "81": "duzce",
}

def _normalize_il_name(il_name: str) -> str:
    """Normalize il name for comparison (lowercase, remove extra spaces)."""
    return il_name.strip().lower().replace("ı", "i").replace("ğ", "g").replace("ü", "u").replace("ş", "s").replace("ö", "o").replace("ç", "c")


def _extract_postal_code_from_address(address: str) -> Optional[str]:
    """Extract postal code (5 digits) from address string."""
    # 5 haneli posta kodu ara (örn: "55020", "55300")
    match = re.search(r'\b(\d{5})\b', address)
    if match:
        return match.group(1)
    return None


def _get_il_from_postal_code(postal_code: str) -> Optional[str]:
    """Get il name from postal code (first 2 digits)."""
    if len(postal_code) >= 2:
        prefix = postal_code[:2]
        return POSTAL_CODE_TO_IL.get(prefix)
    return None


def _is_ilce_valid_for_il(ilce: Optional[str], il: Optional[str], address: Optional[str] = None) -> bool:
    """
    Dynamically check if ilçe belongs to il using postal code validation.
    If postal code in address doesn't match il, ilçe is invalid.
    """
    if not ilce or not il:
        return True  # Bilinmeyen durumlar için True döndür
    
    # Posta kodundan il tespiti yap
    if address:
        postal_code = _extract_postal_code_from_address(address)
        if postal_code:
            postal_il = _get_il_from_postal_code(postal_code)
            if postal_il:
                il_norm = _normalize_il_name(il)
                postal_il_norm = _normalize_il_name(postal_il)
                # Posta kodu il'i ile location_text il'i eşleşmiyorsa, ilçe geçersiz
                if il_norm != postal_il_norm:
                    return False
    
    # Posta kodu yoksa veya eşleşiyorsa, True döndür (filtreleme yapma)
    return True


def _extract_il_from_location_text(location_text: str) -> Optional[str]:
    """Extract il (province) name from location_text."""
    if not location_text:
        return None
    # "Türkiye" kelimesini temizle
    loc_clean = location_text.replace("Türkiye", "").replace("turkey", "").strip()
    # Virgülle split et, son kısmı al
    parts = [p.strip() for p in loc_clean.split(",")]
    if parts:
        return parts[-1].strip()
    return loc_clean.strip() if loc_clean else None


def _parse_il_from_address_only(address: Optional[str]) -> Optional[str]:
    """
    Parse il (province) from address ONLY, without location_text dependency.
    Used for strict filtering - returns the actual il in the address.
    """
    if not address:
        return None
    
    # POSTA KODU İLE İL TESPİTİ (en güvenilir - ÖNCELİKLİ)
    postal_code = _extract_postal_code_from_address(address)
    if postal_code:
        postal_il = _get_il_from_postal_code(postal_code)
        if postal_il:
            return postal_il
    
    # Adres içinden il parse et
    parts = [p.strip() for p in address.split(",")]
    if not parts:
        return None
    
    # Son kısmı al (genelde il burada)
    last_part = parts[-1].strip()
    
    # "Türkiye" kelimesini temizle
    last_part = last_part.replace("Türkiye", "").replace("turkey", "").strip()
    
    # Posta kodunu kaldır (örn: "55020 Samsun" -> "Samsun")
    last_part = re.sub(r'\d{5}\s*', '', last_part).strip()
    
    if not last_part:
        # Son kısım boşsa, bir önceki kısmı kontrol et
        if len(parts) >= 2:
            second_last = parts[-2].strip()
            # Posta kodu içermiyorsa ve kısa bir isimse, il olabilir
            if not re.search(r'\d{5}', second_last) and len(second_last.split()) <= 3:
                last_part = second_last
    
    # "/" ile split et (İlçe/İl formatı)
    if "/" in last_part:
        ilce_il = [p.strip() for p in last_part.split("/")]
        if len(ilce_il) >= 2:
            parsed_il = ilce_il[1]
        elif len(ilce_il) == 1:
            parsed_il = ilce_il[0]
        else:
            parsed_il = last_part
    else:
        parsed_il = last_part
    
    # Temizle ve normalize et
    if parsed_il:
        parsed_il = parsed_il.strip()
        # Bilinen il isimlerini kontrol et (POSTAL_CODE_TO_IL'deki değerlerle eşleş)
        parsed_il_norm = _normalize_il_name(parsed_il)
        # POSTAL_CODE_TO_IL'deki değerlerle eşleşen bir il var mı?
        for il_name in POSTAL_CODE_TO_IL.values():
            if _normalize_il_name(il_name) == parsed_il_norm:
                return il_name  # Normalize edilmiş haliyle döndür
        
        # Eşleşme yoksa, yine de parse edilen il'i döndür (belki yeni bir il ismi)
        return parsed_il
    
    return None


def _parse_il_ilce(address: Optional[str], location_text: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse il (province) and ilçe (district) from formatted_address.
    CRITICAL: Validates il against location_text using postal code - if they don't match, uses location_text's il.
    This prevents wrong il/ilce combinations (e.g., "Samsun" search returning "Malatya" results).
    """
    # Önce location_text'ten il bilgisini çıkar
    location_il = None
    if location_text:
        location_il = _extract_il_from_location_text(location_text)
    
    if not address:
        # Adres yoksa, location_text'ten il döndür
        return location_il, None
    
    # POSTA KODU İLE İL DOĞRULAMASI (en güvenilir yöntem)
    postal_code = _extract_postal_code_from_address(address)
    postal_il = None
    if postal_code:
        postal_il = _get_il_from_postal_code(postal_code)
    
    # Adres içinden il/ilçe parse et
    parts = [p.strip() for p in address.split(",")]
    if not parts:
        return location_il, None
    
    last_part = parts[-1]
    parsed_il = None
    parsed_ilce = None
    
    # "/" ile split et (İlçe/İl formatı)
    if "/" in last_part:
        ilce_il = [p.strip() for p in last_part.split("/")]
        if len(ilce_il) >= 2:
            parsed_il = ilce_il[1]
            # İlçe'yi temizle (posta kodu varsa kaldır)
            parsed_ilce = re.sub(r'^\d{5}\s*', '', ilce_il[0]).strip()
        elif len(ilce_il) == 1:
            parsed_il = ilce_il[0]
    else:
        # "/" yoksa, son kısmı il olarak kabul et
        parsed_il = last_part
    
    # İlçe'yi adres içinden daha iyi çıkarmaya çalış (virgülle ayrılmış kısımlardan)
    if not parsed_ilce and len(parts) >= 2:
        # Son iki kısmı kontrol et (genelde "İlçe, İl" formatı)
        # Örnek: "İlkadım, 55020 Samsun" -> parts[-2] = "İlkadım"
        second_last = parts[-2].strip()
        # Posta kodu içermiyorsa ve kısa bir isimse, ilçe olabilir
        if not re.search(r'\d{5}', second_last) and len(second_last.split()) <= 3:
            parsed_ilce = second_last
    
    # KRİTİK DOĞRULAMA: Posta kodu ile il kontrolü
    final_il = parsed_il
    final_ilce = parsed_ilce
    
    if location_il:
        loc_il_norm = _normalize_il_name(location_il)
        
        # Posta kodu varsa, onu kullan (en güvenilir)
        if postal_il:
            postal_il_norm = _normalize_il_name(postal_il)
            if loc_il_norm != postal_il_norm:
                # Posta kodu il'i location_text ile eşleşmiyor - location_text'ten il kullan
                final_il = location_il
                final_ilce = None  # İl yanlışsa, ilçe de yanlıştır
            else:
                # Posta kodu eşleşiyor, parse edilen il'i kullan
                if parsed_il:
                    parsed_il_norm = _normalize_il_name(parsed_il)
                    if loc_il_norm != parsed_il_norm:
                        # Parse edilen il yanlış, location_text'ten al
                        final_il = location_il
                else:
                    final_il = location_il
        else:
            # Posta kodu yok, parse edilen il ile location_text'i karşılaştır
            if parsed_il:
                parsed_il_norm = _normalize_il_name(parsed_il)
                if loc_il_norm not in parsed_il_norm and parsed_il_norm not in loc_il_norm:
                    # Eşleşmiyor, location_text'ten il kullan
                    final_il = location_il
                    final_ilce = None  # İl yanlışsa, ilçe de yanlıştır
            else:
                final_il = location_il
    
    # İlçe doğrulaması: Posta kodu ile kontrol et
    if final_il and final_ilce and address:
        if not _is_ilce_valid_for_il(final_ilce, final_il, address):
            final_ilce = None
    
    return final_il, final_ilce


def _is_turkish_mobile(phone_clean: str) -> bool:
    """
    Check if phone number is Turkish mobile (cep telefonu).
    Turkish mobile numbers: 05XX format, 11 digits total.
    Valid mobile prefixes: 0505, 0506, 0507, 0530-0539, 0541-0546, 0549, 0551-0555
    """
    if len(phone_clean) != 11:
        return False
    if not phone_clean.startswith("05"):
        return False
    
    # İlk 4 haneyi al (05XX)
    prefix = phone_clean[:4]
    # Geçerli cep telefonu prefix'leri
    valid_mobile_prefixes = [
        "0505", "0506", "0507",
        "0530", "0531", "0532", "0533", "0534", "0535", "0536", "0537", "0538", "0539",
        "0541", "0542", "0543", "0544", "0545", "0546",
        "0549",
        "0551", "0552", "0553", "0554", "0555",
    ]
    return prefix in valid_mobile_prefixes


def _normalize_phone(phone: str) -> str:
    """Normalize phone number: remove spaces, dashes, parentheses, +90 prefix."""
    # +90 prefix'ini kaldır
    phone = phone.replace("+90", "").replace("+ 90", "").strip()
    # Boşluk, tire, parantez kaldır
    phone = re.sub(r'[\s\-\(\)]', '', phone)
    return phone


def _split_phone(phone: Optional[str], phone_intl: Optional[str]) -> Tuple[str, str]:
    """
    Split phone into Cep Telefonu (mobile) and Normal Telefon (landline) according to Turkish standards.
    Cep: 05XX format, 11 digits (Turkish mobile prefixes)
    Normal: Diğerleri (sabit hat, kurumsal hatlar vb.)
    Returns "-----" if not found.
    """
    cep = ""
    normal = ""
    
    # Önce phone_intl'e bak (uluslararası format: +90 ...)
    if phone_intl:
        phone_intl_clean = _normalize_phone(phone_intl)
        if _is_turkish_mobile(phone_intl_clean):
            cep = phone_intl  # Orijinal formatı koru
        else:
            normal = phone_intl
    
    # phone varsa ve cep boşsa, phone'a bak
    if phone and not cep:
        phone_clean = _normalize_phone(phone)
        if _is_turkish_mobile(phone_clean):
            cep = phone  # Orijinal formatı koru
        else:
            normal = phone
    
    return cep or "-----", normal or "-----"


def _pick_category_for_keyword(keyword: str) -> str:
    """
    Pick category name based on keyword for Google Sheets.
    Returns human-readable category name.
    """
    k = (keyword or "").strip().lower()

    # Built-in category mapping
    category_map: Dict[str, str] = {
        "tarım makina": "Tarım Makina",
        "tarim makina": "Tarım Makina",
        "tarım makine": "Tarım Makina",
        "tarim makine": "Tarım Makina",
        "makina": "Tarım Makina",
        "makine": "Tarım Makina",
        "ilaç bayi": "İlaç Bayi",
        "ilac bayi": "İlaç Bayi",
        "ilaç": "İlaç Bayi",
        "ilac": "İlaç Bayi",
        "ziraat odası": "Ziraat Odaları",
        "ziraat odasi": "Ziraat Odaları",
        "ziraat odaları": "Ziraat Odaları",
        "ziraat odalari": "Ziraat Odaları",
        "çiftçi kooperatifi": "Çiftçi Kooperatifi",
        "ciftci kooperatifi": "Çiftçi Kooperatifi",
        "kooperatif": "Çiftçi Kooperatifi",
        "kooparatif": "Çiftçi Kooperatifi",
    }

    user_map_raw = os.environ.get("GOOGLE_SHEETS_KEYWORD_CATEGORY_MAP", "").strip()
    if user_map_raw:
        try:
            user_map = json.loads(user_map_raw)
            if isinstance(user_map, dict):
                for kk, vv in user_map.items():
                    if isinstance(kk, str) and isinstance(vv, str) and kk.strip() and vv.strip():
                        category_map[kk.strip().lower()] = vv.strip()
        except Exception:
            pass

    for sub in sorted(category_map.keys(), key=len, reverse=True):
        if sub and sub in k:
            return category_map[sub]

    return "Genel Tarım"


def _extract_email_from_website(website: Optional[str]) -> str:
    """
    Try to extract email from website or return "-----".
    Google Places API doesn't provide email directly.
    """
    if not website:
        return "-----"
    # Website'den e-posta çıkarmak genelde mümkün değil
    # Şimdilik boş bırakıyoruz, ileride website'den contact formu parse edilebilir
    return "-----"


def leads_to_sheet_rows(
    *,
    leads: Sequence[Dict[str, Any]],
    meta: Dict[str, Any],
    keyword: str = "",
    include_raw_json: bool = False,
) -> Tuple[List[str], List[List[Any]]]:
    """
    Convert maps_mcp output to (header, rows) for Google Sheets.
    Format: Kategori, Bayi Adı, İl, İlçe, Cep Telefonu, Normal Telefon, E-posta
    Missing values are shown as "-----".
    """
    category = _pick_category_for_keyword(keyword)
    location_text = (meta.get("query") or {}).get("location_text", "")
    
    header: List[str] = [
        "Kategori",
        "Bayi Adı",
        "İl",
        "İlçe",
        "Cep Telefonu",
        "Normal Telefon",
        "E-posta",
    ]

    rows: List[List[Any]] = []
    for x in leads:
        address = x.get("formatted_address")
        il, ilce = _parse_il_ilce(address, location_text)
        cep, normal = _split_phone(x.get("phone"), x.get("phone_intl"))
        email = _extract_email_from_website(x.get("website"))
        
        row = [
            category,
            x.get("name") or "-----",
            il or "-----",
            ilce or "-----",
            cep,
            normal,
            email,
        ]
        rows.append(row)

    return header, rows


