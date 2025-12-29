#!/usr/bin/env python3
"""
MCP Server for European Government Tenders from TED (Tenders Electronic Daily)
"""
from datetime import date, timedelta
from typing import Optional, List
import logging

from fastmcp import FastMCP
from ted_api_client import TEDApiClient
from ted_models import TedSearchResponse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [MCP] - %(message)s')

app = FastMCP()

@app.tool()
async def search_ted_tenders(
    search_text: str,
    country_codes: Optional[List[str]] = None,
    limit: int = 10,
    page: int = 1
):
    """
    TED'de kamu ihalelerini arar.
    - Expert query içinde son 30 günü (PD>=) ve sıralamayı uygular.
    - scope='ACTIVE' ile güncel ilanları getirir.
    - FT aramasında OR mantığını kullanır (FT~(...) OR FT~(...)).
    - İleri tarihli son başvuru (deadline) olan ilanları öne çıkarır.
    """
    client = TEDApiClient()
    api = await client.search_tenders(
        search_text=search_text,
        country_codes=country_codes,
        limit=limit,
        page=page,
        days_back=120,  # Açık ilanlar için yayın penceresini genişlet
        scope="ACTIVE"
    )

    if "error" in api:
        return api

    parsed = TedSearchResponse.model_validate(api)
    logging.info(f"Received {len(parsed.tenders)} results from API (page={page}).")

    today = date.today()

    # 1) Başvurusu açık ilanlar: deadline >= bugün
    open_by_deadline = [t for t in parsed.tenders if t.deadline and t.deadline >= today]

    # 2) Eğer deadline yok veya hiç açık ilan gelmediyse, yayın tarihine göre 30 gün kontrolü uygula (geri uyum)
    if not open_by_deadline:
        thirty_days_ago = today - timedelta(days=30)
        open_by_pubdate = [t for t in parsed.tenders if thirty_days_ago <= t.publication_date <= today]
        final_list = open_by_pubdate if open_by_pubdate else parsed.tenders
        results_are_recent = bool(open_by_pubdate)
    else:
        # Deadline'a göre en yakın tarihten uzağa sırala
        final_list = sorted(open_by_deadline, key=lambda t: (t.deadline or today))
        results_are_recent = True

    final = TedSearchResponse(
        total_found=len(final_list),
        tenders=final_list,
        page=parsed.page,
        results_are_recent=results_are_recent
    )
    return final.model_dump()

if __name__ == "__main__":
    app.run()
