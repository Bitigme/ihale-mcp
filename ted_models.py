from typing import List, Optional
from pydantic import BaseModel, Field
from datetime import date

class CpvCode(BaseModel):
    code: str = Field(..., description="The CPV code, e.g., '71317210-4'.")
    description: Optional[str] = Field(None, description="The description of the CPV code.")

class TedTender(BaseModel):
    id: str = Field(..., description="The unique notice ID (publication-number), e.g., '00248363-2024'.")
    title: str = Field(..., description="The title of the tender notice.")
    publication_date: date = Field(..., description="The date when the notice was published.")
    country_code: str = Field(..., description="Top-level place-of-performance (ISO3 if available).")
    buyer_name: Optional[str] = Field(None, description="The contracting authority (buyer).")
    deadline: Optional[date] = Field(None, description="Submission deadline if available.")
    cpv_codes: List[CpvCode] = Field([], description="CPV codes associated with the tender.")
    url: str = Field(..., description="Direct URL to the notice on TED.")

class TedSearchResponse(BaseModel):
    total_found: int = Field(..., description="Total number of tenders found for the query.")
    tenders: List[TedTender] = Field(..., description="List of tenders on this page.")
    page: int = Field(..., description="Current page number.")
    results_are_recent: bool = Field(True, description="True if results are within last 30 days (or ACTIVE scope).")
