"""
RECKONING RADAR — AGENT PIPELINE
=================================
Adapted from Breakout Radar's three-agent architecture.

Breakout Radar:          →    Reckoning Radar:
─────────────────────────────────────────────────
Scout Agent              →    Document Scout Agent
Verification Harness     →    Evidence Verification Harness  
Decision Engine          →    Intelligence Decision Engine

Instead of finding talent, we find:
- Named persons in documents
- Locations referenced
- Dates and events
- Relationships between entities
- Confidence levels for every extraction

Stack:
- Anthropic API (claude-haiku for Scout, claude-sonnet for Verification + Decision)
- Supabase (database from schema we built)
- httpx (async HTTP for DOJ/Jmail document fetching)
- pdfplumber (PDF text extraction)
- Plausible Analytics (anonymous usage tracking)

Run: python pipeline.py --document_url <url> --batch <batch_name>
"""

import os
import json
import asyncio
import hashlib
import re
from datetime import datetime, date
from typing import Optional
import anthropic
import httpx
import pdfplumber
import io
from supabase import create_client, Client

# ─── CONFIGURATION ─────────────────────────────────────────────────────────────

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
SUPABASE_URL      = os.environ.get("SUPABASE_URL")
SUPABASE_KEY      = os.environ.get("SUPABASE_SERVICE_KEY")  # Service key for agent writes

# Model routing — same hybrid logic as Breakout Radar
SCOUT_MODEL       = "claude-haiku-4-5-20251001"   # Fast, cheap — first pass extraction
VERIFICATION_MODEL = "claude-sonnet-4-6"           # Careful — quality checking
DECISION_MODEL    = "claude-sonnet-4-6"            # Thorough — final confidence scoring

client     = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ─── VICTIM PROTECTION — NON-NEGOTIABLE ────────────────────────────────────────
# These terms trigger immediate flagging and human review
# Agents NEVER write victim records — they flag for human review only
VICTIM_PROTECTION_KEYWORDS = [
    "victim", "survivor", "minor", "underage", "trafficking victim",
    "jane doe", "john doe", "alleged victim", "complainant"
]


# ══════════════════════════════════════════════════════════════════════════════
# STEP 0: DOCUMENT FETCHER
# Retrieves and extracts text from DOJ PDFs or Jmail pages
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_document(url: str) -> dict:
    """
    Fetches a document from DOJ or Jmail and extracts raw text.
    Returns metadata + extracted text ready for Scout Agent.
    """
    print(f"\n📄 FETCHING: {url}")

    async with httpx.AsyncClient(timeout=httpx.Timeout(300.0, connect=30.0)) as http:
        response = await http.get(url, follow_redirects=True)
        response.raise_for_status()

    content_type = response.headers.get("content-type", "")
    raw_bytes    = response.content

    # ── PDF extraction ──────────────────────────────────────────────────────
    if "pdf" in content_type or url.endswith(".pdf"):
        text = ""
        has_encoding_artifacts = False

        with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
            page_count = len(pdf.pages)
            for page in pdf.pages:
                page_text = page.extract_text() or ""
                text += page_text + "\n"

                # Detect DOJ encoding artifacts (the = sign issue)
                if page_text.count("=") > 20:
                    has_encoding_artifacts = True

        # Assess OCR quality
        total_chars   = len(text)
        readable_chars = len(re.findall(r'[a-zA-Z0-9\s.,;:\'"!?-]', text))
        quality_ratio  = readable_chars / total_chars if total_chars > 0 else 0

        if quality_ratio > 0.85:
            ocr_quality = "clean"
        elif quality_ratio > 0.70:
            ocr_quality = "minor_artifacts"
        elif quality_ratio > 0.50:
            ocr_quality = "degraded"
        else:
            ocr_quality = "poor"

        return {
            "url":                   url,
            "content_type":          "pdf",
            "raw_text":              text,
            "page_count":            page_count,
            "ocr_quality":           ocr_quality,
            "has_encoding_artifacts": has_encoding_artifacts,
            "file_size_kb":          len(raw_bytes) // 1024,
            "fetched_at":            datetime.utcnow().isoformat()
        }

    # ── HTML/text extraction (Jmail pages) ─────────────────────────────────
    else:
        text = response.text
        # Strip HTML tags for clean text
        clean_text = re.sub(r'<[^>]+>', ' ', text)
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()

        return {
            "url":                   url,
            "content_type":          "html",
            "raw_text":              clean_text,
            "page_count":            1,
            "ocr_quality":           "clean",
            "has_encoding_artifacts": False,
            "file_size_kb":          len(raw_bytes) // 1024,
            "fetched_at":            datetime.utcnow().isoformat()
        }


# ══════════════════════════════════════════════════════════════════════════════
# AGENT 1: DOCUMENT SCOUT
# Adapted from Breakout Radar's Scout Agent
# Instead of searching 8 platforms, scans documents for entities
# Uses cheap/fast Haiku for first-pass extraction
# ══════════════════════════════════════════════════════════════════════════════

SCOUT_SYSTEM_PROMPT = """
You are the Document Scout for Reckoning Radar, an accountability intelligence system
analyzing publicly released Jeffrey Epstein investigation files from the US Department of Justice.

Your role is FIRST-PASS EXTRACTION. Read the document text and extract every:
1. Named person (full names, partial names, initials, titles)
2. Location (properties, cities, countries, aircraft tail numbers, vessels)
3. Date or time reference (exact dates, approximate dates, date ranges, years)
4. Event (meetings, flights, transactions, communications)
5. Organization (companies, institutions, agencies)

CRITICAL VICTIM PROTECTION RULES:
- If any person is described as a victim, survivor, minor, trafficking victim, 
  complainant, or Jane/John Doe — DO NOT extract their name
- Instead flag: {"victim_flag": true, "requires_human_review": true}
- This is non-negotiable. Victim privacy is absolute.

DOCUMENT TYPE DETECTION:
Identify what type of document this is:
- flight_manifest: passenger lists, tail numbers, routes, dates
- email: sender, recipient, date, subject, body
- financial_record: transactions, amounts, accounts
- fbi_report: interview summaries, investigation notes
- court_filing: legal documents, depositions
- photograph: image metadata, caption references
- contact_book_entry: names, phone numbers, addresses
- other: anything else

OUTPUT FORMAT — respond only with valid JSON:
{
  "document_type": "flight_manifest|email|financial_record|fbi_report|court_filing|photograph|contact_book_entry|other",
  "document_date": "YYYY-MM-DD or null",
  "date_precision": "exact|approximate|range|year_only|unknown",
  "persons_found": [
    {
      "name": "Full Name",
      "name_as_written": "Exactly as it appears in document",
      "context": "Brief note on how they appear",
      "is_redacted": false,
      "possible_victim": false
    }
  ],
  "locations_found": [
    {
      "name": "Location name",
      "location_type": "private_residence|island|private_aircraft|hotel|city|country|other",
      "context": "How it appears in document"
    }
  ],
  "events_found": [
    {
      "event_type": "flight|property_visit|meeting|communication|financial_transaction|other",
      "date": "YYYY-MM-DD or null",
      "date_precision": "exact|approximate|range|year_only|unknown",
      "persons_involved": ["name1", "name2"],
      "location": "location name or null",
      "description": "brief description"
    }
  ],
  "organizations_found": ["org1", "org2"],
  "victim_flags": [],
  "scout_notes": "Any observations about document quality, unusual content, encoding issues",
  "requires_human_review": false
}
"""

async def run_scout_agent(document: dict, doc_text_chunk: str) -> dict:
    """
    Scout Agent — fast first-pass extraction using Haiku.
    Processes document in chunks if too long.
    """
    print(f"\n🔍 SCOUT AGENT running on {document['content_type']} document...")

    start_time = datetime.utcnow()

    response = client.messages.create(
        model=SCOUT_MODEL,
        max_tokens=4096,
        system=SCOUT_SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"""
Document URL: {document['url']}
Document OCR Quality: {document['ocr_quality']}
Has Encoding Artifacts: {document['has_encoding_artifacts']}
Page Count: {document['page_count']}

DOCUMENT TEXT:
{doc_text_chunk}

Extract all entities following the JSON format specified.
"""
            }
        ]
    )

    processing_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)

    # Parse JSON response
    raw_output = response.content[0].text
    # Strip any markdown code fences if present
    clean_output = re.sub(r'```json\n?|\n?```', '', raw_output).strip()

    try:
        scout_data = json.loads(clean_output)
    except json.JSONDecodeError:
        print("⚠️  Scout JSON parse error — attempting recovery...")
        scout_data = {
            "document_type":    "other",
            "persons_found":    [],
            "locations_found":  [],
            "events_found":     [],
            "organizations_found": [],
            "victim_flags":     [],
            "scout_notes":      f"JSON parse failed. Raw: {raw_output[:200]}",
            "requires_human_review": True
        }

    scout_data["_processing_ms"] = processing_ms
    scout_data["_model"]         = SCOUT_MODEL
    scout_data["_tokens_used"]   = response.usage.input_tokens + response.usage.output_tokens

    persons_count   = len(scout_data.get("persons_found", []))
    locations_count = len(scout_data.get("locations_found", []))
    events_count    = len(scout_data.get("events_found", []))

    print(f"   ✓ Found: {persons_count} persons, {locations_count} locations, {events_count} events")
    print(f"   ✓ Document type: {scout_data.get('document_type', 'unknown')}")
    print(f"   ✓ Completed in {processing_ms}ms using {scout_data['_tokens_used']} tokens")

    return scout_data


# ══════════════════════════════════════════════════════════════════════════════
# AGENT 2: EVIDENCE VERIFICATION HARNESS
# Adapted from Breakout Radar's Verification Harness
# Instead of auditing social media data quality,
# cross-references extractions against existing database records
# and assigns confidence levels
# Uses Sonnet for careful, deliberate analysis
# ══════════════════════════════════════════════════════════════════════════════

VERIFICATION_SYSTEM_PROMPT = """
You are the Evidence Verification Harness for Reckoning Radar.

Your role is to take the Scout Agent's raw extractions and:

1. ASSESS CONFIDENCE for each entity extracted:
   - confirmed: Multiple independent source types, clean document, unredacted name
   - corroborated: Multiple docs same type OR mixed quality sources  
   - indicated: Single credible source OR quality/redaction issues present
   - unverified: Heavy redaction, degraded OCR, single mention, or conflicting info

2. IDENTIFY CONFLICTS — any extraction that contradicts known database records

3. VALIDATE DATES — flag impossible or suspicious date combinations

4. ASSESS EVIDENCE STRENGTH for person-to-person relationships:
   - strong: 3+ independent documents
   - moderate: 2 documents or 1 strong primary source
   - weak: single mention only

5. GENERATE UPGRADE GAP NOTES — for each unverified or indicated item,
   what specific evidence would upgrade its confidence level?

6. FLIGHT MANIFEST SPECIFIC — for flight_manifest document type:
   - Extract tail number
   - Identify origin and destination clearly
   - List ALL passengers with their confidence level
   - Flag any redacted passenger slots

CRITICAL: You are working with COURT-QUALITY evidence standards.
Every confidence assessment must be defensible to a lawyer or judge.
Do not upgrade confidence beyond what the evidence actually supports.

OUTPUT FORMAT — respond only with valid JSON:
{
  "verification_summary": "Brief narrative of what this document proves",
  "document_confidence": "confirmed|corroborated|indicated|unverified",
  "ocr_reliability": "reliable|questionable|unreliable",
  "verified_persons": [
    {
      "name": "Full Name",
      "confidence": "confirmed|corroborated|indicated|unverified",
      "verification_notes": "Why this confidence level",
      "upgrade_gap": "What would upgrade this to next level",
      "is_redacted": false,
      "name_recovered": false
    }
  ],
  "verified_locations": [
    {
      "name": "Location name",
      "confidence": "confirmed|corroborated|indicated|unverified",
      "verification_notes": "Why this confidence level"
    }
  ],
  "verified_events": [
    {
      "event_type": "flight|property_visit|meeting|communication|other",
      "date": "YYYY-MM-DD or null",
      "date_precision": "exact|approximate|range|year_only|unknown",
      "confidence": "confirmed|corroborated|indicated|unverified",
      "persons_present": ["name1", "name2"],
      "location": "location name",
      "upgrade_gap": "What would upgrade confidence",
      "verification_notes": "Reasoning"
    }
  ],
  "flight_details": null,
  "conflicts_detected": [
    {
      "conflict_type": "date|person|location|other",
      "description": "What conflicts with what",
      "document_claim": "What this document says",
      "conflicting_claim": "What conflicts with it"
    }
  ],
  "anomalies": ["Any suspicious patterns or data quality issues"],
  "verification_notes": "Overall assessment of document reliability",
  "requires_human_review": false,
  "human_review_reason": null
}
"""

async def run_verification_harness(
    scout_data: dict,
    document: dict,
    existing_db_context: str
) -> dict:
    """
    Verification Harness — careful cross-referencing using Sonnet.
    Checks Scout extractions against existing database records.
    """
    print(f"\n🔬 VERIFICATION HARNESS running...")

    start_time = datetime.utcnow()

    response = client.messages.create(
        model=VERIFICATION_MODEL,
        max_tokens=4096,
        system=VERIFICATION_SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"""
SCOUT AGENT EXTRACTIONS:
{json.dumps(scout_data, indent=2)}

DOCUMENT METADATA:
- OCR Quality: {document['ocr_quality']}
- Has Encoding Artifacts: {document['has_encoding_artifacts']}
- Content Type: {document['content_type']}
- Page Count: {document['page_count']}

EXISTING DATABASE CONTEXT (persons/locations already known):
{existing_db_context}

Verify all extractions, assign confidence levels, and identify any conflicts
with existing database records. Apply court-quality evidence standards.
"""
            }
        ]
    )

    processing_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)

    raw_output  = response.content[0].text
    clean_output = re.sub(r'```json\n?|\n?```', '', raw_output).strip()

    try:
        verification_data = json.loads(clean_output)
    except json.JSONDecodeError:
        print("⚠️  Verification JSON parse error...")
        verification_data = {
            "document_confidence":    "unverified",
            "verified_persons":       [],
            "verified_locations":     [],
            "verified_events":        [],
            "conflicts_detected":     [],
            "anomalies":              ["JSON parse failed"],
            "requires_human_review":  True,
            "human_review_reason":    "Verification agent output unparseable"
        }

    verification_data["_processing_ms"] = processing_ms
    verification_data["_model"]         = VERIFICATION_MODEL
    verification_data["_tokens_used"]   = response.usage.input_tokens + response.usage.output_tokens

    conflicts = len(verification_data.get("conflicts_detected", []))
    persons   = len(verification_data.get("verified_persons", []))
    doc_conf  = verification_data.get("document_confidence", "unknown")

    print(f"   ✓ Document confidence: {doc_conf}")
    print(f"   ✓ Verified {persons} persons")
    print(f"   ✓ Conflicts detected: {conflicts}")
    print(f"   ✓ Completed in {processing_ms}ms")

    return verification_data


# ══════════════════════════════════════════════════════════════════════════════
# AGENT 3: INTELLIGENCE DECISION ENGINE
# Adapted from Breakout Radar's Decision Engine
# Instead of calculating Crossover Velocity,
# calculates Power Index scores, network centrality,
# and generates upgrade pathways for evidence gaps
# ══════════════════════════════════════════════════════════════════════════════

DECISION_SYSTEM_PROMPT = """
You are the Intelligence Decision Engine for Reckoning Radar.

You receive verified extractions from the Evidence Verification Harness and make
final determinations about:

1. POWER INDEX SCORES (0-100) for each person:
   - public_profile: How prominent/influential is this person publicly?
     (Head of state = 95, Cabinet member = 85, Celebrity = 75, Executive = 65, Unknown = 20)
   - institutional: Strength of institutional affiliations
     (Multiple major institutions = 90, Single major = 70, Minor = 40, None = 10)
   - network_centrality: How connected are they to other high-profile names in this document?
     (Connected to 5+ known figures = 90, 3-4 = 70, 1-2 = 40, none = 10)
   - corroboration: How well-evidenced is their presence?
     Maps directly to confidence: confirmed=90, corroborated=70, indicated=40, unverified=15

2. RELATIONSHIP STRENGTH between co-present persons:
   - strong: 3+ independent documents showing connection
   - moderate: 2 documents or 1 strong primary
   - weak: single mention

3. PATTERN FLAGS — suspicious patterns that warrant attention:
   - Communication drop after key dates (arrests, investigations)
   - Multiple document types showing same person
   - Presence at multiple Epstein properties
   - Financial + physical co-presence combination

4. FINAL CONFIDENCE DETERMINATION for the overall document's intelligence value

5. EVIDENCE CHAIN SUMMARY — a clear, defensible narrative of what this document
   proves, suitable for use in a legal brief or investigative article

OUTPUT FORMAT — respond only with valid JSON:
{
  "intelligence_value": "high|medium|low",
  "intelligence_summary": "2-3 sentence summary suitable for legal brief or article",
  "persons_intelligence": [
    {
      "name": "Full Name",
      "final_confidence": "confirmed|corroborated|indicated|unverified",
      "power_index": {
        "public_profile": 0,
        "institutional": 0,
        "network_centrality": 0,
        "corroboration": 0
      },
      "category_inference": "finance|politics|royalty|entertainment|academia|technology|legal|media|other",
      "pattern_flags": [],
      "upgrade_gap": "Specific evidence needed to upgrade confidence"
    }
  ],
  "relationship_determinations": [
    {
      "person_a": "Name",
      "person_b": "Name",
      "relationship_type": "co_traveler|financial|social|professional|unknown",
      "evidence_strength": "strong|moderate|weak",
      "co_occurrence_count": 1,
      "notes": "Context of relationship"
    }
  ],
  "pattern_flags": [
    {
      "flag_type": "communication_drop|multi_location|multi_doc_type|financial_physical|other",
      "description": "What pattern was detected",
      "persons_involved": ["name1"],
      "significance": "high|medium|low"
    }
  ],
  "flight_intelligence": null,
  "decision_log": [
    "Step 1: ...",
    "Step 2: ...",
    "Step 3: ..."
  ],
  "evidence_chain": "Formal evidence chain statement suitable for legal use"
}
"""

async def run_decision_engine(
    verification_data: dict,
    scout_data: dict,
    document: dict
) -> dict:
    """
    Intelligence Decision Engine — final scoring and pattern detection.
    """
    print(f"\n⚖️  DECISION ENGINE running...")

    start_time = datetime.utcnow()

    response = client.messages.create(
        model=DECISION_MODEL,
        max_tokens=4096,
        system=DECISION_SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"""
VERIFICATION HARNESS OUTPUT:
{json.dumps(verification_data, indent=2)}

SCOUT AGENT ORIGINAL EXTRACTIONS:
{json.dumps(scout_data, indent=2)}

DOCUMENT: {document['url']}
TYPE: {scout_data.get('document_type', 'unknown')}
DATE: {scout_data.get('document_date', 'unknown')}

Generate final intelligence determinations including Power Index scores,
relationship strengths, pattern flags, and an evidence chain summary
suitable for use in a legal brief or investigative article.
"""
            }
        ]
    )

    processing_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)

    raw_output   = response.content[0].text
    clean_output = re.sub(r'```json\n?|\n?```', '', raw_output).strip()

    try:
        decision_data = json.loads(clean_output)
    except json.JSONDecodeError:
        print("⚠️  Decision Engine JSON parse error...")
        decision_data = {
            "intelligence_value":       "low",
            "intelligence_summary":     "Processing error — human review required",
            "persons_intelligence":     [],
            "relationship_determinations": [],
            "pattern_flags":            [],
            "decision_log":             ["JSON parse failed"],
            "evidence_chain":           "Unable to generate — human review required"
        }

    decision_data["_processing_ms"] = processing_ms
    decision_data["_model"]         = DECISION_MODEL
    decision_data["_tokens_used"]   = response.usage.input_tokens + response.usage.output_tokens

    intel_value = decision_data.get("intelligence_value", "unknown")
    patterns    = len(decision_data.get("pattern_flags", []))
    relations   = len(decision_data.get("relationship_determinations", []))

    print(f"   ✓ Intelligence value: {intel_value}")
    print(f"   ✓ Relationships mapped: {relations}")
    print(f"   ✓ Pattern flags: {patterns}")
    print(f"   ✓ Completed in {processing_ms}ms")

    return decision_data


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE WRITER
# Takes all three agent outputs and writes to Supabase
# Handles deduplication, conflict detection, and trigger-safe upserts
# ══════════════════════════════════════════════════════════════════════════════

async def write_to_database(
    document_meta: dict,
    scout_data: dict,
    verification_data: dict,
    decision_data: dict,
    batch_id: str
) -> dict:
    """
    Writes all agent outputs to Supabase.
    Deduplicates persons/locations by name.
    Creates all junction table records.
    Logs conflicts and flags victims for human review.
    """
    print(f"\n💾 WRITING TO DATABASE...")
    results = {
        "document_id":     None,
        "persons_written": 0,
        "locations_written": 0,
        "events_written":  0,
        "conflicts_logged": 0,
        "victim_flags":    0
    }

    # ── 1. Write document record ──────────────────────────────────────────────
    doc_hash = hashlib.md5(document_meta["url"].encode()).hexdigest()

    doc_record = {
        "doc_reference_id":        f"DOC-{doc_hash[:8].upper()}",
        "document_type":           scout_data.get("document_type", "other"),
        "source":                  "doj_release",
        "document_date":           scout_data.get("document_date"),
        "date_precision":          scout_data.get("date_precision", "unknown"),
        "release_batch":           batch_id,
        "ocr_quality":             document_meta.get("ocr_quality", "clean"),
        "redaction_status":        "none",
        "has_encoding_artifacts":  document_meta.get("has_encoding_artifacts", False),
        "doj_url":                 document_meta["url"],
        "page_count":              document_meta.get("page_count", 1),
        "file_size_kb":            document_meta.get("file_size_kb", 0),
        "is_processed":            True,
        "processed_at":            datetime.utcnow().isoformat(),
        "notes":                   verification_data.get("verification_summary", "")
    }

    doc_result = supabase.table("documents").upsert(
        doc_record,
        on_conflict="doc_reference_id"
    ).execute()

    document_id = doc_result.data[0]["id"] if doc_result.data else None
    results["document_id"] = document_id
    print(f"   ✓ Document written: {doc_record['doc_reference_id']}")

    # ── 2. Write persons ──────────────────────────────────────────────────────
    persons_intel = {
        p["name"]: p
        for p in decision_data.get("persons_intelligence", [])
    }

    for person in verification_data.get("verified_persons", []):
        name = person.get("name", "").strip()
        if not name or len(name) < 2:
            continue

        # VICTIM PROTECTION — flag and skip, never write
        if person.get("possible_victim") or any(
            kw in name.lower() for kw in VICTIM_PROTECTION_KEYWORDS
        ):
            results["victim_flags"] += 1
            supabase.table("conflict_records").insert({
                "entity_type":    "person",
                "entity_id":      document_id,
                "conflict_field": "victim_flag",
                "document_a_id":  document_id,
                "document_a_claim": f"Possible victim: {name}",
                "document_b_claim": "Requires human review before any database write"
            }).execute()
            print(f"   🛡️  Victim flag — human review required: {name}")
            continue

        # Get intelligence scores for this person
        intel = persons_intel.get(name, {})
        power = intel.get("power_index", {})

        # Upsert person (deduplication by full_name)
        person_record = {
            "full_name":              name,
            "confidence":             person.get("confidence", "unverified"),
            "redaction_status":       "partial" if person.get("is_redacted") else "none",
            "name_recovered":         person.get("name_recovered", False),
            "power_public_profile":   power.get("public_profile"),
            "power_institutional":    power.get("institutional"),
            "power_network_centrality": power.get("network_centrality"),
            "power_corroboration":    power.get("corroboration"),
            "upgrade_gap_note":       person.get("upgrade_gap"),
            "victim_protected":       False,
            "is_victim":              False,
            "flagged_for_review":     person.get("requires_human_review", False)
        }

        # Category inference if available
        if intel.get("category_inference"):
            person_record["category"] = intel["category_inference"]

        person_result = supabase.table("persons").upsert(
            person_record,
            on_conflict="full_name"
        ).execute()

        if person_result.data:
            person_id = person_result.data[0]["id"]

            # Write person ↔ document junction
            supabase.table("person_documents").upsert({
                "person_id":   person_id,
                "document_id": document_id,
                "confidence":  person.get("confidence", "unverified")
            }, on_conflict="person_id,document_id").execute()

            results["persons_written"] += 1

    # ── 3. Write locations ────────────────────────────────────────────────────
    location_id_map = {}  # name → id for event creation

    for location in verification_data.get("verified_locations", []):
        name = location.get("name", "").strip()
        if not name or len(name) < 2:
            continue

        location_record = {
            "name":          name,
            "location_type": location.get("location_type", "other"),
            "confidence":    location.get("confidence", "unverified")
        }

        loc_result = supabase.table("locations").upsert(
            location_record,
            on_conflict="name"
        ).execute()

        if loc_result.data:
            location_id_map[name] = loc_result.data[0]["id"]
            results["locations_written"] += 1

    # ── 4. Write events and event ↔ person junctions ─────────────────────────
    for event in verification_data.get("verified_events", []):
        # Find location ID
        location_name = event.get("location")
        location_id   = location_id_map.get(location_name) if location_name else None

        event_record = {
            "event_type":           event.get("event_type", "other"),
            "title":                event.get("description", f"{event.get('event_type', 'Event')} — {event.get('date', 'unknown date')}"),
            "event_date":           event.get("date"),
            "date_precision":       event.get("date_precision", "unknown"),
            "primary_location_id":  location_id,
            "confidence":           event.get("confidence", "unverified"),
            "upgrade_gap_note":     event.get("upgrade_gap"),
            "notes":                event.get("verification_notes", "")
        }

        event_result = supabase.table("events").insert(event_record).execute()

        if event_result.data:
            event_id = event_result.data[0]["id"]
            results["events_written"] += 1

            # Link event ↔ document
            supabase.table("event_documents").insert({
                "event_id":    event_id,
                "document_id": document_id,
                "support_type": "primary_proof"
            }).execute()

            # Link event ↔ persons present
            for person_name in event.get("persons_present", []):
                person_lookup = supabase.table("persons").select("id").eq(
                    "full_name", person_name
                ).execute()

                if person_lookup.data:
                    supabase.table("event_persons").upsert({
                        "event_id":  event_id,
                        "person_id": person_lookup.data[0]["id"],
                        "confidence": event.get("confidence", "unverified")
                    }, on_conflict="event_id,person_id").execute()

    # ── 5. Write relationships ────────────────────────────────────────────────
    for rel in decision_data.get("relationship_determinations", []):
        person_a_lookup = supabase.table("persons").select("id").eq(
            "full_name", rel.get("person_a", "")
        ).execute()
        person_b_lookup = supabase.table("persons").select("id").eq(
            "full_name", rel.get("person_b", "")
        ).execute()

        if person_a_lookup.data and person_b_lookup.data:
            supabase.table("person_relationships").upsert({
                "person_a_id":        person_a_lookup.data[0]["id"],
                "person_b_id":        person_b_lookup.data[0]["id"],
                "relationship_type":  rel.get("relationship_type", "unknown"),
                "evidence_strength":  rel.get("evidence_strength", "weak"),
                "co_occurrence_count": rel.get("co_occurrence_count", 1),
                "notes":              rel.get("notes", ""),
                "source_document_ids": [document_id] if document_id else []
            }, on_conflict="person_a_id,person_b_id").execute()

    # ── 6. Write conflict records ─────────────────────────────────────────────
    for conflict in verification_data.get("conflicts_detected", []):
        supabase.table("conflict_records").insert({
            "entity_type":    conflict.get("conflict_type", "other"),
            "entity_id":      document_id,
            "conflict_field": conflict.get("description", ""),
            "document_a_id":  document_id,
            "document_a_claim": conflict.get("document_claim", ""),
            "document_b_claim": conflict.get("conflicting_claim", "")
        }).execute()
        results["conflicts_logged"] += 1

    # ── 7. Log agent processing ───────────────────────────────────────────────
    total_tokens = (
        scout_data.get("_tokens_used", 0) +
        verification_data.get("_tokens_used", 0) +
        decision_data.get("_tokens_used", 0)
    )

    supabase.table("agent_processing_log").insert({
        "agent_name":          "full_pipeline",
        "document_id":         document_id,
        "batch_id":            batch_id,
        "status":              "complete",
        "persons_extracted":   results["persons_written"],
        "locations_extracted": results["locations_written"],
        "events_created":      results["events_written"],
        "conflicts_flagged":   results["conflicts_logged"],
        "model_used":          f"scout:{SCOUT_MODEL} | verify:{VERIFICATION_MODEL} | decision:{DECISION_MODEL}",
        "tokens_used":         total_tokens,
        "completed_at":        datetime.utcnow().isoformat()
    }).execute()

    print(f"\n   📊 DATABASE WRITE COMPLETE:")
    print(f"      Persons:   {results['persons_written']}")
    print(f"      Locations: {results['locations_written']}")
    print(f"      Events:    {results['events_written']}")
    print(f"      Conflicts: {results['conflicts_logged']}")
    print(f"      Victim flags (human review): {results['victim_flags']}")
    print(f"      Total tokens: {total_tokens:,}")

    return results


# ══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE ORCHESTRATOR
# Runs all three agents in sequence on a single document
# ══════════════════════════════════════════════════════════════════════════════

async def get_database_context(person_names: list, location_names: list) -> str:
    """
    Pulls existing database records for context during verification.
    Helps the Verification Harness spot conflicts with known data.
    """
    context_parts = []

    if person_names:
        # Check which persons are already in database
        existing = supabase.table("persons").select(
            "full_name, confidence, category, total_document_count"
        ).in_("full_name", person_names[:20]).execute()  # Limit to 20 for context window

        if existing.data:
            context_parts.append("KNOWN PERSONS IN DATABASE:")
            for p in existing.data:
                context_parts.append(
                    f"  - {p['full_name']} | confidence: {p['confidence']} | "
                    f"docs: {p['total_document_count']} | category: {p.get('category', 'unknown')}"
                )

    if location_names:
        existing_locs = supabase.table("locations").select(
            "name, location_type, total_visit_count"
        ).in_("name", location_names[:10]).execute()

        if existing_locs.data:
            context_parts.append("\nKNOWN LOCATIONS IN DATABASE:")
            for l in existing_locs.data:
                context_parts.append(
                    f"  - {l['name']} | type: {l['location_type']} | "
                    f"visits: {l['total_visit_count']}"
                )

    return "\n".join(context_parts) if context_parts else "No existing records found for these entities."


async def process_document(url: str, batch_id: str = "manual") -> dict:
    """
    Full pipeline orchestrator.
    Runs Document Scout → Evidence Verification → Intelligence Decision → Database Write.
    """
    print(f"\n{'='*60}")
    print(f"RECKONING RADAR — PROCESSING DOCUMENT")
    print(f"{'='*60}")
    print(f"URL:   {url}")
    print(f"Batch: {batch_id}")
    print(f"Time:  {datetime.utcnow().isoformat()}")
    print(f"{'='*60}")

    pipeline_start = datetime.utcnow()

    # ── Step 0: Fetch document ────────────────────────────────────────────────
    document = await fetch_document(url)
    raw_text = document["raw_text"]

    # Chunk if too long (Haiku context limit safety)
    MAX_CHARS = 80000
    if len(raw_text) > MAX_CHARS:
        print(f"   ⚠️  Document too long ({len(raw_text):,} chars) — processing first {MAX_CHARS:,} chars")
        text_chunk = raw_text[:MAX_CHARS]
    else:
        text_chunk = raw_text

    # ── Step 1: Scout Agent ───────────────────────────────────────────────────
    scout_data = await run_scout_agent(document, text_chunk)

    # Check for human review flag from Scout
    if scout_data.get("requires_human_review"):
        print(f"\n🚨 HUMAN REVIEW REQUIRED — Scout flagged this document")
        print(f"   Reason: {scout_data.get('scout_notes', 'Unknown')}")

    # ── Step 2: Get database context for Verification ─────────────────────────
    person_names   = [p["name"] for p in scout_data.get("persons_found", [])]
    location_names = [l["name"] for l in scout_data.get("locations_found", [])]
    db_context     = await get_database_context(person_names, location_names)

    # ── Step 3: Verification Harness ─────────────────────────────────────────
    verification_data = await run_verification_harness(scout_data, document, db_context)

    # ── Step 4: Decision Engine ───────────────────────────────────────────────
    decision_data = await run_decision_engine(verification_data, scout_data, document)

    # ── Step 5: Write to database ─────────────────────────────────────────────
    db_results = await write_to_database(
        document, scout_data, verification_data, decision_data, batch_id
    )

    # ── Pipeline complete ─────────────────────────────────────────────────────
    total_ms = int((datetime.utcnow() - pipeline_start).total_seconds() * 1000)

    print(f"\n{'='*60}")
    print(f"✅ PIPELINE COMPLETE — {total_ms/1000:.1f}s total")
    print(f"   Intelligence value: {decision_data.get('intelligence_value', 'unknown').upper()}")
    print(f"   Evidence chain: {decision_data.get('evidence_chain', '')[:100]}...")
    print(f"{'='*60}\n")

    return {
        "document_id":      db_results["document_id"],
        "intelligence_value": decision_data.get("intelligence_value"),
        "evidence_chain":   decision_data.get("evidence_chain"),
        "persons_written":  db_results["persons_written"],
        "locations_written": db_results["locations_written"],
        "events_written":   db_results["events_written"],
        "victim_flags":     db_results["victim_flags"],
        "total_ms":         total_ms,
        "pattern_flags":    decision_data.get("pattern_flags", [])
    }


# ══════════════════════════════════════════════════════════════════════════════
# BATCH PROCESSOR
# Process multiple documents from a list of URLs
# Includes rate limiting to be respectful of DOJ servers
# ══════════════════════════════════════════════════════════════════════════════

async def process_batch(urls: list[str], batch_name: str, delay_seconds: float = 2.0) -> list:
    """
    Process a batch of document URLs sequentially with rate limiting.
    """
    print(f"\n🚀 BATCH PROCESSING: {len(urls)} documents")
    print(f"   Batch name: {batch_name}")
    print(f"   Rate limit: {delay_seconds}s between documents\n")

    results = []

    for i, url in enumerate(urls, 1):
        print(f"\n[{i}/{len(urls)}] Processing...")
        try:
            result = await process_document(url, batch_id=batch_name)
            results.append({"url": url, "status": "success", **result})
        except Exception as e:
            print(f"   ❌ ERROR: {e}")
            results.append({"url": url, "status": "error", "error": str(e)})

        # Rate limiting — be respectful of DOJ servers
        if i < len(urls):
            print(f"   ⏳ Waiting {delay_seconds}s before next document...")
            await asyncio.sleep(delay_seconds)

    # Summary
    successful = len([r for r in results if r["status"] == "success"])
    print(f"\n{'='*60}")
    print(f"BATCH COMPLETE: {successful}/{len(urls)} successful")
    print(f"{'='*60}")

    return results


# ══════════════════════════════════════════════════════════════════════════════
# PLAUSIBLE ANALYTICS — Anonymous Usage Tracking
# Privacy-first, no personal data, GDPR compliant
# Tracks: page views, search queries (anonymized), feature usage
# ══════════════════════════════════════════════════════════════════════════════

async def track_event(event_name: str, props: dict = None):
    """
    Sends anonymous event to Plausible Analytics.
    No personal data, no cookies, no fingerprinting.
    Install: add <script> tag to frontend pointing to plausible.io
    """
    # This is called from the frontend, not the pipeline
    # Included here as reference for the frontend integration
    payload = {
        "name":   event_name,
        "url":    "https://thereckoningradar.com",
        "domain": "thereckoningradar.com",
        "props":  props or {}
    }
    # Frontend calls: plausible('search', {props: {query_type: 'person'}})
    # No personal data in props — only query types and feature names
    return payload


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python pipeline.py <document_url> [batch_name]")
        print("Example: python pipeline.py https://www.justice.gov/epstein/doc-001.pdf dec_2025")
        sys.exit(1)

    url        = sys.argv[1]
    batch_name = sys.argv[2] if len(sys.argv) > 2 else "manual"

    asyncio.run(process_document(url, batch_name))
