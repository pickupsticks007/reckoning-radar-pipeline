#!/usr/bin/env python3
"""
Reckoning Radar - Jwiki Scraper Pipeline v2
Maps to existing Supabase schema with enum types
"""

import os
import re
import time
import json
import requests
from datetime import datetime
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

JWIKI_PROFILES = [
    {"slug": "lesley-groff", "name": "Lesley Groff", "emailCount": 230115, "isTopic": False},
    {"slug": "rich-kahn", "name": "Rich Kahn", "emailCount": 92049, "isTopic": False},
    {"slug": "karyna-shuliak", "name": "Karyna Shuliak", "emailCount": 48933, "isTopic": False},
    {"slug": "larry-visosky", "name": "Larry Visoski", "emailCount": 38726, "isTopic": False},
    {"slug": "bella-klein-yale", "name": "Bella Klein", "emailCount": 28113, "isTopic": False},
    {"slug": "ann-rodriguez", "name": "Ann Rodriguez", "emailCount": 28068, "isTopic": False},
    {"slug": "boris-nikolic", "name": "Boris Nikolic", "emailCount": 21736, "isTopic": False},
    {"slug": "natalia-molotkova--3", "name": "Natalia Molotkova", "emailCount": 20559, "isTopic": False},
    {"slug": "stewart-oldfield-", "name": "Stewart Oldfield", "emailCount": 17320, "isTopic": False},
    {"slug": "paul-morris-gmail", "name": "Paul Morris", "emailCount": 15246, "isTopic": False},
    {"slug": "darren-indyke", "name": "Darren Indyke", "emailCount": 14742, "isTopic": False},
    {"slug": "daphne-wallace-gmail", "name": "Daphne Wallace", "emailCount": 14718, "isTopic": False},
    {"slug": "brice-gordon", "name": "Brice Gordon", "emailCount": 14644, "isTopic": False},
    {"slug": "kathy-ruemmler-egroupware", "name": "Kathy Ruemmler", "emailCount": 13773, "isTopic": False},
    {"slug": "ghislaine-maxwell", "name": "Ghislaine Maxwell", "emailCount": 13428, "isTopic": False},
    {"slug": "jojo-fontanilla", "name": "Jojo Fontanilla", "emailCount": 13294, "isTopic": False},
    {"slug": "melanie-spinella-", "name": "Melanie Spinella", "emailCount": 12701, "isTopic": False},
    {"slug": "merwin-dela-cruz-example", "name": "Merwin dela Cruz", "emailCount": 11790, "isTopic": False},
    {"slug": "joi-ito", "name": "Joi Ito", "emailCount": 10338, "isTopic": False},
    {"slug": "cecile-de-jongh--2", "name": "Cecile de Jongh", "emailCount": 9992, "isTopic": False},
    {"slug": "vahe-stepanian-db-2", "name": "Vahe Stepanian", "emailCount": 9796, "isTopic": False},
    {"slug": "david-mitchell", "name": "David Mitchell", "emailCount": 9407, "isTopic": False},
    {"slug": "farkas-andrew-l-", "name": "Andrew Farkas", "emailCount": 8942, "isTopic": False},
    {"slug": "sarah-kellen", "name": "Sarah Kellen", "emailCount": 8741, "isTopic": False},
    {"slug": "gary-kerney-verizon", "name": "Gary Kerney", "emailCount": 8717, "isTopic": False},
    {"slug": "peggy-siegal", "name": "Peggy Siegal", "emailCount": 8636, "isTopic": False},
    {"slug": "david-stern-", "name": "David Stern", "emailCount": 8145, "isTopic": False},
    {"slug": "nicole-junkermann", "name": "Nicole Junkermann", "emailCount": 7984, "isTopic": False},
    {"slug": "lawrence-krauss", "name": "Lawrence Krauss", "emailCount": 7966, "isTopic": False},
    {"slug": "eric-roth-intljet", "name": "Eric Roth", "emailCount": 7718, "isTopic": False},
    {"slug": "martin-a-nowak", "name": "Martin A. Nowak", "emailCount": 7378, "isTopic": False},
    {"slug": "richard-joslin", "name": "Richard Joslin", "emailCount": 7169, "isTopic": False},
    {"slug": "eva-dubin-dubinandco", "name": "Eva Dubin", "emailCount": 7050, "isTopic": False},
    {"slug": "noam-chomsky", "name": "Noam Chomsky", "emailCount": 7013, "isTopic": False},
    {"slug": "tom-pritzker", "name": "Tom Pritzker", "emailCount": 6995, "isTopic": False},
    {"slug": "faith-kates-extmodels", "name": "Faith Kates", "emailCount": 6945, "isTopic": False},
    {"slug": "a-de-rothschild", "name": "A. de Rothschild", "emailCount": 6740, "isTopic": False},
    {"slug": "eileen-alexanderson-apollo-adv=sors", "name": "Eileen Alexanderson", "emailCount": 6446, "isTopic": False},
    {"slug": "amanda-kirby", "name": "Amanda Kirby", "emailCount": 6349, "isTopic": False},
    {"slug": "brad-wechsler-wechsler", "name": "Brad Wechsler", "emailCount": 6300, "isTopic": False},
    {"slug": "deepak-chopra", "name": "Deepak Chopra", "emailCount": 6273, "isTopic": False},
    {"slug": "petermandelson", "name": "Peter Mandelson", "emailCount": 6270, "isTopic": False},
    {"slug": "sultan-bin-sulayem", "name": "Sultan Bin Sulayem", "emailCount": 6194, "isTopic": False},
    {"slug": "stephen-hanson--3", "name": "Steve Hanson", "emailCount": 6115, "isTopic": False},
    {"slug": "jean-luc-brunel", "name": "Jean-Luc Brunel", "emailCount": 6062, "isTopic": False},
    {"slug": "jes-staley-jpmorgan", "name": "Jes Staley", "emailCount": 6021, "isTopic": False},
    {"slug": "tazia-smith", "name": "Tazia Smith", "emailCount": 6019, "isTopic": False},
    {"slug": "paul-barrett-example", "name": "Paul Barrett", "emailCount": 5990, "isTopic": False},
    {"slug": "ehud-barak", "name": "Ehud Barak", "emailCount": 5182, "isTopic": False},
    {"slug": "larry-summers", "name": "Larry Summers", "emailCount": 5027, "isTopic": False},
    {"slug": "joscha-bach", "name": "Joscha Bach", "emailCount": 3298, "isTopic": False},
    {"slug": "reid-hoffman-greylock", "name": "Reid Hoffman", "emailCount": 2461, "isTopic": False},
    {"slug": "peter-thiel", "name": "Peter Thiel", "emailCount": 2429, "isTopic": False},
    {"slug": "woody-allen", "name": "Woody Allen", "emailCount": 1240, "isTopic": False},
    {"slug": "prince-andrew-duke-of-york", "name": "Prince Andrew, Duke of York", "emailCount": 1163, "isTopic": False},
    {"slug": "leon-black", "name": "Leon Black", "emailCount": 862, "isTopic": False},
    {"slug": "bill-gates", "name": "Bill Gates", "emailCount": 706, "isTopic": False},
    {"slug": "elon-musk", "name": "Elon Musk", "emailCount": 692, "isTopic": False},
    {"slug": "alan-dershowitz", "name": "Alan Dershowitz", "emailCount": 214, "isTopic": False},
    {"slug": "casey-wasserman", "name": "Casey Wasserman", "emailCount": 122, "isTopic": False},
    {"slug": "kimbal-musk", "name": "Kimbal Musk", "emailCount": 106, "isTopic": False},
    {"slug": "masha-drokov", "name": "Masha Drokova", "emailCount": 41, "isTopic": False},
    {"slug": "wexner-les", "name": "Les Wexner", "emailCount": 19, "isTopic": False},
    {"slug": "jason-calacanis", "name": "Jason Calacanis", "emailCount": 18, "isTopic": False},
    {"slug": "bill-clinton", "name": "Bill Clinton", "emailCount": 1, "isTopic": False},
    {"slug": "donald-trump", "name": "Donald Trump", "emailCount": 0, "isTopic": False},
    {"slug": "justin-trudeau", "name": "Justin Trudeau", "emailCount": 0, "isTopic": False},
    {"slug": "virginia-giuffre", "name": "Virginia Giuffre", "emailCount": 0, "isTopic": False},
]

# Category mapping for Claude's analysis -> Supabase enum
CATEGORY_MAP = {
    "finance": "finance", "financial": "finance", "banker": "finance", "investor": "finance",
    "politics": "politics", "political": "politics", "government": "politics",
    "royalty": "royalty", "royal": "royalty",
    "entertainment": "entertainment", "media": "media", "film": "entertainment",
    "academia": "academia", "academic": "academia", "scientist": "academia",
    "technology": "technology", "tech": "technology",
    "legal": "legal", "lawyer": "legal", "attorney": "legal",
    "law_enforcement": "law_enforcement",
    "other": "other"
}

RELATIONSHIP_MAP = {
    "employee": "employee", "assistant": "employee", "staff": "employee",
    "associate": "associate",
    "client": "alleged_client",
    "co-conspirator": "direct_contact", "accomplice": "direct_contact",
    "victim": "victim",
    "witness": "witness",
    "legal": "legal_counsel", "lawyer": "legal_counsel",
    "investigator": "investigator",
    "mentioned": "mentioned_only"
}


def analyze_with_claude(profile):
    """Use Claude to generate structured person data"""
    import anthropic
    
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    
    prompt = f"""You are analyzing {profile['name']} from the Epstein files archive (jmail.world).
They appear in {profile['emailCount']} emails in the archive.

Based on publicly documented information, provide a JSON analysis:
{{
  "public_title": "Their job title/role (e.g., 'Executive Assistant', 'Financier', 'Model Agent')",
  "category": "ONE of: finance|politics|royalty|entertainment|academia|technology|legal|law_enforcement|media|religion|military|other",
  "relationship": "ONE of: direct_contact|associate|alleged_client|employee|victim|investigator|legal_counsel|witness|mentioned_only",
  "summary": "2-3 sentence summary of who they are and documented connection to Epstein",
  "power_public": 0-100 (public profile score),
  "power_institutional": 0-100 (institutional power score),
  "nationality": "Country name or empty string",
  "is_deceased": true/false,
  "is_convicted": true/false
}}

Return ONLY valid JSON, no other text."""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}]
        )
        result_text = message.content[0].text.strip()
        if result_text.startswith("```"):
            result_text = re.sub(r'^```json?\n?', '', result_text)
            result_text = re.sub(r'\n?```$', '', result_text)
        return json.loads(result_text)
    except Exception as e:
        print(f"  Claude error: {e}")
        return {
            "public_title": "Associate",
            "category": "other",
            "relationship": "associate",
            "summary": f"{profile['name']} appears in the Epstein email archive.",
            "power_public": 20,
            "power_institutional": 20,
            "nationality": "",
            "is_deceased": False,
            "is_convicted": False
        }


def upsert_person(supabase, profile, analysis):
    """Insert person into Supabase using correct schema"""
    try:
        # Map to enum values
        category = analysis.get('category', 'other')
        if category not in ['finance','politics','royalty','entertainment','academia','technology','legal','law_enforcement','media','religion','military','other']:
            category = 'other'
        
        relationship = analysis.get('relationship', 'mentioned_only')
        if relationship not in ['direct_contact','associate','alleged_client','employee','victim','investigator','legal_counsel','witness','mentioned_only']:
            relationship = 'associate'
        
        is_deceased = analysis.get('is_deceased', False)
        is_convicted = analysis.get('is_convicted', False)
        if is_convicted:
            status = 'convicted'
        elif is_deceased:
            status = 'deceased'
        else:
            status = 'unknown'
        
        data = {
            "full_name": profile['name'],
            "public_title": analysis.get('public_title', ''),
            "category": category,
            "relationship_to_epstein": relationship,
            "current_status": status,
            "total_document_count": profile['emailCount'],
            "notes": analysis.get('summary', ''),
            "power_public_profile": min(100, max(0, int(analysis.get('power_public', 20)))),
            "power_institutional": min(100, max(0, int(analysis.get('power_institutional', 20)))),
            "confidence": "indicated",
        }
        
        # Include nationality if present
        nat = analysis.get('nationality', '')
        if nat and nat.strip():
            data['nationality'] = [nat.strip()]
        
        # Use notes to also store slug and jwiki url for now
        data['notes'] = f"[jwiki:{profile['slug']}] {analysis.get('summary', '')}"
        
        result = supabase.table("persons").insert(data).execute()
        return True
    except Exception as e:
        print(f"  DB error: {e}")
        return False


def main():
    print("=== Reckoning Radar - Jwiki Import Pipeline ===")
    print(f"Processing {len(JWIKI_PROFILES)} profiles")
    
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("ERROR: Missing Supabase env vars")
        return
    
    supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    print("✓ Supabase connected")
    
    success = 0
    errors = 0
    
    for i, profile in enumerate(JWIKI_PROFILES):
        print(f"\n[{i+1}/{len(JWIKI_PROFILES)}] {profile['name']} ({profile['emailCount']} emails)")
        
        if ANTHROPIC_API_KEY:
            analysis = analyze_with_claude(profile)
            print(f"  → {analysis.get('category')} | {analysis.get('relationship')}")
        else:
            analysis = {"public_title": "Associate", "category": "other", 
                       "relationship": "associate", "summary": "", 
                       "power_public": 20, "power_institutional": 20,
                       "nationality": "", "is_deceased": False, "is_convicted": False}
        
        if upsert_person(supabase, profile, analysis):
            success += 1
            print(f"  ✓ Saved")
        else:
            errors += 1
        
        time.sleep(0.5)  # Rate limit Claude API
    
    print(f"\n=== DONE: {success} saved, {errors} failed ===")


if __name__ == "__main__":
    main()
