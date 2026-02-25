#!/usr/bin/env python3
"""
Reckoning Radar - Enrichment Pipeline
Updates existing persons rows with proper Claude analysis
"""

import os
import re
import time
import json
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

JWIKI_PROFILES = [
    {"slug": "lesley-groff", "name": "Lesley Groff", "emailCount": 230115},
    {"slug": "rich-kahn", "name": "Rich Kahn", "emailCount": 92049},
    {"slug": "karyna-shuliak", "name": "Karyna Shuliak", "emailCount": 48933},
    {"slug": "larry-visosky", "name": "Larry Visoski", "emailCount": 38726},
    {"slug": "bella-klein-yale", "name": "Bella Klein", "emailCount": 28113},
    {"slug": "ann-rodriguez", "name": "Ann Rodriguez", "emailCount": 28068},
    {"slug": "boris-nikolic", "name": "Boris Nikolic", "emailCount": 21736},
    {"slug": "natalia-molotkova--3", "name": "Natalia Molotkova", "emailCount": 20559},
    {"slug": "stewart-oldfield-", "name": "Stewart Oldfield", "emailCount": 17320},
    {"slug": "paul-morris-gmail", "name": "Paul Morris", "emailCount": 15246},
    {"slug": "darren-indyke", "name": "Darren Indyke", "emailCount": 14742},
    {"slug": "daphne-wallace-gmail", "name": "Daphne Wallace", "emailCount": 14718},
    {"slug": "brice-gordon", "name": "Brice Gordon", "emailCount": 14644},
    {"slug": "kathy-ruemmler-egroupware", "name": "Kathy Ruemmler", "emailCount": 13773},
    {"slug": "ghislaine-maxwell", "name": "Ghislaine Maxwell", "emailCount": 13428},
    {"slug": "jojo-fontanilla", "name": "Jojo Fontanilla", "emailCount": 13294},
    {"slug": "melanie-spinella-", "name": "Melanie Spinella", "emailCount": 12701},
    {"slug": "merwin-dela-cruz-example", "name": "Merwin dela Cruz", "emailCount": 11790},
    {"slug": "joi-ito", "name": "Joi Ito", "emailCount": 10338},
    {"slug": "cecile-de-jongh--2", "name": "Cecile de Jongh", "emailCount": 9992},
    {"slug": "vahe-stepanian-db-2", "name": "Vahe Stepanian", "emailCount": 9796},
    {"slug": "david-mitchell", "name": "David Mitchell", "emailCount": 9407},
    {"slug": "farkas-andrew-l-", "name": "Andrew Farkas", "emailCount": 8942},
    {"slug": "sarah-kellen", "name": "Sarah Kellen", "emailCount": 8741},
    {"slug": "gary-kerney-verizon", "name": "Gary Kerney", "emailCount": 8717},
    {"slug": "peggy-siegal", "name": "Peggy Siegal", "emailCount": 8636},
    {"slug": "david-stern-", "name": "David Stern", "emailCount": 8145},
    {"slug": "nicole-junkermann", "name": "Nicole Junkermann", "emailCount": 7984},
    {"slug": "lawrence-krauss", "name": "Lawrence Krauss", "emailCount": 7966},
    {"slug": "eric-roth-intljet", "name": "Eric Roth", "emailCount": 7718},
    {"slug": "martin-a-nowak", "name": "Martin A. Nowak", "emailCount": 7378},
    {"slug": "richard-joslin", "name": "Richard Joslin", "emailCount": 7169},
    {"slug": "eva-dubin-dubinandco", "name": "Eva Dubin", "emailCount": 7050},
    {"slug": "noam-chomsky", "name": "Noam Chomsky", "emailCount": 7013},
    {"slug": "tom-pritzker", "name": "Tom Pritzker", "emailCount": 6995},
    {"slug": "faith-kates-extmodels", "name": "Faith Kates", "emailCount": 6945},
    {"slug": "a-de-rothschild", "name": "A. de Rothschild", "emailCount": 6740},
    {"slug": "eileen-alexanderson-apollo-adv=sors", "name": "Eileen Alexanderson", "emailCount": 6446},
    {"slug": "amanda-kirby", "name": "Amanda Kirby", "emailCount": 6349},
    {"slug": "brad-wechsler-wechsler", "name": "Brad Wechsler", "emailCount": 6300},
    {"slug": "deepak-chopra", "name": "Deepak Chopra", "emailCount": 6273},
    {"slug": "petermandelson", "name": "Peter Mandelson", "emailCount": 6270},
    {"slug": "sultan-bin-sulayem", "name": "Sultan Bin Sulayem", "emailCount": 6194},
    {"slug": "stephen-hanson--3", "name": "Steve Hanson", "emailCount": 6115},
    {"slug": "jean-luc-brunel", "name": "Jean-Luc Brunel", "emailCount": 6062},
    {"slug": "jes-staley-jpmorgan", "name": "Jes Staley", "emailCount": 6021},
    {"slug": "tazia-smith", "name": "Tazia Smith", "emailCount": 6019},
    {"slug": "paul-barrett-example", "name": "Paul Barrett", "emailCount": 5990},
    {"slug": "ehud-barak", "name": "Ehud Barak", "emailCount": 5182},
    {"slug": "larry-summers", "name": "Larry Summers", "emailCount": 5027},
    {"slug": "joscha-bach", "name": "Joscha Bach", "emailCount": 3298},
    {"slug": "reid-hoffman-greylock", "name": "Reid Hoffman", "emailCount": 2461},
    {"slug": "peter-thiel", "name": "Peter Thiel", "emailCount": 2429},
    {"slug": "woody-allen", "name": "Woody Allen", "emailCount": 1240},
    {"slug": "prince-andrew-duke-of-york", "name": "Prince Andrew, Duke of York", "emailCount": 1163},
    {"slug": "leon-black", "name": "Leon Black", "emailCount": 862},
    {"slug": "bill-gates", "name": "Bill Gates", "emailCount": 706},
    {"slug": "elon-musk", "name": "Elon Musk", "emailCount": 692},
    {"slug": "alan-dershowitz", "name": "Alan Dershowitz", "emailCount": 214},
    {"slug": "casey-wasserman", "name": "Casey Wasserman", "emailCount": 122},
    {"slug": "kimbal-musk", "name": "Kimbal Musk", "emailCount": 106},
    {"slug": "masha-drokov", "name": "Masha Drokova", "emailCount": 41},
    {"slug": "wexner-les", "name": "Les Wexner", "emailCount": 19},
    {"slug": "jason-calacanis", "name": "Jason Calacanis", "emailCount": 18},
    {"slug": "bill-clinton", "name": "Bill Clinton", "emailCount": 1},
    {"slug": "donald-trump", "name": "Donald Trump", "emailCount": 0},
    {"slug": "justin-trudeau", "name": "Justin Trudeau", "emailCount": 0},
    {"slug": "virginia-giuffre", "name": "Virginia Giuffre", "emailCount": 0},
]


def analyze_with_claude(profile):
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    
    prompt = f"""You are analyzing {profile['name']} from the Epstein files archive.
They appear in {profile['emailCount']} emails.

Based on publicly documented information, return ONLY a JSON object:
{{
  "public_title": "Their job title (e.g. Executive Assistant, Financier, Model Agent, Attorney)",
  "category": "ONE of: finance|politics|royalty|entertainment|academia|technology|legal|law_enforcement|media|religion|military|other",
  "relationship": "ONE of: direct_contact|associate|alleged_client|employee|victim|investigator|legal_counsel|witness|mentioned_only",
  "summary": "2-3 sentence factual summary of who they are and their documented connection to Epstein",
  "power_public": <integer 0-100>,
  "power_institutional": <integer 0-100>,
  "nationality": "Country or empty string",
  "is_deceased": <true or false>,
  "is_convicted": <true or false>
}}"""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}]
        )
        text = message.content[0].text.strip()
        text = re.sub(r'^```json?\n?', '', text)
        text = re.sub(r'\n?```$', '', text)
        return json.loads(text)
    except Exception as e:
        print(f"  Claude error: {e}")
        return None


def main():
    print("=== Reckoning Radar - Enrichment Pipeline ===")
    print(f"Enriching {len(JWIKI_PROFILES)} profiles")

    supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    print("✓ Supabase connected")

    success = 0
    errors = 0

    for i, profile in enumerate(JWIKI_PROFILES):
        print(f"\n[{i+1}/{len(JWIKI_PROFILES)}] {profile['name']}")

        analysis = analyze_with_claude(profile)
        if not analysis:
            errors += 1
            continue

        # Validate enums
        category = analysis.get('category', 'other')
        if category not in ['finance','politics','royalty','entertainment','academia','technology','legal','law_enforcement','media','religion','military','other']:
            category = 'other'

        relationship = analysis.get('relationship', 'associate')
        if relationship not in ['direct_contact','associate','alleged_client','employee','victim','investigator','legal_counsel','witness','mentioned_only']:
            relationship = 'associate'

        if analysis.get('is_convicted'):
            status = 'convicted'
        elif analysis.get('is_deceased'):
            status = 'deceased'
        else:
            status = 'unknown'

        update_data = {
            "public_title": analysis.get('public_title', ''),
            "category": category,
            "relationship_to_epstein": relationship,
            "current_status": status,
            "notes": f"[jwiki:{profile['slug']}] {analysis.get('summary', '')}",
            "power_public_profile": min(100, max(0, int(analysis.get('power_public', 20)))),
            "power_institutional": min(100, max(0, int(analysis.get('power_institutional', 20)))),
            "confidence": "indicated",
        }

        nat = analysis.get('nationality', '')
        if nat and nat.strip():
            update_data['nationality'] = [nat.strip()]

        try:
            supabase.table("persons").update(update_data).eq("full_name", profile['name']).execute()
            success += 1
            print(f"  ✓ {category} | {relationship} | {analysis.get('public_title', '')}")
        except Exception as e:
            print(f"  DB error: {e}")
            errors += 1

        time.sleep(0.3)

    print(f"\n=== DONE: {success} enriched, {errors} failed ===")


if __name__ == "__main__":
    main()
