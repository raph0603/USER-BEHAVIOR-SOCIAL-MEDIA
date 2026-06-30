"""
Extension manuelle du lexique VADER pour le domaine des véhicules électriques (EN).

Pourquoi : VADER (lexique général, ~7500 termes) ne couvre pas le vocabulaire
spécifique aux VE. Des expressions comme "range anxiety", "battery degradation"
ou "tax credit" sont absentes ou mal notées par le lexique générique, alors
qu'elles portent une charge de sentiment forte et très spécifique dans ce
domaine. Cette extension est notée manuellement (pas générée automatiquement),
sur la même échelle que VADER : de -4 (très négatif) à +4 (très positif).

Méthode de notation : chaque terme/expression a été noté à la main en
imaginant son usage typique dans un post marketing ou un commentaire public
sur les VE (pas le sens du mot en français/anglais "neutre" hors contexte).
Ex. "recall" est neutre dans le dictionnaire général, mais quasi toujours
négatif dans le contexte automobile (rappel produit = défaut).

Usage : ce dict est fusionné dans le lexicon de SentimentIntensityAnalyzer
via `analyzer.lexicon.update(EV_LEXICON_EN)` avant l'appel à polarity_scores.
Les expressions multi-mots ne sont PAS gérées nativement par VADER (qui
tokenize mot par mot) : un pré-traitement remplace les expressions par un
token unique avant scoring (voir sentiment_engine.py, fonction
`_substitute_multiword_terms`).
"""

EV_LEXICON_EN = {
    # --- Positif : expérience de conduite / produit ---
    "instant_torque": 1.6,
    "smooth_acceleration": 1.6,
    "quiet_ride": 1.4,
    "regenerative_braking": 1.0,
    "fast_acceleration": 1.4,
    "long_range": 1.4,
    "fast_charging": 1.6,
    "supercharger": 1.4,
    "low_maintenance": 1.9,
    "zero_emission": 2.1,
    "eco_friendly": 2.0,
    "green_energy": 1.8,
    "clean_energy": 1.8,
    "tax_credit": 1.9,
    "tax_incentive": 1.8,
    "government_subsidy": 1.5,
    "autopilot": 0.9,
    "self_driving": 0.7,
    "full_self_driving": 0.6,
    "over_the_air_update": 1.0,
    "free_charging": 2.0,
    "home_charging": 1.0,
    "cheap_to_run": 1.8,
    "fun_to_drive": 1.7,
    "future_proof": 1.3,

    # --- Négatif : anxiété / contraintes techniques ---
    "range_anxiety": -2.4,
    "battery_degradation": -2.1,
    "battery_fire": -3.3,
    "thermal_runaway": -2.9,
    "spontaneous_combustion": -3.6,
    "charging_anxiety": -1.9,
    "charger_broken": -2.5,
    "broken_charger": -2.5,
    "charging_station_down": -2.1,
    "stranded": -2.2,
    "bricked_battery": -2.9,
    "software_glitch": -1.8,
    "fire_risk": -2.9,
    "recall": -2.0,
    "safety_recall": -2.3,
    "dealer_markup": -2.1,
    "supply_chain_issue": -1.3,
    "delivery_delay": -1.5,
    "long_wait_time": -1.5,
    "charging_infrastructure": -0.7,  # souvent évoqué négativement ("manque d'infra")
    "subsidy_cut": -1.9,
    "price_hike": -2.0,
    "overpriced": -2.1,
    "depreciation": -1.7,
    "resale_value": -0.6,  # contexte-dépendant, légèrement négatif par défaut (souvent "faible")
    "phantom_drain": -1.8,
    "cold_weather_range": -1.4,
    "wait_list": -1.0,
    "production_hell": -2.3,
    "build_quality": -0.5,  # souvent mentionné en se plaignant
}
