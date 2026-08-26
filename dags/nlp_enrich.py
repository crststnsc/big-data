from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


UNSUPPORTED_LANGUAGES = {"bn", "fa", "id"}

SPACY_MODEL_BY_LANG = {
    "en": "en_core_web_sm",
    "de": "de_core_news_sm",
    "es": "es_core_news_sm",
    "fr": "fr_core_news_sm",
    "it": "it_core_news_sm",
    "ja": "ja_core_news_sm",
    "pl": "pl_core_news_sm",
    "pt": "pt_core_news_sm",
    "ro": "ro_core_news_sm",
    "ru": "ru_core_news_sm",
    "uk": "uk_core_news_sm",
    "zh": "zh_core_web_sm",
}


PERSON_LABELS = {"PERSON", "PER"}
ORG_LABELS = {"ORG"}
LOCATION_LABELS = {"GPE", "LOC"}

_MAX_CHARS = 20_000

_spacy_models: dict[str, object] = {}


def get_spacy_model(lang: str):
    import spacy

    model_name = SPACY_MODEL_BY_LANG.get(lang)
    if model_name is None:
        return None
    if lang not in _spacy_models:
        # NER only: the rest of the default pipeline is dead weight here.
        _spacy_models[lang] = spacy.load(
            model_name, exclude=["parser", "tagger", "lemmatizer", "attribute_ruler"]
        )
    return _spacy_models[lang]


def extract_entities(text: str, lang: str) -> dict[str, list[str]] | None:
    nlp = get_spacy_model(lang)
    if nlp is None:
        return None

    doc = nlp(text[:_MAX_CHARS])
    persons, orgs, locs = set(), set(), set()
    for ent in doc.ents:
        name = ent.text.strip()
        if not name:
            continue
        if ent.label_ in PERSON_LABELS:
            persons.add(name)
        elif ent.label_ in ORG_LABELS:
            orgs.add(name)
        elif ent.label_ in LOCATION_LABELS:
            locs.add(name)
    return {
        "persons": sorted(persons),
        "organizations": sorted(orgs),
        "locations": sorted(locs),
    }


_translation_cache: dict[tuple[str, str], str] = {}


def translate_entities(entities: dict[str, list[str]], lang: str) -> dict[str, list[str]]:
    if lang == "en":
        return entities

    import argostranslate.translate

    def tr(text: str) -> str:
        key = (lang, text)
        cached = _translation_cache.get(key)
        if cached is not None:
            return cached
        try:
            translated = argostranslate.translate.translate(text, lang, "en")
        except Exception as exc:
            logger.warning("Translation failed lang=%s text=%r error=%s", lang, text, exc)
            translated = text
        _translation_cache[key] = translated
        return translated

    return {bucket: [tr(name) for name in names] for bucket, names in entities.items()}
