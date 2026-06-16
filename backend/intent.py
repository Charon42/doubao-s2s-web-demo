from __future__ import annotations

import re
from typing import Any


DEFAULT_SLOTS = {
    "budget": None,
    "people_count": None,
    "location": None,
    "checkin_date": None,
    "checkout_date": None,
}

INTENT_RULES = [
    (
        "hotel_booking_intent",
        ["订酒店", "帮我订", "下单", "预订", "入住", "离店", "房间"],
        0.86,
    ),
    (
        "route_query",
        ["怎么去", "路线", "导航", "开车", "步行", "地铁", "打车"],
        0.82,
    ),
    (
        "nearby_food",
        ["附近", "吃", "美食", "餐厅", "火锅", "粤菜", "烧烤", "咖啡", "奶茶"],
        0.8,
    ),
    (
        "nearby_place",
        ["附近", "玩", "景点", "去哪", "游玩", "逛逛", "公园", "博物馆", "打卡"],
        0.78,
    ),
    (
        "nearby_hotel",
        ["酒店", "住宿", "住哪里", "住一晚", "订房", "民宿", "宾馆"],
        0.8,
    ),
]

CHINESE_NUMBERS = {
    "一": 1,
    "二": 2,
    "两": 2,
    "俩": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
    "十": 10,
}


def detect_intent(text: str) -> dict[str, Any]:
    clean = (text or "").strip()
    keywords: list[str] = []
    intent = "normal_chat"
    confidence = 0.3

    for candidate_intent, candidate_keywords, base_confidence in INTENT_RULES:
        matched = [keyword for keyword in candidate_keywords if keyword in clean]
        if not matched:
            continue
        if candidate_intent in {"nearby_food", "nearby_place"} and matched == ["附近"]:
            continue
        if candidate_intent in {"nearby_food", "nearby_place"} and "附近" not in matched:
            confidence_penalty = 0.1
        else:
            confidence_penalty = 0.0
        intent = candidate_intent
        confidence = max(0.5, base_confidence - confidence_penalty)
        keywords = matched
        break

    return {
        "intent": intent,
        "confidence": confidence,
        "keywords": keywords,
        "slots": {
            "budget": _extract_budget(clean),
            "people_count": _extract_people_count(clean),
            "location": _extract_location(clean),
            "checkin_date": None,
            "checkout_date": None,
        },
    }


def _extract_budget(text: str) -> str | None:
    patterns = [
        r"\d+\s*(?:元|块|人民币)?\s*(?:以内|以下|内)",
        r"\d+\s*(?:到|-|~|至)\s*\d+\s*(?:元|块|人民币)?",
        r"(?:预算|人均|价格|价位)\s*\d+\s*(?:元|块|人民币)?",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return re.sub(r"\s+", "", match.group(0))
    if "别太贵" in text or "不要太贵" in text or "便宜点" in text:
        return "别太贵"
    return None


def _extract_people_count(text: str) -> int | None:
    match = re.search(r"(\d+)\s*(?:个人|人位|人|位)", text)
    if match:
        return int(match.group(1))
    match = re.search(r"([一二两俩三四五六七八九十])\s*(?:个人|人位|人|位)", text)
    if match:
        return CHINESE_NUMBERS.get(match.group(1))
    return None


def _extract_location(text: str) -> str | None:
    match = re.search(r"([\u4e00-\u9fa5A-Za-z0-9]{2,20})(?:附近|周边|旁边)", text)
    if match:
        location = match.group(1)
        for prefix in ("我在", "想去", "帮我找", "找一下", "看看"):
            if location.startswith(prefix):
                location = location[len(prefix) :]
        return location or None
    return None
