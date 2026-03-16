import asyncio
import logging
import random
import sqlite3
import os
import json
import math
import shutil
import glob
from datetime import datetime, timedelta
from typing import Optional, Union

from aiogram import Bot, Dispatcher, types, F
from aiogram.types import (
    Message, 
    CallbackQuery, 
    InlineKeyboardMarkup, 
    InlineKeyboardButton,
    BotCommand,
    BotCommandScopeDefault,
    BotCommandScopeChat
)
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ===== НАСТРОЙКИ =====
TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
PREFIX = "/"
DB_FOLDER = "/app/data/telegram_databases"
COOLDOWN_HOURS = 1
TESTER_IDS = []

# Настройки вероятностей
BASE_MINUS_CHANCE = 0.2
MAX_MINUS_CHANCE = 0.6
PITY_INCREMENT = 0.1
CONSECUTIVE_MINUS_BOOST = 0.2
MAX_CONSECUTIVE_MINUS_BOOST = 0.8

# Настройки джекпота
BASE_JACKPOT_CHANCE = 0.001
JACKPOT_PITY_INCREMENT = 0.001
MAX_JACKPOT_CHANCE = 0.05
JACKPOT_MIN = 100
JACKPOT_MAX = 500

# Настройки кейса
CASE_COOLDOWN_HOURS = 24

# Призы в ежедневном кейсе
CASE_PRIZES = [
    {"value": 0, "chance": 20.0, "emoji": "🔄", "name": "Ничего"},
    {"value": 10, "chance": 20.0, "emoji": "📈", "name": "+10 кг"},
    {"value": 20, "chance": 20.0, "emoji": "⬆️", "name": "+20 кг"},
    {"value": 50, "chance": 20.0, "emoji": "🚀", "name": "+50 кг"},
    {"value": 100, "chance": 10.0, "emoji": "🚀", "name": "+100 кг"},
    {"value": 200, "chance": 5.0, "emoji": "🚀", "name": "+200 кг"},
    {"value": 300, "chance": 5.0, "emoji": "💫", "name": "+300 кг"},
    {"value": 400, "chance": 5.0, "emoji": "💫", "name": "+400 кг"},
    {"value": 500, "chance": 5.0, "emoji": "💫", "name": "+500 кг"},
    {"value": 1000, "chance": 2.0, "emoji": "⭐", "name": "+1000 кг"},
    {"value": 1500, "chance": 2.0, "emoji": "⭐", "name": "+1500 кг"},
    {"value": 2500, "chance": 1.0, "emoji": "💥", "name": "+2500 кг"},
    {"value": 5000, "chance": 1.0, "emoji": "💥", "name": "+5000 кг"},
    {"value": "autoburger", "chance": 1.0, "emoji": "🍔", "name": "АВТОБУРГЕР"},
]

total_chance = sum(prize["chance"] for prize in CASE_PRIZES)
for prize in CASE_PRIZES:
    prize["normalized_chance"] = (prize["chance"] / total_chance) * 100

# Настройки Автобургера
AUTOBURGER_INTERVALS = [6, 4, 2, 1]
AUTOBURGER_MAX_BONUS = 0.6
AUTOBURGER_GROWTH_RATE = 0.03

# ===== НАСТРОЙКИ ЛЕГЕНДАРНЫХ БУРГЕРОВ =====
BURGER_RANKS = [
    {"name": "Железный бургер", "emoji": "⬛", "multiplier": 1.5,
     "fat_cooldown": 45, "case_cooldown": 16, "weight_required": 3600, "chance": 0.7},
    {"name": "Золотой бургер", "emoji": "🟨", "multiplier": 2.0,
     "fat_cooldown": 30, "case_cooldown": 12, "weight_required": 4300, "chance": 0.5},
    {"name": "Платиновый бургер", "emoji": "⬜", "multiplier": 2.5,
     "fat_cooldown": 20, "case_cooldown": 6, "weight_required": 6000, "chance": 0.3},
    {"name": "Алмазный бургер", "emoji": "🟦", "multiplier": 3.0,
     "fat_cooldown": 10, "case_cooldown": 3, "weight_required": 8000, "chance": 0.2,
     "plus_bonus": 0.1, "rare_multiplier": 2.0},
]

IRON_BURGER = 0
GOLD_BURGER = 1
PLATINUM_BURGER = 2
DIAMOND_BURGER = 3

# ===== НАСТРОЙКИ КЕЙСОВ =====
CASES = {
    "daily": {
        "name": "Жиркейс",
        "emoji": "📦",
        "tradable": True,
        "daily": True,
        "prizes": CASE_PRIZES
    },
    "chicken": {
        "name": "Коробка от чикенбургера",
        "emoji": "🍗",
        "tradable": True,
        "daily": False,
        "shop_chance": 0.3,
        "min_shop": 1,
        "max_shop": 3,
        "price": 10,
        "prizes": [
            {"value": -10, "chance": 20, "emoji": "📉"},
            {"value": 0, "chance": 30, "emoji": "🔄"},
            {"value": 10, "chance": 20, "emoji": "📈"},
            {"value": 15, "chance": 10, "emoji": "📈"},
            {"value": 20, "chance": 10, "emoji": "⬆️"},
            {"value": 25, "chance": 10, "emoji": "⬆️"}
        ]
    },
    "bigmac": {
        "name": "Коробка от Биг Мака",
        "emoji": "🍔",
        "tradable": True,
        "daily": False,
        "shop_chance": 0.25,
        "min_shop": 1,
        "max_shop": 3,
        "price": 15,
        "prizes": [
            {"value": -15, "chance": 20, "emoji": "📉"},
            {"value": 0, "chance": 30, "emoji": "🔄"},
            {"value": 15, "chance": 20, "emoji": "📈"},
            {"value": 20, "chance": 10, "emoji": "⬆️"},
            {"value": 25, "chance": 10, "emoji": "⬆️"},
            {"value": 30, "chance": 10, "emoji": "🚀"}
        ]
    },
    "whopper": {
        "name": "Коробка от Воппера",
        "emoji": "🔥",
        "tradable": True,
        "daily": False,
        "shop_chance": 0.23,
        "min_shop": 1,
        "max_shop": 3,
        "price": 25,
        "prizes": [
            {"value": -25, "chance": 20, "emoji": "📉"},
            {"value": 0, "chance": 30, "emoji": "🔄"},
            {"value": 25, "chance": 20, "emoji": "📈"},
            {"value": 30, "chance": 10, "emoji": "🚀"},
            {"value": 40, "chance": 9, "emoji": "🚀"},
            {"value": 50, "chance": 1, "emoji": "💫"}
        ]
    },
    "green_whopper": {
        "name": "Коробка от Зеленого Воппера",
        "emoji": "💚",
        "tradable": True,
        "daily": False,
        "shop_chance": 0.17,
        "min_shop": 1,
        "max_shop": 2,
        "price": 50,
        "prizes": [
            {"value": -25, "chance": 20, "emoji": "📉"},
            {"value": 0, "chance": 10, "emoji": "🔄"},
            {"value": 10, "chance": 20, "emoji": "📈"},
            {"value": 30, "chance": 10, "emoji": "🚀"},
            {"value": 50, "chance": 10, "emoji": "💫"},
            {"value": 100, "chance": 9, "emoji": "⭐"},
            {"value": 250, "chance": 1, "emoji": "💥"}
        ]
    },
    "burger_pizza": {
        "name": "Коробка от Бургер пиццы",
        "emoji": "🍕",
        "tradable": True,
        "daily": False,
        "shop_chance": 0.15,
        "min_shop": 1,
        "max_shop": 2,
        "price": 100,
        "prizes": [
            {"value": -10, "chance": 20, "emoji": "📉"},
            {"value": 0, "chance": 10, "emoji": "🔄"},
            {"value": 30, "chance": 20, "emoji": "🚀"},
            {"value": 50, "chance": 30, "emoji": "💫"},
            {"value": 100, "chance": 5, "emoji": "⭐"},
            {"value": 250, "chance": 5, "emoji": "⭐"},
            {"value": 500, "chance": 4, "emoji": "💥"},
            {"value": 1000, "chance": 1, "emoji": "💥"}
        ]
    },
    "mcguffin": {
        "name": "Коробка от МакГаффина",
        "emoji": "🎁",
        "tradable": True,
        "daily": False,
        "shop_chance": 0.1,
        "min_shop": 1,
        "max_shop": 1,
        "price": 200,
        "prizes": [
            {"value": 100, "chance": 80, "emoji": "⭐"},
            {"value": 200, "chance": 5, "emoji": "💥"},
            {"value": 250, "chance": 5, "emoji": "💥"},
            {"value": 500, "chance": 5, "emoji": "💥"},
            {"value": 750, "chance": 1, "emoji": "✨"},
            {"value": 1000, "chance": 1, "emoji": "✨"},
            {"value": 1200, "chance": 1, "emoji": "✨"},
            {"value": 1500, "chance": 1, "emoji": "✨"}
        ]
    },
    "autoburger_pack": {
        "name": "Упаковка Автобургера",
        "emoji": "🍔📦",
        "tradable": True,
        "daily": False,
        "shop_chance": 0.05,
        "min_shop": 1,
        "max_shop": 5,
        "price": 250,
        "prizes": [
            {"value": 0, "chance": 98, "emoji": "🔄"},
            {"value": "autoburger", "chance": 2, "emoji": "🍔"}
        ]
    },
    "rotten_pack": {
        "name": "Упаковка Гнилой Ножки KFC",
        "emoji": "💀📦",
        "tradable": True,
        "daily": False,
        "shop_chance": 0.1,
        "min_shop": 1,
        "max_shop": 10,
        "price": 100,
        "prizes": [
            {"value": 0, "chance": 98, "emoji": "🔄"},
            {"value": "rotten_leg", "chance": 2, "emoji": "💀"}
        ]
    },
    "water_pack": {
        "name": "Упаковка Стакана Воды",
        "emoji": "💧📦",
        "tradable": True,
        "daily": False,
        "shop_chance": 0.1,
        "min_shop": 1,
        "max_shop": 10,
        "price": 100,
        "prizes": [
            {"value": 0, "chance": 98, "emoji": "🔄"},
            {"value": "water", "chance": 2, "emoji": "💧"}
        ]
    }
}

# ===== НАСТРОЙКИ МАГАЗИНА =====
SHOP_SLOTS = 10
SHOP_UPDATE_HOURS = 12

SHOP_ITEMS = [
    {"name": "Горелый бекон", "chance": 1.0, "min_amount": 3, "max_amount": 20,
     "price": 20, "gain_per_24h": 1, "description": "🏭 Даёт +1 кг каждые 24 часа"},
    {"name": "Горелый бутерброд", "chance": 0.4, "min_amount": 1, "max_amount": 5,
     "price": 70, "gain_per_24h": 3, "description": "🥪 Даёт +3 кг каждые 24 часа"},
    {"name": "Горелый додстер", "chance": 0.4, "min_amount": 1, "max_amount": 3,
     "price": 100, "gain_per_24h": 5, "description": "🌯 Даёт +5 кг каждые 24 часа"},
    {"name": "Тарелка макарон", "chance": 0.3, "min_amount": 1, "max_amount": 2,
     "price": 200, "gain_per_24h": 10, "description": "🍝 Даёт +10 кг каждые 24 часа"},
    {"name": "Тарелка хинкалей", "chance": 0.2, "min_amount": 1, "max_amount": 2,
     "price": 300, "gain_per_24h": 15, "description": "🥟 Даёт +15 кг каждые 24 часа"},
    {"name": "Бургер", "chance": 0.15, "min_amount": 1, "max_amount": 2,
     "price": 400, "gain_per_24h": 20, "description": "🍔 Даёт +20 кг каждые 24 часа"},
    {"name": "Пицца", "chance": 0.1, "min_amount": 1, "max_amount": 2,
     "price": 500, "gain_per_24h": 30, "description": "🍕 Даёт +30 кг каждые 24 часа"},
    {"name": "Ведро KFC", "chance": 0.08, "min_amount": 1, "max_amount": 2,
     "price": 800, "gain_per_24h": 50, "description": "🍗 Даёт +50 кг каждые 24 часа"},
    {"name": "Комбо за 1000!", "chance": 0.06, "min_amount": 1, "max_amount": 2,
     "price": 1000, "gain_per_24h": 100, "description": "🍱 Даёт +100 кг каждые 24 часа"},
    {"name": "Бездонное ведро KFC", "chance": 0.04, "min_amount": 1, "max_amount": 1,
     "price": 1500, "gain_per_24h": 150, "description": "🪣 Даёт +150 кг каждые 24 часа"},
    {"name": "Бездонная пачка чипсов", "chance": 0.03, "min_amount": 1, "max_amount": 1,
     "price": 3000, "gain_per_24h": 250, "description": "🥨 Даёт +250 кг каждые 24 часа"},
    {"name": "Пожизненный запас чикенбургеров", "chance": 0.02, "min_amount": 1, "max_amount": 1,
     "price": 5000, "gain_per_24h": 500, "description": "🍔🍔🍔 Даёт +500 кг каждые 24 часа"},
    {"name": "Автоматическая система подачи холестерина", "chance": 0.01, "min_amount": 1, "max_amount": 1,
     "price": 7000, "gain_per_24h": 1000, "description": "⚙️💉 Даёт +1000 кг каждые 24 часа"},
    {"name": "Святой сэндвич", "chance": 0.005, "min_amount": 1, "max_amount": 1,
     "price": 10000, "gain_per_24h": 0, "description": "✨ **ЛЕГЕНДАРНО** ✨\nУвеличивает шанс джекпота до 30% за шт"},
    {"name": "Гнилая ножка KFC", "chance": 0.005, "min_amount": 1, "max_amount": 5,
     "price": 1, "gain_per_24h": 0, "description": "💀 **ПРОКЛЯТО** 💀\n60% потерять 50% массы, 40% джекпот"},
    {"name": "Стакан воды", "chance": 0.005, "min_amount": 1, "max_amount": 5,
     "price": 1, "gain_per_24h": 0, "description": "💧 **ОЧИЩЕНИЕ** 💧\nНет минусов, но весь прирост в 3 раза меньше"},
    {"name": "Автохолестерол", "chance": 0.05, "min_amount": 1, "max_amount": 1,
     "price": 1000, "gain_per_24h": 0, "description": "💊 Даёт от 1кг до 10кг в час",
     "effect": "auto_cholesterol", "effect_value": (1, 10), "effect_type": "hourly"},
    {"name": "Холестеринимус", "chance": 0.05, "min_amount": 1, "max_amount": 1,
     "price": 500, "gain_per_24h": 0, "description": "💊 Даёт от 1кг до 5кг в час",
     "effect": "cholesterol", "effect_value": (1, 5), "effect_type": "hourly"},
    {"name": "Яблоко", "chance": 0.05, "min_amount": 1, "max_amount": 1,
     "price": 500, "gain_per_24h": 0, "description": "🍎 Уменьшает кулдаун /жир на 5% за штуку",
     "effect": "fat_cooldown_reduction", "effect_value": 0.05, "effect_type": "passive"},
    {"name": "Апельсин", "chance": 0.05, "min_amount": 1, "max_amount": 1,
     "price": 750, "gain_per_24h": 0, "description": "🍊 Уменьшает кулдаун /жиркейс на 5% за штуку",
     "effect": "case_cooldown_reduction", "effect_value": 0.05, "effect_type": "passive"},
    {"name": "Золотое Яблоко", "chance": 0.01, "min_amount": 1, "max_amount": 1,
     "price": 1000, "gain_per_24h": 0, "description": "🍎✨ Уменьшает кулдаун /жир на 10% за штуку",
     "effect": "fat_cooldown_reduction", "effect_value": 0.10, "effect_type": "passive"},
    {"name": "Золотой Апельсин", "chance": 0.01, "min_amount": 1, "max_amount": 1,
     "price": 1000, "gain_per_24h": 0, "description": "🍊✨ Уменьшает кулдаун /жиркейс на 10% за штуку",
     "effect": "case_cooldown_reduction", "effect_value": 0.10, "effect_type": "passive"},
    {"name": "Драгонфрукт", "chance": 0.01, "min_amount": 1, "max_amount": 1,
     "price": 1000, "gain_per_24h": 0, "description": "🐉🍈 Повышает шанс джекпота на 1% за штуку",
     "effect": "jackpot_boost", "effect_value": 0.01, "effect_type": "passive"},
    {"name": "Золотой Драгонфрукт", "chance": 0.005, "min_amount": 1, "max_amount": 1,
     "price": 3000, "gain_per_24h": 0, "description": "🐉🍈✨ Повышает шанс джекпота на 5% за штуку",
     "effect": "jackpot_boost", "effect_value": 0.05, "effect_type": "passive"},
    {"name": "Снатчер", "chance": 0.001, "min_amount": 1, "max_amount": 1,
     "price": 2000, "gain_per_24h": 0, "description": "👾 **СНАТЧЕР** 👾\nКаждые 6 часов генерирует предмет"},
]

ITEM_EMOJIS = {
    "Горелый бекон": "🥓", "Горелый бутерброд": "🥪", "Горелый додстер": "🌯",
    "Тарелка макарон": "🍝", "Тарелка хинкалей": "🥟", "Бургер": "🍔",
    "Пицца": "🍕", "Ведро KFC": "🍗", "Комбо за 1000!": "🍱",
    "Бездонное ведро KFC": "🪣", "Бездонная пачка чипсов": "🥨",
    "Пожизненный запас чикенбургеров": "🍔🍔🍔",
    "Автоматическая система подачи холестерина": "⚙️💉",
    "Святой сэндвич": "✨", "Гнилая ножка KFC": "💀", "Стакан воды": "💧",
    "Автохолестерол": "💊", "Холестеринимус": "💊", "Яблоко": "🍎",
    "Апельсин": "🍊", "Золотое Яблоко": "🍎✨", "Золотой Апельсин": "🍊✨",
    "Драгонфрукт": "🐉🍈", "Золотой Драгонфрукт": "🐉🍈✨", "Снатчер": "👾"
}

# Добавляем магазинный кейс
CASES["shop_case"] = {
    "name": "Магазинный кейс",
    "emoji": "🏪",
    "tradable": True,
    "daily": False,
    "shop_chance": 0.3,
    "min_shop": 1,
    "max_shop": 5,
    "price": 150,
    "prizes": []
}

# Заполняем призы для магазинного кейса
shop_case_prizes = []
for item in SHOP_ITEMS:
    chance_percent = item["chance"] * 100
    emoji = ITEM_EMOJIS.get(item["name"], "🎁")
    shop_case_prizes.append({
        "value": item["name"],
        "chance": chance_percent,
        "emoji": emoji,
        "name": item["name"]
    })

total = sum(p["chance"] for p in shop_case_prizes)
if total < 100:
    shop_case_prizes.append({
        "value": 0,
        "chance": 100 - total,
        "emoji": "🔄",
        "name": "Ничего"
    })
else:
    for prize in shop_case_prizes:
        prize["chance"] = (prize["chance"] / total) * 100

CASES["shop_case"]["prizes"] = shop_case_prizes

# ===== ТЕНЕВАЯ СТОИМОСТЬ ДЛЯ РАСЧЕТА ШАНСОВ АПГРЕЙДА =====
LEGENDARY_UPGRADE_PRICES = {
    "Святой сэндвич": 20000,
    "Гнилая ножка KFC": 5000,
    "Стакан воды": 3000,
    "Автохолестерол": 5000,
    "Холестеринимус": 2500,
    "Яблоко": 1500,
    "Золотое Яблоко": 3000,
    "Апельсин": 2000,
    "Золотой Апельсин": 4000,
    "Драгонфрукт": 4000,
    "Золотой Драгонфрукт": 8000,
    "Снатчер": 20000
}

print("="*60)
print("🍔 ЖИРНЫЙ ТЕЛЕГРАМ БОТ - ЗАПУСК")
print("="*60)
print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📁 Папка БД: {DB_FOLDER}")

if TOKEN is None:
    print("❌ КРИТИЧЕСКАЯ ОШИБКА: Не найден TELEGRAM_BOT_TOKEN!")
    exit(1)

# ===== НАСТРОЙКА ЛОГИРОВАНИЯ =====
logging.basicConfig(level=logging.INFO)

# ===== ИНИЦИАЛИЗАЦИЯ БОТА =====
storage = MemoryStorage()
bot = Bot(token=TOKEN)
dp = Dispatcher(storage=storage)

# ===== СИСТЕМА ЗВАНИЙ =====
RANKS = [
    {"name": "Задолженность по кг", "min": -999, "max": -51, "emoji": "👻"},
    {"name": "Невесомый", "min": -50, "max": -21, "emoji": "🍃"},
    {"name": "Бедыч", "min": -20, "max": -1, "emoji": "🎈"},
    {"name": "Абсолютный ноль", "min": 0, "max": 0, "emoji": "⚖️"},
    {"name": "Микро жирик", "min": 1, "max": 29, "emoji": "🏃"},
    {"name": "Мини жирик", "min": 30, "max": 69, "emoji": "🍔"},
    {"name": "Вес имеет", "min": 70, "max": 119, "emoji": "🐘"},
    {"name": "Толстый", "min": 120, "max": 199, "emoji": "🏋️"},
    {"name": "Бронзовая лига Бургер Кинга", "min": 200, "max": 599, "emoji": "🟤"},
    {"name": "Серебрянная лига Бургер Кинга", "min": 600, "max": 1199, "emoji": "🔘"},
    {"name": "Золотая лига Бургер Кинга", "min": 1200, "max": 1799, "emoji": "🟡"},
    {"name": "Платиновая лига Бургер Кинга", "min": 1800, "max": 2399, "emoji": "💠"},
    {"name": "Алмазная лига Бургер Кинга", "min": 2400, "max": 2999, "emoji": "💎"},
    {"name": "Ониксовая лига Бургер Кинга", "min": 3000, "max": 3599, "emoji": "◆︎"},
    {"name": "Жирмезис", "min": 3600, "max": 5000, "emoji": "⚜️"},
    {"name": "Арчжирмезис", "min": 5000, "max": 10000, "emoji": "♛"},
    {"name": "ЖИРНАЯ ТОЛСТАЯ ОГРОМНАЯ СВИНЬЯ", "min": 10001, "max": 99999999, "emoji": "🐖"},
]

def get_rank(weight):
    for rank in RANKS:
        if rank["min"] <= weight <= rank["max"]:
            return rank["name"], rank["emoji"]
    if weight > 99999999:
        return "🌀 Бесконечность", "🌀"
    if weight < -999:
        return "Черная дыра", "💀"
    return "❓ Неопределённый", "❓"

# ===== РАБОТА С БАЗОЙ ДАННЫХ =====
def get_db_path(chat_id):
    return os.path.join(DB_FOLDER, f"chat_{chat_id}.db")

def add_missing_columns(db_path, existing_columns):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    required_columns = {
        'legendary_burger': "INTEGER DEFAULT -1",
        'item_counts': "TEXT DEFAULT '{}'",
        'last_command': "TEXT",
        'last_command_target': "TEXT",
        'last_command_use_time': "TIMESTAMP",
        'fat_cooldown_time': "TIMESTAMP",
        'active_case_message_id': "TEXT",
        'daily_case_last_time': "TIMESTAMP",
        'snatcher_last_time': "TIMESTAMP",
        'duel_active': "INTEGER DEFAULT 0",
        'duel_opponent': "TEXT",
        'duel_amount': "INTEGER DEFAULT 0",
        'duel_message_id': "TEXT",
        'duel_initiator': "INTEGER DEFAULT 0",
        'last_case_type': "TEXT",
        'last_case_prize': "TEXT",
        'upgrade_active': "INTEGER DEFAULT 0",
        'upgrade_data': "TEXT",
        'duel_start_time': "TIMESTAMP",
    }
    
    for col_name, col_type in required_columns.items():
        if col_name not in existing_columns:
            try:
                print(f"📦 Добавляю колонку {col_name}")
                cursor.execute(f"ALTER TABLE user_fat ADD COLUMN {col_name} {col_type}")
            except Exception as e:
                print(f"⚠️ Ошибка при добавлении колонки {col_name}: {e}")
    
    for case_id in CASES.keys():
        if case_id != "daily":
            col_name = f"case_{case_id}_count"
            if col_name not in existing_columns:
                try:
                    print(f"📦 Добавляю колонку {col_name}")
                    cursor.execute(f"ALTER TABLE user_fat ADD COLUMN {col_name} INTEGER DEFAULT 0")
                except Exception as e:
                    print(f"⚠️ Ошибка при добавлении колонки {col_name}: {e}")
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='shop'")
    if not cursor.fetchone():
        print(f"📦 Создаю таблицу shop")
        cursor.execute('''CREATE TABLE shop (
            chat_id TEXT PRIMARY KEY, 
            slots TEXT, 
            last_update TIMESTAMP, 
            next_update TIMESTAMP
        )''')
    
    conn.commit()
    conn.close()

def safe_init_chat_database(chat_id, chat_name="Unknown"):
    db_path = get_db_path(chat_id)
    
    if os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='user_fat'")
            if not cursor.fetchone():
                conn.close()
                print(f"⚠️ Таблица user_fat не найдена в БД чата {chat_name}, создаю заново")
                return create_new_database(db_path, chat_id, chat_name)
            cursor.execute("PRAGMA table_info(user_fat)")
            columns = [col[1] for col in cursor.fetchall()]
            conn.close()
            add_missing_columns(db_path, columns)
            print(f"✅ База данных для чата {chat_name} в порядке")
            return True
        except sqlite3.DatabaseError:
            print(f"⚠️ Обнаружена повреждённая БД для чата {chat_name}")
            backup_path = db_path + f".corrupted_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            shutil.copy2(db_path, backup_path)
            os.remove(db_path)
            return create_new_database(db_path, chat_id, chat_name)
    else:
        print(f"📁 Создаю новую БД для чата {chat_name}")
        return create_new_database(db_path, chat_id, chat_name)

def create_new_database(db_path, chat_id, chat_name):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    base_columns = '''
        user_id TEXT PRIMARY KEY, 
        user_name TEXT, 
        current_number INTEGER DEFAULT 0, 
        last_command_time TIMESTAMP, 
        consecutive_plus INTEGER DEFAULT 0, 
        consecutive_minus INTEGER DEFAULT 0, 
        jackpot_pity INTEGER DEFAULT 0, 
        autoburger_count INTEGER DEFAULT 0, 
        last_case_time TIMESTAMP, 
        next_autoburger_time TIMESTAMP, 
        total_autoburger_activations INTEGER DEFAULT 0, 
        total_autoburger_gain INTEGER DEFAULT 0, 
        last_autoburger_result TEXT, 
        last_autoburger_time TIMESTAMP,
        legendary_burger INTEGER DEFAULT -1,
        item_counts TEXT DEFAULT '{}',
        last_command TEXT,
        last_command_target TEXT,
        last_command_use_time TIMESTAMP,
        fat_cooldown_time TIMESTAMP,
        active_case_message_id TEXT,
        daily_case_last_time TIMESTAMP,
        snatcher_last_time TIMESTAMP,
        duel_active INTEGER DEFAULT 0,
        duel_opponent TEXT,
        duel_amount INTEGER DEFAULT 0,
        duel_message_id TEXT,
        duel_initiator INTEGER DEFAULT 0,
        last_case_type TEXT,
        last_case_prize TEXT,
        upgrade_active INTEGER DEFAULT 0,
        upgrade_data TEXT,
        duel_start_time TIMESTAMP
    '''
    
    case_columns = []
    for case_id in CASES.keys():
        if case_id != "daily":
            case_columns.append(f"case_{case_id}_count INTEGER DEFAULT 0")
    
    all_columns = base_columns + ", " + ", ".join(case_columns) if case_columns else base_columns
    
    cursor.execute(f'''CREATE TABLE user_fat ({all_columns})''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS shop (
        chat_id TEXT PRIMARY KEY, 
        slots TEXT, 
        last_update TIMESTAMP, 
        next_update TIMESTAMP
    )''')
    
    conn.commit()
    conn.close()
    print(f"✅ Новая база данных создана для чата {chat_name}")
    return True

def get_user_data(chat_id, user_id, user_name=None):
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA table_info(user_fat)")
    all_columns = [col[1] for col in cursor.fetchall()]
    
    base_cols = [
        'user_id', 'user_name', 'current_number', 'last_command_time',
        'consecutive_plus', 'consecutive_minus', 'jackpot_pity', 'autoburger_count',
        'last_case_time', 'next_autoburger_time', 'total_autoburger_activations',
        'total_autoburger_gain', 'last_autoburger_result', 'last_autoburger_time',
        'legendary_burger', 'item_counts', 'last_command', 'last_command_target',
        'last_command_use_time', 'fat_cooldown_time', 'active_case_message_id',
        'daily_case_last_time', 'snatcher_last_time', 'duel_active', 'duel_opponent',
        'duel_amount', 'duel_message_id', 'duel_initiator',
        'last_case_type', 'last_case_prize', 'upgrade_active', 'upgrade_data', 'duel_start_time'
    ]
    
    select_cols = [col for col in base_cols if col in all_columns]
    
    case_cols = []
    for case_id in CASES.keys():
        if case_id != "daily":
            col_name = f"case_{case_id}_count"
            if col_name in all_columns:
                case_cols.append(col_name)
    
    all_select_cols = select_cols + case_cols
    query = f"SELECT {', '.join(all_select_cols)} FROM user_fat WHERE user_id = ?"
    
    cursor.execute(query, (str(user_id),))
    result = cursor.fetchone()
    
    if result:
        data = list(result)
        idx = 0
        user_data = {}
        
        for col in select_cols:
            user_data[col] = data[idx]
            idx += 1
        
        cases_dict = {}
        for i, case_col in enumerate(case_cols):
            case_id = case_col.replace("case_", "").replace("_count", "")
            cases_dict[case_id] = data[idx + i] or 0

        if 'shop' in cases_dict:
            cases_dict['shop_case'] = cases_dict.get('shop_case', 0) + cases_dict['shop']
            del cases_dict['shop']
            print(f"🔧 Автоисправление: shop -> shop_case для пользователя {user_id}")
        
        for key in list(cases_dict.keys()):
            if key not in CASES and key != 'daily':
                print(f"⚠️ Удаляем неизвестный ключ: {key}")
                del cases_dict[key]

        user_data['cases_dict'] = cases_dict
        conn.close()
        return user_data
    else:
        user_data = {
            'user_id': str(user_id),
            'user_name': user_name or "Unknown",
            'current_number': 0,
            'last_command_time': None,
            'consecutive_plus': 0,
            'consecutive_minus': 0,
            'jackpot_pity': 0,
            'autoburger_count': 0,
            'last_case_time': None,
            'next_autoburger_time': None,
            'total_autoburger_activations': 0,
            'total_autoburger_gain': 0,
            'last_autoburger_result': None,
            'last_autoburger_time': None,
            'legendary_burger': -1,
            'item_counts': '{}',
            'last_command': None,
            'last_command_target': None,
            'last_command_use_time': None,
            'fat_cooldown_time': None,
            'active_case_message_id': None,
            'daily_case_last_time': None,
            'snatcher_last_time': None,
            'duel_active': 0,
            'duel_opponent': None,
            'duel_amount': 0,
            'duel_message_id': None,
            'duel_initiator': 0,
            'last_case_type': None,
            'last_case_prize': None,
            'upgrade_active': 0,
            'upgrade_data': None,
            'duel_start_time': None,
            'cases_dict': {}
        }
        
        for case_id in CASES.keys():
            if case_id != "daily":
                user_data['cases_dict'][case_id] = 0
        
        create_new_user(cursor, user_data, all_columns)
        conn.commit()
        conn.close()
        return user_data

def create_new_user(cursor, user_data, all_columns):
    cols = []
    values = []
    
    base_fields = ['user_id', 'user_name', 'current_number', 'last_command_time',
                   'consecutive_plus', 'consecutive_minus', 'jackpot_pity', 'autoburger_count',
                   'last_case_time', 'next_autoburger_time', 'total_autoburger_activations',
                   'total_autoburger_gain', 'last_autoburger_result', 'last_autoburger_time',
                   'legendary_burger', 'item_counts', 'last_command', 'last_command_target',
                   'last_command_use_time', 'fat_cooldown_time', 'active_case_message_id',
                   'daily_case_last_time', 'snatcher_last_time', 'duel_active', 'duel_opponent',
                   'duel_amount', 'duel_message_id', 'duel_initiator',
                   'last_case_type', 'last_case_prize', 'upgrade_active', 'upgrade_data', 'duel_start_time']
    
    for field in base_fields:
        if field in all_columns:
            cols.append(field)
            values.append(user_data.get(field))
    
    for case_id, count in user_data['cases_dict'].items():
        col_name = f"case_{case_id}_count"
        if col_name in all_columns:
            cols.append(col_name)
            values.append(count)
    
    placeholders = ["?"] * len(cols)
    query = f"INSERT INTO user_fat ({', '.join(cols)}) VALUES ({', '.join(placeholders)})"
    cursor.execute(query, values)

def update_user_data(chat_id, user_id, **kwargs):
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA table_info(user_fat)")
    existing_columns = [col[1] for col in cursor.fetchall()]
    
    updates = []
    values = []
    
    for key, value in kwargs.items():
        if key == 'number':
            if 'current_number' in existing_columns:
                updates.append("current_number = ?")
                values.append(value)
        elif key == 'cases_dict' and isinstance(value, dict):
            for case_id, count in value.items():
                col_name = f"case_{case_id}_count"
                if col_name in existing_columns:
                    updates.append(f"{col_name} = ?")
                    values.append(count)
        elif key in existing_columns:
            updates.append(f"{key} = ?")
            values.append(value)
    
    if not updates:
        conn.close()
        return
    
    values.append(str(user_id))
    query = f"UPDATE user_fat SET {', '.join(updates)} WHERE user_id = ?"
    
    try:
        cursor.execute(query, values)
    except sqlite3.OperationalError as e:
        print(f"❌ Ошибка SQL: {e}")
        conn.close()
        return
    
    conn.commit()
    conn.close()

def get_user_items(item_counts_str):
    try:
        return json.loads(item_counts_str) if item_counts_str and item_counts_str != '{}' else {}
    except:
        return {}

def save_user_items(items_dict):
    return json.dumps(items_dict)

def format_user_info(user_number, user_name, legendary_burger=-1):
    if legendary_burger >= 0 and legendary_burger < len(BURGER_RANKS):
        icon = BURGER_RANKS[legendary_burger]["emoji"]
        return f"{icon} {user_number}kg {user_name}"
    else:
        return f"{user_number}kg {user_name}"

def check_ascension_available(current_weight, legendary_burger):
    if legendary_burger >= DIAMOND_BURGER:
        return False, -1, None, 0, 0
    next_burger = legendary_burger + 1 if legendary_burger >= 0 else 0
    if next_burger < len(BURGER_RANKS):
        burger = BURGER_RANKS[next_burger]
        if current_weight >= burger["weight_required"]:
            return True, next_burger, burger["name"], burger["weight_required"], burger["chance"]
    return False, -1, None, 0, 0

def can_get_daily_case(chat_id, user_id, custom_cooldown=None):
    data = get_user_data(chat_id, user_id)
    daily_case_last_time = data.get('daily_case_last_time')
    
    if not daily_case_last_time:
        return True, 0
    
    if isinstance(daily_case_last_time, str):
        last_time = datetime.fromisoformat(daily_case_last_time)
    else:
        last_time = daily_case_last_time
    
    time_diff = datetime.now() - last_time
    cooldown = (custom_cooldown or CASE_COOLDOWN_HOURS) * 3600
    
    if time_diff.total_seconds() >= cooldown:
        return True, 0
    else:
        remaining = cooldown - time_diff.total_seconds()
        return False, remaining

def update_daily_case_time(chat_id, user_id):
    update_user_data(chat_id, user_id, daily_case_last_time=datetime.now())

def check_cooldown(last_command_time, cooldown_hours):
    if last_command_time is None:
        return True, 0
    try:
        if isinstance(last_command_time, str):
            last_time = datetime.fromisoformat(last_command_time)
        else:
            last_time = last_command_time
        time_diff = datetime.now() - last_time
        cooldown_seconds = cooldown_hours * 3600
        if time_diff.total_seconds() >= cooldown_seconds:
            return True, 0
        else:
            remaining = cooldown_seconds - time_diff.total_seconds()
            return False, remaining
    except:
        return True, 0

def format_time(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    if hours > 0:
        return f"{hours} ч {minutes} мин"
    elif minutes > 0:
        return f"{minutes} мин {seconds} сек"
    else:
        return f"{seconds} сек"

def is_tester(user_id):
    return user_id in TESTER_IDS

def get_all_users_sorted(chat_id):
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA table_info(user_fat)")
    columns = [col[1] for col in cursor.fetchall()]
    
    select_cols = ['user_name', 'current_number', 'last_command_time', 
                   'consecutive_plus', 'consecutive_minus', 'jackpot_pity', 
                   'autoburger_count', 'total_autoburger_activations', 
                   'total_autoburger_gain']
    
    if 'legendary_burger' in columns:
        select_cols.append('legendary_burger')
    
    query = f"SELECT {', '.join(select_cols)} FROM user_fat ORDER BY current_number DESC"
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results

def get_chat_stats(chat_id):
    users = get_all_users_sorted(chat_id)
    total_users = len(users)
    total_weight = sum(u[1] for u in users)
    avg_weight = total_weight / total_users if total_users > 0 else 0
    positive = sum(1 for u in users if u[1] > 0)
    negative = sum(1 for u in users if u[1] < 0)
    zero = sum(1 for u in users if u[1] == 0)
    total_autoburgers = sum(u[6] for u in users if len(u) > 6)
    total_activations = sum(u[7] for u in users if len(u) > 7)
    total_gain = sum(u[8] for u in users if len(u) > 8)
    
    burger_counts = [0, 0, 0, 0]
    for u in users:
        if len(u) > 9 and u[9] is not None and u[9] >= 0:
            burger_idx = u[9]
            if burger_idx < len(burger_counts):
                burger_counts[burger_idx] += 1
    
    return {
        'total_users': total_users, 'total_weight': total_weight, 'avg_weight': avg_weight,
        'positive': positive, 'negative': negative, 'zero': zero,
        'total_autoburgers': total_autoburgers, 'total_activations': total_activations,
        'total_gain': total_gain, 'burger_counts': burger_counts
    }

def get_shop_data(chat_id):
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT slots, last_update, next_update FROM shop WHERE chat_id = ?', (str(chat_id),))
    result = cursor.fetchone()
    conn.close()
    
    if result:
        slots_json, last_update, next_update = result
        try:
            slots = json.loads(slots_json) if slots_json else []
            for slot in slots:
                if slot is not None and "type" not in slot:
                    if "case_id" in slot:
                        slot["type"] = "case"
                    else:
                        slot["type"] = "item"
            return slots, last_update, next_update
        except:
            return [], None, None
    return None, None, None

def update_shop_data(chat_id, slots, last_update, next_update):
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    clean_slots = []
    for slot in slots:
        if slot is not None:
            if "type" not in slot:
                if "case_id" in slot:
                    slot["type"] = "case"
                else:
                    slot["type"] = "item"
            clean_slots.append(slot)
        else:
            clean_slots.append(None)
    
    slots_json = json.dumps(clean_slots)
    last_update_str = last_update.isoformat() if last_update else None
    next_update_str = next_update.isoformat() if next_update else None
    
    cursor.execute('''INSERT OR REPLACE INTO shop (chat_id, slots, last_update, next_update) VALUES (?, ?, ?, ?)''', 
                  (str(chat_id), slots_json, last_update_str, next_update_str))
    conn.commit()
    conn.close()

def get_change_with_pity_and_jackpot(consecutive_plus, consecutive_minus, jackpot_pity, 
                                      autoburger_count=0, legendary_burger=-1, items_dict=None, 
                                      current_weight=None):
    if items_dict is None:
        items_dict = {}
    
    has_rotten_leg = items_dict.get("Гнилая ножка KFC", 0) > 0
    has_holy_sandwich = items_dict.get("Святой сэндвич", 0) > 0
    has_water = items_dict.get("Стакан воды", 0) > 0
    
    fat_cooldown_reduction = 0
    case_cooldown_reduction = 0
    jackpot_boost = 0
    
    for item_name, count in items_dict.items():
        if item_name == "Яблоко":
            fat_cooldown_reduction += count * 0.05
        elif item_name == "Золотое Яблоко":
            fat_cooldown_reduction += count * 0.10
        elif item_name == "Апельсин":
            case_cooldown_reduction += count * 0.05
        elif item_name == "Золотой Апельсин":
            case_cooldown_reduction += count * 0.10
        elif item_name == "Драгонфрукт":
            jackpot_boost += count * 0.01
        elif item_name == "Золотой Драгонфрукт":
            jackpot_boost += count * 0.05
    
    active_legendary_item = None
    if has_water:
        active_legendary_item = "water"
    elif has_rotten_leg:
        active_legendary_item = "rotten_leg"
    
    multiplier = 1.0
    if legendary_burger >= 0 and legendary_burger < len(BURGER_RANKS):
        multiplier = BURGER_RANKS[legendary_burger]["multiplier"]
    
    if autoburger_count > 0:
        autoburger_boost = AUTOBURGER_MAX_BONUS * (1 - math.exp(-AUTOBURGER_GROWTH_RATE * autoburger_count))
    else:
        autoburger_boost = 0
    
    minus_boost = min(consecutive_minus * CONSECUTIVE_MINUS_BOOST, MAX_CONSECUTIVE_MINUS_BOOST)
    
    diamond_bonus = 0
    if legendary_burger == DIAMOND_BURGER:
        diamond_bonus = 0.1
    
    minus_chance = BASE_MINUS_CHANCE + (consecutive_plus * PITY_INCREMENT) - autoburger_boost - minus_boost - diamond_bonus
    minus_chance = max(0.1, min(minus_chance, MAX_MINUS_CHANCE))
    
    jackpot_chance = BASE_JACKPOT_CHANCE + (jackpot_pity * JACKPOT_PITY_INCREMENT) + jackpot_boost
    if legendary_burger == DIAMOND_BURGER:
        jackpot_chance *= 2
    if has_holy_sandwich:
        sandwich_count = items_dict.get("Святой сэндвич", 0)
        sandwich_bonus = 0.3 * sandwich_count
        jackpot_chance = max(jackpot_chance, sandwich_bonus)
        jackpot_chance = min(jackpot_chance, 0.9)
    else:
        jackpot_chance = min(jackpot_chance, MAX_JACKPOT_CHANCE)
    
    if active_legendary_item == "water":
        jackpot_roll = random.random()
        if jackpot_roll < jackpot_chance:
            change = random.randint(JACKPOT_MIN, JACKPOT_MAX) // 3
            change = int(change * multiplier)
            new_consecutive_plus = consecutive_plus + 1
            new_consecutive_minus = 0
            new_jackpot_pity = 0
            was_minus = False
            was_jackpot = True
            return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot
        else:
            change = random.randint(1, 20) // 3
            change = int(change * multiplier)
            new_consecutive_plus = consecutive_plus + 1
            new_consecutive_minus = 0
            new_jackpot_pity = jackpot_pity + 1
            was_minus = False
            was_jackpot = False
            return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot
    
    elif active_legendary_item == "rotten_leg":
        if random.random() < 0.6:
            if current_weight is not None:
                loss = int(current_weight * 0.5)
                change = -loss
            else:
                change = -int(consecutive_plus * 0.5)
            new_consecutive_plus = 0
            new_consecutive_minus = consecutive_minus + 1
            new_jackpot_pity = jackpot_pity + 1
            was_minus = True
            was_jackpot = False
            return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot
        else:
            change = random.randint(JACKPOT_MIN, JACKPOT_MAX)
            change = int(change * multiplier)
            new_consecutive_plus = consecutive_plus + 1
            new_consecutive_minus = 0
            new_jackpot_pity = 0
            was_minus = False
            was_jackpot = True
            return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot
    
    else:
        jackpot_roll = random.random()
        if jackpot_roll < jackpot_chance:
            change = random.randint(JACKPOT_MIN, JACKPOT_MAX)
            change = int(change * multiplier)
            new_consecutive_plus = consecutive_plus + 1
            new_consecutive_minus = 0
            new_jackpot_pity = 0
            was_minus = False
            was_jackpot = True
            return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot
        
        roll = random.random()
        if roll < minus_chance:
            change = random.randint(-20, -1)
            change = int(change * multiplier)
            new_consecutive_plus = 0
            new_consecutive_minus = consecutive_minus + 1
            new_jackpot_pity = jackpot_pity + 1
            was_minus = True
            was_jackpot = False
        else:
            change = random.randint(1, 20)
            change = int(change * multiplier)
            new_consecutive_plus = consecutive_plus + 1
            new_consecutive_minus = 0
            new_jackpot_pity = jackpot_pity + 1
            was_minus = False
            was_jackpot = False
        
        return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot

def open_case(case_id, legendary_burger=-1):
    case = CASES[case_id]
    prizes = case["prizes"]
    
    total_chance = sum(p["chance"] for p in prizes)
    for prize in prizes:
        prize["normalized_chance"] = (prize["chance"] / total_chance) * 100
    
    if legendary_burger == DIAMOND_BURGER and case_id != "daily":
        modified_prizes = []
        for prize in prizes:
            p = prize.copy()
            if (isinstance(p["value"], int) and p["value"] >= 100) or p["value"] in ["autoburger", "rotten_leg", "water"]:
                p["normalized_chance"] = prize["normalized_chance"] * 2
            modified_prizes.append(p)
        
        total = sum(p["normalized_chance"] for p in modified_prizes)
        for p in modified_prizes:
            p["normalized_chance"] = (p["normalized_chance"] / total) * 100
        prizes = modified_prizes
    
    roll = random.random() * 100
    cumulative = 0
    for prize in prizes:
        cumulative += prize["normalized_chance"]
        if roll < cumulative:
            return prize
    
    return prizes[-1]

def get_autoburger_interval(autoburger_count):
    if autoburger_count <= 0:
        return None
    elif autoburger_count == 1:
        return AUTOBURGER_INTERVALS[0]
    elif autoburger_count == 2:
        return AUTOBURGER_INTERVALS[1]
    elif autoburger_count == 3:
        return AUTOBURGER_INTERVALS[2]
    else:
        return AUTOBURGER_INTERVALS[3]

def generate_shop_items():
    slots = []
    used_indices = set()
    
    available_cases = [cid for cid, case in CASES.items() if cid != "daily" and case.get("shop_chance", 0) > 0]
    
    for _ in range(4):
        if random.random() < 0.7 and available_cases:
            case_choices = []
            for cid in available_cases:
                case = CASES[cid]
                weight = case["shop_chance"] * 100
                case_choices.extend([cid] * int(weight))
            
            if case_choices:
                chosen_id = random.choice(case_choices)
                case = CASES[chosen_id]
                amount = random.randint(case["min_shop"], case["max_shop"])
                
                min_prize = 0
                max_prize = 0
                for p in case["prizes"]:
                    if isinstance(p["value"], int):
                        if p["value"] < min_prize:
                            min_prize = p["value"]
                        if p["value"] > max_prize:
                            max_prize = p["value"]
                
                slots.append({
                    "type": "case",
                    "case_id": chosen_id,
                    "name": case["name"],
                    "amount": amount,
                    "price": case["price"],
                    "description": f"{case['emoji']} Содержит случайные призы!\nОт {min_prize}кг до {max_prize}кг",
                    "emoji": case['emoji']
                })
            else:
                slots.append(None)
        else:
            slots.append(None)
    
    for _ in range(6):
        chosen_item = None
        for _ in range(50):
            item_idx = random.randint(0, len(SHOP_ITEMS) - 1)
            if item_idx in used_indices:
                continue
            
            item = SHOP_ITEMS[item_idx]
            if random.random() < item["chance"]:
                chosen_item = item
                used_indices.add(item_idx)
                break
        
        if chosen_item:
            amount = random.randint(chosen_item["min_amount"], chosen_item["max_amount"])
            slots.append({
                "type": "item",
                "name": chosen_item["name"],
                "amount": amount,
                "price": chosen_item["price"],
                "description": chosen_item["description"],
                "gain_per_24h": chosen_item.get("gain_per_24h", 0),
                "emoji": ITEM_EMOJIS.get(chosen_item["name"], "📦")
            })
        else:
            slots.append(None)
    
    random.shuffle(slots)
    return slots

async def ensure_shop_updated(chat_id):
    result = get_shop_data(chat_id)
    current_time = datetime.now()
    
    if result[0] is not None:
        slots, last_update_str, next_update_str = result
        
        last_update = None
        next_update = None
        if last_update_str:
            try:
                last_update = datetime.fromisoformat(last_update_str) if isinstance(last_update_str, str) else last_update_str
            except:
                last_update = None
        if next_update_str:
            try:
                next_update = datetime.fromisoformat(next_update_str) if isinstance(next_update_str, str) else next_update_str
            except:
                next_update = None
        
        if next_update and current_time >= next_update:
            new_slots = generate_shop_items()
            last_update = current_time
            next_update = current_time + timedelta(hours=SHOP_UPDATE_HOURS)
            update_shop_data(chat_id, new_slots, last_update, next_update)
            return new_slots, last_update, next_update
        else:
            return slots, last_update, next_update
    else:
        new_slots = generate_shop_items()
        last_update = current_time
        next_update = current_time + timedelta(hours=SHOP_UPDATE_HOURS)
        update_shop_data(chat_id, new_slots, last_update, next_update)
        return new_slots, last_update, next_update

def can_duel(user_data):
    return not user_data.get('duel_active', 0)

def get_duel_info(user_data):
    return {
        'active': user_data.get('duel_active', 0),
        'opponent': user_data.get('duel_opponent'),
        'amount': user_data.get('duel_amount', 0),
        'message_id': user_data.get('duel_message_id'),
        'initiator': user_data.get('duel_initiator', 0),
        'start_time': user_data.get('duel_start_time')
    }

def get_item_price(item_name):
    if item_name in LEGENDARY_UPGRADE_PRICES:
        return LEGENDARY_UPGRADE_PRICES[item_name]
    for shop_item in SHOP_ITEMS:
        if shop_item["name"] == item_name:
            return shop_item["price"]
    return 0

def get_possible_upgrades(item_name, item_count):
    if item_count <= 0:
        return []
    
    current_price = get_item_price(item_name)
    if current_price == 0:
        return []
    
    possible_upgrades = []
    seen_items = set()
    
    all_items = set()
    for shop_item in SHOP_ITEMS:
        all_items.add(shop_item["name"])
    for leg_name in LEGENDARY_UPGRADE_PRICES.keys():
        all_items.add(leg_name)
    
    for shop_item in SHOP_ITEMS:
        item_name_check = shop_item["name"]
        if item_name_check in seen_items:
            continue
            
        target_price = get_item_price(item_name_check)
        if target_price <= current_price:
            continue
        
        chance = current_price / target_price
        if chance < 0.01:
            continue
        
        possible_upgrades.append({
            "name": item_name_check,
            "price": target_price,
            "chance": chance,
            "emoji": ITEM_EMOJIS.get(item_name_check, "🎁")
        })
        seen_items.add(item_name_check)
    
    if current_price >= 1000:
        for leg_name, leg_price in LEGENDARY_UPGRADE_PRICES.items():
            if leg_name in seen_items:
                continue
            if leg_name not in all_items:
                continue
            if leg_price <= current_price:
                continue
            chance = current_price / leg_price
            if chance < 0.01:
                continue
            possible_upgrades.append({
                "name": leg_name,
                "price": leg_price,
                "chance": chance,
                "emoji": ITEM_EMOJIS.get(leg_name, "✨")
            })
            seen_items.add(leg_name)
    
    possible_upgrades.sort(key=lambda x: x["price"])
    return possible_upgrades

# ===== АКТИВНЫЕ ЧАТЫ =====
active_chats = set()

def register_chat(chat_id):
    """Регистрирует чат как активный"""
    active_chats.add(chat_id)

# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ ФОНОВЫХ ЗАДАЧ =====

def get_users_with_autoburgers(chat_id):
    """Возвращает пользователей с автобургерами"""
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''SELECT user_id, user_name, autoburger_count, next_autoburger_time 
                    FROM user_fat WHERE autoburger_count > 0''')
    users = cursor.fetchall()
    conn.close()
    return users

def get_users_with_items(chat_id):
    """Возвращает пользователей с предметами (для пассивного дохода)"""
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA table_info(user_fat)")
    columns = [col[1] for col in cursor.fetchall()]
    
    if 'last_passive_income' not in columns:
        cursor.execute("ALTER TABLE user_fat ADD COLUMN last_passive_income TIMESTAMP")
    if 'last_hourly_income' not in columns:
        cursor.execute("ALTER TABLE user_fat ADD COLUMN last_hourly_income TIMESTAMP")
    conn.commit()
    
    cursor.execute('''SELECT user_id, user_name, current_number, item_counts, legendary_burger, last_passive_income 
                    FROM user_fat WHERE item_counts != '{}' AND item_counts IS NOT NULL''')
    users = cursor.fetchall()
    conn.close()
    return users

def get_users_with_snatcher(chat_id):
    """Возвращает пользователей со снатчером"""
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA table_info(user_fat)")
    columns = [col[1] for col in cursor.fetchall()]
    
    if 'snatcher_last_time' not in columns:
        cursor.execute("ALTER TABLE user_fat ADD COLUMN snatcher_last_time TIMESTAMP")
        conn.commit()
    
    cursor.execute('''SELECT user_id, user_name, item_counts, snatcher_last_time 
                    FROM user_fat WHERE item_counts LIKE '%"Снатчер"%' ''')
    users = cursor.fetchall()
    conn.close()
    return users

def get_users_with_hourly_items(chat_id):
    """Возвращает пользователей с почасовыми предметами"""
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''SELECT user_id, user_name, current_number, item_counts, legendary_burger, last_hourly_income 
                    FROM user_fat''')
    users = cursor.fetchall()
    conn.close()
    return users

# ===== АНИМАЦИИ =====
async def upgrade_animation(message: types.Message, user_id, user_name, source_item, target_item, item_count):
    chat_id = message.chat.id
    
    data = get_user_data(chat_id, user_id, user_name)
    items_dict = get_user_items(data['item_counts'])
    
    upgrade_emojis = ["🟥", "🟩"]
    
    line = []
    for i in range(100):
        line.append(random.choice(upgrade_emojis))
    
    roll = random.random()
    success = roll < target_item['chance']
    
    if success:
        result_emoji = "🟩"
        result_text = f"✅ **УСПЕХ!** ✅"
        result_color = "🟢"
    else:
        result_emoji = "🟥"
        result_text = f"❌ **НЕУДАЧА!** ❌"
        result_color = "🔴"
    
    line[57] = result_emoji
    
    anim_text = f"**{user_name}** улучшает:\n"
    anim_text += f"{ITEM_EMOJIS.get(source_item, '📦')} **{source_item}** → {target_item['emoji']} **{target_item['name']}**\n\n"
    anim_text += f"Шанс: **{target_item['chance']*100:.1f}%**"
    
    anim_msg = await message.reply(anim_text)
    
    animation_frames = [
        (1, 5), (2, 10), (3, 15), (4, 20), (5, 25),
        (6, 30), (7, 35), (8, 39), (9, 43), (10, 47),
        (11, 50), (12, 52), (13, 54), (14, 55), (15, 56),
        (16, 56), (17, 57), (18, 57), (19, 57), (20, 57)
    ]
    
    for frame_num, center_pos in animation_frames:
        visible = line[center_pos-4:center_pos+5]
        display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
        
        frame_text = f"**{user_name}** улучшает:\n"
        frame_text += f"{ITEM_EMOJIS.get(source_item, '📦')} **{source_item}** → {target_item['emoji']} **{target_item['name']}**\n\n"
        frame_text += f"**{display_line}**\n\n"
        frame_text += f"Шанс: **{target_item['chance']*100:.1f}%**"
        
        await anim_msg.edit_text(frame_text)
        await asyncio.sleep(0.5)
    
    if success:
        items_dict[source_item] -= 1
        if items_dict[source_item] <= 0:
            del items_dict[source_item]
        
        items_dict[target_item['name']] = items_dict.get(target_item['name'], 0) + 1
        
        result_description = f"✅ **Поздравляем!**\n\n"
        result_description += f"{ITEM_EMOJIS.get(source_item, '📦')} **{source_item}** → {target_item['emoji']} **{target_item['name']}**\n\n"
        result_description += f"Предмет успешно улучшен!"
    else:
        items_dict[source_item] -= 1
        if items_dict[source_item] <= 0:
            del items_dict[source_item]
        
        result_description = f"❌ **Неудача!**\n\n"
        result_description += f"{ITEM_EMOJIS.get(source_item, '📦')} **{source_item}** был утерян в процессе улучшения!"
    
    update_user_data(chat_id, user_id, item_counts=save_user_items(items_dict))
    
    result_text_full = f"**{display_line}**\n\n{result_text}\n\n{result_description}\n\nШанс был: {target_item['chance']*100:.1f}%"
    await anim_msg.edit_text(result_text_full)

async def upgrade_kg_animation(message: types.Message, user_id, user_name, amount, target_item):
    chat_id = message.chat.id
    
    data = get_user_data(chat_id, user_id, user_name)
    
    upgrade_emojis = ["🟥", "🟩"]
    
    line = []
    for i in range(100):
        line.append(random.choice(upgrade_emojis))
    
    roll = random.random()
    success = roll < target_item['chance']
    
    if success:
        result_emoji = "🟩"
        result_text = f"✅ **УСПЕХ!** ✅"
        result_color = "🟢"
    else:
        result_emoji = "🟥"
        result_text = f"❌ **НЕУДАЧА!** ❌"
        result_color = "🔴"
    
    line[57] = result_emoji
    
    anim_text = f"**{user_name}** улучшает {amount} кг в:\n"
    anim_text += f"{target_item['emoji']} **{target_item['name']}**\n\n"
    anim_text += f"Шанс: **{target_item['chance']*100:.1f}%**"
    
    anim_msg = await message.reply(anim_text)
    
    animation_frames = [
        (1, 5), (2, 10), (3, 15), (4, 20), (5, 25),
        (6, 30), (7, 35), (8, 39), (9, 43), (10, 47),
        (11, 50), (12, 52), (13, 54), (14, 55), (15, 56),
        (16, 56), (17, 57), (18, 57), (19, 57), (20, 57)
    ]
    
    for frame_num, center_pos in animation_frames:
        visible = line[center_pos-4:center_pos+5]
        display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
        
        frame_text = f"**{user_name}** улучшает {amount} кг в:\n"
        frame_text += f"{target_item['emoji']} **{target_item['name']}**\n\n"
        frame_text += f"**{display_line}**\n\n"
        frame_text += f"Шанс: **{target_item['chance']*100:.1f}%**"
        
        await anim_msg.edit_text(frame_text)
        await asyncio.sleep(0.5)
    
    new_number = data['current_number']
    
    if success:
        new_number = data['current_number'] - amount
        
        if target_item.get("is_case", False):
            cases_dict = data.get('cases_dict', {}).copy()
            cases_dict[target_item["case_id"]] = cases_dict.get(target_item["case_id"], 0) + 1
            update_user_data(
                chat_id, user_id,
                number=new_number,
                cases_dict=cases_dict,
                last_command=None,
                last_command_use_time=None
            )
            result_description = f"✅ **Поздравляем!**\n\n"
            result_description += f"{amount} кг → {target_item['emoji']} **{target_item['name']}**\n\n"
            result_description += f"Предмет успешно получен! Потрачено: {amount} кг"
        else:
            items_dict = get_user_items(data['item_counts'])
            items_dict[target_item["name"]] = items_dict.get(target_item["name"], 0) + 1
            update_user_data(
                chat_id, user_id,
                number=new_number,
                item_counts=save_user_items(items_dict),
                last_command=None,
                last_command_use_time=None
            )
            result_description = f"✅ **Поздравляем!**\n\n"
            result_description += f"{amount} кг → {target_item['emoji']} **{target_item['name']}**\n\n"
            result_description += f"Предмет успешно получен! Потрачено: {amount} кг"
    else:
        new_number = data['current_number'] - amount
        update_user_data(
            chat_id, user_id,
            number=new_number,
            last_command=None,
            last_command_use_time=None
        )
        result_description = f"❌ **Неудача!**\n\n"
        result_description += f"{amount} кг сгорели в процессе улучшения!"
    
    result_text_full = f"**{display_line}**\n\n{result_text}\n\n{result_description}\n\nШанс был: {target_item['chance']*100:.1f}%"
    await anim_msg.edit_text(result_text_full)

async def duel_animation(message: types.Message, challenger_name, opponent_name):
    duel_emojis = ["⬆️", "⬇️", "⚔️"]
    
    line = []
    for i in range(100):
        line.append(random.choice(duel_emojis))
    
    result = random.randint(0, 2)
    
    if result == 0:
        result_emoji = "⬆️"
        result_text = f"🏆 **Победитель:** {challenger_name}"
    elif result == 1:
        result_emoji = "⬇️"
        result_text = f"🏆 **Победитель:** {opponent_name}"
    else:
        result_emoji = "⚔️"
        result_text = "🤝 **НИЧЬЯ!** 🤝"
    
    line[57] = result_emoji
    
    anim_text = f"**{challenger_name}**\n**⚔️ ДУЭЛЬ ⚔️**\n**{opponent_name}**"
    anim_msg = await message.reply(anim_text)
    
    animation_frames = [
        (1, 5), (2, 10), (3, 15), (4, 20), (5, 25),
        (6, 30), (7, 35), (8, 39), (9, 43), (10, 47),
        (11, 50), (12, 52), (13, 54), (14, 55), (15, 56),
        (16, 56), (17, 57), (18, 57), (19, 57), (20, 57)
    ]
    
    c_name = challenger_name[:15] + "..." if len(challenger_name) > 15 else challenger_name
    o_name = opponent_name[:15] + "..." if len(opponent_name) > 15 else opponent_name
    
    max_len = max(len(c_name), len(o_name))
    c_name = c_name.ljust(max_len)
    o_name = o_name.ljust(max_len)
    
    for frame_num, center_pos in animation_frames:
        visible = line[center_pos-4:center_pos+5]
        display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
        
        frame_text = f"**{c_name}**\n**{display_line}**\n**{o_name}**"
        await anim_msg.edit_text(frame_text)
        await asyncio.sleep(0.5)
    
    final_text = f"**{c_name}**\n**{display_line}**\n**{o_name}**\n\n{result_text}"
    await anim_msg.edit_text(final_text)
    await asyncio.sleep(1.5)
    
    return result

# ===== КОМАНДЫ =====
async def cmd_start(message: types.Message):
    register_chat(message.chat.id)
    help_text = """
🍔 **ЖИРНЫЙ ТЕЛЕГРАМ БОТ** 🍔

**Основные команды:**
/жир - изменить свой вес
/жиркейс - открыть кейс (ежедневный или из инвентаря) с кнопками открытия и пропуска
/жиркейс_шансы - шансы в ежедневном кейсе
/жиротрясы - таблица рекордов в чате
/жиринфо - информация о вашем весе
/жирзвания - список всех званий
/жиркулдаун - статус кулдаунов
/жирстат - статистика автобургеров
/инвентарь - посмотреть инвентарь

**Дуэли:**
/дуэль @username [кг/"все"] - вызвать на дуэль (с защитой от дюпа)

**Апгрейды:**
/апгрейд - улучшить предмет (с защитой от дюпа)
/апгрейдкг [количество] - улучшить кг в предмет (с защитой от дюпа)
/выбрать [номер] - выбрать цель апгрейда

**Экономика:**
/магазин - магазин предметов и кейсов
/купить [слот] [кол-во] - купить предмет
/датьжир @user [кол-во] - передать кг
/датьпредмет @user [кол-во] [предмет] - передать предмет

**Возвышение:**
/возвышение - попытка получить легендарный бургер

🔥❄️💰🍔⚡👾 - следите за показателями!
    """
    await message.reply(help_text)

async def cmd_fat(message: types.Message):
    register_chat(message.chat.id)
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    data = get_user_data(chat_id, user_id, user_name)
    
    actual_cooldown = COOLDOWN_HOURS
    if data['legendary_burger'] >= 0 and data['legendary_burger'] < len(BURGER_RANKS):
        actual_cooldown = BURGER_RANKS[data['legendary_burger']]["fat_cooldown"] / 60
    
    items_dict = get_user_items(data['item_counts'])
    for item_name, count in items_dict.items():
        if item_name == "Яблоко":
            actual_cooldown *= (1 - count * 0.05)
        elif item_name == "Золотое Яблоко":
            actual_cooldown *= (1 - count * 0.10)
    
    actual_cooldown = max(0.1, actual_cooldown)
    
    can_use, remaining = check_cooldown(data['fat_cooldown_time'], actual_cooldown)
    
    if not can_use:
        await message.reply(
            f"⏳ Подождите! Осталось: {format_time(remaining)}\n"
            f"Кулдаун: {actual_cooldown*60:.0f} мин"
        )
        return
    
    change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot = get_change_with_pity_and_jackpot(
        data['consecutive_plus'], data['consecutive_minus'], data['jackpot_pity'], 
        data['autoburger_count'], data['legendary_burger'], items_dict, data['current_number'])
    
    new_number = data['current_number'] + change
    
    update_user_data(
        chat_id, user_id,
        number=new_number,
        user_name=user_name,
        consecutive_plus=new_consecutive_plus,
        consecutive_minus=new_consecutive_minus,
        jackpot_pity=new_jackpot_pity,
        fat_cooldown_time=datetime.now()
    )
    
    rank_name, rank_emoji = get_rank(new_number)
    
    if was_jackpot:
        header = "💰 **ДЖЕКПОТ!** 💰"
    else:
        header = "🍔 Набор массы"
    
    response = f"{header}\n\n"
    response += f"**{user_name}** теперь весит **{abs(new_number)}kg**!\n\n"
    
    if was_jackpot:
        response += f"💰 Джекпот: +{change} кг\n"
    elif change > 0:
        response += f"📈 +{change} кг\n"
    elif change < 0:
        response += f"📉 {change} кг\n"
    
    response += f"🍖 Текущий вес: {new_number}kg\n"
    response += f"🎖️ Звание: {rank_emoji} {rank_name}\n\n"
    
    pity_info = []
    if was_jackpot:
        pity_info.append("💰 Джекпот сброшен!")
    elif was_minus:
        if data['consecutive_plus'] > 0:
            pity_info.append(f"❌ Серия плюсов ({data['consecutive_plus']}) прервана!")
        pity_info.append(f"📉 Минусов подряд: {new_consecutive_minus}")
    else:
        if new_consecutive_plus > 0:
            pity_info.append(f"🔥 Плюсов подряд: {new_consecutive_plus}")
        if data['consecutive_minus'] > 0:
            pity_info.append(f"✅ Серия минусов ({data['consecutive_minus']}) прервана!")
    
    if pity_info:
        response += "📊 " + "\n📊 ".join(pity_info) + "\n\n"
    
    if data['autoburger_count'] > 0:
        interval = get_autoburger_interval(data['autoburger_count'])
        current_boost = AUTOBURGER_MAX_BONUS * (1 - math.exp(-AUTOBURGER_GROWTH_RATE * data['autoburger_count'])) * 100
        response += f"🍔 Автобургеры: {data['autoburger_count']} шт (каждые {interval} ч)\n"
        response += f"⚡ Бонус: +{current_boost:.1f}%\n\n"
    
    available, burger_idx, burger_name, req_weight, chance = check_ascension_available(new_number, data['legendary_burger'])
    if available:
        response += f"✨ **ВОЗВЫШЕНИЕ ДОСТУПНО!** ✨\n"
        response += f"Цель: {req_weight}кг, шанс: {chance*100:.0f}%\n"
        response += f"Используйте /возвышение\n\n"
    
    response += f"⏰ Следующая команда через {actual_cooldown*60:.0f} мин"
    
    await message.reply(response)

async def cmd_fat_case(message: types.Message):
    register_chat(message.chat.id)
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    data = get_user_data(chat_id, user_id, user_name)
    
    # Удаляем старый активный кейс если есть
    if data.get('active_case_message_id'):
        try:
            await bot.delete_message(chat_id, int(data['active_case_message_id']))
        except:
            pass
    
    items_dict = get_user_items(data['item_counts'])
    
    # Расчёт кулдауна с учётом апельсинов
    actual_case_cooldown = CASE_COOLDOWN_HOURS
    if data['legendary_burger'] >= 0 and data['legendary_burger'] < len(BURGER_RANKS):
        actual_case_cooldown = BURGER_RANKS[data['legendary_burger']]["case_cooldown"]
    
    for item_name, count in items_dict.items():
        if item_name == "Апельсин":
            actual_case_cooldown *= (1 - count * 0.05)
        elif item_name == "Золотой Апельсин":
            actual_case_cooldown *= (1 - count * 0.10)
    
    actual_case_cooldown = max(1, int(actual_case_cooldown))
    
    can_get_daily, daily_remaining = can_get_daily_case(chat_id, user_id, actual_case_cooldown)
    
    cases_dict = data.get('cases_dict', {})
    case_to_open = None
    case = None
    
    if can_get_daily:
        case_to_open = "daily"
        case = CASES["daily"]
    else:
        for case_id, count in cases_dict.items():
            if count > 0:
                case_to_open = case_id
                case = CASES[case_id]
                break
    
    if not case_to_open:
        time_str = format_time(daily_remaining) if daily_remaining > 0 else "скоро"
        await message.reply(
            f"📭 Нет кейсов!\n\n"
            f"Ежедневный кейс будет доступен через: {time_str}\n\n"
            f"Купить кейсы можно в магазине (/магазин)"
        )
        return
    
    # Создаём клавиатуру с кнопками открытия и пропуска
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🖱️ ОТКРЫТЬ", callback_data=f"open_case_{case_to_open}"),
                InlineKeyboardButton(text="⏩ ПРОПУСТИТЬ", callback_data=f"skip_case_{case_to_open}")
            ]
        ]
    )
    
    case_text = f"{case['emoji']} **{case['name']}** {case['emoji']}\n\n"
    case_text += f"{message.from_user.full_name}, у вас есть кейс!\n\n"
    case_text += f"┌───────────────┐\n"
    case_text += f"│----{case['emoji']}---{case['emoji']}---{case['emoji']}----│\n"
    case_text += f"│----К-Е-Й-С-------│\n"
    case_text += f"│----{case['name'][:10]}--│\n"
    case_text += f"│----{case['emoji']}---{case['emoji']}---{case['emoji']}----│\n"
    case_text += f"└───────────────┘"
    
    case_msg = await message.reply(case_text, reply_markup=keyboard)
    
    # Сохраняем информацию о сообщении и типе кейса
    update_user_data(
        chat_id, user_id,
        active_case_message_id=str(case_msg.message_id),
        last_case_type=case_to_open
    )

@dp.callback_query(lambda c: c.data and c.data.startswith('skip_case_'))
async def process_case_skip(callback: CallbackQuery):
    register_chat(callback.message.chat.id)
    await callback.answer("Анимация пропущена!")
    
    chat_id = callback.message.chat.id
    user_id = callback.from_user.id
    user_name = callback.from_user.full_name
    
    case_to_open = callback.data.replace('skip_case_', '')
    
    data = get_user_data(chat_id, user_id, user_name)
    
    # ===== ИСПРАВЛЕНО: проверяем наличие кейса =====
    if case_to_open != "daily":
        cases_dict = data.get('cases_dict', {}).copy()
        if cases_dict.get(case_to_open, 0) <= 0:
            await callback.answer("❌ У вас больше нет этого кейса!", show_alert=True)
            await callback.message.delete()
            return
        
        # СПИСЫВАЕМ КЕЙС СРАЗУ
        cases_dict[case_to_open] -= 1
        update_user_data(chat_id, user_id, cases_dict=cases_dict)
    else:
        # Проверка ежедневного кейса
        actual_case_cooldown = CASE_COOLDOWN_HOURS
        if data['legendary_burger'] >= 0:
            actual_case_cooldown = BURGER_RANKS[data['legendary_burger']]["case_cooldown"]
        
        can_get_daily, daily_remaining = can_get_daily_case(chat_id, user_id, actual_case_cooldown)
        if not can_get_daily:
            await callback.answer(f"⏳ Ежедневный кейс ещё не доступен!", show_alert=True)
            await callback.message.delete()
            return
        
        update_daily_case_time(chat_id, user_id)
    
    prize = open_case(case_to_open, data['legendary_burger'])
    
    try:
        await callback.message.delete_reply_markup()
    except:
        pass
    
    await show_case_result(callback.message, chat_id, user_id, user_name, prize, CASES[case_to_open])

@dp.callback_query(lambda c: c.data and c.data.startswith('open_case_'))
async def process_case_open(callback: CallbackQuery):
    register_chat(callback.message.chat.id)
    await callback.answer()
    
    chat_id = callback.message.chat.id
    user_id = callback.from_user.id
    user_name = callback.from_user.full_name
    
    case_to_open = callback.data.replace('open_case_', '')
    
    data = get_user_data(chat_id, user_id, user_name)
    
    if case_to_open == "daily":
        prize = open_case("daily", data['legendary_burger'])
        update_daily_case_time(chat_id, user_id)
    else:
        prize = open_case(case_to_open, data['legendary_burger'])
        cases_dict = data.get('cases_dict', {}).copy()
        cases_dict[case_to_open] -= 1
        update_user_data(chat_id, user_id, cases_dict=cases_dict)
    
    try:
        await callback.message.delete_reply_markup()
    except:
        pass
    
    prize_emojis = []
    case = CASES[case_to_open]
    for p in case["prizes"]:
        if "emoji" in p:
            emoji = p["emoji"]
        else:
            if p["value"] == "autoburger":
                emoji = "🍔"
            elif p["value"] == "rotten_leg":
                emoji = "💀"
            elif p["value"] == "water":
                emoji = "💧"
            elif isinstance(p["value"], int):
                if p["value"] < 0:
                    emoji = "📉"
                elif p["value"] == 0:
                    emoji = "🔄"
                elif p["value"] < 50:
                    emoji = "📈"
                elif p["value"] < 100:
                    emoji = "⬆️"
                elif p["value"] < 500:
                    emoji = "🚀"
                elif p["value"] < 1000:
                    emoji = "⭐"
                else:
                    emoji = "💥"
            else:
                emoji = "🎁"
        
        if emoji not in prize_emojis:
            prize_emojis.append(emoji)
    
    # ИСПРАВЛЕНО: Создаём сообщение для анимации
    anim_text = f"🎰 **{case['name']}** 🎰"
    anim_msg = await callback.message.reply(anim_text)
    
    # Генерируем линию
    line = [random.choice(prize_emojis) for _ in range(100)]
    
    # Определяем эмодзи приза
    if "emoji" in prize:
        prize_emoji = prize["emoji"]
    elif prize["value"] == "autoburger":
        prize_emoji = "🍔"
    elif prize["value"] == "rotten_leg":
        prize_emoji = "💀"
    elif prize["value"] == "water":
        prize_emoji = "💧"
    elif isinstance(prize["value"], int):
        if prize["value"] < 0:
            prize_emoji = "📉"
        elif prize["value"] == 0:
            prize_emoji = "🔄"
        elif prize["value"] < 50:
            prize_emoji = "📈"
        elif prize["value"] < 100:
            prize_emoji = "⬆️"
        elif prize["value"] < 500:
            prize_emoji = "🚀"
        elif prize["value"] < 1000:
            prize_emoji = "⭐"
        else:
            prize_emoji = "💥"
    else:
        prize_emoji = "🎁"
    
    # ИСПРАВЛЕНО: Анимация с проверкой на изменение текста
    animation_frames = [
        (1, 5), (2, 10), (3, 15), (4, 20), (5, 25),
        (6, 30), (7, 35), (8, 39), (9, 43), (10, 47),
        (11, 50), (12, 52), (13, 54), (14, 55), (15, 56),
        (16, 56), (17, 57), (18, 57), (19, 57), (20, 57)
    ]
    
    last_text = None
    
    for frame_num, center_pos in animation_frames:
        visible = line[center_pos-4:center_pos+5]
        display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
        current_text = f"**{display_line}**"
        
        # Проверяем, изменился ли текст
        if current_text != last_text:
            try:
                await anim_msg.edit_text(current_text)
                last_text = current_text
            except Exception as e:
                print(f"Ошибка при анимации: {e}")
        
        await asyncio.sleep(0.5)
    
    # ИСПРАВЛЕНО: Показываем результат (ставим приз в центр)
    line[57] = prize_emoji
    visible = line[52:61]
    display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
    
    # ИСПРАВЛЕНО: Убеждаемся, что текст отличается от предыдущего
    result_text = f"**{display_line}**\n\n**РЕЗУЛЬТАТ!**"
    
    if result_text != last_text:
        try:
            await anim_msg.edit_text(result_text)
        except Exception as e:
            print(f"Ошибка при показе результата: {e}")
    
    await asyncio.sleep(1.5)
    
    # Обработка приза (как в твоём коде)
    items_dict = get_user_items(data['item_counts'])
    new_number = data['current_number']
    new_autoburger_count = data['autoburger_count']
    new_next_autoburger_time = data['next_autoburger_time']
    prize_value = prize["value"]
    
    has_water = items_dict.get("Стакан воды", 0) > 0
    
    if prize_value == "autoburger":
        new_autoburger_count += 1
        interval = get_autoburger_interval(new_autoburger_count)
        if interval:
            new_next_autoburger_time = datetime.now() + timedelta(hours=interval)
        result_display = f"🎉 **АВТОБУРГЕР!** 🍔✨"
        
    elif prize_value == "rotten_leg":
        items_dict["Гнилая ножка KFC"] = items_dict.get("Гнилая ножка KFC", 0) + 1
        result_display = f"💀 **Гнилая ножка KFC!** 💀"
        
    elif prize_value == "water":
        items_dict["Стакан воды"] = items_dict.get("Стакан воды", 0) + 1
        result_display = f"💧 **Стакан воды!** 💧"
        
    elif isinstance(prize_value, str):
        items_dict[prize_value] = items_dict.get(prize_value, 0) + 1
        result_display = f"🎁 **{prize_value}** {prize_emoji}"
        
    else:
        if has_water and case_to_open != "daily":
            prize_value = prize_value // 3
        new_number = data['current_number'] + prize_value
        result_display = f"🎉 **{prize_value:+d} кг** {prize_emoji}"
    
    update_data = {
        'number': new_number,
        'user_name': user_name,
        'autoburger_count': new_autoburger_count,
        'next_autoburger_time': new_next_autoburger_time,
        'item_counts': save_user_items(items_dict),
        'active_case_message_id': None
    }
    
    if case_to_open == "daily":
        update_data['daily_case_last_time'] = datetime.now()
    
    update_user_data(chat_id, user_id, **update_data)
    
    rank_name, rank_emoji = get_rank(new_number)
    
    # Финальное сообщение
    final_text = f"{case['emoji']} Открытие {case['name']}\n\n"
    
    if prize_value == "autoburger":
        final_text += f"🍔✨ **АВТОБУРГЕР** ✨🍔\n\n"
        final_text += f"Всего автобургеров: {new_autoburger_count}\n"
        final_text += f"Интервал: каждые {get_autoburger_interval(new_autoburger_count)} ч\n"
        final_text += f"Бонус: +{AUTOBURGER_MAX_BONUS * (1 - math.exp(-AUTOBURGER_GROWTH_RATE * new_autoburger_count)) * 100:.1f}%"
    elif prize_value in ["rotten_leg", "water"]:
        final_text += f"🎁 Приз: {result_display}\n\n"
        final_text += f"📦 Теперь у вас: {items_dict.get(prize_value, 0)} шт"
    elif isinstance(prize_value, str):
        final_text += f"🎁 Приз: {result_display}\n\n"
        final_text += f"📦 Теперь у вас: {items_dict.get(prize_value, 0)} шт"
    else:
        final_text += f"🎁 Приз: {result_display}\n\n"
        final_text += f"🍖 Новый вес: {new_number}kg\n"
        final_text += f"🎖️ Звание: {rank_emoji} {rank_name}"
    
    try:
        await anim_msg.reply(final_text)
    except Exception as e:
        print(f"Ошибка при отправке финального сообщения: {e}")
        await callback.message.reply(final_text)

async def case_animation(message, chat_id, user_id, user_name, case_id, prize, skip=False):
    """Анимация открытия кейса с возможностью пропуска"""
    
    case = CASES[case_id]
    data = get_user_data(chat_id, user_id, user_name)
    
    # Определяем эмодзи для анимации
    prize_emojis = []
    for p in case["prizes"]:
        if "emoji" in p:
            emoji = p["emoji"]
        else:
            if p["value"] == "autoburger":
                emoji = "🍔"
            elif p["value"] == "rotten_leg":
                emoji = "💀"
            elif p["value"] == "water":
                emoji = "💧"
            elif isinstance(p["value"], int):
                if p["value"] < 0:
                    emoji = "📉"
                elif p["value"] == 0:
                    emoji = "🔄"
                elif p["value"] < 50:
                    emoji = "📈"
                elif p["value"] < 100:
                    emoji = "⬆️"
                elif p["value"] < 500:
                    emoji = "🚀"
                elif p["value"] < 1000:
                    emoji = "⭐"
                else:
                    emoji = "💥"
            else:
                emoji = "🎁"
        
        if emoji not in prize_emojis:
            prize_emojis.append(emoji)
    
    # Определяем эмодзи приза
    if "emoji" in prize:
        prize_emoji = prize["emoji"]
    elif prize["value"] == "autoburger":
        prize_emoji = "🍔"
    elif prize["value"] == "rotten_leg":
        prize_emoji = "💀"
    elif prize["value"] == "water":
        prize_emoji = "💧"
    elif isinstance(prize["value"], int):
        if prize["value"] < 0:
            prize_emoji = "📉"
        elif prize["value"] == 0:
            prize_emoji = "🔄"
        elif prize["value"] < 50:
            prize_emoji = "📈"
        elif prize["value"] < 100:
            prize_emoji = "⬆️"
        elif prize["value"] < 500:
            prize_emoji = "🚀"
        elif prize["value"] < 1000:
            prize_emoji = "⭐"
        else:
            prize_emoji = "💥"
    else:
        prize_emoji = "🎁"
    
    # Генерируем линию
    line = [random.choice(prize_emojis) for _ in range(100)]
    line[57] = prize_emoji
    
    if skip:
        # Пропускаем анимацию - сразу показываем результат
        visible = line[52:61]
        display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
        
        result_embed = f"**{display_line}**\n\n**РЕЗУЛЬТАТ!**"
        
        try:
            await message.edit_text(result_embed)
        except:
            pass
        
        await asyncio.sleep(1)
        
        # Показываем финальный результат
        await show_case_result(message, chat_id, user_id, user_name, prize, case)
        return
    
    # Полная анимация
    anim_text = f"🎰 **{case['name']}** 🎰"
    try:
        await message.edit_text(anim_text)
    except:
        anim_msg = await message.reply(anim_text)
        message = anim_msg
    
    animation_frames = [
        (1, 5), (2, 10), (3, 15), (4, 20), (5, 25),
        (6, 30), (7, 35), (8, 39), (9, 43), (10, 47),
        (11, 50), (12, 52), (13, 54), (14, 55), (15, 56),
        (16, 56), (17, 57), (18, 57), (19, 57), (20, 57)
    ]
    
    last_text = None
    
    for frame_num, center_pos in animation_frames:
        visible = line[center_pos-4:center_pos+5]
        display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
        current_text = f"**{display_line}**"
        
        if current_text != last_text:
            try:
                await message.edit_text(current_text)
                last_text = current_text
            except:
                pass
        
        await asyncio.sleep(0.3)
    
    # Показываем результат анимации
    visible = line[52:61]
    display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
    result_embed = f"**{display_line}**\n\n**РЕЗУЛЬТАТ!**"
    
    try:
        await message.edit_text(result_embed)
    except:
        pass
    
    await asyncio.sleep(1)
    
    # Показываем финальный результат
    await show_case_result(message, chat_id, user_id, user_name, prize, case)

async def show_case_result(message, chat_id, user_id, user_name, prize, case):
    """Показывает финальный результат открытия кейса"""
    
    # Получаем актуальные данные пользователя
    data = get_user_data(chat_id, user_id, user_name)
    
    items_dict = get_user_items(data['item_counts'])
    new_number = data['current_number']
    new_autoburger_count = data['autoburger_count']
    new_next_autoburger_time = data['next_autoburger_time']
    prize_value = prize["value"]
    
    has_water = items_dict.get("Стакан воды", 0) > 0
    
    # Обрабатываем приз
    if prize_value == "autoburger":
        new_autoburger_count += 1
        interval = get_autoburger_interval(new_autoburger_count)
        if interval:
            new_next_autoburger_time = datetime.now() + timedelta(hours=interval)
        result_display = f"🎉 **АВТОБУРГЕР!** 🍔✨"
        
    elif prize_value == "rotten_leg":
        items_dict["Гнилая ножка KFC"] = items_dict.get("Гнилая ножка KFC", 0) + 1
        result_display = f"💀 **Гнилая ножка KFC!** 💀"
        
    elif prize_value == "water":
        items_dict["Стакан воды"] = items_dict.get("Стакан воды", 0) + 1
        result_display = f"💧 **Стакан воды!** 💧"
        
    elif isinstance(prize_value, str):
        items_dict[prize_value] = items_dict.get(prize_value, 0) + 1
        result_display = f"🎁 **{prize_value}**"
        
    else:
        if has_water and case["name"] != "Жиркейс":
            prize_value = prize_value // 3
        new_number = data['current_number'] + prize_value
        result_display = f"🎉 **{prize_value:+d} кг**"
    
    # Обновляем данные
    update_data = {
        'number': new_number,
        'autoburger_count': new_autoburger_count,
        'next_autoburger_time': new_next_autoburger_time,
        'item_counts': save_user_items(items_dict),
        'active_case_message_id': None,
        'last_case_type': None,
        'last_case_prize': None
    }
    
    update_user_data(chat_id, user_id, **update_data)
    
    rank_name, rank_emoji = get_rank(new_number)
    
    # Формируем финальное сообщение
    final_text = f"{case['emoji']} Открытие {case['name']}\n\n"
    
    if prize_value == "autoburger":
        final_text += f"🍔✨ **АВТОБУРГЕР** ✨🍔\n\n"
        final_text += f"Всего автобургеров: {new_autoburger_count}\n"
        final_text += f"Интервал: каждые {get_autoburger_interval(new_autoburger_count)} ч\n"
        final_text += f"Бонус: +{AUTOBURGER_MAX_BONUS * (1 - math.exp(-AUTOBURGER_GROWTH_RATE * new_autoburger_count)) * 100:.1f}%"
    elif prize_value in ["rotten_leg", "water"]:
        final_text += f"🎁 Приз: {result_display}\n\n"
        final_text += f"📦 Теперь у вас: {items_dict.get(prize_value, 0)} шт"
    elif isinstance(prize_value, str):
        final_text += f"🎁 Приз: {result_display}\n\n"
        final_text += f"📦 Теперь у вас: {items_dict.get(prize_value, 0)} шт"
    else:
        final_text += f"🎁 Приз: {result_display}\n\n"
        final_text += f"🍖 Новый вес: {new_number}kg\n"
        final_text += f"🎖️ Звание: {rank_emoji} {rank_name}"
    
    # Отправляем финальное сообщение
    try:
        await message.reply(final_text)
    except:
        await message.reply("✅ Кейс открыт! (ошибка отображения)")

async def cmd_fat_case_chances(message: types.Message):
    register_chat(message.chat.id)
    """Шансы в кейсе"""
    embed_text = "📊 **ШАНСЫ В КЕЙСЕ** 📊\n\nВероятность выпадения каждого приза в ежедневном кейсе:\n\n"
    
    sorted_prizes = sorted(CASE_PRIZES, key=lambda x: x['chance'] if x['chance'] > 0 else 999, reverse=True)
    
    chances_text = ""
    rare_text = ""
    legendary_text = ""
    
    for prize in sorted_prizes:
        if prize["value"] == "autoburger":
            legendary_text += f"{prize['emoji']} **{prize['name']}** — {prize['chance']:.5f}%\n"
        elif prize["value"] >= 1000:
            rare_text += f"{prize['emoji']} **{prize['name']}** — {prize['chance']}%\n"
        else:
            chances_text += f"{prize['emoji']} **{prize['name']}** — {prize['chance']}%\n"
    
    if chances_text:
        embed_text += f"📦 **Обычные призы**\n{chances_text}\n"
    if rare_text:
        embed_text += f"✨ **Редкие призы**\n{rare_text}\n"
    if legendary_text:
        embed_text += f"🌟 **Легендарные призы**\n{legendary_text}\n"
    
    embed_text += f"\n⏰ Кулдаун ежедневного кейса: **{CASE_COOLDOWN_HOURS} часов**\n"
    embed_text += f"💎 Бонус алмазного бургера: шансы на редкие призы x2"
    
    await message.reply(embed_text)

async def cmd_fat_leaderboard(message: types.Message):
    register_chat(message.chat.id)
    """Таблица лидеров"""
    chat_id = message.chat.id
    chat_name = message.chat.title or "этом чате"
    users = get_all_users_sorted(chat_id)
    
    if not users:
        await message.reply(f"📭 В {chat_name} пока никто не участвовал!")
        return
    
    response = f"🏆 **Таблица жиротрясов - {chat_name}** 🏆\n"
    response += "Рейтинг пользователей по весу (от самых толстых до самых худых)\n\n"
    
    leaderboard_text = ""
    for i, user_data in enumerate(users, 1):
        if len(user_data) >= 10:
            user_name, number, last_update, consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count, total_acts, total_gain, legendary_burger = user_data
        else:
            user_name, number, last_update, consecutive_plus, consecutive_minus, jackpot_pity, autoburger_count, total_acts, total_gain = user_data
            legendary_burger = -1
        
        if i == 1:
            place_icon = "🥇"
        elif i == 2:
            place_icon = "🥈"
        elif i == 3:
            place_icon = "🥉"
        else:
            place_icon = "🔹"
        
        rank_name, rank_emoji = get_rank(number)
        
        display_name = user_name
        if legendary_burger is not None and legendary_burger >= 0:
            burger_emoji = BURGER_RANKS[legendary_burger]["emoji"]
            display_name = f"{burger_emoji}{user_name}"
        
        pity_emojis = []
        if consecutive_plus and consecutive_plus > 0:
            pity_emojis.append("🔥")
        if consecutive_minus and consecutive_minus > 0:
            pity_emojis.append("❄️")
        if jackpot_pity and jackpot_pity > 0:
            pity_emojis.append("💰")
        if autoburger_count and autoburger_count > 0:
            pity_emojis.append(f"🍔{autoburger_count}")
        if total_acts and total_acts > 0:
            pity_emojis.append(f"⚡{total_acts}")
        
        pity_str = f" {' '.join(pity_emojis)}" if pity_emojis else ""
        
        leaderboard_text += f"{place_icon} **{i}.** {display_name} — **{number}kg** {rank_emoji} *{rank_name}*{pity_str}\n"
        
        if len(leaderboard_text) > 3000:
            leaderboard_text += "... и ещё несколько участников"
            break
    
    response += leaderboard_text
    
    stats = get_chat_stats(chat_id)
    
    burger_stats = ""
    for i, count in enumerate(stats['burger_counts']):
        if count > 0:
            burger_stats += f"{BURGER_RANKS[i]['emoji']} {BURGER_RANKS[i]['name']}: {count}\n"
    
    response += f"\n📊 **Статистика чата**\n"
    response += f"Участников: {stats['total_users']}\n"
    response += f"Суммарный вес: {stats['total_weight']}kg\n"
    response += f"Средний вес: {stats['avg_weight']:.1f}kg\n"
    response += f"🔼 Толстых: {stats['positive']} | 🔽 Худых: {stats['negative']} | ⚖️ Нулевых: {stats['zero']}\n"
    response += f"🍔 Всего автобургеров: {stats['total_autoburgers']}\n"
    response += f"⚡ Всего срабатываний: {stats['total_activations']}\n"
    response += f"📈 Всего набрано: {stats['total_gain']} кг"
    
    if burger_stats:
        response += f"\n\n✨ **Легендарные бургеры**\n{burger_stats}"
    
    await message.reply(response)

async def cmd_fat_stats(message: types.Message):
    register_chat(message.chat.id)
    """Статистика автобургеров"""
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    data = get_user_data(chat_id, user_id, user_name)
    
    response = f"📊 **Статистика автобургеров - {user_name}**\n\n"
    response += f"🍔 Всего автобургеров: {data['autoburger_count']}\n"
    response += f"⚡ Срабатываний: {data['total_autoburger_activations']}\n"
    response += f"📈 Всего набрано: {data['total_autoburger_gain']} кг\n"
    
    if data['total_autoburger_activations'] > 0:
        avg_gain = data['total_autoburger_gain'] / data['total_autoburger_activations']
        response += f"📊 Средний прирост: {avg_gain:.1f} кг\n"
    
    if data['last_autoburger_result'] and data['last_autoburger_time']:
        try:
            last_time = data['last_autoburger_time']
            if isinstance(last_time, str):
                last_time = datetime.fromisoformat(last_time)
            time_diff = datetime.now() - last_time
            hours = time_diff.total_seconds() / 3600
            response += f"🕒 Последнее: {data['last_autoburger_result']} ({hours:.1f} ч назад)\n"
        except:
            pass
    
    if data['autoburger_count'] > 0:
        interval = get_autoburger_interval(data['autoburger_count'])
        current_boost = AUTOBURGER_MAX_BONUS * (1 - math.exp(-AUTOBURGER_GROWTH_RATE * data['autoburger_count'])) * 100
        response += f"\n⚡ Текущий бонус: +{current_boost:.1f}% к плюсу (каждые {interval} ч)\n"
    
    if data.get('next_autoburger_time'):
        try:
            next_time = data['next_autoburger_time']
            if isinstance(next_time, str):
                next_time = datetime.fromisoformat(next_time)
            time_diff = next_time - datetime.now()
            if time_diff.total_seconds() > 0:
                response += f"⏰ Следующий автобургер: через {format_time(time_diff.total_seconds())}\n"
        except:
            pass
    
    await message.reply(response)

async def cmd_fat_info(message: types.Message):
    register_chat(message.chat.id)
    """Информация о пользователе"""
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    data = get_user_data(chat_id, user_id, user_name)
    
    rank_name, rank_emoji = get_rank(data['current_number'])
    
    actual_fat_cooldown = COOLDOWN_HOURS
    actual_case_cooldown = CASE_COOLDOWN_HOURS
    if data['legendary_burger'] >= 0 and data['legendary_burger'] < len(BURGER_RANKS):
        actual_fat_cooldown = BURGER_RANKS[data['legendary_burger']]["fat_cooldown"] / 60
        actual_case_cooldown = BURGER_RANKS[data['legendary_burger']]["case_cooldown"]
    
    items_dict = get_user_items(data['item_counts'])
    
    # Расчёт пассивного дохода
    total_passive_income = 0
    income_details = []
    
    for item_name, count in items_dict.items():
        for shop_item in SHOP_ITEMS:
            if shop_item["name"] == item_name:
                gain = shop_item.get("gain_per_24h", 0)
                if gain > 0:
                    item_total = gain * count
                    total_passive_income += item_total
                    income_details.append(f"{item_name} x{count}: +{item_total} кг/24ч")
                break
    
    multiplier = 1.0
    if data['legendary_burger'] >= 0 and data['legendary_burger'] < len(BURGER_RANKS):
        multiplier = BURGER_RANKS[data['legendary_burger']]["multiplier"]
        if multiplier != 1.0:
            total_passive_income = int(total_passive_income * multiplier)
    
    for item_name, count in items_dict.items():
        if item_name == "Яблоко":
            actual_fat_cooldown *= (1 - count * 0.05)
        elif item_name == "Золотое Яблоко":
            actual_fat_cooldown *= (1 - count * 0.10)
        elif item_name == "Апельсин":
            actual_case_cooldown *= (1 - count * 0.05)
        elif item_name == "Золотой Апельсин":
            actual_case_cooldown *= (1 - count * 0.10)
    
    actual_fat_cooldown = max(0.1, actual_fat_cooldown)
    actual_case_cooldown = max(0.1, actual_case_cooldown)
    
    response = f"🍔 **Информация о {user_name}**\n\n"
    response += f"Текущий вес: {data['current_number']}kg\n"
    response += f"🎖️ Звание: {rank_emoji} {rank_name}\n"
    
    if data['legendary_burger'] >= 0 and data['legendary_burger'] < len(BURGER_RANKS):
        burger = BURGER_RANKS[data['legendary_burger']]
        response += f"{burger['emoji']} Легендарный бургер: **{burger['name']}** (x{burger['multiplier']})\n"
    
    if total_passive_income > 0:
        passive_text = f"**{total_passive_income} кг/24ч**"
        if multiplier != 1.0:
            passive_text += f" (x{multiplier})"
        response += f"💰 Пассивный доход: {passive_text}\n"
    
    pity_emojis = []
    if data['consecutive_plus'] > 0:
        pity_emojis.append(f"🔥{data['consecutive_plus']}")
    if data['consecutive_minus'] > 0:
        pity_emojis.append(f"❄️{data['consecutive_minus']}")
    if data['jackpot_pity'] > 0:
        pity_emojis.append(f"💰{data['jackpot_pity']}")
    
    if pity_emojis:
        response += f"📊 Счётчики: {' '.join(pity_emojis)}\n"
    
    if data['autoburger_count'] > 0:
        interval = get_autoburger_interval(data['autoburger_count'])
        current_boost = AUTOBURGER_MAX_BONUS * (1 - math.exp(-AUTOBURGER_GROWTH_RATE * data['autoburger_count'])) * 100
        response += f"🍔 Автобургеры: {data['autoburger_count']} шт (каждые {interval} ч, бонус +{current_boost:.1f}%)\n"
    
    cases_dict = data.get('cases_dict', {})
    cases_text = ""
    for case_id, count in cases_dict.items():
        if count > 0:
            cases_text += f"{CASES[case_id]['emoji']} {CASES[case_id]['name']}: {count}\n"
    
    if cases_text:
        response += f"\n📦 **Кейсы**\n{cases_text}"
    
    can_use, remaining = check_cooldown(data['fat_cooldown_time'], actual_fat_cooldown)
    fat_status = f"✅ Доступен" if can_use else f"⏳ через {format_time(remaining)}"
    
    can_use_case, case_remaining = check_cooldown(data['last_case_time'], actual_case_cooldown)
    case_status = f"✅ Доступен" if can_use_case else f"⏳ через {format_time(case_remaining)}"
    
    response += f"\n⏰ **Кулдауны**\n"
    response += f"/жир: {fat_status}\n"
    response += f"/жиркейс: {case_status}\n"
    
    available, burger_idx, burger_name, req_weight, chance = check_ascension_available(data['current_number'], data['legendary_burger'])
    if available:
        response += f"\n✨ **ВОЗВЫШЕНИЕ ДОСТУПНО!** ✨\n"
        response += f"Цель: {req_weight}кг, шанс: {chance*100:.0f}%\n"
    
    await message.reply(response)

async def cmd_show_ranks(message: types.Message):
    register_chat(message.chat.id)
    """Список званий"""
    response = "🎖️ **Система званий**\n\n"
    
    for rank in RANKS:
        if rank["min"] == rank["max"]:
            range_str = f"{rank['min']}"
        else:
            range_str = f"{rank['min']} – {rank['max']}"
        response += f"{rank['emoji']} **{rank['name']}** — {range_str} kg\n"
    
    await message.reply(response)

async def cmd_cooldown_info(message: types.Message):
    register_chat(message.chat.id)
    """Информация о кулдаунах"""
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    data = get_user_data(chat_id, user_id, user_name)
    
    actual_fat_cooldown = COOLDOWN_HOURS
    actual_case_cooldown = CASE_COOLDOWN_HOURS
    if data['legendary_burger'] >= 0 and data['legendary_burger'] < len(BURGER_RANKS):
        actual_fat_cooldown = BURGER_RANKS[data['legendary_burger']]["fat_cooldown"] / 60
        actual_case_cooldown = BURGER_RANKS[data['legendary_burger']]["case_cooldown"]
    
    items_dict = get_user_items(data['item_counts'])
    for item_name, count in items_dict.items():
        if item_name == "Яблоко":
            actual_fat_cooldown *= (1 - count * 0.05)
        elif item_name == "Золотое Яблоко":
            actual_fat_cooldown *= (1 - count * 0.10)
        elif item_name == "Апельсин":
            actual_case_cooldown *= (1 - count * 0.05)
        elif item_name == "Золотой Апельсин":
            actual_case_cooldown *= (1 - count * 0.10)
    
    actual_fat_cooldown = max(0.1, actual_fat_cooldown)
    actual_case_cooldown = max(0.1, actual_case_cooldown)
    
    fat_can_use, fat_remaining = check_cooldown(data['fat_cooldown_time'], actual_fat_cooldown)
    case_can_use, case_remaining = check_cooldown(data['last_case_time'], actual_case_cooldown)
    
    response = f"⏰ **Кулдауны**\n\n"
    
    if fat_can_use:
        fat_status = "✅ Доступна"
    else:
        fat_status = f"⏳ {format_time(fat_remaining)}"
    
    response += f"**/жир**\n"
    response += f"Кулдаун: {actual_fat_cooldown*60:.0f} мин\n"
    response += f"Статус: {fat_status}\n\n"
    
    if case_can_use:
        case_status = "✅ Доступен"
    else:
        case_status = f"⏳ {format_time(case_remaining)}"
    
    response += f"**/жиркейс**\n"
    response += f"Кулдаун: {actual_case_cooldown:.1f} ч\n"
    response += f"Статус: {case_status}\n\n"
    
    response += f"🍖 Текущий вес: {data['current_number']}kg"
    
    await message.reply(response)

async def cmd_show_inventory(message: types.Message):
    register_chat(message.chat.id)
    """Показывает инвентарь"""
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    data = get_user_data(chat_id, user_id, user_name)
    
    if 'cases_dict' in data:
        clean_cases = {}
        for case_id, count in data['cases_dict'].items():
            if case_id in CASES:
                clean_cases[case_id] = count
            elif case_id == 'shop':
                clean_cases['shop_case'] = clean_cases.get('shop_case', 0) + count
        data['cases_dict'] = clean_cases
    
    response = f"🎒 **Инвентарь - {user_name}**\n\n"
    response += f"🍔 Автобургеры: {data['autoburger_count']}\n"
    response += f"⚡ Срабатываний: {data['total_autoburger_activations']}\n"
    response += f"📈 Всего набрано: {data['total_autoburger_gain']} кг\n\n"
    
    if data['legendary_burger'] >= 0 and data['legendary_burger'] < len(BURGER_RANKS):
        burger = BURGER_RANKS[data['legendary_burger']]
        response += f"{burger['emoji']} Легендарный бургер: **{burger['name']}** (x{burger['multiplier']})\n\n"
    
    cases_dict = data.get('cases_dict', {})
    cases_text = ""
    for case_id, count in cases_dict.items():
        if count > 0:
            cases_text += f"{CASES[case_id]['emoji']} {CASES[case_id]['name']}: {count}\n"
    
    if cases_text:
        response += f"📦 **Кейсы**\n{cases_text}\n"
    
    items_dict = get_user_items(data['item_counts'])
    if items_dict:
        items_text = ""
        regular_items = []
        legendary_items = []
        
        for item_name, count in items_dict.items():
            if item_name in ["Снатчер", "Святой сэндвич", "Гнилая ножка KFC", "Стакан воды",
                            "Автохолестерол", "Холестеринимус", "Яблоко", "Золотое Яблоко",
                            "Апельсин", "Золотой Апельсин", "Драгонфрукт", "Золотой Драгонфрукт"]:
                legendary_items.append(f"• {item_name}: {count} шт")
            else:
                regular_items.append(f"• {item_name}: {count} шт")
        
        if regular_items:
            items_text += "**Обычные предметы:**\n" + "\n".join(regular_items[:8]) + "\n"
            if len(regular_items) > 8:
                items_text += f"... и ещё {len(regular_items) - 8} предметов\n"
        
        if legendary_items:
            items_text += "**✨ Легендарные предметы:**\n" + "\n".join(legendary_items)
        
        response += f"📦 **Предметы**\n{items_text}"
    
    if data['autoburger_count'] > 0 and data.get('next_autoburger_time'):
        try:
            next_time = data['next_autoburger_time']
            if isinstance(next_time, str):
                next_time = datetime.fromisoformat(next_time)
            time_diff = next_time - datetime.now()
            if time_diff.total_seconds() > 0:
                response += f"\n⏰ Следующий автобургер: через {format_time(time_diff.total_seconds())}"
        except:
            pass
    
    await message.reply(response[:4000])

async def cmd_shop(message: types.Message):
    register_chat(message.chat.id)
    """Магазин предметов"""
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    data = get_user_data(chat_id, user_id, user_name)
    
    update_user_data(
        chat_id, user_id,
        last_command="shop",
        last_command_use_time=datetime.now()
    )
    
    slots, last_update, next_update = await ensure_shop_updated(chat_id)
    
    if not isinstance(slots, list):
        slots = []
    
    response = "🏪 **МАГАЗИН** 🏪\n\n"
    response += "Доступные предметы (используйте `/купить [слот] [количество]`):\n"
    response += "📦 **Слоты 1-4:** Кейсы | 🛒 **Слоты 5-10:** Предметы\n\n"
    
    items_text = ""
    for i in range(1, SHOP_SLOTS + 1):
        slot = slots[i-1] if i-1 < len(slots) else None
        
        if slot is not None and isinstance(slot, dict):
            if "type" not in slot:
                if "case_id" in slot:
                    slot["type"] = "case"
                else:
                    slot["type"] = "item"
            
            if slot["type"] == "case":
                prefix = "📦" if i <= 4 else "🎲"
                items_text += f"**{i}.** {prefix} {slot.get('emoji', '📦')} {slot.get('name', 'Неизвестный кейс')} — {slot.get('amount', 0)} шт — **{slot.get('price', 0)} кг/шт**\n"
                items_text += f"   └ {slot.get('description', 'Нет описания')}\n"
            else:
                prefix = "🛒" if i > 4 else "🎁"
                items_text += f"**{i}.** {prefix} {slot.get('name', 'Неизвестный предмет')} — {slot.get('amount', 0)} шт — **{slot.get('price', 0)} кг/шт**\n"
                items_text += f"   └ {slot.get('description', 'Нет описания')}\n"
        else:
            if i <= 4:
                items_text += f"**{i}.** 📦🕳️ Пустой слот для кейса\n"
            else:
                items_text += f"**{i}.** 🛒🕳️ Пустой слот для предмета\n"
    
    response += items_text
    
    last_update_str = last_update.strftime("%d.%m.%Y %H:%M") if last_update else "Никогда"
    next_update_str = next_update.strftime("%d.%m.%Y %H:%M") if next_update else "Скоро"
    
    case_count = sum(1 for s in slots[:4] if s is not None and isinstance(s, dict))
    item_count = sum(1 for s in slots[4:] if s is not None and isinstance(s, dict))
    
    response += f"\n📊 **Статистика магазина**\n"
    response += f"📦 Кейсов в наличии: {case_count}/4\n"
    response += f"🛒 Предметов в наличии: {item_count}/6\n"
    response += f"⏰ Обновление каждые {SHOP_UPDATE_HOURS} часов\n\n"
    response += f"Последнее: {last_update_str}\n"
    response += f"Следующее: {next_update_str}"
    
    await message.reply(response)

async def cmd_buy(message: types.Message):
    register_chat(message.chat.id)
    """Покупка предметов"""
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    # ИСПРАВЛЕНО: парсим аргументы вручную
    if not message.text or ' ' not in message.text:
        await message.reply("❌ Использование: `/купить [номер слота] [количество]`\nПример: `/купить 1 2`")
        return
    
    parts = message.text.split()
    if len(parts) < 3:
        await message.reply("❌ Использование: `/купить [номер слота] [количество]`\nПример: `/купить 1 2`")
        return
    
    try:
        slot = int(parts[1])
        amount = int(parts[2])
    except ValueError:
        await message.reply("❌ Номер слота и количество должны быть числами!")
        return
    
    if slot < 1 or slot > SHOP_SLOTS:
        await message.reply(f"❌ Слот должен быть от 1 до {SHOP_SLOTS}!")
        return
    
    if amount <= 0:
        await message.reply("❌ Количество должно быть больше 0!")
        return
    
    data = get_user_data(chat_id, user_id, user_name)
    
    last_command_use_time = data.get('last_command_use_time')
    if last_command_use_time and isinstance(last_command_use_time, str):
        try:
            last_command_use_time = datetime.fromisoformat(last_command_use_time)
        except:
            last_command_use_time = None
    
    if data.get('last_command') != "shop" or not last_command_use_time:
        await message.reply("❌ Сначала используйте `/магазин` для просмотра доступных товаров!")
        return
    
    time_since_shop = datetime.now() - last_command_use_time
    if time_since_shop.total_seconds() > 300:
        await message.reply("❌ Время ожидания истекло. Используйте `/магазин` заново!")
        update_user_data(chat_id, user_id, last_command=None, last_command_use_time=None)
        return
    
    slots, last_update, next_update = await ensure_shop_updated(chat_id)
    
    if slot - 1 >= len(slots) or slots[slot - 1] is None:
        await message.reply(f"❌ В слоте {slot} ничего нет!")
        return
    
    item = slots[slot - 1]
    
    if not isinstance(item, dict):
        await message.reply(f"❌ Ошибка в данных слота {slot}!")
        return
    
    if "amount" not in item or "price" not in item:
        await message.reply(f"❌ Ошибка в данных слота {slot}!")
        return
    
    if amount > item["amount"]:
        await message.reply(f"❌ В наличии только {item['amount']} шт!")
        return
    
    total_price = item["price"] * amount
    if data['current_number'] < total_price:
        await message.reply(f"❌ Недостаточно кг! Нужно: {total_price} кг, у вас: {data['current_number']} кг")
        return
    
    new_number = data['current_number'] - total_price
    item["amount"] -= amount
    
    cases_dict = data.get('cases_dict', {}).copy()
    
    if item.get("type") == "case" or "case_id" in item:
        case_id = item.get("case_id")
        if not case_id:
            await message.reply(f"❌ Ошибка: не удалось определить тип кейса!")
            return
        
        if case_id not in CASES:
            await message.reply(f"❌ Ошибка: неизвестный тип кейса {case_id}!")
            return
        
        cases_dict[case_id] = cases_dict.get(case_id, 0) + amount
        purchase_desc = f"{item.get('emoji', '📦')} {item.get('name', 'Кейс')} x{amount}"
        
        update_user_data(
            chat_id, user_id,
            number=new_number,
            cases_dict=cases_dict,
            last_command=None,
            last_command_use_time=None
        )
    else:
        items_dict = get_user_items(data['item_counts'])
        items_dict[item["name"]] = items_dict.get(item["name"], 0) + amount
        purchase_desc = f"{item['name']} x{amount}"
        
        update_user_data(
            chat_id, user_id,
            number=new_number,
            item_counts=save_user_items(items_dict),
            last_command=None,
            last_command_use_time=None
        )
    
    update_shop_data(chat_id, slots, last_update, next_update)
    
    response = f"✅ **Покупка совершена!**\n\n"
    response += f"📦 Предмет: {purchase_desc}\n"
    response += f"💰 Цена: {total_price} кг\n"
    response += f"💸 Осталось: {new_number} кг"
    
    await message.reply(response)

async def cmd_give_fat(message: types.Message):
    register_chat(message.chat.id)
    """Передача кг другому пользователю"""
    chat_id = message.chat.id
    giver_id = message.from_user.id
    giver_name = message.from_user.full_name
    
    # ИСПРАВЛЕНО: парсим аргументы вручную
    if not message.text or ' ' not in message.text:
        await message.reply("❌ Использование: `/датьжир @username [количество]`\nПример: `/датьжир @user 100`")
        return
    
    parts = message.text.split()
    if len(parts) < 3:
        await message.reply("❌ Использование: `/датьжир @username [количество]`\nПример: `/датьжир @user 100`")
        return
    
    target_username = parts[1].replace('@', '')
    try:
        amount = int(parts[2])
    except ValueError:
        await message.reply("❌ Количество должно быть числом!")
        return
    
    if amount <= 0:
        await message.reply("❌ Количество должно быть больше 0!")
        return
    
    target_user = None
    try:
        chat = await bot.get_chat(chat_id)
        async for member in chat.get_members():
            if member.user.username and member.user.username.lower() == target_username.lower():
                target_user = member.user
                break
    except:
        pass
    
    if not target_user:
        await message.reply(f"❌ Пользователь @{target_username} не найден в этом чате!")
        return
    
    target_id = target_user.id
    target_name = target_user.full_name
    
    if giver_id == target_id:
        await message.reply("❌ Нельзя передавать кг самому себе!")
        return
    
    giver_data = get_user_data(chat_id, giver_id, giver_name)
    target_data = get_user_data(chat_id, target_id, target_name)
    
    if giver_data['current_number'] < amount:
        await message.reply(f"❌ У вас недостаточно кг! Есть: {giver_data['current_number']} кг, нужно: {amount} кг")
        return
    
    new_giver_number = giver_data['current_number'] - amount
    new_target_number = target_data['current_number'] + amount
    
    update_user_data(chat_id, giver_id, number=new_giver_number)
    update_user_data(chat_id, target_id, number=new_target_number)
    
    giver_rank, giver_rank_emoji = get_rank(new_giver_number)
    target_rank, target_rank_emoji = get_rank(new_target_number)
    
    response = f"⚖️ **Перевод жира**\n\n"
    response += f"**{giver_name}** передал кг **{target_name}**!\n\n"
    response += f"📤 Отправитель: {giver_name}\n"
    response += f"Было: {giver_data['current_number']}kg, Стало: {new_giver_number}kg\n"
    response += f"{giver_rank_emoji} {giver_rank}\n\n"
    response += f"📥 Получатель: {target_name}\n"
    response += f"Было: {target_data['current_number']}kg, Стало: {new_target_number}kg\n"
    response += f"{target_rank_emoji} {target_rank}\n\n"
    response += f"📦 Количество: {amount} кг"
    
    await message.reply(response)

async def cmd_ascension(message: types.Message):
    register_chat(message.chat.id)
    """Возвышение - получение легендарного бургера"""
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    data = get_user_data(chat_id, user_id, user_name)
    
    available, burger_idx, burger_name, req_weight, chance = check_ascension_available(data['current_number'], data['legendary_burger'])
    
    if not available:
        if data['legendary_burger'] >= DIAMOND_BURGER:
            await message.reply(f"❌ У вас уже есть **Алмазный бургер**! Это максимальный уровень.")
        else:
            next_burger = data['legendary_burger'] + 1 if data['legendary_burger'] >= 0 else 0
            if next_burger < len(BURGER_RANKS):
                req = BURGER_RANKS[next_burger]["weight_required"]
                await message.reply(f"❌ Вам нужно достичь **{req}кг**. Текущий вес: **{data['current_number']}кг**")
            else:
                await message.reply("❌ Для вас больше нет возвышений!")
        return
    
    roll = random.random()
    success = roll < chance
    
    items_dict = get_user_items(data['item_counts'])
    
    if success:
        new_burger_idx = burger_idx
        new_number = 0
        new_autoburger_count = 0
        new_next_autoburger_time = None
        
        update_data = {
            'number': new_number,
            'user_name': user_name,
            'autoburger_count': new_autoburger_count,
            'next_autoburger_time': new_next_autoburger_time,
            'legendary_burger': new_burger_idx,
            'item_counts': save_user_items(items_dict)
        }
        
        update_user_data(chat_id, user_id, **update_data)
        
        burger_emoji = BURGER_RANKS[new_burger_idx]["emoji"]
        burger = BURGER_RANKS[new_burger_idx]
        
        response = f"✨ **ВОЗВЫШЕНИЕ УСПЕШНО!** ✨\n\n"
        response += f"**{user_name}** получил {burger_emoji} **{burger_name}**!\n\n"
        response += f"📊 Вес сброшен до **0кг**, автобургеры обнулены\n\n"
        response += f"⚡ **Полученные бонусы**\n"
        response += f"Множитель: x{burger['multiplier']}\n"
        response += f"КД /жир: {burger['fat_cooldown']} мин\n"
        response += f"КД /жиркейс: {burger['case_cooldown']} ч"
        
        if new_burger_idx == DIAMOND_BURGER:
            response += f"\n+10% к шансу плюса\nРедкие предметы x2"
        
        await message.reply(response)
    else:
        new_number = data['current_number'] // 2
        
        update_user_data(chat_id, user_id, number=new_number, item_counts=save_user_items(items_dict))
        
        response = f"💔 **ВОЗВЫШЕНИЕ НЕ УДАЛОСЬ** 💔\n\n"
        response += f"**{user_name}** попытался возвыситься, но потерпел неудачу!\n\n"
        response += f"📊 Вес уменьшен вдвое: **{new_number}кг**\n"
        response += f"Шанс был: {chance*100:.0f}%\n\n"
        response += f"Повезёт в следующий раз!"
        
        await message.reply(response)

async def cmd_upgrade(message: types.Message):
    register_chat(message.chat.id)
    """Улучшение предметов с защитой от дюпа"""
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    data = get_user_data(chat_id, user_id, user_name)
    
    # ===== ИСПРАВЛЕНО: Проверка активного апгрейда =====
    if data.get('upgrade_active', 0) == 1:
        await message.reply("⚠️ У вас уже есть активный апгрейд! Дождитесь его завершения.")
        return
    
    items_dict = get_user_items(data['item_counts'])
    
    available_items = []
    for item_name, count in items_dict.items():
        if count > 0:
            price = get_item_price(item_name)
            available_items.append({
                "name": item_name,
                "count": count,
                "price": price,
                "emoji": ITEM_EMOJIS.get(item_name, "📦")
            })
    
    available_items.sort(key=lambda x: x["price"])
    
    if not available_items:
        await message.reply("❌ У вас нет предметов для улучшения!")
        return
    
    # ИСПРАВЛЕНО: парсим аргументы
    parts = message.text.split() if message.text else []
    
    if len(parts) < 2:  # Нет аргументов
        response = f"🔧 **АПГРЕЙД ПРЕДМЕТОВ** 🔧\n\n"
        response += f"{user_name}, выберите предмет для улучшения:\n"
        response += f"Используйте `/апгрейд [номер]`\n\n"
        
        items_text = ""
        for i, item in enumerate(available_items, 1):
            item_line = f"**{i}.** {item['emoji']} **{item['name']}** — {item['count']} шт — {item['price']} кг\n"
            
            if len(items_text + item_line) <= 3000:
                items_text += item_line
            else:
                items_text += f"... и ещё {len(available_items) - i + 1} предметов"
                break
        
        response += items_text
        await message.reply(response)
        return
    
    try:
        item_index = int(parts[1]) - 1
        if item_index < 0 or item_index >= len(available_items):
            await message.reply(f"❌ Неверный номер! Введите число от 1 до {len(available_items)}")
            return
    except ValueError:
        await message.reply("❌ Введите корректный номер!")
        return
    
    selected_item = available_items[item_index]
    possible_upgrades = get_possible_upgrades(selected_item["name"], selected_item["count"])
    
    if not possible_upgrades:
        await message.reply(f"❌ Для **{selected_item['emoji']} {selected_item['name']}** нет доступных улучшений!")
        return
    
    # ===== ИСПРАВЛЕНО: Помечаем апгрейд как активный =====
    update_user_data(
        chat_id, user_id,
        last_command="upgrade_select",
        last_command_target=selected_item["name"],
        last_command_use_time=datetime.now(),
        upgrade_active=1,
        upgrade_data=json.dumps({
            'source_item': selected_item["name"],
            'source_count': selected_item["count"]
        })
    )
    
    response = f"🔧 **ВЫБОР ЦЕЛИ АПГРЕЙДА** 🔧\n\n"
    response += f"{user_name}, вы выбрали: **{selected_item['emoji']} {selected_item['name']}**\n\n"
    response += f"Теперь выберите, во что хотите его улучшить:\n"
    response += f"Используйте `/выбрать [номер]`\n\n"
    
    upgrades_text = ""
    for i, upgrade in enumerate(possible_upgrades, 1):
        chance_percent = upgrade['chance'] * 100
        upgrade_line = f"**{i}.** {upgrade['emoji']} **{upgrade['name']}** — {chance_percent:.1f}% шанс\n"
        
        if len(upgrades_text + upgrade_line) <= 3000:
            upgrades_text += upgrade_line
        else:
            upgrades_text += f"... и ещё {len(possible_upgrades) - i + 1} вариантов"
            break
    
    response += upgrades_text
    await message.reply(response)

async def cmd_upgrade_kg(message: types.Message):
    register_chat(message.chat.id)
    """Улучшение кг в предметы с защитой от дюпа"""
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    data = get_user_data(chat_id, user_id, user_name)
    
    # ===== ИСПРАВЛЕНО: Проверка активного апгрейда =====
    if data.get('upgrade_active', 0) == 1:
        await message.reply("⚠️ У вас уже есть активный апгрейд! Дождитесь его завершения.")
        return
    
    # ИСПРАВЛЕНО: парсим аргументы
    parts = message.text.split() if message.text else []
    
    if len(parts) < 2:
        await message.reply("❌ Использование: `/апгрейдкг [количество кг]`\nПример: `/апгрейдкг 1000`")
        return
    
    try:
        amount = int(parts[1])
    except ValueError:
        await message.reply("❌ Количество должно быть числом!")
        return
    
    if amount <= 0:
        await message.reply("❌ Количество кг должно быть больше 0!")
        return
    
    if data['current_number'] < amount:
        await message.reply(f"❌ У вас недостаточно кг! Есть: {data['current_number']} кг, нужно: {amount} кг")
        return
    
    possible_upgrades = []
    seen_items = set()
    
    all_game_items = set()
    for shop_item in SHOP_ITEMS:
        all_game_items.add(shop_item["name"])
    for leg_name in LEGENDARY_UPGRADE_PRICES.keys():
        all_game_items.add(leg_name)
    
    for item_name in all_game_items:
        if item_name in seen_items:
            continue
            
        target_price = get_item_price(item_name)
        if target_price == 0 or target_price < amount:
            continue
            
        chance = amount / target_price
        chance = min(chance, 1.0)
        
        if chance < 0.01:
            continue
        
        is_case = False
        case_id = None
        for cid, case in CASES.items():
            if case.get("name") == item_name:
                is_case = True
                case_id = cid
                break
        
        possible_upgrades.append({
            "name": item_name,
            "price": target_price,
            "chance": chance,
            "emoji": ITEM_EMOJIS.get(item_name, "🎁"),
            "is_case": is_case,
            "case_id": case_id
        })
        seen_items.add(item_name)
    
    if not possible_upgrades:
        await message.reply(f"❌ На {amount} кг нет доступных улучшений!")
        return
    
    possible_upgrades.sort(key=lambda x: x["price"])
    
    # ===== ИСПРАВЛЕНО: Помечаем апгрейд как активный =====
    update_user_data(
        chat_id, user_id,
        last_command="upgrade_kg_select",
        last_command_target=str(amount),
        last_command_use_time=datetime.now(),
        upgrade_active=1,
        upgrade_data=json.dumps({'amount': amount})
    )
    
    response = f"💱 **АПГРЕЙД КГ В ПРЕДМЕТЫ** 💱\n\n"
    response += f"{user_name}, у вас **{amount} кг** для улучшения!\n\n"
    response += f"Выберите, во что хотите их улучшить (используйте `/выбрать [номер]`):\n\n"
    
    upgrades_text = ""
    for i, upgrade in enumerate(possible_upgrades, 1):
        chance_percent = upgrade['chance'] * 100
        upgrade_line = f"**{i}.** {upgrade['emoji']} **{upgrade['name']}** — {chance_percent:.1f}% шанс (нужно: {upgrade['price']} кг)\n"
        
        if len(upgrades_text + upgrade_line) <= 3000:
            upgrades_text += upgrade_line
        else:
            upgrades_text += f"... и ещё {len(possible_upgrades) - i + 1} вариантов"
            break
    
    response += upgrades_text
    await message.reply(response)

async def cmd_choose(message: types.Message):
    register_chat(message.chat.id)
    """Выбор цели для апгрейда с защитой от дюпа"""
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    # ИСПРАВЛЕНО: парсим аргументы
    parts = message.text.split() if message.text else []
    
    if len(parts) < 2:
        await message.reply("❌ Укажите номер!")
        return
    
    try:
        choice = parts[1]
        count = int(parts[2]) if len(parts) > 2 else 1
    except ValueError:
        await message.reply("❌ Номер должен быть числом!")
        return
    
    data = get_user_data(chat_id, user_id, user_name)
    
    # ===== ИСПРАВЛЕНО: Проверяем активность апгрейда =====
    if data.get('upgrade_active', 0) != 1:
        await message.reply("❌ У вас нет активного апгрейда! Сначала используйте `/апгрейд` или `/апгрейдкг`.")
        return
    
    last_command = data.get('last_command')
    last_use = data.get('last_command_use_time')
    
    if not last_command or not last_use:
        await message.reply("❌ Ошибка состояния апгрейда!")
        update_user_data(chat_id, user_id, last_command=None, last_command_target=None, 
                        last_command_use_time=None, upgrade_active=0, upgrade_data=None)
        return
    
    if isinstance(last_use, str):
        last_use = datetime.fromisoformat(last_use)
    
    if datetime.now() - last_use > timedelta(minutes=5):
        await message.reply("❌ Время ожидания истекло. Используйте команду заново!")
        update_user_data(chat_id, user_id, last_command=None, last_command_target=None, 
                        last_command_use_time=None, upgrade_active=0, upgrade_data=None)
        return
    
    if last_command == "upgrade_kg_select":
        try:
            amount = int(data['last_command_target'])
        except:
            await message.reply("❌ Ошибка в данных апгрейда!")
            update_user_data(chat_id, user_id, upgrade_active=0, upgrade_data=None)
            return
        
        possible_upgrades = []
        seen_items = set()
        
        all_game_items = set()
        for shop_item in SHOP_ITEMS:
            all_game_items.add(shop_item["name"])
        for leg_name in LEGENDARY_UPGRADE_PRICES.keys():
            all_game_items.add(leg_name)
        
        for item_name in all_game_items:
            if item_name in seen_items:
                continue
                
            target_price = get_item_price(item_name)
            if target_price == 0 or target_price < amount:
                continue
                
            chance = amount / target_price
            chance = min(chance, 1.0)
            
            if chance < 0.01:
                continue
            
            is_case = False
            case_id = None
            for cid, case in CASES.items():
                if case.get("name") == item_name:
                    is_case = True
                    case_id = cid
                    break
            
            possible_upgrades.append({
                "name": item_name,
                "price": target_price,
                "chance": chance,
                "emoji": ITEM_EMOJIS.get(item_name, "🎁"),
                "is_case": is_case,
                "case_id": case_id
            })
            seen_items.add(item_name)
        
        possible_upgrades.sort(key=lambda x: x["price"])
        
        try:
            item_index = int(choice) - 1
            if item_index < 0 or item_index >= len(possible_upgrades):
                await message.reply(f"❌ Неверный номер! Введите число от 1 до {len(possible_upgrades)}")
                update_user_data(chat_id, user_id, upgrade_active=0, upgrade_data=None)
                return
        except ValueError:
            await message.reply("❌ Введите корректный номер!")
            update_user_data(chat_id, user_id, upgrade_active=0, upgrade_data=None)
            return
        
        target_item = possible_upgrades[item_index]
        
        # ===== Сбрасываем флаг активности перед анимацией =====
        update_user_data(chat_id, user_id, upgrade_active=0, upgrade_data=None)
        
        await upgrade_kg_animation(message, user_id, user_name, amount, target_item)
    
    elif last_command == "upgrade_select":
        source_item_name = data.get('last_command_target')
        if not source_item_name:
            await message.reply("❌ Ошибка: не выбран исходный предмет!")
            update_user_data(chat_id, user_id, upgrade_active=0, upgrade_data=None)
            return
        
        items_dict = get_user_items(data['item_counts'])
        
        if items_dict.get(source_item_name, 0) <= 0:
            await message.reply(f"❌ У вас больше нет **{source_item_name}** для улучшения!")
            update_user_data(chat_id, user_id, last_command=None, last_command_target=None, 
                            last_command_use_time=None, upgrade_active=0, upgrade_data=None)
            return
        
        possible_upgrades = get_possible_upgrades(source_item_name, items_dict[source_item_name])
        
        if not possible_upgrades:
            await message.reply("❌ Для этого предмета больше нет доступных улучшений!")
            update_user_data(chat_id, user_id, last_command=None, last_command_target=None, 
                            last_command_use_time=None, upgrade_active=0, upgrade_data=None)
            return
        
        try:
            upgrade_index = int(choice) - 1
            if upgrade_index < 0 or upgrade_index >= len(possible_upgrades):
                await message.reply(f"❌ Неверный номер! Введите число от 1 до {len(possible_upgrades)}")
                update_user_data(chat_id, user_id, upgrade_active=0, upgrade_data=None)
                return
        except ValueError:
            await message.reply("❌ Введите корректный номер!")
            update_user_data(chat_id, user_id, upgrade_active=0, upgrade_data=None)
            return
        
        target_item = possible_upgrades[upgrade_index]
        
        # ===== Сбрасываем флаг активности перед анимацией =====
        update_user_data(chat_id, user_id, upgrade_active=0, upgrade_data=None)
        
        await upgrade_animation(message, user_id, user_name, source_item_name, target_item, items_dict[source_item_name])
    
    else:
        await message.reply("❌ Неизвестный тип апгрейда!")
        update_user_data(chat_id, user_id, last_command=None, last_command_target=None, 
                        last_command_use_time=None, upgrade_active=0, upgrade_data=None)

# ===== ТЕСТОВЫЕ КОМАНДЫ =====
async def cmd_give_autoburger(message: types.Message):
    register_chat(message.chat.id)
    """Выдача автобургеров (только для тестеров)"""
    if not is_tester(message.from_user.id):
        await message.reply("❌ У вас нет прав для этой команды!")
        return
    
    # ИСПРАВЛЕНО: ручной парсинг
    parts = message.text.split() if message.text else []
    try:
        количество = int(parts[1]) if len(parts) > 1 else 1
    except (ValueError, IndexError):
        await message.reply("❌ Укажите корректное количество!")
        return
    
    if количество <= 0:
        await message.reply("❌ Количество должно быть больше 0!")
        return
    
    if количество > 100:
        количество = 100
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    data = get_user_data(chat_id, user_id, user_name)
    
    new_autoburger_count = data['autoburger_count'] + количество
    
    interval = get_autoburger_interval(new_autoburger_count)
    if interval:
        new_next_autoburger_time = datetime.now() + timedelta(hours=interval)
    else:
        new_next_autoburger_time = None
    
    update_user_data(
        chat_id, user_id,
        autoburger_count=new_autoburger_count,
        next_autoburger_time=new_next_autoburger_time
    )
    
    response = f"🍔 **Выдача автобургеров**\n\n"
    response += f"**{user_name}** получил автобургеры!\n\n"
    response += f"📦 Получено: +{количество} 🍔\n"
    response += f"📊 Всего: {new_autoburger_count} 🍔\n"
    
    if new_autoburger_count > 0:
        interval = get_autoburger_interval(new_autoburger_count)
        current_boost = AUTOBURGER_MAX_BONUS * (1 - math.exp(-AUTOBURGER_GROWTH_RATE * new_autoburger_count)) * 100
        response += f"\n⚡ Эффект: авто-жир каждые {interval} ч, бонус +{current_boost:.1f}%"
    
    await message.reply(response)

async def cmd_reset_autoburger(message: types.Message):
    register_chat(message.chat.id)
    """Сброс автобургеров (только для тестеров)"""
    if not is_tester(message.from_user.id):
        await message.reply("❌ У вас нет прав для этой команды!")
        return
    
    # ИСПРАВЛЕНО: ручной парсинг
    chat_id = message.chat.id
    parts = message.text.split() if message.text else []
    target_user = message.from_user
    
    if len(parts) > 1 and parts[1].startswith('@'):
        target_username = parts[1].replace('@', '')
        try:
            chat = await bot.get_chat(chat_id)
            async for member in chat.get_members():
                if member.user.username and member.user.username.lower() == target_username.lower():
                    target_user = member.user
                    break
        except:
            pass
    
    target_id = target_user.id
    target_name = target_user.full_name
    
    data = get_user_data(chat_id, target_id, target_name)
    
    if data['autoburger_count'] == 0:
        await message.reply(f"ℹ️ У {target_name} нет автобургеров!")
        return
    
    update_user_data(
        chat_id, target_id,
        autoburger_count=0,
        next_autoburger_time=None
    )
    
    await message.reply(f"🔄 Сброс автобургеров у {target_name}")

async def cmd_autoburger_info(message: types.Message):
    register_chat(message.chat.id)
    """Информация об автобургерах (только для тестеров)"""
    if not is_tester(message.from_user.id):
        await message.reply("❌ У вас нет прав для этой команды!")
        return
    
    # ИСПРАВЛЕНО: ручной парсинг
    chat_id = message.chat.id
    parts = message.text.split() if message.text else []
    target_user = message.from_user
    
    if len(parts) > 1 and parts[1].startswith('@'):
        target_username = parts[1].replace('@', '')
        try:
            chat = await bot.get_chat(chat_id)
            async for member in chat.get_members():
                if member.user.username and member.user.username.lower() == target_username.lower():
                    target_user = member.user
                    break
        except:
            pass
    
    target_id = target_user.id
    target_name = target_user.full_name
    
    data = get_user_data(chat_id, target_id, target_name)
    
    response = f"🍔 **Информация об автобургерах**\n\n"
    response += f"Для {target_name}\n\n"
    response += f"Количество: {data['autoburger_count']} 🍔\n"
    response += f"Срабатываний: {data['total_autoburger_activations']}\n"
    response += f"Всего набрано: {data['total_autoburger_gain']} кг\n"
    
    if data['total_autoburger_activations'] > 0:
        avg_gain = data['total_autoburger_gain'] / data['total_autoburger_activations']
        response += f"Средний прирост: {avg_gain:.1f} кг\n"
    
    if data['autoburger_count'] > 0:
        interval = get_autoburger_interval(data['autoburger_count'])
        current_boost = AUTOBURGER_MAX_BONUS * (1 - math.exp(-AUTOBURGER_GROWTH_RATE * data['autoburger_count'])) * 100
        response += f"\nИнтервал: каждые {interval} ч\n"
        response += f"Бонус к плюсу: +{current_boost:.1f}%\n"
        
        if data.get('next_autoburger_time'):
            try:
                next_time = data['next_autoburger_time']
                if isinstance(next_time, str):
                    next_time = datetime.fromisoformat(next_time)
                time_diff = next_time - datetime.now()
                if time_diff.total_seconds() > 0:
                    response += f"⏰ Следующий: через {format_time(time_diff.total_seconds())}\n"
            except:
                pass
    
    if data['last_autoburger_result'] and data['last_autoburger_time']:
        try:
            last_time = data['last_autoburger_time']
            if isinstance(last_time, str):
                last_time = datetime.fromisoformat(last_time)
            time_diff = datetime.now() - last_time
            hours = time_diff.total_seconds() / 3600
            response += f"\n🕒 Последний результат: {data['last_autoburger_result']} ({hours:.1f} ч назад)"
        except:
            pass
    
    await message.reply(response)

async def cmd_give_shop_item(message: types.Message):
    register_chat(message.chat.id)
    """Выдача предмета (только для тестеров)"""
    if not is_tester(message.from_user.id):
        await message.reply("❌ У вас нет прав для этой команды!")
        return
    
    # ИСПРАВЛЕНО: более сложный парсинг для названия с пробелами
    text = message.text
    if not text or ' ' not in text:
        await message.reply("❌ Использование: `/выдатьпредмет количество \"название предмета\"`\nПример: `/выдатьпредмет 5 Горелый бекон`")
        return
    
    # Убираем команду из текста
    without_command = text.split(' ', 1)[1] if ' ' in text else ''
    
    # Пытаемся найти первую часть (количество) и остальное (название)
    import re
    match = re.match(r'(\d+)\s+(.+)$', without_command)
    
    if not match:
        await message.reply("❌ Использование: `/выдатьпредмет количество \"название предмета\"`\nПример: `/выдатьпредмет 5 Горелый бекон`")
        return
    
    try:
        amount = int(match.group(1))
        item_name = match.group(2).strip()
    except ValueError:
        await message.reply("❌ Количество должно быть числом!")
        return
    
    if amount <= 0:
        await message.reply("❌ Количество должно быть больше 0!")
        return
    
    if amount > 1000:
        amount = 1000
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    data = get_user_data(chat_id, user_id, user_name)
    
    for case_id, case in CASES.items():
        if case_id != "daily" and case["name"].lower() == item_name.lower():
            cases_dict = data.get('cases_dict', {}).copy()
            cases_dict[case_id] = cases_dict.get(case_id, 0) + amount
            
            update_user_data(chat_id, user_id, cases_dict=cases_dict)
            
            response = f"🎁 **Выдача кейса**\n\n"
            response += f"**{user_name}** выдал себе кейс!\n\n"
            response += f"📦 Кейс: **{case['name']}** x{amount}\n"
            response += f"📊 Всего кейсов: {cases_dict.get(case_id, 0)} шт"
            
            await message.reply(response)
            return
    
    found_item = None
    for shop_item in SHOP_ITEMS:
        if shop_item["name"].lower() == item_name.lower():
            found_item = shop_item
            break
    
    if not found_item:
        items_list = "\n".join([f"• {item['name']}" for item in SHOP_ITEMS[:10]])
        await message.reply(f"❌ Предмет '{item_name}' не найден!\n\n📦 **Доступные предметы:**\n{items_list}")
        return
    
    items_dict = get_user_items(data['item_counts'])
    items_dict[found_item["name"]] = items_dict.get(found_item["name"], 0) + amount
    
    update_user_data(chat_id, user_id, item_counts=save_user_items(items_dict))
    
    response = f"🎁 **Выдача предмета**\n\n"
    response += f"**{user_name}** выдал себе предмет!\n\n"
    response += f"📦 Предмет: **{found_item['name']}** x{amount}\n"
    response += f"📝 Описание: {found_item['description']}\n\n"
    
    items_list = "\n".join([f"• {item}: {count} шт" for item, count in list(items_dict.items())[:8]])
    if len(items_dict) > 8:
        items_list += f"\n... и ещё {len(items_dict) - 8} предметов"
    
    response += f"📊 Ваш инвентарь:\n{items_list or 'Пусто'}"
    
    await message.reply(response)

async def cmd_reset_cooldowns(message: types.Message):
    register_chat(message.chat.id)
    """Сброс кулдаунов (только для тестеров)"""
    if not is_tester(message.from_user.id):
        await message.reply("❌ У вас нет прав для этой команды!")
        return
    
    chat_id = message.chat.id
    
    db_path = get_db_path(chat_id)
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('UPDATE user_fat SET fat_cooldown_time = NULL')
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        
        await message.reply(f"🔄 Кулдаун !жир сброшен для {affected} пользователей!")
    else:
        await message.reply("❌ База данных не найдена!")

async def cmd_reset_all_users(message: types.Message):
    register_chat(message.chat.id)
    """Глобальный сброс веса (только для тестеров)"""
    if not is_tester(message.from_user.id):
        await message.reply("❌ У вас нет прав для этой команды!")
        return
    
    chat_id = message.chat.id
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton("✅ ДА", callback_data="reset_confirm")],
            [InlineKeyboardButton("❌ НЕТ", callback_data="reset_cancel")]
        ]
    )
    
    await message.reply(
        "⚠️ **Внимание!** Сбросить вес **ВСЕХ** на 0?\n"
        "Это действие нельзя отменить!",
        reply_markup=keyboard
    )

@dp.callback_query(lambda c: c.data in ['reset_confirm', 'reset_cancel'])
async def process_reset_confirmation(callback: CallbackQuery):
    register_chat(callback.message.chat.id)
    await callback.answer()
    
    if callback.data == 'reset_cancel':
        await callback.message.edit_text("❌ Отмена")
        return
    
    chat_id = callback.message.chat.id
    db_path = get_db_path(chat_id)
    
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''UPDATE user_fat SET 
            current_number = 0, 
            consecutive_plus = 0, 
            consecutive_minus = 0, 
            jackpot_pity = 0, 
            autoburger_count = 0, 
            total_autoburger_activations = 0, 
            total_autoburger_gain = 0, 
            last_autoburger_result = NULL, 
            last_autoburger_time = NULL, 
            legendary_burger = -1, 
            item_counts = "{}"''')
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        
        await callback.message.edit_text(f"⚖️ **Глобальный сброс**\n\nЗатронуто пользователей: {affected}")
    else:
        await callback.message.edit_text("❌ База данных не найдена!")

async def cmd_fat_reset(message: types.Message):
    register_chat(message.chat.id)
    """Сброс веса конкретного пользователя (только для тестеров)"""
    if not is_tester(message.from_user.id):
        await message.reply("❌ У вас нет прав для этой команды!")
        return
    
    # ИСПРАВЛЕНО: ручной парсинг
    chat_id = message.chat.id
    parts = message.text.split() if message.text else []
    target_user = message.from_user
    
    if len(parts) > 1 and parts[1].startswith('@'):
        target_username = parts[1].replace('@', '')
        try:
            chat = await bot.get_chat(chat_id)
            async for member in chat.get_members():
                if member.user.username and member.user.username.lower() == target_username.lower():
                    target_user = member.user
                    break
        except:
            pass
    
    target_id = target_user.id
    target_name = target_user.full_name
    
    data = get_user_data(chat_id, target_id, target_name)
    
    update_data = {
        'number': 0,
        'consecutive_plus': 0,
        'consecutive_minus': 0,
        'jackpot_pity': 0,
        'autoburger_count': 0,
        'total_autoburger_activations': 0,
        'total_autoburger_gain': 0,
        'last_autoburger_result': None,
        'last_autoburger_time': None,
        'legendary_burger': -1,
        'item_counts': '{}'
    }
    
    update_user_data(chat_id, target_id, **update_data)
    
    await message.reply(f"✅ Вес {target_name} сброшен на 0kg")

# ===== ДУЭЛИ =====
class DuelState(StatesGroup):
    waiting_for_accept = State()

async def cmd_duel(message: types.Message):
    register_chat(message.chat.id)
    """Вызов на дуэль"""
    chat_id = message.chat.id
    challenger = message.from_user
    
    # ИСПРАВЛЕНО: парсим аргументы
    parts = message.text.split() if message.text else []
    
    if len(parts) < 3:
        await message.reply("❌ Использование: `/дуэль @username [количество кг или \"все\"]`\nПример: `/дуэль @user 100`")
        return
    
    target_username = parts[1].replace('@', '')
    amount_str = parts[2]
    
    target_user = None
    try:
        chat = await bot.get_chat(chat_id)
        async for member in chat.get_members():
            if member.user.username and member.user.username.lower() == target_username.lower():
                target_user = member.user
                break
    except:
        pass
    
    if not target_user:
        await message.reply(f"❌ Пользователь @{target_username} не найден в этом чате!")
        return
    
    if challenger.id == target_user.id:
        await message.reply("❌ Нельзя вызвать на дуэль самого себя!")
        return
    
    if target_user.is_bot:
        await message.reply("❌ Нельзя вызвать на дуэль бота!")
        return
    
    challenger_data = get_user_data(chat_id, challenger.id, challenger.full_name)
    opponent_data = get_user_data(chat_id, target_user.id, target_user.full_name)
    
    if not can_duel(challenger_data):
        await message.reply("❌ Вы уже участвуете в дуэли!")
        return
    
    if not can_duel(opponent_data):
        await message.reply(f"❌ {target_user.full_name} уже участвует в дуэли!")
        return
    
    duel_amount = 0
    if amount_str.lower() == "все":
        duel_amount = min(challenger_data['current_number'], opponent_data['current_number'])
        amount_text = f"**всё ({duel_amount}кг)**"
    else:
        try:
            duel_amount = int(amount_str)
            if duel_amount <= 0:
                await message.reply("❌ Сумма дуэли должна быть положительным числом!")
                return
            amount_text = f"**{duel_amount}кг**"
        except ValueError:
            await message.reply("❌ Укажите корректное число кг или 'все'!")
            return
    
    if challenger_data['current_number'] < duel_amount:
        await message.reply(f"❌ У вас недостаточно кг! Есть: {challenger_data['current_number']}кг")
        return
    
    if opponent_data['current_number'] < duel_amount:
        await message.reply(f"❌ У {target_user.full_name} недостаточно кг! У него: {opponent_data['current_number']}кг")
        return
    
    # ===== ИСПРАВЛЕНО: Защита от дюпа и отслеживание времени =====
    current_time = datetime.now()
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ ПРИНЯТЬ", callback_data=f"duel_accept_{challenger.id}_{target_user.id}_{duel_amount}"),
                InlineKeyboardButton(text="❌ ОТКЛОНИТЬ", callback_data=f"duel_decline_{challenger.id}_{target_user.id}")
            ]
        ]
    )
    
    duel_msg = await message.reply(
        f"🔫 **ВЫЗОВ НА ДУЭЛЬ!** 🔫\n\n"
        f"{challenger.full_name} вызывает {target_user.full_name} на дуэль!\n\n"
        f"**Ставка:** {amount_text}\n\n"
        f"У вас 30 секунд чтобы принять решение!",
        reply_markup=keyboard
    )
    
    update_user_data(
        chat_id, challenger.id,
        duel_active=1,
        duel_opponent=str(target_user.id),
        duel_amount=duel_amount,
        duel_message_id=str(duel_msg.message_id),
        duel_initiator=1,
        duel_start_time=current_time
    )
    
    update_user_data(
        chat_id, target_user.id,
        duel_active=1,
        duel_opponent=str(challenger.id),
        duel_amount=duel_amount,
        duel_message_id=str(duel_msg.message_id),
        duel_initiator=0,
        duel_start_time=current_time
    )
    
    await asyncio.sleep(30)
    
    challenger_data = get_user_data(chat_id, challenger.id, challenger.full_name)
    if challenger_data.get('duel_active') == 1:
        update_user_data(chat_id, challenger.id, duel_active=0, duel_opponent=None, duel_amount=0, 
                         duel_message_id=None, duel_initiator=0, duel_start_time=None)
        update_user_data(chat_id, target_user.id, duel_active=0, duel_opponent=None, duel_amount=0, 
                         duel_message_id=None, duel_initiator=0, duel_start_time=None)
        
        try:
            await duel_msg.edit_text(
                "⏰ **Время вышло**\n\n"
                f"{target_user.full_name} не принял дуэль вовремя. Дуэль отменена.",
                reply_markup=None
            )
        except:
            pass

@dp.callback_query(lambda c: c.data and c.data.startswith('duel_'))
async def process_duel(callback: CallbackQuery):
    register_chat(callback.message.chat.id)
    await callback.answer()
    
    data = callback.data.split('_')
    action = data[1]
    
    if action == 'accept':
        challenger_id = int(data[2])
        opponent_id = int(data[3])
        amount = int(data[4])
        
        if callback.from_user.id != opponent_id:
            await callback.message.reply("❌ Это не ваша дуэль!")
            return
        
        chat_id = callback.message.chat.id
        
        challenger = await bot.get_chat_member(chat_id, challenger_id)
        opponent = await bot.get_chat_member(chat_id, opponent_id)
        
        challenger_data = get_user_data(chat_id, challenger_id, challenger.user.full_name)
        opponent_data = get_user_data(chat_id, opponent_id, opponent.user.full_name)
        
        result = await duel_animation(callback.message, challenger.user.full_name, opponent.user.full_name)
        
        if result == 0:
            winner = challenger.user
            winner_id = challenger_id
            loser = opponent.user
            loser_id = opponent_id
            winner_new_weight = challenger_data['current_number'] + amount
            loser_new_weight = opponent_data['current_number'] - amount
            
            update_user_data(chat_id, winner_id, number=winner_new_weight)
            update_user_data(chat_id, loser_id, number=loser_new_weight)
            
            result_text = f"**Победитель:** {winner.full_name}\n\n"
            result_text += f"📊 **Результаты:**\n"
            result_text += f"{winner.full_name}: {challenger_data['current_number']}кг → **{winner_new_weight}кг** (+{amount})\n"
            result_text += f"{loser.full_name}: {opponent_data['current_number']}кг → **{loser_new_weight}кг** (-{amount})"
            
        elif result == 1:
            winner = opponent.user
            winner_id = opponent_id
            loser = challenger.user
            loser_id = challenger_id
            winner_new_weight = opponent_data['current_number'] + amount
            loser_new_weight = challenger_data['current_number'] - amount
            
            update_user_data(chat_id, winner_id, number=winner_new_weight)
            update_user_data(chat_id, loser_id, number=loser_new_weight)
            
            result_text = f"**Победитель:** {winner.full_name}\n\n"
            result_text += f"📊 **Результаты:**\n"
            result_text += f"{winner.full_name}: {opponent_data['current_number']}кг → **{winner_new_weight}кг** (+{amount})\n"
            result_text += f"{loser.full_name}: {challenger_data['current_number']}кг → **{loser_new_weight}кг** (-{amount})"
            
        else:
            result_text = f"🤝 **НИЧЬЯ!** 🤝\n\n"
            result_text += f"📊 **Результаты:**\n"
            result_text += f"{challenger.user.full_name}: {challenger_data['current_number']}кг (без изменений)\n"
            result_text += f"{opponent.user.full_name}: {opponent_data['current_number']}кг (без изменений)"
        
        update_user_data(chat_id, challenger_id, duel_active=0, duel_opponent=None, duel_amount=0, 
                         duel_message_id=None, duel_initiator=0, duel_start_time=None)
        update_user_data(chat_id, opponent_id, duel_active=0, duel_opponent=None, duel_amount=0, 
                         duel_message_id=None, duel_initiator=0, duel_start_time=None)
        
        await callback.message.reply(f"⚔️ **ДУЭЛЬ ЗАВЕРШЕНА!** ⚔️\n\n{result_text}")
        
    elif action == 'decline':
        challenger_id = int(data[2])
        opponent_id = int(data[3])
        
        if callback.from_user.id not in [challenger_id, opponent_id]:
            await callback.message.reply("❌ Это не ваша дуэль!")
            return
        
        chat_id = callback.message.chat.id
        
        update_user_data(chat_id, challenger_id, duel_active=0, duel_opponent=None, duel_amount=0, 
                         duel_message_id=None, duel_initiator=0, duel_start_time=None)
        update_user_data(chat_id, opponent_id, duel_active=0, duel_opponent=None, duel_amount=0, 
                         duel_message_id=None, duel_initiator=0, duel_start_time=None)
        
        decliner = callback.from_user.full_name
        await callback.message.edit_text(
            f"❌ **Дуэль отклонена**\n\n"
            f"{decliner} отказался от дуэли!",
            reply_markup=None
        )

async def cmd_cancel_duel(message: types.Message):
    register_chat(message.chat.id)
    """Отмена дуэли (только для тестеров)"""
    if not is_tester(message.from_user.id):
        await message.reply("❌ У вас нет прав для этой команды!")
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    data = get_user_data(chat_id, user_id, user_name)
    duel_info = get_duel_info(data)
    
    if not duel_info['active']:
        await message.reply("❌ Вы не участвуете в дуэли!")
        return
    
    opponent_data = get_user_data(chat_id, duel_info['opponent'])
    
    update_user_data(chat_id, user_id,
                   duel_active=0, duel_opponent=None, duel_amount=0,
                   duel_message_id=None, duel_initiator=0, duel_start_time=None)
    
    update_user_data(chat_id, duel_info['opponent'],
                   duel_active=0, duel_opponent=None, duel_amount=0,
                   duel_message_id=None, duel_initiator=0, duel_start_time=None)
    
    try:
        if duel_info['message_id']:
            await bot.delete_message(chat_id, int(duel_info['message_id']))
    except:
        pass
    
    await message.reply(f"✅ Дуэль отменена!")

async def cmd_give_item(message: types.Message):
    """Передаёт предметы другому пользователю"""
    register_chat(message.chat.id)
    
    chat_id = message.chat.id
    giver_id = message.from_user.id
    giver_name = message.from_user.full_name
    
    # Парсим аргументы
    text = message.text
    if not text or ' ' not in text:
        await message.reply(
            "❌ Использование: `/датьпредмет @username количество \"название предмета\"`\n"
            "Пример: `/датьпредмет @user 5 Горелый бекон`\n"
            "Пример: `/датьпредмет @user 1 Автобургер`"
        )
        return
    
    # Убираем команду из текста
    without_command = text.split(' ', 1)[1] if ' ' in text else ''
    
    # Парсим @username, количество и название предмета
    import re
    match = re.match(r'@(\S+)\s+(\d+)\s+(.+)$', without_command)
    
    if not match:
        await message.reply(
            "❌ Неправильный формат команды!\n"
            "Использование: `/датьпредмет @username количество \"название предмета\"`\n"
            "Пример: `/датьпредмет @user 5 Горелый бекон`"
        )
        return
    
    target_username = match.group(1)
    try:
        amount = int(match.group(2))
        item_name = match.group(3).strip()
    except ValueError:
        await message.reply("❌ Количество должно быть числом!")
        return
    
    if amount <= 0:
        await message.reply("❌ Количество должно быть больше 0!")
        return
    
    # Ищем целевого пользователя в чате
    target_user = None
    try:
        chat = await bot.get_chat(chat_id)
        async for member in chat.get_members():
            if member.user.username and member.user.username.lower() == target_username.lower():
                target_user = member.user
                break
    except Exception as e:
        print(f"Ошибка при поиске пользователя: {e}")
    
    if not target_user:
        await message.reply(f"❌ Пользователь @{target_username} не найден в этом чате!")
        return
    
    target_id = target_user.id
    target_name = target_user.full_name
    
    if giver_id == target_id:
        await message.reply("❌ Нельзя передавать предметы самому себе!")
        return
    
    giver_data = get_user_data(chat_id, giver_id, giver_name)
    target_data = get_user_data(chat_id, target_id, target_name)
    
    item_lower = item_name.lower()
    
    # ===== ПРОВЕРЯЕМ КЕЙСЫ =====
    for case_id, case in CASES.items():
        if case_id != "daily" and case["name"].lower() in item_lower or case_id.lower() in item_lower:
            if not case.get("tradable", True):
                await message.reply(f"❌ Кейс '{case['name']}' нельзя передавать!")
                return
            
            giver_cases = giver_data.get('cases_dict', {}).copy()
            target_cases = target_data.get('cases_dict', {}).copy()
            
            if giver_cases.get(case_id, 0) < amount:
                await message.reply(f"❌ У вас недостаточно кейсов '{case['name']}'! Есть: {giver_cases.get(case_id, 0)}")
                return
            
            giver_cases[case_id] = giver_cases.get(case_id, 0) - amount
            target_cases[case_id] = target_cases.get(case_id, 0) + amount
            
            update_user_data(chat_id, giver_id, cases_dict=giver_cases)
            update_user_data(chat_id, target_id, cases_dict=target_cases)
            
            response = f"📦 **Передача кейса**\n\n"
            response += f"**{giver_name}** передал кейс **{target_name}**!\n\n"
            response += f"🎁 Кейс: **{case['name']}** x{amount}\n"
            response += f"📤 У вас осталось: {giver_cases.get(case_id, 0)} шт\n"
            response += f"📥 У получателя: {target_cases.get(case_id, 0)} шт"
            
            await message.reply(response)
            return
    
    # ===== ПРОВЕРЯЕМ АВТОБУРГЕРЫ =====
    autoburger_keywords = ["автобургер", "бургер", "autoburger", "🍔"]
    is_autoburger = any(word in item_lower for word in autoburger_keywords)
    
    if is_autoburger:
        if giver_data['autoburger_count'] < amount:
            await message.reply(f"❌ У вас недостаточно автобургеров! Есть: {giver_data['autoburger_count']}")
            return
        
        new_giver_burgers = giver_data['autoburger_count'] - amount
        new_target_burgers = target_data['autoburger_count'] + amount
        
        new_target_next_burger = None
        if new_target_burgers > 0:
            interval = get_autoburger_interval(new_target_burgers)
            if interval:
                new_target_next_burger = datetime.now() + timedelta(hours=interval)
        
        update_user_data(chat_id, giver_id, autoburger_count=new_giver_burgers)
        update_user_data(chat_id, target_id, 
                        autoburger_count=new_target_burgers, 
                        next_autoburger_time=new_target_next_burger)
        
        response = f"🍔 **Передача автобургера**\n\n"
        response += f"**{giver_name}** передал автобургер **{target_name}**!\n\n"
        response += f"📦 Количество: {amount} шт\n"
        response += f"📤 У вас осталось: {new_giver_burgers} 🍔\n"
        response += f"📥 У получателя: {new_target_burgers} 🍔"
        
        if new_target_burgers > 0:
            interval = get_autoburger_interval(new_target_burgers)
            response += f"\n⏰ Интервал получателя: каждые {interval} ч"
        
        await message.reply(response)
        return
    
    # ===== ПРОВЕРЯЕМ ОБЫЧНЫЕ ПРЕДМЕТЫ =====
    giver_items = get_user_items(giver_data['item_counts'])
    target_items = get_user_items(target_data['item_counts'])
    
    found_item = None
    for key in giver_items.keys():
        if key.lower() == item_lower:
            found_item = key
            break
    
    if not found_item:
        for key in giver_items.keys():
            if item_lower in key.lower():
                found_item = key
                break
    
    if not found_item:
        if giver_items:
            items_list = "\n".join([f"• {item}: {count} шт" for item, count in list(giver_items.items())[:10]])
            await message.reply(f"❌ У вас нет предмета '{item_name}'!\n\n📦 **Ваши предметы:**\n{items_list}")
        else:
            await message.reply("❌ У вас нет предметов в инвентаре!")
        return
    
    if giver_items[found_item] < amount:
        await message.reply(f"❌ У вас недостаточно '{found_item}'! Есть: {giver_items[found_item]}")
        return
    
    legendary_burger_names = ["Железный бургер", "Золотой бургер", "Платиновый бургер", "Алмазный бургер"]
    if found_item in legendary_burger_names:
        await message.reply(f"❌ Легендарные бургеры нельзя передавать!")
        return
    
    giver_items[found_item] -= amount
    if giver_items[found_item] <= 0:
        del giver_items[found_item]
    
    target_items[found_item] = target_items.get(found_item, 0) + amount
    
    update_user_data(chat_id, giver_id, item_counts=save_user_items(giver_items))
    update_user_data(chat_id, target_id, item_counts=save_user_items(target_items))
    
    response = f"🎁 **Передача предмета**\n\n"
    response += f"**{giver_name}** передал предмет **{target_name}**!\n\n"
    response += f"📦 Предмет: **{found_item}** x{amount}\n\n"
    
    if giver_items:
        giver_inv = "\n".join([f"• {item}: {count} шт" for item, count in list(giver_items.items())[:5]])
        if len(giver_items) > 5:
            giver_inv += f"\n... и ещё {len(giver_items) - 5} предметов"
    else:
        giver_inv = "Пусто"
    
    if target_items:
        target_inv = "\n".join([f"• {item}: {count} шт" for item, count in list(target_items.items())[:5]])
        if len(target_items) > 5:
            target_inv += f"\n... и ещё {len(target_items) - 5} предметов"
    else:
        target_inv = "Пусто"
    
    response += f"📤 **Ваш инвентарь:**\n{giver_inv}\n\n"
    response += f"📥 **Инвентарь получателя:**\n{target_inv}"
    
    await message.reply(response)

# ===== ОБНОВЛЁННАЯ ФУНКЦИЯ АВТОБУРГЕРА =====
async def apply_autoburger(chat_id, user_id, user_name):
    """Применяет эффект автобургера без уведомлений в ЛС"""
    try:
        data = get_user_data(chat_id, user_id, user_name)
        
        items_dict = get_user_items(data['item_counts'])
        change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot = get_change_with_pity_and_jackpot(
            data['consecutive_plus'], data['consecutive_minus'], data['jackpot_pity'], 
            data['autoburger_count'], data['legendary_burger'], items_dict, data['current_number'])
        
        new_number = data['current_number'] + change
        
        update_data = {
            'number': new_number,
            'user_name': user_name,
            'consecutive_plus': new_consecutive_plus,
            'consecutive_minus': new_consecutive_minus,
            'jackpot_pity': new_jackpot_pity,
            'total_autoburger_activations': data['total_autoburger_activations'] + 1,
            'total_autoburger_gain': data['total_autoburger_gain'] + change,
            'last_autoburger_result': f"{change:+d} кг",
            'last_autoburger_time': datetime.now()
        }
        
        update_user_data(chat_id, user_id, **update_data)
        
        print(f"🤖 Автобургер сработал для {user_name} в чате {chat_id}: {change:+d} кг")
    except Exception as e:
        print(f"❌ Ошибка в автобургере: {e}")

# ===== ОБНОВЛЁННАЯ ФУНКЦИЯ СНАТЧЕРА =====
async def apply_snatcher_effect(chat_id, user_id, user_name):
    """Применяет эффект Снатчера без уведомлений в ЛС"""
    try:
        data = get_user_data(chat_id, user_id, user_name)
        items_dict = get_user_items(data['item_counts'])
        
        snatcher_count = items_dict.get("Снатчер", 0)
        if snatcher_count == 0:
            return
        
        if random.random() > 0.2:
            update_user_data(chat_id, user_id, snatcher_last_time=datetime.now())
            return
        
        virtual_slots = []
        used_indices = set()
        
        for _ in range(10):
            chosen_item = None
            for _ in range(50):
                item_idx = random.randint(0, len(SHOP_ITEMS) - 1)
                if item_idx in used_indices:
                    continue
                
                item = SHOP_ITEMS[item_idx]
                if random.random() < item["chance"]:
                    chosen_item = item
                    used_indices.add(item_idx)
                    break
            
            if chosen_item:
                amount = random.randint(chosen_item["min_amount"], chosen_item["max_amount"])
                virtual_slots.append({
                    "name": chosen_item["name"],
                    "amount": amount,
                    "price": chosen_item["price"],
                    "description": chosen_item["description"],
                    "gain_per_24h": chosen_item.get("gain_per_24h", 0)
                })
            else:
                virtual_slots.append(None)
        
        chosen_slot = random.randint(0, 9)
        selected_item = virtual_slots[chosen_slot]
        
        if not selected_item:
            update_user_data(chat_id, user_id, snatcher_last_time=datetime.now())
            return
        
        items_dict[selected_item["name"]] = items_dict.get(selected_item["name"], 0) + 1
        update_user_data(
            chat_id, user_id,
            item_counts=save_user_items(items_dict),
            snatcher_last_time=datetime.now()
        )
        
        print(f"👾 Снатчер сработал для {user_name} в чате {chat_id}: +1 {selected_item['name']} (слот {chosen_slot + 1}/10)")
            
    except Exception as e:
        print(f"❌ Ошибка в работе Снатчера: {e}")

# ===== ФУНКЦИИ ДЛЯ ФОНОВЫХ ЗАДАЧ =====
async def autoburger_loop():
    """Цикл автобургеров - проверка каждую минуту"""
    await asyncio.sleep(10)
    print("🚀 Автобургер цикл запущен")
    
    while True:
        try:
            current_time = datetime.now()
            
            for chat_id in list(active_chats):
                try:
                    users = get_users_with_autoburgers(chat_id)
                    
                    for user_id, user_name, autoburger_count, next_time_str in users:
                        try:
                            if not next_time_str:
                                continue
                                
                            if isinstance(next_time_str, str):
                                next_time = datetime.fromisoformat(next_time_str)
                            else:
                                next_time = next_time_str
                                
                            if current_time >= next_time:
                                await apply_autoburger(chat_id, user_id, user_name)
                                
                                interval = get_autoburger_interval(autoburger_count)
                                if interval:
                                    new_next_time = current_time + timedelta(hours=interval)
                                    update_user_data(chat_id, user_id, next_autoburger_time=new_next_time)
                                    
                        except Exception as e:
                            print(f"❌ Ошибка обработки автобургера для {user_id} в чате {chat_id}: {e}")
                            
                except Exception as e:
                    print(f"❌ Ошибка при работе с чатом {chat_id}: {e}")
                    
        except Exception as e:
            print(f"❌ Ошибка в цикле автобургеров: {e}")
            
        await asyncio.sleep(60)

async def passive_income_loop():
    """Начисляет пассивный доход раз в 24 часа"""
    await asyncio.sleep(10)
    print("💰 Пассивный доход цикл запущен")
    
    while True:
        try:
            current_time = datetime.now()
            print(f"💰 Начисление пассивного дохода: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            for chat_id in list(active_chats):
                try:
                    users = get_users_with_items(chat_id)
                    
                    for user_id, user_name, current_number, item_counts_str, legendary_burger, last_income in users:
                        try:
                            should_pay = False
                            
                            if not last_income:
                                print(f"⏭️ {user_name}: первое начисление, пропускаем")
                                update_user_data(chat_id, user_id, last_passive_income=current_time)
                                continue
                            
                            if isinstance(last_income, str):
                                last_time = datetime.fromisoformat(last_income)
                            else:
                                last_time = last_income
                            
                            time_diff = current_time - last_time
                            if time_diff.total_seconds() >= 24 * 60 * 60:
                                should_pay = True
                            
                            if should_pay:
                                items_dict = get_user_items(item_counts_str)
                                if not items_dict:
                                    continue
                                
                                total_gain = 0
                                gained_items = []
                                
                                for item_name, count in items_dict.items():
                                    for shop_item in SHOP_ITEMS:
                                        if shop_item["name"] == item_name:
                                            gain = shop_item.get("gain_per_24h", 0) * count
                                            if gain > 0:
                                                total_gain += gain
                                                gained_items.append(f"{item_name} x{count} (+{gain}кг)")
                                            break
                                
                                if total_gain > 0:
                                    multiplier = 1.0
                                    if legendary_burger >= 0 and legendary_burger < len(BURGER_RANKS):
                                        multiplier = BURGER_RANKS[legendary_burger]["multiplier"]
                                    
                                    final_gain = int(total_gain * multiplier)
                                    new_number = current_number + final_gain
                                    
                                    update_user_data(
                                        chat_id, user_id,
                                        number=new_number,
                                        last_passive_income=current_time
                                    )
                                    
                                    print(f"💰 {user_name} получил {final_gain}кг от предметов: {', '.join(gained_items)}")
                            else:
                                update_user_data(chat_id, user_id, last_passive_income=current_time)
                                
                        except Exception as e:
                            print(f"❌ Ошибка при начислении дохода для {user_id}: {e}")
                            
                except Exception as e:
                    print(f"❌ Ошибка при работе с чатом {chat_id}: {e}")
                    
        except Exception as e:
            print(f"❌ Ошибка в цикле пассивного дохода: {e}")
            
        await asyncio.sleep(24 * 60 * 60)

async def snatcher_loop():
    """Цикл проверки Снатчеров каждые 30 минут"""
    await asyncio.sleep(10)
    print("👾 Снатчер цикл запущен")
    
    while True:
        try:
            current_time = datetime.now()
            if current_time.minute % 30 == 0:
                print(f"👾 Проверка Снатчеров: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                
                for chat_id in list(active_chats):
                    try:
                        users = get_users_with_snatcher(chat_id)
                        
                        for user_id, user_name, item_counts_str, last_snatch in users:
                            try:
                                should_trigger = False
                                
                                if not last_snatch:
                                    update_user_data(chat_id, user_id, snatcher_last_time=current_time)
                                    continue
                                
                                if isinstance(last_snatch, str):
                                    last_time = datetime.fromisoformat(last_snatch)
                                else:
                                    last_time = last_snatch
                                
                                time_diff = current_time - last_time
                                if time_diff.total_seconds() >= 6 * 3600:
                                    should_trigger = True
                                
                                if should_trigger:
                                    await apply_snatcher_effect(chat_id, user_id, user_name)
                                    
                            except Exception as e:
                                print(f"❌ Ошибка при обработке Снатчера для {user_id}: {e}")
                                
                    except Exception as e:
                        print(f"❌ Ошибка при работе с чатом {chat_id}: {e}")
                        
        except Exception as e:
            print(f"❌ Ошибка в цикле Снатчера: {e}")
            
        await asyncio.sleep(1800)

async def hourly_effects_loop():
    """Применяет эффекты предметов, которые работают каждый час"""
    await asyncio.sleep(10)
    print("💊 Почасовые эффекты цикл запущен")
    
    while True:
        try:
            current_time = datetime.now()
            print(f"💊 Проверка почасовых эффектов: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            for chat_id in list(active_chats):
                try:
                    users = get_users_with_hourly_items(chat_id)
                    
                    for user_id, user_name, current_number, item_counts_str, legendary_burger, last_hourly in users:
                        try:
                            should_pay = False
                            
                            if not last_hourly:
                                update_user_data(chat_id, user_id, last_hourly_income=current_time)
                                continue
                            
                            if isinstance(last_hourly, str):
                                last_time = datetime.fromisoformat(last_hourly)
                            else:
                                last_time = last_hourly
                            
                            time_diff = current_time - last_time
                            if time_diff.total_seconds() >= 3600:
                                should_pay = True
                            
                            if should_pay:
                                items_dict = get_user_items(item_counts_str)
                                if not items_dict:
                                    continue
                                
                                total_gain = 0
                                gained_items = []
                                
                                for item_name, count in items_dict.items():
                                    if item_name == "Автохолестерол":
                                        gain = random.randint(1, 10) * count
                                        total_gain += gain
                                        gained_items.append(f"Автохолестерол x{count} (+{gain}кг)")
                                    elif item_name == "Холестеринимус":
                                        gain = random.randint(1, 5) * count
                                        total_gain += gain
                                        gained_items.append(f"Холестеринимус x{count} (+{gain}кг)")
                                
                                if total_gain > 0:
                                    multiplier = 1.0
                                    if legendary_burger >= 0 and legendary_burger < len(BURGER_RANKS):
                                        multiplier = BURGER_RANKS[legendary_burger]["multiplier"]
                                    
                                    final_gain = int(total_gain * multiplier)
                                    new_number = current_number + final_gain
                                    
                                    update_user_data(
                                        chat_id, user_id,
                                        number=new_number,
                                        last_hourly_income=current_time
                                    )
                                    
                                    print(f"💊 {user_name} получил {final_gain}кг от почасовых предметов: {', '.join(gained_items)}")
                            else:
                                update_user_data(chat_id, user_id, last_hourly_income=current_time)
                                
                        except Exception as e:
                            print(f"❌ Ошибка при начислении почасового дохода для {user_id}: {e}")
                            
                except Exception as e:
                    print(f"❌ Ошибка при работе с чатом {chat_id}: {e}")
                    
        except Exception as e:
            print(f"❌ Ошибка в цикле почасовых эффектов: {e}")
            
        await asyncio.sleep(3600)

# ===== УНИВЕРСАЛЬНЫЙ ОБРАБОТЧИК КОМАНД =====
COMMAND_MAP = {
    # русские команды
    'start': 'cmd_start',
    'help': 'cmd_start',
    'жир': 'cmd_fat',
    'жиркейс': 'cmd_fat_case',
    'жиркейс_шансы': 'cmd_fat_case_chances',
    'жиротрясы': 'cmd_fat_leaderboard',
    'жирстат': 'cmd_fat_stats',
    'жиринфо': 'cmd_fat_info',
    'жирзвания': 'cmd_show_ranks',
    'жиркулдаун': 'cmd_cooldown_info',
    'инвентарь': 'cmd_show_inventory',
    'магазин': 'cmd_shop',
    'купить': 'cmd_buy',
    'датьжир': 'cmd_give_fat',
    'возвышение': 'cmd_ascension',
    'апгрейд': 'cmd_upgrade',
    'апгрейдкг': 'cmd_upgrade_kg',
    'выбрать': 'cmd_choose',
    'автобургер': 'cmd_give_autoburger',
    'автобургер_сброс': 'cmd_reset_autoburger',
    'автобургер_инфо': 'cmd_autoburger_info',
    'выдатьпредмет': 'cmd_give_shop_item',
    'сброскд': 'cmd_reset_cooldowns',
    'сбросвсех': 'cmd_reset_all_users',
    'жир_сброс': 'cmd_fat_reset',
    'дуэль': 'cmd_duel',
    'отмена': 'cmd_cancel_duel',
    'датьпредмет': 'cmd_give_item',
    
    # латинские команды (для меню BotFather)
    'fat': 'cmd_fat',
    'fatcase': 'cmd_fat_case',
    'fatcase_chances': 'cmd_fat_case_chances',
    'fattys': 'cmd_fat_leaderboard',
    'fatstat': 'cmd_fat_stats',
    'fatinfo': 'cmd_fat_info',
    'ranks': 'cmd_show_ranks',
    'cooldowns': 'cmd_cooldown_info',
    'inventory': 'cmd_show_inventory',
    'shop': 'cmd_shop',
    'buy': 'cmd_buy',
    'givefat': 'cmd_give_fat',
    'ascend': 'cmd_ascension',
    'upgrade': 'cmd_upgrade',
    'upgradekg': 'cmd_upgrade_kg',
    'choose': 'cmd_choose',
    'autoburger': 'cmd_give_autoburger',
    'autoburger_reset': 'cmd_reset_autoburger',
    'autoburger_info': 'cmd_autoburger_info',
    'spawnitem': 'cmd_give_shop_item',
    'resetcd': 'cmd_reset_cooldowns',
    'resetall': 'cmd_reset_all_users',
    'fatreset': 'cmd_fat_reset',
    'duel': 'cmd_duel',
    'cancel': 'cmd_cancel_duel',
    'giveitems': 'cmd_give_item',
}

async def force_update_commands():
    """Принудительно обновляет список команд для всех чатов"""
    
    # Полный список команд (латиница для меню, но бот понимает и русские)
    commands = [
        BotCommand(command="fat", description="набор массы"),
        BotCommand(command="fatcase", description="Открыть доступный кейс"),
        BotCommand(command="fatcase_chances", description="Шансы в ежедневном кейсе"),
        BotCommand(command="fattys", description="Лидерборд для чата"),
        BotCommand(command="fatstat", description="Статистика Автобургеров"),
        BotCommand(command="fatinfo", description="Информация"),
        BotCommand(command="ranks", description="Звания"),
        BotCommand(command="cooldowns", description="Информация о кулдаунах"),
        BotCommand(command="inventory", description="Показывает инвентарь"),
        BotCommand(command="shop", description="Магазин"),
        BotCommand(command="buy", description="Купить предмет"),
        BotCommand(command="givefat", description="Передать кг"),
        BotCommand(command="ascend", description="Возвышение"),
        BotCommand(command="upgrade", description="Улучшить предмет"),
        BotCommand(command="upgradekg", description="Улучшить кг"),
        BotCommand(command="choose", description="Выбрать цель улучшения"),
        BotCommand(command="duel", description="Дуэль"),
        BotCommand(command="giveitems", description="Передать предмет"),
        BotCommand(command="start", description="Запустить бота"),
        BotCommand(command="help", description="Помощь"),
    ]
    
    # Устанавливаем для всех чатов
    await bot.set_my_commands(commands, scope=BotCommandScopeDefault())
    
    # Также устанавливаем для конкретных чатов, где уже был бот
    for chat_id in active_chats:
        try:
            await bot.set_my_commands(
                commands, 
                scope=BotCommandScopeChat(chat_id=chat_id)
            )
        except:
            pass
    
    print("✅ Список команд принудительно обновлен")

@dp.message()
async def universal_handler(message: types.Message):
    # Проверяем, что это команда
    if not message.text or not message.text.startswith('/'):
        return
    
    # Получаем username бота (один раз и кэшируем)
    if not hasattr(universal_handler, "bot_username"):
        bot_info = await bot.me()
        universal_handler.bot_username = bot_info.username.lower()
        print(f"✅ Бот username: @{universal_handler.bot_username}")
    
    # Полный текст команды
    full_text = message.text[1:].strip()
    parts = full_text.split()
    
    # Первая часть - это команда (может быть с @)
    raw_command = parts[0].lower()
    
    # Обрабатываем все возможные форматы команд
    if '@' in raw_command:
        cmd_parts = raw_command.split('@')
        command = cmd_parts[0]
        mentioned_bot = cmd_parts[1].lower()
        
        if mentioned_bot != universal_handler.bot_username:
            print(f"Игнорирую команду для @{mentioned_bot}")
            return
        
        print(f"Команда с @: {raw_command} -> {command}")
    else:
        command = raw_command
    
    # Аргументы команды (если есть)
    args = parts[1:] if len(parts) > 1 else []
    
    print(f"🔍 Получено: '{message.text}'")
    print(f"   → Команда: '{command}', Аргументы: {args}")
    
    # Маппинг латинских команд на русские
    latin_to_russian = {
        'fat': 'жир', 'fatcase': 'жиркейс', 'fatcase_chances': 'жиркейс_шансы',
        'fattys': 'жиротрясы', 'fatstat': 'жирстат', 'fatinfo': 'жиринфо',
        'ranks': 'жирзвания', 'cooldowns': 'жиркулдаун', 'inventory': 'инвентарь',
        'shop': 'магазин', 'buy': 'купить', 'givefat': 'датьжир',
        'ascend': 'возвышение', 'upgrade': 'апгрейд', 'upgradekg': 'апгрейдкг',
        'choose': 'выбрать', 'duel': 'дуэль', 'giveitems': 'датьпредмет',
        'start': 'start', 'help': 'help'
    }
    
    if command in latin_to_russian:
        original_command = command
        command = latin_to_russian[command]
        print(f"   🔄 Конвертация: {original_command} -> {command}")
    
    if command in COMMAND_MAP:
        func_name = COMMAND_MAP[command]
        func = globals().get(func_name)
        
        if func:
            register_chat(message.chat.id)
            
            # Подготавливаем "чистый" текст команды
            if args:
                clean_text = f"/{command} " + " ".join(args)
            else:
                clean_text = f"/{command}"
            
            # Сохраняем оригинальное значение
            original_text = message.text
            
            try:
                # Меняем текст через object.__setattr__ (обходим защиту Pydantic)
                object.__setattr__(message, '_text', clean_text)
                object.__setattr__(message, 'text', clean_text)
                
                print(f"   ✅ Выполняю: {func_name} с текстом '{clean_text}'")
                await func(message)
            except Exception as e:
                print(f"   ❌ Ошибка: {e}")
                await message.reply(f"❌ Ошибка при выполнении команды: {e}")
            finally:
                # Восстанавливаем оригинал
                object.__setattr__(message, '_text', original_text)
                object.__setattr__(message, 'text', original_text)
        else:
            await message.reply(f"❌ Ошибка: функция {func_name} не найдена")
    else:
        # Неизвестная команда - игнорируем
        print(f"❓ Неизвестная команда: '{command}'")

# ===== ЗАПУСК БОТА =====
async def on_startup(dp):
    print("\n" + "="*60)
    print("✅ TELEGRAM БОТ ЗАПУЩЕН")
    print(f"📅 Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    os.makedirs(DB_FOLDER, exist_ok=True)

    await force_update_commands()
    asyncio.create_task(autoburger_loop())
    asyncio.create_task(passive_income_loop())
    asyncio.create_task(snatcher_loop())
    asyncio.create_task(hourly_effects_loop())

async def main():
    await on_startup(dp)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
