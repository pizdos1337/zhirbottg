import asyncio
import logging
import random
import sqlite3
import os
import json
import math
import shutil
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, types, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    BotCommand, BotCommandScopeDefault
)
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage

# ===== НАСТРОЙКИ =====
TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
DB_FOLDER = "/app/data/telegram_databases"
COOLDOWN_HOURS = 1
CASE_COOLDOWN_HOURS = 24
TESTER_IDS = [1776742823]

# Вероятности
BASE_MINUS_CHANCE = 0.2
MAX_MINUS_CHANCE = 0.6
PITY_INCREMENT = 0.1
CONSECUTIVE_MINUS_BOOST = 0.2
MAX_CONSECUTIVE_MINUS_BOOST = 0.8

# Джекпот
BASE_JACKPOT_CHANCE = 0.001
JACKPOT_PITY_INCREMENT = 0.001
MAX_JACKPOT_CHANCE = 0.05
JACKPOT_MIN = 500
JACKPOT_MAX = 1000

# Авто-жир
AUTO_FAT_INTERVALS = {1: 6, 2: 3, 3: 1, 4: 0.5, 5: 0.25, 6: 0.1}
AUTO_FAT_BASE_COST = 500
AUTO_FAT_COST_INCREMENT = 500
AUTO_FAT_MAX_LEVEL = 6

# Престиж
PRESTIGE_BONUS_PER_LEVEL = 0.10
PRESTIGE_LUCK_PER_LEVEL = 0.01
PRESTIGE_XP_BONUS_PER_LEVEL = 0.5
PRESTIGE_BASE_COST = 2000
PRESTIGE_COST_INCREMENT = 1000

# Прибавка
INCOME_BONUS_PER_LEVEL = 0.05
INCOME_BASE_COST = 250
INCOME_COST_INCREMENT = 100

# Удача
LUCK_CASE_BONUS_PER_LEVEL = 0.25
LUCK_UPGRADE_BONUS_PER_LEVEL = 0.5
LUCK_BASE_COST = 1000
LUCK_COST_INCREMENT = 500

# КД
FAT_CD_REDUCTION_PER_LEVEL = 5
FAT_CD_BASE_COST = 150
FAT_CD_COST_INCREMENT = 50
CASE_CD_REDUCTION_PER_LEVEL = 60
CASE_CD_BASE_COST = 100
CASE_CD_COST_INCREMENT = 100

# Опыт
XP_PER_FAT = 30
XP_PER_UPGRADE = 50
XP_PER_UPGRADE_KG = 40
XP_PER_CASE = 100
XP_PER_DUEL_WIN = 100
XP_PER_SHOP_BUY = 20
LEVEL_UP_REWARD_PER_LEVEL = 15

# Магазин
SHOP_SLOTS = 10
SHOP_UPDATE_HOURS = 12

# Призы кейса
CASE_PRIZES = [
    {"value": 0, "chance": 21.0, "emoji": "🔄", "name": "Ничего"},
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
    {"value": 10000, "chance": 0.5, "emoji": "💥", "name": "+10000 кг"},
]

total_chance = sum(p["chance"] for p in CASE_PRIZES)
for p in CASE_PRIZES:
    p["normalized_chance"] = (p["chance"] / total_chance) * 100

# Кейсы
CASES = {
    "daily": {"name": "Жиркейс", "emoji": "📦", "tradable": False, "daily": True, "prizes": CASE_PRIZES},
    "chicken": {"name": "Коробка от чикенбургера", "emoji": "🍗", "tradable": True, "daily": False, "shop_chance": 0.3, "min_shop": 1, "max_shop": 3, "price": 10, "prizes": [{"value": -10, "chance": 20, "emoji": "📉"}, {"value": 0, "chance": 30, "emoji": "🔄"}, {"value": 10, "chance": 20, "emoji": "📈"}, {"value": 15, "chance": 10, "emoji": "📈"}, {"value": 20, "chance": 10, "emoji": "⬆️"}, {"value": 25, "chance": 10, "emoji": "⬆️"}]},
    "bigmac": {"name": "Коробка от Биг Мака", "emoji": "🍔", "tradable": True, "daily": False, "shop_chance": 0.25, "min_shop": 1, "max_shop": 3, "price": 15, "prizes": [{"value": -15, "chance": 20, "emoji": "📉"}, {"value": 0, "chance": 30, "emoji": "🔄"}, {"value": 15, "chance": 20, "emoji": "📈"}, {"value": 20, "chance": 10, "emoji": "⬆️"}, {"value": 25, "chance": 10, "emoji": "⬆️"}, {"value": 30, "chance": 10, "emoji": "🚀"}]},
    "whopper": {"name": "Коробка от Воппера", "emoji": "🔥", "tradable": True, "daily": False, "shop_chance": 0.23, "min_shop": 1, "max_shop": 3, "price": 25, "prizes": [{"value": -25, "chance": 20, "emoji": "📉"}, {"value": 0, "chance": 30, "emoji": "🔄"}, {"value": 25, "chance": 20, "emoji": "📈"}, {"value": 30, "chance": 10, "emoji": "🚀"}, {"value": 40, "chance": 9, "emoji": "🚀"}, {"value": 50, "chance": 1, "emoji": "💫"}]},
    "green_whopper": {"name": "Коробка от Зеленого Воппера", "emoji": "💚", "tradable": True, "daily": False, "shop_chance": 0.17, "min_shop": 1, "max_shop": 2, "price": 50, "prizes": [{"value": -25, "chance": 20, "emoji": "📉"}, {"value": 0, "chance": 10, "emoji": "🔄"}, {"value": 10, "chance": 20, "emoji": "📈"}, {"value": 30, "chance": 10, "emoji": "🚀"}, {"value": 50, "chance": 10, "emoji": "💫"}, {"value": 100, "chance": 9, "emoji": "⭐"}, {"value": 250, "chance": 1, "emoji": "💥"}]},
    "burger_pizza": {"name": "Коробка от Бургер пиццы", "emoji": "🍕", "tradable": True, "daily": False, "shop_chance": 0.15, "min_shop": 1, "max_shop": 2, "price": 100, "prizes": [{"value": -10, "chance": 20, "emoji": "📉"}, {"value": 0, "chance": 10, "emoji": "🔄"}, {"value": 30, "chance": 20, "emoji": "🚀"}, {"value": 50, "chance": 30, "emoji": "💫"}, {"value": 100, "chance": 5, "emoji": "⭐"}, {"value": 250, "chance": 5, "emoji": "⭐"}, {"value": 500, "chance": 4, "emoji": "💥"}, {"value": 1000, "chance": 1, "emoji": "💥"}]},
    "mcguffin": {"name": "Коробка от МакГаффина", "emoji": "🎁", "tradable": True, "daily": False, "shop_chance": 0.1, "min_shop": 1, "max_shop": 1, "price": 200, "prizes": [{"value": 100, "chance": 80, "emoji": "⭐"}, {"value": 200, "chance": 5, "emoji": "💥"}, {"value": 250, "chance": 5, "emoji": "💥"}, {"value": 500, "chance": 5, "emoji": "💥"}, {"value": 750, "chance": 1, "emoji": "✨"}, {"value": 1000, "chance": 1, "emoji": "✨"}, {"value": 1200, "chance": 1, "emoji": "✨"}, {"value": 1500, "chance": 1, "emoji": "✨"}]},
    "rotten_pack": {"name": "Упаковка Гнилой Ножки KFC", "emoji": "💀📦", "tradable": True, "daily": False, "shop_chance": 0.1, "min_shop": 1, "max_shop": 10, "price": 100, "prizes": [{"value": 0, "chance": 90, "emoji": "🔄"}, {"value": "rotten_leg", "chance": 10, "emoji": "💀"}]},
    "water_pack": {"name": "Упаковка Стакана Воды", "emoji": "💧📦", "tradable": True, "daily": False, "shop_chance": 0.1, "min_shop": 1, "max_shop": 10, "price": 100, "prizes": [{"value": 0, "chance": 90, "emoji": "🔄"}, {"value": "water", "chance": 10, "emoji": "💧"}]}
}

# Магазин
SHOP_ITEMS = [
    {"name": "Горелый бекон", "chance": 1.0, "min_amount": 3, "max_amount": 20, "price": 20, "gain_per_24h": 1, "description": "🏭 Даёт +1 кг каждые 24 часа"},
    {"name": "Горелый бутерброд", "chance": 0.4, "min_amount": 1, "max_amount": 5, "price": 70, "gain_per_24h": 3, "description": "🥪 Даёт +3 кг каждые 24 часа"},
    {"name": "Горелый додстер", "chance": 0.4, "min_amount": 1, "max_amount": 3, "price": 100, "gain_per_24h": 5, "description": "🌯 Даёт +5 кг каждые 24 часа"},
    {"name": "Тарелка макарон", "chance": 0.3, "min_amount": 1, "max_amount": 2, "price": 200, "gain_per_24h": 10, "description": "🍝 Даёт +10 кг каждые 24 часа"},
    {"name": "Тарелка хинкалей", "chance": 0.2, "min_amount": 1, "max_amount": 2, "price": 300, "gain_per_24h": 15, "description": "🥟 Даёт +15 кг каждые 24 часа"},
    {"name": "Бургер", "chance": 0.15, "min_amount": 1, "max_amount": 2, "price": 400, "gain_per_24h": 20, "description": "🍔 Даёт +20 кг каждые 24 часа"},
    {"name": "Пицца", "chance": 0.1, "min_amount": 1, "max_amount": 2, "price": 500, "gain_per_24h": 30, "description": "🍕 Даёт +30 кг каждые 24 часа"},
    {"name": "Ведро KFC", "chance": 0.08, "min_amount": 1, "max_amount": 2, "price": 800, "gain_per_24h": 50, "description": "🍗 Даёт +50 кг каждые 24 часа"},
    {"name": "Комбо за 1000!", "chance": 0.06, "min_amount": 1, "max_amount": 2, "price": 1000, "gain_per_24h": 100, "description": "🍱 Даёт +100 кг каждые 24 часа"},
    {"name": "Бездонное ведро KFC", "chance": 0.04, "min_amount": 1, "max_amount": 1, "price": 1500, "gain_per_24h": 150, "description": "🪣 Даёт +150 кг каждые 24 часа"},
    {"name": "Бездонная пачка чипсов", "chance": 0.03, "min_amount": 1, "max_amount": 1, "price": 3000, "gain_per_24h": 250, "description": "🥨 Даёт +250 кг каждые 24 часа"},
    {"name": "Пожизненный запас чикенбургеров", "chance": 0.02, "min_amount": 1, "max_amount": 1, "price": 5000, "gain_per_24h": 500, "description": "🍔🍔🍔 Даёт +500 кг каждые 24 часа"},
    {"name": "Автоматическая система подачи холестерина", "chance": 0.01, "min_amount": 1, "max_amount": 1, "price": 7000, "gain_per_24h": 1000, "description": "⚙️💉 Даёт +1000 кг каждые 24 часа"},
    {"name": "Святой сэндвич", "chance": 0.005, "min_amount": 1, "max_amount": 1, "price": 10000, "gain_per_24h": 0, "description": "✨ **ЛЕГЕНДАРНО** ✨"},
    {"name": "Гнилая ножка KFC", "chance": 0.005, "min_amount": 1, "max_amount": 5, "price": 1, "gain_per_24h": 0, "description": "💀 **ПРОКЛЯТО** 💀"},
    {"name": "Стакан воды", "chance": 0.005, "min_amount": 1, "max_amount": 5, "price": 1, "gain_per_24h": 0, "description": "💧 **ОЧИЩЕНИЕ** 💧"},
    {"name": "Автохолестерол", "chance": 0.05, "min_amount": 1, "max_amount": 1, "price": 1000, "gain_per_24h": 0, "description": "💊 Даёт от 1кг до 10кг в час"},
    {"name": "Холестеринимус", "chance": 0.05, "min_amount": 1, "max_amount": 1, "price": 500, "gain_per_24h": 0, "description": "💊 Даёт от 1кг до 5кг в час"},
    {"name": "Яблоко", "chance": 0.05, "min_amount": 1, "max_amount": 1, "price": 500, "gain_per_24h": 0, "description": "🍎 Уменьшает кулдаун /жир на 5% за штуку"},
    {"name": "Апельсин", "chance": 0.05, "min_amount": 1, "max_amount": 1, "price": 750, "gain_per_24h": 0, "description": "🍊 Уменьшает кулдаун /жиркейс на 5% за штуку"},
    {"name": "Золотое Яблоко", "chance": 0.01, "min_amount": 1, "max_amount": 1, "price": 1000, "gain_per_24h": 0, "description": "🍎✨ Уменьшает кулдаун /жир на 10% за штуку"},
    {"name": "Золотой Апельсин", "chance": 0.01, "min_amount": 1, "max_amount": 1, "price": 1000, "gain_per_24h": 0, "description": "🍊✨ Уменьшает кулдаун /жиркейс на 10% за штуку"},
    {"name": "Драгонфрукт", "chance": 0.01, "min_amount": 1, "max_amount": 1, "price": 1000, "gain_per_24h": 0, "description": "🐉🍈 Повышает шанс джекпота на 1% за штуку"},
    {"name": "Золотой Драгонфрукт", "chance": 0.005, "min_amount": 1, "max_amount": 1, "price": 3000, "gain_per_24h": 0, "description": "🐉🍈✨ Повышает шанс джекпота на 5% за штуку"},
    {"name": "Снатчер", "chance": 0.001, "min_amount": 1, "max_amount": 1, "price": 2000, "gain_per_24h": 0, "description": "👾 **СНАТЧЕР** 👾"},
]

ITEM_EMOJIS = {item["name"]: "📦" for item in SHOP_ITEMS}
ITEM_EMOJIS.update({"Снатчер": "👾", "Святой сэндвич": "✨", "Гнилая ножка KFC": "💀", "Стакан воды": "💧", "Автохолестерол": "💊", "Холестеринимус": "💊", "Яблоко": "🍎", "Апельсин": "🍊", "Золотое Яблоко": "🍎✨", "Золотой Апельсин": "🍊✨", "Драгонфрукт": "🐉🍈", "Золотой Драгонфрукт": "🐉🍈✨"})

CASES["shop_case"] = {"name": "Магазинный кейс", "emoji": "🏪", "tradable": True, "daily": False, "shop_chance": 0.2, "min_shop": 1, "max_shop": 5, "price": 100, "prizes": []}
shop_case_prizes = [{"value": item["name"], "chance": item["chance"] * 100, "emoji": ITEM_EMOJIS.get(item["name"], "🎁"), "name": item["name"]} for item in SHOP_ITEMS]
total = sum(p["chance"] for p in shop_case_prizes)
if total < 100:
    shop_case_prizes.append({"value": 0, "chance": 100 - total, "emoji": "🔄", "name": "Ничего"})
else:
    for p in shop_case_prizes:
        p["chance"] = (p["chance"] / total) * 100
CASES["shop_case"]["prizes"] = shop_case_prizes

LEGENDARY_UPGRADE_PRICES = {
    "Святой сэндвич": 20000, "Гнилая ножка KFC": 5000, "Стакан воды": 3000,
    "Автохолестерол": 5000, "Холестеринимус": 2500, "Яблоко": 1500,
    "Золотое Яблоко": 3000, "Апельсин": 2000, "Золотой Апельсин": 4000,
    "Драгонфрукт": 4000, "Золотой Драгонфрукт": 8000, "Снатчер": 20000
}

# Звания
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
    for r in RANKS:
        if r["min"] <= weight <= r["max"]:
            return r["name"], r["emoji"]
    if weight > 99999999:
        return "🌀 Бесконечность", "🌀"
    if weight < -999:
        return "Черная дыра", "💀"
    return "❓ Неопределённый", "❓"

print("="*60)
print("🍔 ЖИРНЫЙ ТЕЛЕГРАМ БОТ - ЗАПУСК")
print("="*60)

if TOKEN is None:
    print("❌ КРИТИЧЕСКАЯ ОШИБКА: Не найден TELEGRAM_BOT_TOKEN!")
    exit(1)

logging.basicConfig(level=logging.INFO)
storage = MemoryStorage()
bot = Bot(token=TOKEN)
dp = Dispatcher(storage=storage)

# ===== ФУНКЦИИ XP =====
def get_xp_for_next_level(level):
    return (50 * (level + 1)) + ((level + 1) * 5)

def get_level_and_xp(total_xp):
    level = 0
    remaining = total_xp
    while True:
        needed = get_xp_for_next_level(level)
        if remaining < needed:
            break
        remaining -= needed
        level += 1
    return level, remaining

def add_xp(chat_id, user_id, xp_amount):
    data = get_user_data(chat_id, user_id)
    old_level = data.get('user_level', 0)
    prestige = data.get('prestige', 0)
    xp_bonus = 1 + (prestige * PRESTIGE_XP_BONUS_PER_LEVEL)
    xp_amount = int(xp_amount * xp_bonus)
    new_total = data.get('user_xp', 0) + xp_amount
    new_level, current_xp = get_level_and_xp(new_total)
    total_reward = 0
    for lvl in range(old_level + 1, new_level + 1):
        total_reward += LEVEL_UP_REWARD_PER_LEVEL * lvl
    new_weight = data['current_number'] + total_reward
    update_user_data(chat_id, user_id, user_xp=new_total, user_level=new_level, number=new_weight)
    return new_level - old_level, total_reward, new_level

def get_prestige_bonus(prestige):
    return 1 + (prestige * PRESTIGE_BONUS_PER_LEVEL)

def get_prestige_luck(prestige):
    return prestige * PRESTIGE_LUCK_PER_LEVEL

def get_income_bonus(income_upgrade):
    return 1 + (income_upgrade * INCOME_BONUS_PER_LEVEL)

def get_fat_cd_reduction(upgrade_count):
    return upgrade_count * FAT_CD_REDUCTION_PER_LEVEL

def get_case_cd_reduction(upgrade_count):
    return upgrade_count * CASE_CD_REDUCTION_PER_LEVEL

def get_auto_fat_interval(auto_fat_level):
    if auto_fat_level <= 0:
        return None
    return AUTO_FAT_INTERVALS.get(auto_fat_level, 1)

def get_upgrade_cost(upgrade_type, current_level):
    if upgrade_type == "fat_cd":
        return FAT_CD_BASE_COST + (current_level * FAT_CD_COST_INCREMENT)
    elif upgrade_type == "case_cd":
        return CASE_CD_BASE_COST + (current_level * CASE_CD_COST_INCREMENT)
    elif upgrade_type == "luck":
        return LUCK_BASE_COST + (current_level * LUCK_COST_INCREMENT)
    elif upgrade_type == "income":
        return INCOME_BASE_COST + (current_level * INCOME_COST_INCREMENT)
    elif upgrade_type == "prestige":
        return PRESTIGE_BASE_COST + (current_level * PRESTIGE_COST_INCREMENT)
    elif upgrade_type == "auto_fat":
        return AUTO_FAT_BASE_COST + (current_level * AUTO_FAT_COST_INCREMENT)
    return 0

def is_tester(user_id):
    return user_id in TESTER_IDS

# ===== БАЗА ДАННЫХ =====
def get_db_path(chat_id):
    return os.path.join(DB_FOLDER, f"chat_{chat_id}.db")

def add_missing_columns(db_path, existing_columns):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    required = {
        'item_counts': "TEXT DEFAULT '{}'", 'last_command': "TEXT", 'last_command_target': "TEXT",
        'last_command_use_time': "TIMESTAMP", 'fat_cooldown_time': "TIMESTAMP", 'active_case_message_id': "TEXT",
        'daily_case_last_time': "TIMESTAMP", 'snatcher_last_time': "TIMESTAMP", 'duel_active': "INTEGER DEFAULT 0",
        'duel_opponent': "TEXT", 'duel_amount': "INTEGER DEFAULT 0", 'duel_message_id': "TEXT",
        'duel_initiator': "INTEGER DEFAULT 0", 'last_case_type': "TEXT", 'last_case_prize': "TEXT",
        'upgrade_active': "INTEGER DEFAULT 0", 'upgrade_data': "TEXT", 'duel_start_time': "TIMESTAMP",
        'shadow_upgrade_chance': "INTEGER DEFAULT 0", 'user_xp': "INTEGER DEFAULT 0", 'user_level': "INTEGER DEFAULT 0",
        'fat_cd_upgrade': "INTEGER DEFAULT 0", 'case_cd_upgrade': "INTEGER DEFAULT 0", 'luck_upgrade': "INTEGER DEFAULT 0",
        'income_upgrade': "INTEGER DEFAULT 0", 'prestige': "INTEGER DEFAULT 0", 'auto_fat_level': "INTEGER DEFAULT 0",
        'next_auto_fat_time': "TIMESTAMP", 'animations_enabled': "INTEGER DEFAULT 1", 'last_passive_income': "TIMESTAMP",
        'last_hourly_income': "TIMESTAMP"
    }
    for col, typ in required.items():
        if col not in existing_columns:
            try:
                cursor.execute(f"ALTER TABLE user_fat ADD COLUMN {col} {typ}")
            except:
                pass
    for case_id in CASES.keys():
        if case_id != "daily":
            col = f"case_{case_id}_count"
            if col not in existing_columns:
                try:
                    cursor.execute(f"ALTER TABLE user_fat ADD COLUMN {col} INTEGER DEFAULT 0")
                except:
                    pass
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='shop'")
    if not cursor.fetchone():
        cursor.execute('''CREATE TABLE shop (chat_id TEXT PRIMARY KEY, slots TEXT, last_update TIMESTAMP, next_update TIMESTAMP)''')
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
                return create_new_database(db_path, chat_id, chat_name)
            cursor.execute("PRAGMA table_info(user_fat)")
            cols = [c[1] for c in cursor.fetchall()]
            conn.close()
            add_missing_columns(db_path, cols)
            return True
        except sqlite3.DatabaseError:
            backup_path = db_path + f".corrupted_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            shutil.copy2(db_path, backup_path)
            os.remove(db_path)
            return create_new_database(db_path, chat_id, chat_name)
    else:
        return create_new_database(db_path, chat_id, chat_name)

def create_new_database(db_path, chat_id, chat_name):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE user_fat (
        user_id TEXT PRIMARY KEY, user_name TEXT, current_number INTEGER DEFAULT 0, last_command_time TIMESTAMP,
        consecutive_plus INTEGER DEFAULT 0, consecutive_minus INTEGER DEFAULT 0, jackpot_pity INTEGER DEFAULT 0,
        last_case_time TIMESTAMP, item_counts TEXT DEFAULT '{}', last_command TEXT, last_command_target TEXT,
        last_command_use_time TIMESTAMP, fat_cooldown_time TIMESTAMP, active_case_message_id TEXT,
        daily_case_last_time TIMESTAMP, snatcher_last_time TIMESTAMP, duel_active INTEGER DEFAULT 0,
        duel_opponent TEXT, duel_amount INTEGER DEFAULT 0, duel_message_id TEXT, duel_initiator INTEGER DEFAULT 0,
        last_case_type TEXT, last_case_prize TEXT, upgrade_active INTEGER DEFAULT 0, upgrade_data TEXT,
        duel_start_time TIMESTAMP, shadow_upgrade_chance INTEGER DEFAULT 0, user_xp INTEGER DEFAULT 0,
        user_level INTEGER DEFAULT 0, fat_cd_upgrade INTEGER DEFAULT 0, case_cd_upgrade INTEGER DEFAULT 0,
        luck_upgrade INTEGER DEFAULT 0, income_upgrade INTEGER DEFAULT 0, prestige INTEGER DEFAULT 0,
        auto_fat_level INTEGER DEFAULT 0, next_auto_fat_time TIMESTAMP, animations_enabled INTEGER DEFAULT 1,
        last_passive_income TIMESTAMP, last_hourly_income TIMESTAMP)''')
    for case_id in CASES.keys():
        if case_id != "daily":
            try:
                cursor.execute(f"ALTER TABLE user_fat ADD COLUMN case_{case_id}_count INTEGER DEFAULT 0")
            except:
                pass
    cursor.execute('''CREATE TABLE IF NOT EXISTS shop (chat_id TEXT PRIMARY KEY, slots TEXT, last_update TIMESTAMP, next_update TIMESTAMP)''')
    conn.commit()
    conn.close()
    return True

def get_user_data(chat_id, user_id, user_name=None):
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(user_fat)")
    all_cols = [c[1] for c in cursor.fetchall()]
    select_cols = [c for c in ['user_id', 'user_name', 'current_number', 'last_command_time', 'consecutive_plus', 'consecutive_minus', 'jackpot_pity', 'last_case_time', 'item_counts', 'last_command', 'last_command_target', 'last_command_use_time', 'fat_cooldown_time', 'active_case_message_id', 'daily_case_last_time', 'snatcher_last_time', 'duel_active', 'duel_opponent', 'duel_amount', 'duel_message_id', 'duel_initiator', 'last_case_type', 'last_case_prize', 'upgrade_active', 'upgrade_data', 'duel_start_time', 'shadow_upgrade_chance', 'user_xp', 'user_level', 'fat_cd_upgrade', 'case_cd_upgrade', 'luck_upgrade', 'income_upgrade', 'prestige', 'auto_fat_level', 'next_auto_fat_time', 'animations_enabled', 'last_passive_income', 'last_hourly_income'] if c in all_cols]
    case_cols = [f"case_{cid}_count" for cid in CASES.keys() if cid != "daily" and f"case_{cid}_count" in all_cols]
    query = f"SELECT {', '.join(select_cols + case_cols)} FROM user_fat WHERE user_id = ?"
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
        user_data['cases_dict'] = cases_dict
        conn.close()
        return user_data
    else:
        user_data = {
            'user_id': str(user_id), 'user_name': user_name or "Unknown", 'current_number': 0, 'last_command_time': None,
            'consecutive_plus': 0, 'consecutive_minus': 0, 'jackpot_pity': 0, 'last_case_time': None, 'item_counts': '{}',
            'last_command': None, 'last_command_target': None, 'last_command_use_time': None, 'fat_cooldown_time': None,
            'active_case_message_id': None, 'daily_case_last_time': None, 'snatcher_last_time': None, 'duel_active': 0,
            'duel_opponent': None, 'duel_amount': 0, 'duel_message_id': None, 'duel_initiator': 0, 'last_case_type': None,
            'last_case_prize': None, 'upgrade_active': 0, 'upgrade_data': None, 'duel_start_time': None,
            'shadow_upgrade_chance': 0, 'user_xp': 0, 'user_level': 0, 'fat_cd_upgrade': 0, 'case_cd_upgrade': 0,
            'luck_upgrade': 0, 'income_upgrade': 0, 'prestige': 0, 'auto_fat_level': 0, 'next_auto_fat_time': None,
            'animations_enabled': 1, 'last_passive_income': None, 'last_hourly_income': None, 'cases_dict': {}
        }
        for case_id in CASES.keys():
            if case_id != "daily":
                user_data['cases_dict'][case_id] = 0
        cols = []
        vals = []
        base_fields = ['user_id', 'user_name', 'current_number', 'last_command_time', 'consecutive_plus', 'consecutive_minus', 'jackpot_pity', 'last_case_time', 'item_counts', 'last_command', 'last_command_target', 'last_command_use_time', 'fat_cooldown_time', 'active_case_message_id', 'daily_case_last_time', 'snatcher_last_time', 'duel_active', 'duel_opponent', 'duel_amount', 'duel_message_id', 'duel_initiator', 'last_case_type', 'last_case_prize', 'upgrade_active', 'upgrade_data', 'duel_start_time', 'shadow_upgrade_chance', 'user_xp', 'user_level', 'fat_cd_upgrade', 'case_cd_upgrade', 'luck_upgrade', 'income_upgrade', 'prestige', 'auto_fat_level', 'next_auto_fat_time', 'animations_enabled', 'last_passive_income', 'last_hourly_income']
        for f in base_fields:
            if f in all_cols:
                cols.append(f)
                vals.append(user_data.get(f))
        for case_id, cnt in user_data['cases_dict'].items():
            col = f"case_{case_id}_count"
            if col in all_cols:
                cols.append(col)
                vals.append(cnt)
        query = f"INSERT INTO user_fat ({', '.join(cols)}) VALUES ({', '.join(['?']*len(cols))})"
        cursor.execute(query, vals)
        conn.commit()
        conn.close()
        return user_data

def update_user_data(chat_id, user_id, **kwargs):
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(user_fat)")
    existing = [c[1] for c in cursor.fetchall()]
    updates = []
    vals = []
    for key, value in kwargs.items():
        if key == 'number':
            if 'current_number' in existing:
                updates.append("current_number = ?")
                vals.append(value)
        elif key == 'cases_dict' and isinstance(value, dict):
            for case_id, cnt in value.items():
                col = f"case_{case_id}_count"
                if col in existing:
                    updates.append(f"{col} = ?")
                    vals.append(cnt)
        elif key in existing:
            updates.append(f"{key} = ?")
            vals.append(value)
    if not updates:
        conn.close()
        return
    vals.append(str(user_id))
    cursor.execute(f"UPDATE user_fat SET {', '.join(updates)} WHERE user_id = ?", vals)
    conn.commit()
    conn.close()

def get_user_items(item_counts_str):
    try:
        return json.loads(item_counts_str) if item_counts_str and item_counts_str != '{}' else {}
    except:
        return {}

def save_user_items(items_dict):
    return json.dumps(items_dict)

def format_time(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    if hours > 0:
        return f"{hours} ч {minutes} мин"
    elif minutes > 0:
        return f"{minutes} мин {secs} сек"
    else:
        return f"{secs} сек"

def get_all_users_sorted(chat_id):
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(user_fat)")
    cols = [c[1] for c in cursor.fetchall()]
    select = ['user_name', 'current_number', 'last_command_time', 'consecutive_plus', 'consecutive_minus', 'jackpot_pity']
    if 'prestige' in cols:
        select.append('prestige')
    cursor.execute(f"SELECT {', '.join(select)} FROM user_fat ORDER BY current_number DESC")
    res = cursor.fetchall()
    conn.close()
    return res

def get_chat_stats(chat_id):
    users = get_all_users_sorted(chat_id)
    total_users = len(users)
    total_weight = sum(u[1] for u in users)
    avg = total_weight / total_users if total_users > 0 else 0
    pos = sum(1 for u in users if u[1] > 0)
    neg = sum(1 for u in users if u[1] < 0)
    zero = sum(1 for u in users if u[1] == 0)
    return {'total_users': total_users, 'total_weight': total_weight, 'avg_weight': avg, 'positive': pos, 'negative': neg, 'zero': zero}

def check_cooldown(last_time, cooldown_hours):
    if last_time is None:
        return True, 0
    try:
        if isinstance(last_time, str):
            last = datetime.fromisoformat(last_time)
        else:
            last = last_time
        diff = datetime.now() - last
        secs = cooldown_hours * 3600
        if diff.total_seconds() >= secs:
            return True, 0
        else:
            return False, secs - diff.total_seconds()
    except:
        return True, 0

def can_get_daily_case(chat_id, user_id, custom_cooldown=None):
    data = get_user_data(chat_id, user_id)
    last = data.get('daily_case_last_time')
    if not last:
        return True, 0
    if isinstance(last, str):
        last = datetime.fromisoformat(last)
    diff = datetime.now() - last
    cd = (custom_cooldown or CASE_COOLDOWN_HOURS) * 3600
    if diff.total_seconds() >= cd:
        return True, 0
    else:
        return False, cd - diff.total_seconds()

def update_daily_case_time(chat_id, user_id):
    update_user_data(chat_id, user_id, daily_case_last_time=datetime.now())

def are_animations_enabled(user_data):
    return user_data.get('animations_enabled', 1) == 1

# ===== ОСНОВНЫЕ ФУНКЦИИ =====
def get_change_with_pity_and_jackpot(consecutive_plus, consecutive_minus, jackpot_pity, luck_upgrade=0, prestige_bonus=1.0, items_dict=None, current_weight=None):
    if items_dict is None:
        items_dict = {}
    has_rotten = items_dict.get("Гнилая ножка KFC", 0) > 0
    has_holy = items_dict.get("Святой сэндвич", 0) > 0
    has_water = items_dict.get("Стакан воды", 0) > 0
    minus_boost = min(consecutive_minus * CONSECUTIVE_MINUS_BOOST, MAX_CONSECUTIVE_MINUS_BOOST)
    minus_chance = BASE_MINUS_CHANCE + (consecutive_plus * PITY_INCREMENT) - minus_boost
    minus_chance = max(0.1, min(minus_chance, MAX_MINUS_CHANCE))
    jackpot_chance = BASE_JACKPOT_CHANCE + (jackpot_pity * JACKPOT_PITY_INCREMENT)
    if has_holy:
        sandwich_count = items_dict.get("Святой сэндвич", 0)
        sandwich_bonus = 0.3 * sandwich_count
        jackpot_chance = max(jackpot_chance, sandwich_bonus)
        jackpot_chance = min(jackpot_chance, 0.9)
    else:
        jackpot_chance = min(jackpot_chance, MAX_JACKPOT_CHANCE)
    if has_water:
        if random.random() < jackpot_chance:
            change = random.randint(JACKPOT_MIN, JACKPOT_MAX) // 3
            change = int(change * prestige_bonus)
            return change, False, consecutive_plus + 1, 0, 0, True
        else:
            change = random.randint(1, 20) // 3
            change = int(change * prestige_bonus)
            return change, False, consecutive_plus + 1, 0, jackpot_pity + 1, False
    elif has_rotten:
        if random.random() < 0.6:
            if current_weight is not None:
                loss = int(current_weight * 0.5)
                change = -loss
            else:
                change = -int(consecutive_plus * 0.5)
            change = int(change * prestige_bonus)
            return change, True, 0, consecutive_minus + 1, jackpot_pity + 1, False
        else:
            change = random.randint(JACKPOT_MIN, JACKPOT_MAX)
            change = int(change * prestige_bonus)
            return change, False, consecutive_plus + 1, 0, 0, True
    else:
        if random.random() < jackpot_chance:
            change = random.randint(JACKPOT_MIN, JACKPOT_MAX)
            change = int(change * prestige_bonus)
            return change, False, consecutive_plus + 1, 0, 0, True
        if random.random() < minus_chance:
            change = random.randint(-20, -1)
            change = int(change * prestige_bonus)
            return change, True, 0, consecutive_minus + 1, jackpot_pity + 1, False
        else:
            change = random.randint(1, 20)
            change = int(change * prestige_bonus)
            return change, False, consecutive_plus + 1, 0, jackpot_pity + 1, False

def open_case(case_id, prestige_luck=0, luck_upgrade=0):
    case = CASES[case_id]
    prizes = case["prizes"]
    total = sum(p["chance"] for p in prizes)
    for p in prizes:
        p["normalized_chance"] = (p["chance"] / total) * 100
    bonus = (prestige_luck * 100) + (luck_upgrade * LUCK_CASE_BONUS_PER_LEVEL)
    modified = []
    for p in prizes:
        cp = p.copy()
        if (isinstance(cp["value"], int) and cp["value"] >= 100) or cp["value"] in ["rotten_leg", "water"]:
            cp["normalized_chance"] = p["normalized_chance"] + bonus
        modified.append(cp)
    total = sum(p["normalized_chance"] for p in modified)
    for p in modified:
        p["normalized_chance"] = (p["normalized_chance"] / total) * 100
    roll = random.random() * 100
    cumulative = 0
    for p in modified:
        cumulative += p["normalized_chance"]
        if roll < cumulative:
            return p
    return modified[-1]

def get_item_price(item_name):
    if item_name in LEGENDARY_UPGRADE_PRICES:
        return LEGENDARY_UPGRADE_PRICES[item_name]
    for it in SHOP_ITEMS:
        if it["name"] == item_name:
            return it["price"]
    return 0

def get_possible_upgrades(item_name, item_count):
    if item_count <= 0:
        return []
    current = get_item_price(item_name)
    if current == 0:
        return []
    upgrades = []
    seen = set()
    all_items = set([it["name"] for it in SHOP_ITEMS] + list(LEGENDARY_UPGRADE_PRICES.keys()))
    for it in SHOP_ITEMS:
        if it["name"] in seen:
            continue
        target = get_item_price(it["name"])
        if target <= current:
            continue
        chance = current / target
        if chance < 0.01:
            continue
        upgrades.append({"name": it["name"], "price": target, "chance": chance, "emoji": ITEM_EMOJIS.get(it["name"], "🎁")})
        seen.add(it["name"])
    if current >= 1000:
        for leg, price in LEGENDARY_UPGRADE_PRICES.items():
            if leg in seen:
                continue
            if leg not in all_items:
                continue
            if price <= current:
                continue
            chance = current / price
            if chance < 0.01:
                continue
            upgrades.append({"name": leg, "price": price, "chance": chance, "emoji": ITEM_EMOJIS.get(leg, "✨")})
            seen.add(leg)
    upgrades.sort(key=lambda x: x["price"])
    return upgrades

# ===== МАГАЗИН =====
def generate_shop_items():
    slots = []
    used = set()
    available = [cid for cid, c in CASES.items() if cid != "daily" and c.get("shop_chance", 0) > 0]
    for _ in range(4):
        if random.random() < 0.7 and available:
            choices = []
            for cid in available:
                case = CASES[cid]
                choices.extend([cid] * int(case["shop_chance"] * 100))
            if choices:
                chosen = random.choice(choices)
                case = CASES[chosen]
                amt = random.randint(case["min_shop"], case["max_shop"])
                mins = min([p["value"] for p in case["prizes"] if isinstance(p["value"], int)] + [0])
                maxs = max([p["value"] for p in case["prizes"] if isinstance(p["value"], int)] + [0])
                slots.append({"type": "case", "case_id": chosen, "name": case["name"], "amount": amt, "price": case["price"], "description": f"{case['emoji']} Содержит случайные призы!\nОт {mins}кг до {maxs}кг", "emoji": case['emoji']})
            else:
                slots.append(None)
        else:
            slots.append(None)
    for _ in range(6):
        chosen = None
        for _ in range(50):
            idx = random.randint(0, len(SHOP_ITEMS) - 1)
            if idx in used:
                continue
            item = SHOP_ITEMS[idx]
            if random.random() < item["chance"]:
                chosen = item
                used.add(idx)
                break
        if chosen:
            amt = random.randint(chosen["min_amount"], chosen["max_amount"])
            slots.append({"type": "item", "name": chosen["name"], "amount": amt, "price": chosen["price"], "description": chosen["description"], "gain_per_24h": chosen.get("gain_per_24h", 0), "emoji": ITEM_EMOJIS.get(chosen["name"], "📦")})
        else:
            slots.append(None)
    random.shuffle(slots)
    return slots

def get_shop_data(chat_id):
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT slots, last_update, next_update FROM shop WHERE chat_id = ?', (str(chat_id),))
    res = cursor.fetchone()
    conn.close()
    if res:
        slots_json, last_upd, next_upd = res
        try:
            slots = json.loads(slots_json) if slots_json else []
            for s in slots:
                if s and "type" not in s:
                    if "case_id" in s:
                        s["type"] = "case"
                    else:
                        s["type"] = "item"
            return slots, last_upd, next_upd
        except:
            return [], None, None
    return None, None, None

def update_shop_data(chat_id, slots, last_update, next_update):
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    clean = []
    for s in slots:
        if s:
            if "type" not in s:
                if "case_id" in s:
                    s["type"] = "case"
                else:
                    s["type"] = "item"
            clean.append(s)
        else:
            clean.append(None)
    slots_json = json.dumps(clean)
    last_str = last_update.isoformat() if last_update else None
    next_str = next_update.isoformat() if next_update else None
    cursor.execute('''INSERT OR REPLACE INTO shop (chat_id, slots, last_update, next_update) VALUES (?, ?, ?, ?)''', (str(chat_id), slots_json, last_str, next_str))
    conn.commit()
    conn.close()

async def ensure_shop_updated(chat_id):
    res = get_shop_data(chat_id)
    now = datetime.now()
    if res[0] is not None:
        slots, last_str, next_str = res
        last = datetime.fromisoformat(last_str) if isinstance(last_str, str) else last_str if last_str else None
        nxt = datetime.fromisoformat(next_str) if isinstance(next_str, str) else next_str if next_str else None
        if nxt and now >= nxt:
            new_slots = generate_shop_items()
            last = now
            nxt = now + timedelta(hours=SHOP_UPDATE_HOURS)
            update_shop_data(chat_id, new_slots, last, nxt)
            return new_slots, last, nxt
        else:
            return slots, last, nxt
    else:
        new_slots = generate_shop_items()
        last = now
        nxt = now + timedelta(hours=SHOP_UPDATE_HOURS)
        update_shop_data(chat_id, new_slots, last, nxt)
        return new_slots, last, nxt

# ===== ДУЭЛИ =====
def can_duel(user_data):
    return not user_data.get('duel_active', 0)

def get_duel_info(user_data):
    return {'active': user_data.get('duel_active', 0), 'opponent': user_data.get('duel_opponent'), 'amount': user_data.get('duel_amount', 0), 'message_id': user_data.get('duel_message_id'), 'initiator': user_data.get('duel_initiator', 0), 'start_time': user_data.get('duel_start_time')}

# ===== ФОНОВЫЕ ЗАДАЧИ =====
active_chats = set()
def register_chat(chat_id):
    active_chats.add(chat_id)

def get_users_with_auto_fat(chat_id):
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''SELECT user_id, user_name, auto_fat_level, next_auto_fat_time FROM user_fat WHERE auto_fat_level > 0 AND next_auto_fat_time IS NOT NULL''')
    res = cursor.fetchall()
    conn.close()
    return res

def get_users_with_items(chat_id):
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''SELECT user_id, user_name, current_number, item_counts, income_upgrade, prestige, last_passive_income FROM user_fat WHERE item_counts != '{}' AND item_counts IS NOT NULL''')
    res = cursor.fetchall()
    conn.close()
    return res

def get_users_with_snatcher(chat_id):
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''SELECT user_id, user_name, item_counts, snatcher_last_time FROM user_fat WHERE item_counts LIKE '%"Снатчер"%' ''')
    res = cursor.fetchall()
    conn.close()
    return res

def get_users_with_hourly_items(chat_id):
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''SELECT user_id, user_name, current_number, item_counts, income_upgrade, prestige, last_hourly_income FROM user_fat''')
    res = cursor.fetchall()
    conn.close()
    return res

async def apply_auto_fat(chat_id, user_id, user_name):
    try:
        data = get_user_data(chat_id, user_id, user_name)
        items = get_user_items(data['item_counts'])
        pb = get_prestige_bonus(data.get('prestige', 0))
        change, was_minus, new_plus, new_minus, new_pity, was_jack = get_change_with_pity_and_jackpot(
            data['consecutive_plus'], data['consecutive_minus'], data['jackpot_pity'],
            data.get('luck_upgrade', 0), pb, items, data['current_number'])
        new_num = data['current_number'] + change
        update_user_data(chat_id, user_id, number=new_num, user_name=user_name,
                        consecutive_plus=new_plus, consecutive_minus=new_minus,
                        jackpot_pity=new_pity, fat_cooldown_time=datetime.now())
        add_xp(chat_id, user_id, XP_PER_FAT)
        print(f"🤖 Авто-жир сработал для {user_name}: {change:+d} кг")
    except Exception as e:
        print(f"❌ Ошибка авто-жира: {e}")

async def auto_fat_loop():
    await asyncio.sleep(10)
    while True:
        try:
            now = datetime.now()
            for cid in list(active_chats):
                try:
                    users = get_users_with_auto_fat(cid)
                    for uid, uname, lvl, next_t in users:
                        if not next_t:
                            continue
                        nxt = datetime.fromisoformat(next_t) if isinstance(next_t, str) else next_t
                        if now >= nxt:
                            await apply_auto_fat(cid, uid, uname)
                            iv = get_auto_fat_interval(lvl)
                            if iv:
                                update_user_data(cid, uid, next_auto_fat_time=now + timedelta(hours=iv))
                except Exception as e:
                    print(f"❌ Ошибка чата {cid}: {e}")
        except Exception as e:
            print(f"❌ Ошибка авто-жир цикла: {e}")
        await asyncio.sleep(60)

async def passive_income_loop():
    await asyncio.sleep(10)
    while True:
        try:
            now = datetime.now()
            for cid in list(active_chats):
                try:
                    users = get_users_with_items(cid)
                    for uid, uname, cur, items_str, inc_up, pres, last_inc in users:
                        should = False
                        if not last_inc:
                            update_user_data(cid, uid, last_passive_income=now)
                            continue
                        last = datetime.fromisoformat(last_inc) if isinstance(last_inc, str) else last_inc
                        if (now - last).total_seconds() >= 86400:
                            should = True
                        if should:
                            items = get_user_items(items_str)
                            if not items:
                                continue
                            total = 0
                            for iname, cnt in items.items():
                                for it in SHOP_ITEMS:
                                    if it["name"] == iname:
                                        g = it.get("gain_per_24h", 0) * cnt
                                        if g > 0:
                                            total += g
                                        break
                            if total > 0:
                                inc_bonus = get_income_bonus(inc_up or 0)
                                pres_bonus = get_prestige_bonus(pres or 0)
                                final = int(total * inc_bonus * pres_bonus)
                                new_num = cur + final
                                update_user_data(cid, uid, number=new_num, last_passive_income=now)
                                print(f"💰 {uname} получил {final}кг от предметов")
                except Exception as e:
                    print(f"❌ Ошибка чата {cid}: {e}")
        except Exception as e:
            print(f"❌ Ошибка пассивного дохода: {e}")
        await asyncio.sleep(86400)

async def snatcher_loop():
    await asyncio.sleep(10)
    while True:
        try:
            now = datetime.now()
            if now.minute % 30 == 0:
                for cid in list(active_chats):
                    try:
                        users = get_users_with_snatcher(cid)
                        for uid, uname, items_str, last_sn in users:
                            should = False
                            if not last_sn:
                                update_user_data(cid, uid, snatcher_last_time=now)
                                continue
                            last = datetime.fromisoformat(last_sn) if isinstance(last_sn, str) else last_sn
                            if (now - last).total_seconds() >= 21600:
                                should = True
                            if should:
                                await apply_snatcher_effect(cid, uid, uname)
                    except Exception as e:
                        print(f"❌ Ошибка чата {cid}: {e}")
        except Exception as e:
            print(f"❌ Ошибка снатчера: {e}")
        await asyncio.sleep(1800)

async def apply_snatcher_effect(chat_id, user_id, user_name):
    try:
        data = get_user_data(chat_id, user_id, user_name)
        items = get_user_items(data['item_counts'])
        if items.get("Снатчер", 0) == 0:
            return
        now = datetime.now()
        last = data.get('snatcher_last_time')
        if last:
            last_t = datetime.fromisoformat(last) if isinstance(last, str) else last
            if (now - last_t).total_seconds() < 21600:
                return
        if random.random() > 0.2:
            update_user_data(chat_id, user_id, snatcher_last_time=now)
            return
        virt = []
        used = set()
        for _ in range(10):
            chosen = None
            for _ in range(50):
                idx = random.randint(0, len(SHOP_ITEMS) - 1)
                if idx in used:
                    continue
                it = SHOP_ITEMS[idx]
                if random.random() < it["chance"]:
                    chosen = it
                    used.add(idx)
                    break
            if chosen:
                amt = random.randint(chosen["min_amount"], chosen["max_amount"])
                virt.append({"name": chosen["name"], "amount": amt, "price": chosen["price"], "description": chosen["description"], "gain_per_24h": chosen.get("gain_per_24h", 0)})
            else:
                virt.append(None)
        slot = random.randint(0, 9)
        selected = virt[slot]
        if not selected:
            update_user_data(chat_id, user_id, snatcher_last_time=now)
            return
        items[selected["name"]] = items.get(selected["name"], 0) + 1
        update_user_data(chat_id, user_id, item_counts=save_user_items(items), snatcher_last_time=now)
        print(f"👾 Снатчер сработал для {user_name}: +1 {selected['name']}")
    except Exception as e:
        print(f"❌ Ошибка снатчера: {e}")

async def hourly_effects_loop():
    await asyncio.sleep(10)
    while True:
        try:
            now = datetime.now()
            for cid in list(active_chats):
                try:
                    users = get_users_with_hourly_items(cid)
                    for uid, uname, cur, items_str, inc_up, pres, last_h in users:
                        should = False
                        if not last_h:
                            update_user_data(cid, uid, last_hourly_income=now)
                            continue
                        last = datetime.fromisoformat(last_h) if isinstance(last_h, str) else last_h
                        if (now - last).total_seconds() >= 3600:
                            should = True
                        if should:
                            items = get_user_items(items_str)
                            if not items:
                                continue
                            total = 0
                            for iname, cnt in items.items():
                                if iname == "Автохолестерол":
                                    total += random.randint(1, 10) * cnt
                                elif iname == "Холестеринимус":
                                    total += random.randint(1, 5) * cnt
                            if total > 0:
                                inc_bonus = get_income_bonus(inc_up or 0)
                                pres_bonus = get_prestige_bonus(pres or 0)
                                final = int(total * inc_bonus * pres_bonus)
                                new_num = cur + final
                                update_user_data(cid, uid, number=new_num, last_hourly_income=now)
                                print(f"💊 {uname} получил {final}кг от почасовых предметов")
                except Exception as e:
                    print(f"❌ Ошибка чата {cid}: {e}")
        except Exception as e:
            print(f"❌ Ошибка почасовых эффектов: {e}")
        await asyncio.sleep(3600)

async def migrate_old_data(chat_id):
    db_path = get_db_path(chat_id)
    if not os.path.exists(db_path):
        return
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(user_fat)")
    cols = [c[1] for c in cursor.fetchall()]
    if 'legendary_burger' in cols and 'prestige' in cols:
        burger_to_prestige = {0: 1, 1: 2, 2: 3, 3: 4}
        cursor.execute("SELECT user_id, user_name, legendary_burger FROM user_fat WHERE legendary_burger >= 0")
        for uid, uname, lvl in cursor.fetchall():
            if lvl in burger_to_prestige:
                cursor.execute("UPDATE user_fat SET prestige = ?, legendary_burger = -1 WHERE user_id = ?", (burger_to_prestige[lvl], uid))
                print(f"🔄 {uname}: бургер {lvl} -> {burger_to_prestige[lvl]} престижа")
        conn.commit()
    conn.close()

# ===== ВСЕ КОМАНДЫ =====

# /жир
async def cmd_fat(message: Message):
    register_chat(message.chat.id)
    cid = message.chat.id
    uid = message.from_user.id
    uname = message.from_user.full_name
    data = get_user_data(cid, uid, uname)
    
    fat_up = data.get('fat_cd_upgrade', 0)
    cd_red = get_fat_cd_reduction(fat_up)
    actual_cd = max(0.1, COOLDOWN_HOURS * 60 - cd_red) / 60
    items = get_user_items(data['item_counts'])
    for iname, cnt in items.items():
        if iname == "Яблоко":
            actual_cd *= (1 - cnt * 0.05)
        elif iname == "Золотое Яблоко":
            actual_cd *= (1 - cnt * 0.10)
    actual_cd = max(0.1, actual_cd)
    can, rem = check_cooldown(data['fat_cooldown_time'], actual_cd)
    if not can:
        await message.reply(f"⏳ Подождите! Осталось: {format_time(rem)}\nКулдаун: {actual_cd*60:.0f} мин")
        return
    
    pb = get_prestige_bonus(data.get('prestige', 0))
    change, was_minus, new_plus, new_minus, new_pity, was_jack = get_change_with_pity_and_jackpot(
        data['consecutive_plus'], data['consecutive_minus'], data['jackpot_pity'],
        data.get('luck_upgrade', 0), pb, items, data['current_number'])
    
    update_user_data(cid, uid, number=data['current_number'] + change)
    lvl_gain, kg_reward, new_lvl = add_xp(cid, uid, XP_PER_FAT)
    final = get_user_data(cid, uid, uname)
    final_num = final['current_number']
    update_user_data(cid, uid, consecutive_plus=new_plus, consecutive_minus=new_minus,
                    jackpot_pity=new_pity, fat_cooldown_time=datetime.now())
    rank_name, rank_emoji = get_rank(final_num)
    
    resp = f"{'💰 ДЖЕКПОТ!' if was_jack else '🍔 Набор массы'}\n\n**{uname}** теперь весит **{abs(final_num)}kg**!\n\n"
    if was_jack:
        resp += f"💰 Джекпот: +{change} кг\n"
    elif change > 0:
        resp += f"📈 +{change} кг\n"
    elif change < 0:
        resp += f"📉 {change} кг\n"
    resp += f"🍖 Текущий вес: {final_num}kg\n🎖️ Звание: {rank_emoji} {rank_name}\n"
    if lvl_gain > 0:
        resp += f"\n⭐ **ПОВЫШЕНИЕ УРОВНЯ!** ⭐\n+{kg_reward} кг за {lvl_gain} уровень(ей)!\nТеперь у вас **{new_lvl}** уровень!\n"
    resp += f"\n⏰ Следующая команда через {actual_cd*60:.0f} мин"
    await message.reply(resp)

# /жиркейс
async def cmd_fat_case(message: Message):
    register_chat(message.chat.id)
    cid = message.chat.id
    uid = message.from_user.id
    uname = message.from_user.full_name
    data = get_user_data(cid, uid, uname)
    
    if data.get('active_case_message_id'):
        try:
            await bot.delete_message(cid, int(data['active_case_message_id']))
        except:
            pass
    
    items = get_user_items(data['item_counts'])
    case_up = data.get('case_cd_upgrade', 0)
    cd_red = get_case_cd_reduction(case_up)
    actual_cd = max(1, CASE_COOLDOWN_HOURS * 60 - cd_red) / 60
    for iname, cnt in items.items():
        if iname == "Апельсин":
            actual_cd *= (1 - cnt * 0.05)
        elif iname == "Золотой Апельсин":
            actual_cd *= (1 - cnt * 0.10)
    actual_cd = max(1, int(actual_cd))
    
    can_daily, daily_rem = can_get_daily_case(cid, uid, actual_cd)
    cases_dict = data.get('cases_dict', {})
    to_open = None
    if can_daily:
        to_open = "daily"
    else:
        for cid2, cnt in cases_dict.items():
            if cnt > 0:
                to_open = cid2
                break
    
    if not to_open:
        time_str = format_time(daily_rem) if daily_rem > 0 else "скоро"
        await message.reply(f"📭 Нет кейсов!\nЕжедневный кейс через: {time_str}\nКупить кейсы можно в магазине (/магазин)")
        return
    
    case = CASES[to_open]
    update_user_data(cid, uid, active_case_message_id=None, last_case_type=to_open)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🖱️ ОТКРЫТЬ", callback_data=f"open_case_{to_open}"), InlineKeyboardButton(text="❌ ОТМЕНА", callback_data=f"cancel_case_{to_open}")]])
    text = f"{case['emoji']} **{case['name']}** {case['emoji']}\n\n{uname}, у вас есть кейс!\n\n┌───────────────┐\n│----{case['emoji']}---{case['emoji']}---{case['emoji']}----│\n│----К-Е-Й-С-------│\n│----{case['name'][:10]}--│\n│----{case['emoji']}---{case['emoji']}---{case['emoji']}----│\n└───────────────┘\n\n⏰ У вас 30 секунд!"
    msg = await message.reply(text, reply_markup=kb)
    update_user_data(cid, uid, active_case_message_id=str(msg.message_id))

@dp.callback_query(lambda c: c.data and (c.data.startswith('open_case_') or c.data.startswith('cancel_case_')))
async def process_case_callback(cb: CallbackQuery):
    await cb.answer()
    is_cancel = cb.data.startswith('cancel_case_')
    to_open = cb.data.replace('open_case_', '').replace('cancel_case_', '')
    cid = cb.message.chat.id
    uid = cb.from_user.id
    uname = cb.from_user.full_name
    data = get_user_data(cid, uid, uname)
    
    if str(uid) != data['user_id']:
        await cb.answer("Это не ваш кейс!", show_alert=True)
        return
    try:
        await cb.message.delete_reply_markup()
    except:
        pass
    if is_cancel:
        update_user_data(cid, uid, active_case_message_id=None, last_case_type=None)
        await cb.message.edit_text(f"❌ **{uname}** отменил открытие кейса. Кейс сохранён!")
        return
    
    case = CASES[to_open]
    items = get_user_items(data['item_counts'])
    case_up = data.get('case_cd_upgrade', 0)
    cd_red = get_case_cd_reduction(case_up)
    actual_cd = max(1, CASE_COOLDOWN_HOURS * 60 - cd_red) / 60
    for iname, cnt in items.items():
        if iname == "Апельсин":
            actual_cd *= (1 - cnt * 0.05)
        elif iname == "Золотой Апельсин":
            actual_cd *= (1 - cnt * 0.10)
    actual_cd = max(1, int(actual_cd))
    
    if to_open == "daily":
        can, _ = can_get_daily_case(cid, uid, actual_cd)
        if not can:
            await cb.answer("Ежедневный кейс уже использован!", show_alert=True)
            await cb.message.delete()
            update_user_data(cid, uid, active_case_message_id=None, last_case_type=None)
            return
        update_daily_case_time(cid, uid)
    else:
        cases = data.get('cases_dict', {}).copy()
        if cases.get(to_open, 0) <= 0:
            await cb.answer("У вас больше нет этого кейса!", show_alert=True)
            await cb.message.delete()
            update_user_data(cid, uid, active_case_message_id=None, last_case_type=None)
            return
        cases[to_open] -= 1
        update_user_data(cid, uid, cases_dict=cases)
    
    prestige_luck = get_prestige_luck(data.get('prestige', 0))
    luck_up = data.get('luck_upgrade', 0)
    prize = open_case(to_open, prestige_luck, luck_up)
    update_user_data(cid, uid, active_case_message_id=None, last_case_type=None)
    lvl_gain, kg_reward, new_lvl = add_xp(cid, uid, XP_PER_CASE)
    
    items = get_user_items(data['item_counts'])
    new_num = data['current_number']
    val = prize["value"]
    pb = get_prestige_bonus(data.get('prestige', 0))
    has_water = items.get("Стакан воды", 0) > 0
    
    if val == "rotten_leg":
        items["Гнилая ножка KFC"] = items.get("Гнилая ножка KFC", 0) + 1
        disp = "💀 **Гнилая ножка KFC!** 💀"
    elif val == "water":
        items["Стакан воды"] = items.get("Стакан воды", 0) + 1
        disp = "💧 **Стакан воды!** 💧"
    elif isinstance(val, str):
        items[val] = items.get(val, 0) + 1
        disp = f"🎁 **{val}**"
    else:
        if has_water and to_open != "daily":
            val = val // 3
        val = int(val * pb)
        new_num = data['current_number'] + val
        disp = f"🎉 **{val:+d} кг**"
    
    update_user_data(cid, uid, number=new_num, user_name=uname, item_counts=save_user_items(items))
    rank_name, rank_emoji = get_rank(new_num)
    
    final = f"{case['emoji']} Открытие {case['name']}\n\n**{uname}** открыл кейс и получил:\n\n🎁 Приз: {disp}\n"
    if val not in ["rotten_leg", "water"] and not isinstance(val, str):
        final += f"🍖 Новый вес: {new_num}kg\n🎖️ Звание: {rank_emoji} {rank_name}\n"
    if lvl_gain > 0:
        final += f"\n⭐ **ПОВЫШЕНИЕ УРОВНЯ!** ⭐\n+{kg_reward} кг за {lvl_gain} уровень(ей)!\nТеперь у вас **{new_lvl}** уровень!\n"
    await cb.message.reply(final)

# /профиль
async def cmd_profile(message: Message):
    register_chat(message.chat.id)
    cid = message.chat.id
    uid = message.from_user.id
    uname = message.from_user.full_name
    data = get_user_data(cid, uid, uname)
    
    rank_name, rank_emoji = get_rank(data['current_number'])
    total_xp = data.get('user_xp', 0)
    lvl, cur_xp = get_level_and_xp(total_xp)
    next_xp = get_xp_for_next_level(lvl)
    bar_len = 15
    prog = int((cur_xp / next_xp) * bar_len) if next_xp > 0 else 0
    bar = "█" * prog + "░" * (bar_len - prog)
    
    resp = f"⭐ **ПРОФИЛЬ** ⭐\n\n**{uname}**\n{'🎬 Анимации: ВКЛ' if data.get('animations_enabled', 1) == 1 else '🔇 Анимации: ВЫКЛ'}\n\n"
    resp += f"📊 **ОСНОВНАЯ ИНФОРМАЦИЯ**\n"
    resp += f"🍖 Вес: **{data['current_number']}kg**\n"
    resp += f"🎖️ Звание: {rank_emoji} **{rank_name}**\n"
    resp += f"📈 Уровень: **{lvl}**\n"
    resp += f"✨ Опыт: {cur_xp} / {next_xp}\n`{bar}`\n"
    resp += f"🌟 Престиж: **{data.get('prestige', 0)}** (+{data.get('prestige', 0)*10}% к кг, +{data.get('prestige', 0)}% к шансам, +{data.get('prestige', 0)*50}% к опыту)\n\n"
    
    resp += f"⚡ **ХАРАКТЕРИСТИКИ** ⚡\n"
    resp += f"• **КД /жир** — ур.{data.get('fat_cd_upgrade', 0)} (-{get_fat_cd_reduction(data.get('fat_cd_upgrade', 0))} мин)\n"
    resp += f"• **КД кейса** — ур.{data.get('case_cd_upgrade', 0)} (-{get_case_cd_reduction(data.get('case_cd_upgrade', 0))} мин)\n"
    resp += f"• **Удача** — ур.{data.get('luck_upgrade', 0)} (+{data.get('luck_upgrade', 0) * LUCK_CASE_BONUS_PER_LEVEL:.0f}% к редким, +{data.get('luck_upgrade', 0) * LUCK_UPGRADE_BONUS_PER_LEVEL:.0f}% к апгрейдам)\n"
    resp += f"• **Прибавка** — ур.{data.get('income_upgrade', 0)} (+{data.get('income_upgrade', 0) * 5}% к доходу)\n"
    resp += f"• **Авто-жир** — ур.{data.get('auto_fat_level', 0)} ({get_auto_fat_interval(data.get('auto_fat_level', 0)) or 'не куплен'} ч)\n\n"
    
    resp += f"💡 Используйте `/апгрейдюзер [номер]` для улучшения характеристик\n"
    resp += f"1 - КД /жир | 2 - КД кейса | 3 - Удача | 4 - Прибавка | 5 - Престиж | 6 - Авто-жир\n"
    resp += f"🎬 Для переключения анимаций используйте `/анимации`"
    await message.reply(resp)

# /анимации
async def cmd_animations(message: Message):
    register_chat(message.chat.id)
    cid = message.chat.id
    uid = message.from_user.id
    data = get_user_data(cid, uid)
    cur = data.get('animations_enabled', 1)
    new = 0 if cur == 1 else 1
    update_user_data(cid, uid, animations_enabled=new)
    await message.reply(f"{'🎬 Анимации ВКЛЮЧЕНЫ' if new == 1 else '🔇 Анимации ВЫКЛЮЧЕНЫ'}")

# /жиркейс_шансы
async def cmd_fat_case_chances(message: Message):
    register_chat(message.chat.id)
    resp = "📊 **ШАНСЫ В КЕЙСЕ** 📊\n\n"
    for p in CASE_PRIZES:
        resp += f"{p['emoji']} **{p['name']}** — {p['chance']}%\n"
    resp += f"\n⏰ Кулдаун ежедневного кейса: **{CASE_COOLDOWN_HOURS} часов**\n"
    resp += f"🍀 Бонус удачи: +{LUCK_CASE_BONUS_PER_LEVEL * 100:.0f}% к шансу редких призов за уровень"
    await message.reply(resp)

# /жиротрясы
async def cmd_fat_leaderboard(message: Message):
    register_chat(message.chat.id)
    cid = message.chat.id
    name = message.chat.title or "этом чате"
    users = get_all_users_sorted(cid)
    if not users:
        await message.reply(f"📭 В {name} пока никто не участвовал!")
        return
    resp = f"🏆 **Таблица жиротрясов - {name}** 🏆\n\n"
    for i, u in enumerate(users[:20], 1):
        pres = u[6] if len(u) > 6 else 0
        un, num = u[0], u[1]
        rn, re = get_rank(num)
        disp = f"{pres}🌟{un}" if pres > 0 else un
        place = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        resp += f"{place} **{disp}** — {num}kg {re}\n"
    await message.reply(resp)

# /жирзвания
async def cmd_show_ranks(message: Message):
    register_chat(message.chat.id)
    resp = "🎖️ **Система званий**\n\n"
    for r in RANKS:
        rg = f"{r['min']}" if r["min"] == r["max"] else f"{r['min']} – {r['max']}"
        resp += f"{r['emoji']} **{r['name']}** — {rg} kg\n"
    await message.reply(resp)

# /жиркулдаун
async def cmd_cooldown_info(message: Message):
    register_chat(message.chat.id)
    cid = message.chat.id
    uid = message.from_user.id
    data = get_user_data(cid, uid)
    
    fat_up = data.get('fat_cd_upgrade', 0)
    fat_cd = max(0.1, COOLDOWN_HOURS * 60 - get_fat_cd_reduction(fat_up)) / 60
    case_up = data.get('case_cd_upgrade', 0)
    case_cd = max(1, CASE_COOLDOWN_HOURS * 60 - get_case_cd_reduction(case_up)) / 60
    
    items = get_user_items(data['item_counts'])
    for iname, cnt in items.items():
        if iname in ["Яблоко", "Золотое Яблоко"]:
            fat_cd *= (1 - cnt * (0.05 if iname == "Яблоко" else 0.10))
        elif iname in ["Апельсин", "Золотой Апельсин"]:
            case_cd *= (1 - cnt * (0.05 if iname == "Апельсин" else 0.10))
    fat_cd = max(0.1, fat_cd)
    case_cd = max(0.1, case_cd)
    
    fat_can, fat_rem = check_cooldown(data['fat_cooldown_time'], fat_cd)
    case_can, case_rem = check_cooldown(data['last_case_time'], case_cd)
    
    resp = f"⏰ **Кулдауны**\n\n"
    resp += f"**/жир**\nКулдаун: {fat_cd*60:.0f} мин\nСтатус: {'✅ Доступна' if fat_can else f'⏳ {format_time(fat_rem)}'}\n\n"
    resp += f"**/жиркейс**\nКулдаун: {case_cd:.1f} ч\nСтатус: {'✅ Доступен' if case_can else f'⏳ {format_time(case_rem)}'}"
    await message.reply(resp)

# /инвентарь
async def cmd_show_inventory(message: Message):
    register_chat(message.chat.id)
    cid = message.chat.id
    uid = message.from_user.id
    uname = message.from_user.full_name
    data = get_user_data(cid, uid, uname)
    
    resp = f"🎒 **Инвентарь - {uname}**\n\n"
    if data.get('auto_fat_level', 0) > 0:
        iv = get_auto_fat_interval(data['auto_fat_level'])
        resp += f"🤖 Авто-жир: {data['auto_fat_level']} уровень (каждые {iv} ч)\n\n"
    
    cases = data.get('cases_dict', {})
    cases_txt = ""
    for cid2, cnt in cases.items():
        if cnt > 0 and cid2 in CASES:
            cases_txt += f"{CASES[cid2]['emoji']} {CASES[cid2]['name']}: {cnt}\n"
    if cases_txt:
        resp += f"📦 **Кейсы**\n{cases_txt}\n"
    
    items = get_user_items(data['item_counts'])
    if items:
        regular = []
        leg = []
        for iname, cnt in items.items():
            if iname in ["Снатчер", "Святой сэндвич", "Гнилая ножка KFC", "Стакан воды", "Автохолестерол", "Холестеринимус", "Яблоко", "Золотое Яблоко", "Апельсин", "Золотой Апельсин", "Драгонфрукт", "Золотой Драгонфрукт"]:
                leg.append(f"• {iname}: {cnt} шт")
            else:
                regular.append(f"• {iname}: {cnt} шт")
        itxt = ""
        if regular:
            itxt += "**Обычные предметы:**\n" + "\n".join(regular[:8]) + "\n"
        if leg:
            itxt += "**✨ Легендарные предметы:**\n" + "\n".join(leg)
        resp += f"📦 **Предметы**\n{itxt}"
    await message.reply(resp[:4000])

# /жирглобал
async def cmd_global_leaderboard(message: Message):
    register_chat(message.chat.id)
    stats = []
    for cid in list(active_chats)[:50]:
        try:
            chat = await bot.get_chat(cid)
            name = chat.title or f"Чат {cid}"
            st = get_chat_stats(cid)
            if st['total_users'] > 0:
                stats.append({'name': name[:30], 'total_weight': st['total_weight'], 'users': st['total_users']})
        except:
            continue
    if not stats:
        await message.reply("📭 Нет данных по чатам!")
        return
    stats.sort(key=lambda x: x['total_weight'], reverse=True)
    resp = "🌍 **ГЛОБАЛЬНЫЙ РЕЙТИНГ ЧАТОВ** 🌍\n\n"
    for i, s in enumerate(stats[:10], 1):
        place = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        w = f"{s['total_weight']/1000:.1f}K" if s['total_weight'] >= 1000 else str(s['total_weight'])
        resp += f"{place} **{s['name']}**\n   📦 {w} кг | 👥 {s['users']} уч.\n\n"
    total_w = sum(s['total_weight'] for s in stats)
    total_u = sum(s['users'] for s in stats)
    total_disp = f"{total_w/1000:.1f}K" if total_w >= 1000 else str(total_w)
    resp += f"📊 **Всего:** {len(stats)} чатов | {total_u} уч.\n⚖️ **Общая масса:** {total_disp} кг"
    await message.reply(resp)

# /магазин
async def cmd_shop(message: Message):
    register_chat(message.chat.id)
    cid = message.chat.id
    uid = message.from_user.id
    uname = message.from_user.full_name
    
    data = get_user_data(cid, uid, uname)
    update_user_data(cid, uid, last_command="shop", last_command_use_time=datetime.now())
    slots, last_upd, next_upd = await ensure_shop_updated(cid)
    slots = slots if isinstance(slots, list) else []
    
    resp = "🏪 **МАГАЗИН** 🏪\n\n📦 Слоты 1-4: Кейсы | 🛒 Слоты 5-10: Предметы\n\n"
    for i in range(1, SHOP_SLOTS + 1):
        s = slots[i-1] if i-1 < len(slots) else None
        if s and isinstance(s, dict):
            prefix = "📦" if i <= 4 else "🛒"
            resp += f"**{i}.** {prefix} {s.get('emoji', '📦')} {s.get('name', '?')} — {s.get('amount', 0)} шт — **{s.get('price', 0)} кг/шт**\n   └ {s.get('description', '')}\n"
        else:
            resp += f"**{i}.** {'📦🕳️ Пустой слот для кейса' if i <= 4 else '🛒🕳️ Пустой слот для предмета'}\n"
    last_str = last_upd.strftime("%d.%m.%Y %H:%M") if last_upd else "Никогда"
    next_str = next_upd.strftime("%d.%m.%Y %H:%M") if next_upd else "Скоро"
    resp += f"\n⏰ Обновление каждые {SHOP_UPDATE_HOURS} ч\nПоследнее: {last_str}\nСледующее: {next_str}"
    await message.reply(resp)

# /купить
async def cmd_buy(message: Message):
    register_chat(message.chat.id)
    parts = message.text.split() if message.text else []
    if len(parts) < 3:
        await message.reply("❌ Использование: `/купить [слот] [количество]`")
        return
    try:
        slot = int(parts[1])
        amt = int(parts[2])
    except:
        await message.reply("❌ Номер слота и количество должны быть числами!")
        return
    if slot < 1 or slot > SHOP_SLOTS or amt <= 0:
        await message.reply("❌ Неверные параметры!")
        return
    
    cid = message.chat.id
    uid = message.from_user.id
    uname = message.from_user.full_name
    data = get_user_data(cid, uid, uname)
    
    last_use = data.get('last_command_use_time')
    if isinstance(last_use, str):
        last_use = datetime.fromisoformat(last_use) if last_use else None
    if data.get('last_command') != "shop" or not last_use or (datetime.now() - last_use).total_seconds() > 300:
        await message.reply("❌ Сначала используйте `/магазин`!")
        return
    
    slots, last_upd, next_upd = await ensure_shop_updated(cid)
    if slot - 1 >= len(slots) or not slots[slot - 1]:
        await message.reply(f"❌ В слоте {slot} ничего нет!")
        return
    
    item = slots[slot - 1]
    if amt > item["amount"]:
        await message.reply(f"❌ В наличии только {item['amount']} шт!")
        return
    
    total = item["price"] * amt
    if data['current_number'] < total:
        await message.reply(f"❌ Недостаточно кг! Нужно: {total} кг")
        return
    
    new_num = data['current_number'] - total
    item["amount"] -= amt
    
    if item.get("type") == "case" or "case_id" in item:
        cid2 = item.get("case_id")
        if cid2 not in CASES:
            await message.reply("❌ Неизвестный тип кейса!")
            return
        cases = data.get('cases_dict', {}).copy()
        cases[cid2] = cases.get(cid2, 0) + amt
        update_user_data(cid, uid, number=new_num, cases_dict=cases, last_command=None, last_command_use_time=None)
        desc = f"{item.get('emoji', '📦')} {item.get('name', 'Кейс')} x{amt}"
    else:
        items = get_user_items(data['item_counts'])
        items[item["name"]] = items.get(item["name"], 0) + amt
        update_user_data(cid, uid, number=new_num, item_counts=save_user_items(items), last_command=None, last_command_use_time=None)
        desc = f"{item['name']} x{amt}"
    
    update_shop_data(cid, slots, last_upd, next_upd)
    lvl_gain, kg_reward, new_lvl = add_xp(cid, uid, XP_PER_SHOP_BUY)
    
    resp = f"✅ **Покупка совершена!**\n\n📦 {desc}\n💰 Цена: {total} кг\n💸 Осталось: {new_num} кг"
    if lvl_gain > 0:
        resp += f"\n\n⭐ +{kg_reward} кг за повышение уровня! Теперь {new_lvl} уровень!"
    await message.reply(resp)

# /продать
async def cmd_sell(message: Message):
    register_chat(message.chat.id)
    parts = message.text.split() if message.text else []
    if len(parts) < 2:
        await message.reply("❌ Использование: `/продать [предмет] [количество]` или `/продать всё`")
        return
    
    cid = message.chat.id
    uid = message.from_user.id
    uname = message.from_user.full_name
    data = get_user_data(cid, uid, uname)
    items = get_user_items(data['item_counts'])
    
    if parts[1].lower() in ["всё", "все"]:
        if not items:
            await message.reply("📭 Нет предметов для продажи!")
            return
        total_gain = 0
        sold = []
        for iname, cnt in list(items.items()):
            price = get_item_price(iname)
            if price > 0:
                gain = int(price * 0.7) * cnt
                total_gain += gain
                sold.append(f"{iname} x{cnt} — {gain} кг")
                del items[iname]
        if total_gain == 0:
            await message.reply("❌ Ничего нельзя продать!")
            return
        new_num = data['current_number'] + total_gain
        update_user_data(cid, uid, number=new_num, item_counts=save_user_items(items))
        resp = f"💰 **Продажа всех предметов**\n\n{uname} продал всё!\n\n📦 Продано:\n" + "\n".join(sold[:10]) + f"\n\n💸 Получено: {total_gain} кг\n🍖 Новый вес: {new_num}kg"
        await message.reply(resp)
        return
    
    if len(parts) < 2:
        await message.reply("❌ Укажите предмет!")
        return
    amt = 1
    if len(parts) >= 3:
        try:
            amt = int(parts[-1])
            iname = ' '.join(parts[1:-1])
        except:
            iname = ' '.join(parts[1:])
    else:
        iname = parts[1]
    
    found = None
    for k in items.keys():
        if k.lower() == iname.lower():
            found = k
            break
    if not found:
        await message.reply(f"❌ Нет предмета '{iname}'!")
        return
    if items[found] < amt:
        await message.reply(f"❌ Недостаточно! Есть: {items[found]}")
        return
    
    price = get_item_price(found)
    if price == 0:
        await message.reply(f"❌ '{found}' нельзя продать!")
        return
    
    sell_price = int(price * 0.7)
    total_gain = sell_price * amt
    items[found] -= amt
    if items[found] <= 0:
        del items[found]
    new_num = data['current_number'] + total_gain
    update_user_data(cid, uid, number=new_num, item_counts=save_user_items(items))
    resp = f"💰 **Продажа предмета**\n\n{uname} продал:\n\n📦 **{found}** x{amt}\n💎 Цена: {price} кг/шт\n🏷️ Продажа (70%): {sell_price} кг/шт\n💸 Получено: {total_gain} кг\n🍖 Новый вес: {new_num}kg"
    await message.reply(resp)

# /датьжир
async def cmd_give_fat(message: Message):
    register_chat(message.chat.id)
    if not message.reply_to_message:
        await message.reply("❌ Ответьте на сообщение пользователя!")
        return
    target = message.reply_to_message.from_user
    parts = message.text.split() if message.text else []
    if len(parts) < 2:
        await message.reply("❌ Укажите количество! Пример: `/датьжир 100`")
        return
    try:
        amt = int(parts[1])
    except:
        await message.reply("❌ Количество должно быть числом!")
        return
    if amt <= 0 or message.from_user.id == target.id:
        await message.reply("❌ Некорректная сумма или передача самому себе!")
        return
    
    cid = message.chat.id
    giver = message.from_user
    giver_data = get_user_data(cid, giver.id, giver.full_name)
    target_data = get_user_data(cid, target.id, target.full_name)
    if giver_data['current_number'] < amt:
        await message.reply(f"❌ Недостаточно кг! Есть: {giver_data['current_number']} кг")
        return
    
    new_giver = giver_data['current_number'] - amt
    new_target = target_data['current_number'] + amt
    update_user_data(cid, giver.id, number=new_giver)
    update_user_data(cid, target.id, number=new_target)
    await message.reply(f"⚖️ **Перевод жира**\n\n{giver.full_name} → {target.full_name}\n📦 {amt} кг")

# /датьпредмет
async def cmd_give_item(message: Message):
    register_chat(message.chat.id)
    if not message.reply_to_message:
        await message.reply("❌ Ответьте на сообщение пользователя!")
        return
    target = message.reply_to_message.from_user
    parts = message.text.split() if message.text else []
    if len(parts) < 3:
        await message.reply("❌ Использование: `/датьпредмет количество название`")
        return
    try:
        amt = int(parts[1])
        iname = ' '.join(parts[2:])
    except:
        await message.reply("❌ Неверный формат!")
        return
    if amt <= 0 or message.from_user.id == target.id:
        await message.reply("❌ Некорректные параметры!")
        return
    
    cid = message.chat.id
    giver = message.from_user
    giver_data = get_user_data(cid, giver.id, giver.full_name)
    target_data = get_user_data(cid, target.id, target.full_name)
    
    # проверка кейсов
    for cid2, case in CASES.items():
        if cid2 != "daily" and case["name"].lower() in iname.lower():
            if not case.get("tradable", True):
                await message.reply(f"❌ Кейс '{case['name']}' нельзя передавать!")
                return
            giver_cases = giver_data.get('cases_dict', {}).copy()
            target_cases = target_data.get('cases_dict', {}).copy()
            if giver_cases.get(cid2, 0) < amt:
                await message.reply(f"❌ Недостаточно кейсов! Есть: {giver_cases.get(cid2, 0)}")
                return
            giver_cases[cid2] -= amt
            target_cases[cid2] = target_cases.get(cid2, 0) + amt
            update_user_data(cid, giver.id, cases_dict=giver_cases)
            update_user_data(cid, target.id, cases_dict=target_cases)
            await message.reply(f"📦 **Передача кейса**\n\n{giver.full_name} → {target.full_name}\n{case['emoji']} {case['name']} x{amt}")
            return
    
    giver_items = get_user_items(giver_data['item_counts'])
    target_items = get_user_items(target_data['item_counts'])
    found = None
    for k in giver_items.keys():
        if k.lower() == iname.lower():
            found = k
            break
    if not found:
        await message.reply(f"❌ Нет предмета '{iname}'!")
        return
    if giver_items[found] < amt:
        await message.reply(f"❌ Недостаточно! Есть: {giver_items[found]}")
        return
    
    giver_items[found] -= amt
    if giver_items[found] <= 0:
        del giver_items[found]
    target_items[found] = target_items.get(found, 0) + amt
    update_user_data(cid, giver.id, item_counts=save_user_items(giver_items))
    update_user_data(cid, target.id, item_counts=save_user_items(target_items))
    await message.reply(f"🎁 **Передача предмета**\n\n{giver.full_name} → {target.full_name}\n📦 **{found}** x{amt}")

# /дуэль
async def cmd_duel(message: Message):
    register_chat(message.chat.id)
    if not message.reply_to_message:
        await message.reply("❌ Ответьте на сообщение пользователя! Пример: `/дуэль 100`")
        return
    target = message.reply_to_message.from_user
    parts = message.text.split() if message.text else []
    if len(parts) < 2:
        await message.reply("❌ Укажите ставку! Пример: `/дуэль 100` или `/дуэль всё`")
        return
    
    cid = message.chat.id
    challenger = message.from_user
    if challenger.id == target.id or target.is_bot:
        await message.reply("❌ Нельзя вызвать на дуэль себя или бота!")
        return
    
    challenger_data = get_user_data(cid, challenger.id, challenger.full_name)
    target_data = get_user_data(cid, target.id, target.full_name)
    
    if not can_duel(challenger_data) or not can_duel(target_data):
        await message.reply("❌ Кто-то уже участвует в дуэли!")
        return
    
    amt_str = parts[1]
    if amt_str.lower() == "все":
        duel_amt = min(challenger_data['current_number'], target_data['current_number'])
        amt_text = f"всё ({duel_amt}кг)"
    else:
        try:
            duel_amt = int(amt_str)
            if duel_amt <= 0:
                await message.reply("❌ Сумма должна быть положительной!")
                return
            amt_text = f"{duel_amt}кг"
        except:
            await message.reply("❌ Укажите число или 'все'!")
            return
    
    if challenger_data['current_number'] < duel_amt or target_data['current_number'] < duel_amt:
        await message.reply("❌ У кого-то недостаточно кг!")
        return
    
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ ПРИНЯТЬ", callback_data=f"duel_accept_{challenger.id}_{target.id}_{duel_amt}"), InlineKeyboardButton(text="❌ ОТКЛОНИТЬ", callback_data=f"duel_decline_{challenger.id}_{target.id}")]])
    msg = await message.reply(f"🔫 **ВЫЗОВ НА ДУЭЛЬ!** 🔫\n\n{challenger.full_name} вызывает {target.full_name}!\nСтавка: **{amt_text}**\n\nУ вас 30 секунд!", reply_markup=kb)
    
    now = datetime.now()
    update_user_data(cid, challenger.id, duel_active=1, duel_opponent=str(target.id), duel_amount=duel_amt, duel_message_id=str(msg.message_id), duel_initiator=1, duel_start_time=now)
    update_user_data(cid, target.id, duel_active=1, duel_opponent=str(challenger.id), duel_amount=duel_amt, duel_message_id=str(msg.message_id), duel_initiator=0, duel_start_time=now)
    
    await asyncio.sleep(30)
    if get_user_data(cid, challenger.id).get('duel_active') == 1:
        update_user_data(cid, challenger.id, duel_active=0)
        update_user_data(cid, target.id, duel_active=0)
        try:
            await msg.edit_text("⏰ Время вышло! Дуэль отменена.", reply_markup=None)
        except:
            pass

@dp.callback_query(lambda c: c.data and c.data.startswith('duel_'))
async def process_duel(cb: CallbackQuery):
    await cb.answer()
    parts = cb.data.split('_')
    action = parts[1]
    
    if action == 'accept':
        chall_id = int(parts[2])
        opp_id = int(parts[3])
        amt = int(parts[4])
        if cb.from_user.id != opp_id:
            await cb.message.reply("❌ Это не ваша дуэль!")
            return
        try:
            await cb.message.delete_reply_markup()
        except:
            pass
        
        cid = cb.message.chat.id
        chall = await bot.get_chat_member(cid, chall_id)
        opp = await bot.get_chat_member(cid, opp_id)
        chall_data = get_user_data(cid, chall_id, chall.user.full_name)
        opp_data = get_user_data(cid, opp_id, opp.user.full_name)
        
        # ===== АНИМАЦИЯ ДУЭЛИ (как в Discord) =====
        c_name = chall.user.full_name[:15] + "..." if len(chall.user.full_name) > 15 else chall.user.full_name
        o_name = opp.user.full_name[:15] + "..." if len(opp.user.full_name) > 15 else opp.user.full_name
        max_len = max(len(c_name), len(o_name))
        c_name = c_name.ljust(max_len)
        o_name = o_name.ljust(max_len)
        
        duel_emojis = ["⬆️", "⬇️", "⚔️"]
        line = [random.choice(duel_emojis) for _ in range(100)]
        result = random.randint(0, 2)
        
        if result == 0:
            result_emoji = "⬆️"
            result_text = f"🏆 **Победитель:** {chall.user.full_name}"
        elif result == 1:
            result_emoji = "⬇️"
            result_text = f"🏆 **Победитель:** {opp.user.full_name}"
        else:
            result_emoji = "⚔️"
            result_text = "🤝 **НИЧЬЯ!** 🤝"
        
        line[57] = result_emoji
        anim_msg = await cb.message.reply(f"**{c_name}**\n**⚔️ ДУЭЛЬ ⚔️**\n**{o_name}**")
        
        animation_frames = [(1,5),(2,10),(3,15),(4,20),(5,25),(6,30),(7,35),(8,39),(9,43),(10,47),(11,50),(12,52),(13,54),(14,55),(15,56),(16,56),(17,57),(18,57),(19,57),(20,57)]
        
        for frame_num, center_pos in animation_frames:
            visible = line[center_pos-4:center_pos+5]
            display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
            try:
                await anim_msg.edit_text(f"**{c_name}**\n**{display_line}**\n**{o_name}**")
            except:
                pass
            await asyncio.sleep(0.5)
        
        visible = line[53:62]
        display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
        try:
            await anim_msg.edit_text(f"**{c_name}**\n**{display_line}**\n**{o_name}**\n\n{result_text}")
        except:
            pass
        await asyncio.sleep(1.5)
        # ===== КОНЕЦ АНИМАЦИИ =====
        
        if result == 0:
            winner, loser = chall.user, opp.user
            winner_new = chall_data['current_number'] + amt
            loser_new = opp_data['current_number'] - amt
            update_user_data(cid, winner.id, number=winner_new)
            update_user_data(cid, loser.id, number=loser_new)
            add_xp(cid, winner.id, XP_PER_DUEL_WIN)
            res_text = f"**Победитель:** {winner.full_name}\n\n{winner.full_name}: {chall_data['current_number']} → {winner_new} (+{amt})\n{loser.full_name}: {opp_data['current_number']} → {loser_new} (-{amt})"
        elif result == 1:
            winner, loser = opp.user, chall.user
            winner_new = opp_data['current_number'] + amt
            loser_new = chall_data['current_number'] - amt
            update_user_data(cid, winner.id, number=winner_new)
            update_user_data(cid, loser.id, number=loser_new)
            add_xp(cid, winner.id, XP_PER_DUEL_WIN)
            res_text = f"**Победитель:** {winner.full_name}\n\n{winner.full_name}: {opp_data['current_number']} → {winner_new} (+{amt})\n{loser.full_name}: {chall_data['current_number']} → {loser_new} (-{amt})"
        else:
            res_text = "🤝 **НИЧЬЯ!** 🤝\n\nНикто не потерял кг"
        
        update_user_data(cid, chall_id, duel_active=0)
        update_user_data(cid, opp_id, duel_active=0)
        await anim_msg.reply(f"⚔️ **ДУЭЛЬ ЗАВЕРШЕНА!** ⚔️\n\n{res_text}")
        
    elif action == 'decline':
        chall_id = int(parts[2])
        opp_id = int(parts[3])
        if cb.from_user.id not in [chall_id, opp_id]:
            await cb.message.reply("❌ Это не ваша дуэль!")
            return
        cid = cb.message.chat.id
        update_user_data(cid, chall_id, duel_active=0)
        update_user_data(cid, opp_id, duel_active=0)
        await cb.message.edit_text(f"❌ **Дуэль отклонена**\n\n{cb.from_user.full_name} отказался!", reply_markup=None)
        
# /отменавсё
async def cmd_cancel_all(message: Message):
    register_chat(message.chat.id)
    cid = message.chat.id
    uid = message.from_user.id
    data = get_user_data(cid, uid)
    cancelled = []
    
    if data.get('duel_active'):
        update_user_data(cid, uid, duel_active=0, duel_opponent=None, duel_amount=0, duel_message_id=None, duel_initiator=0, duel_start_time=None)
        cancelled.append("⚔️ Дуэль")
    
    if data.get('upgrade_active'):
        last_cmd = data.get('last_command')
        if last_cmd == "upgrade_select":
            items = get_user_items(data['item_counts'])
            items[data.get('last_command_target')] = items.get(data.get('last_command_target'), 0) + 1
            update_user_data(cid, uid, item_counts=save_user_items(items))
        elif last_cmd == "upgrade_kg_select":
            try:
                amt = int(data.get('last_command_target', 0))
                new_num = data['current_number'] + amt
                update_user_data(cid, uid, number=new_num)
            except:
                pass
        update_user_data(cid, uid, upgrade_active=0, upgrade_data=None, last_command=None, last_command_target=None, last_command_use_time=None)
        cancelled.append("🔧 Апгрейд")
    
    if data.get('active_case_message_id'):
        try:
            await bot.delete_message(cid, int(data['active_case_message_id']))
        except:
            pass
        update_user_data(cid, uid, active_case_message_id=None, last_case_type=None)
        cancelled.append("📦 Открытие кейса")
    
    if cancelled:
        await message.reply(f"✅ **Отменены действия:**\n" + "\n".join(cancelled))
    else:
        await message.reply("ℹ️ Нет активных действий для отмены!")

# /апгрейдюзер
async def cmd_upgrade_user(message: Message):
    register_chat(message.chat.id)
    parts = message.text.split() if message.text else []
    if len(parts) < 2:
        await message.reply("❌ Использование: `/апгрейдюзер [номер]`\n1 - КД /жир | 2 - КД кейса | 3 - Удача | 4 - Прибавка | 5 - Престиж | 6 - Авто-жир")
        return
    try:
        choice = int(parts[1])
    except:
        await message.reply("❌ Введите номер!")
        return
    if choice < 1 or choice > 6:
        await message.reply("❌ Номер от 1 до 6!")
        return
    
    cid = message.chat.id
    uid = message.from_user.id
    uname = message.from_user.full_name
    data = get_user_data(cid, uid, uname)
    
    upgrade_map = {1: "fat_cd", 2: "case_cd", 3: "luck", 4: "income", 5: "prestige", 6: "auto_fat"}
    utype = upgrade_map[choice]
    
    if utype == "fat_cd":
        cur = data.get('fat_cd_upgrade', 0)
        cost = get_upgrade_cost("fat_cd", cur)
    elif utype == "case_cd":
        cur = data.get('case_cd_upgrade', 0)
        cost = get_upgrade_cost("case_cd", cur)
    elif utype == "luck":
        cur = data.get('luck_upgrade', 0)
        cost = get_upgrade_cost("luck", cur)
    elif utype == "income":
        cur = data.get('income_upgrade', 0)
        cost = get_upgrade_cost("income", cur)
    elif utype == "prestige":
        cur = data.get('prestige', 0)
        cost = get_upgrade_cost("prestige", cur)
    else:
        cur = data.get('auto_fat_level', 0)
        if cur >= AUTO_FAT_MAX_LEVEL:
            await message.reply(f"❌ Авто-жир уже на максимуме ({AUTO_FAT_MAX_LEVEL})!")
            return
        cost = get_upgrade_cost("auto_fat", cur)
    
    if data['current_number'] < cost:
        await message.reply(f"❌ Недостаточно кг! Нужно: {cost} кг")
        return
    
    if utype == "prestige":
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ ДА, ПОЛУЧИТЬ ПРЕСТИЖ", callback_data=f"prestige_confirm_{uid}"), InlineKeyboardButton(text="❌ ОТМЕНА", callback_data=f"prestige_cancel_{uid}")]])
        await message.reply(f"⚠️ **ПРЕСТИЖ** ⚠️\n\nВы уверены?\n\n• Вес → 0\n• Предметы удалятся\n• Улучшения СОХРАНЯТСЯ\n• Опыт и уровень сохранятся\n• Престиж +1\n\nСтоимость: {cost} кг", reply_markup=kb)
        return
    
    new_lvl = cur + 1
    new_num = data['current_number'] - cost
    
    if utype == "auto_fat":
        iv = get_auto_fat_interval(new_lvl)
        next_t = datetime.now() + timedelta(hours=iv) if iv else None
        update_user_data(cid, uid, number=new_num, auto_fat_level=new_lvl, next_auto_fat_time=next_t)
        bonus = f"Авто-жир {new_lvl} уровень (каждые {iv} ч)"
    else:
        field_map = {"fat_cd": "fat_cd_upgrade", "case_cd": "case_cd_upgrade", "luck": "luck_upgrade", "income": "income_upgrade"}
        update_user_data(cid, uid, number=new_num, **{field_map[utype]: new_lvl})
        if utype == "fat_cd":
            bonus = f"КД /жир уменьшен на {get_fat_cd_reduction(new_lvl)} мин"
        elif utype == "case_cd":
            bonus = f"КД кейса уменьшен на {get_case_cd_reduction(new_lvl)} мин"
        elif utype == "luck":
            bonus = f"Удача +{new_lvl * LUCK_CASE_BONUS_PER_LEVEL:.0f}% к редким, +{new_lvl * LUCK_UPGRADE_BONUS_PER_LEVEL:.0f}% к апгрейдам"
        else:
            bonus = f"Прибавка +{new_lvl * 5}% к доходу"
    
    await message.reply(f"✅ **Улучшение получено!**\n\nПотрачено: {cost} кг\nОсталось: {new_num} кг\n\n{bonus}")

@dp.callback_query(lambda c: c.data and c.data.startswith('prestige_'))
async def process_prestige(cb: CallbackQuery):
    await cb.answer()
    action = cb.data.split('_')[1]
    uid = int(cb.data.split('_')[2])
    if cb.from_user.id != uid:
        await cb.message.reply("❌ Не ваше подтверждение!")
        return
    if action == "cancel":
        await cb.message.edit_text("❌ Престиж отменён")
        return
    
    cid = cb.message.chat.id
    uname = cb.from_user.full_name
    data = get_user_data(cid, uid, uname)
    cur = data.get('prestige', 0)
    cost = get_upgrade_cost("prestige", cur)
    if data['current_number'] < cost:
        await cb.message.edit_text(f"❌ Недостаточно кг! Нужно: {cost}")
        return
    
    new_prestige = cur + 1
    update_user_data(cid, uid, current_number=0, item_counts='{}', cases_dict={}, prestige=new_prestige, consecutive_plus=0, consecutive_minus=0, jackpot_pity=0, shadow_upgrade_chance=0)
    await cb.message.edit_text(f"🌟 **ПРЕСТИЖ ПОЛУЧЕН!** 🌟\n\n{uname} достиг {new_prestige} уровня престижа!\n\nВес сброшен до 0\nПредметы удалены\nУлучшения сохранены!\nОпыт и уровень сохранены!")

# /апгрейд
async def cmd_upgrade(message: Message):
    register_chat(message.chat.id)
    cid = message.chat.id
    uid = message.from_user.id
    uname = message.from_user.full_name
    data = get_user_data(cid, uid, uname)
    
    if data.get('upgrade_active', 0) == 1:
        await message.reply("⚠️ У вас уже есть активный апгрейд! Дождитесь его завершения или используйте `/отменавсё`.")
        return
    
    items_dict = get_user_items(data['item_counts'])
    
    # Собираем предметы, которые можно улучшить (у которых есть цена и есть в наличии)
    available_items = []
    for item_name, count in items_dict.items():
        price = get_item_price(item_name)
        if price > 0 and count > 0:
            # Проверяем, есть ли куда улучшать
            possible = get_possible_upgrades(item_name, count)
            if possible:
                available_items.append({
                    "name": item_name,
                    "count": count,
                    "price": price,
                    "emoji": ITEM_EMOJIS.get(item_name, "📦")
                })
    
    available_items.sort(key=lambda x: x["price"])
    
    if not available_items:
        await message.reply("❌ У вас нет предметов, которые можно улучшить!")
        return
    
    parts = message.text.split() if message.text else []
    
    # Если нет номера - показываем список
    if len(parts) < 2:
        resp = f"🔧 **АПГРЕЙД ПРЕДМЕТОВ** 🔧\n\n{uname}, выберите предмет для улучшения:\n"
        resp += "Используйте `/апгрейд [номер]`\n\n"
        
        for i, item in enumerate(available_items[:20], 1):
            resp += f"**{i}.** {item['emoji']} **{item['name']}** — {item['count']} шт — {item['price']} кг\n"
        
        if len(available_items) > 20:
            resp += f"\n... и ещё {len(available_items) - 20} предметов"
        
        await message.reply(resp)
        return
    
    # Выбор предмета
    try:
        item_index = int(parts[1]) - 1
        if item_index < 0 or item_index >= len(available_items):
            await message.reply(f"❌ Неверный номер! Введите число от 1 до {len(available_items)}")
            return
    except ValueError:
        await message.reply("❌ Введите корректный номер!")
        return
    
    selected = available_items[item_index]
    
    # Списываем 1 предмет из инвентаря
    items_dict[selected["name"]] -= 1
    if items_dict[selected["name"]] <= 0:
        del items_dict[selected["name"]]
    
    update_user_data(cid, uid, item_counts=save_user_items(items_dict))
    
    # Получаем возможные улучшения для этого предмета
    possible_upgrades = get_possible_upgrades(selected["name"], 1)
    
    if not possible_upgrades:
        # Возвращаем предмет обратно
        items_dict[selected["name"]] = items_dict.get(selected["name"], 0) + 1
        update_user_data(cid, uid, item_counts=save_user_items(items_dict))
        await message.reply(f"❌ Для **{selected['emoji']} {selected['name']}** нет доступных улучшений! Предмет возвращён.")
        return
    
    # Сохраняем состояние апгрейда
    update_user_data(
        cid, uid,
        last_command="upgrade_select",
        last_command_target=selected["name"],
        last_command_use_time=datetime.now(),
        upgrade_active=1,
        upgrade_data=json.dumps({
            'source_item': selected["name"],
            'source_emoji': selected['emoji'],
            'possible': possible_upgrades
        })
    )
    
    resp = f"🔧 **ВЫБОР ЦЕЛИ АПГРЕЙДА** 🔧\n\n"
    resp += f"{uname}, вы выбрали: **{selected['emoji']} {selected['name']}**\n\n"
    resp += f"Теперь выберите цель (используйте `/выбрать [номер]`):\n\n"
    
    for i, up in enumerate(possible_upgrades[:20], 1):
        resp += f"**{i}.** {up['emoji']} **{up['name']}** — {up['chance']*100:.1f}% шанс\n"
    
    if len(possible_upgrades) > 20:
        resp += f"\n... и ещё {len(possible_upgrades) - 20} вариантов"
    
    await message.reply(resp)

# /апгрейдкг
async def cmd_upgrade_kg(message: Message):
    parts = message.text.split() if message.text else []
    if len(parts) < 2:
        await message.reply("❌ Использование: `/апгрейдкг [количество кг]`")
        return
    try:
        amt = int(parts[1])
    except:
        await message.reply("❌ Введите число!")
        return
    if amt <= 0:
        await message.reply("❌ Количество > 0!")
        return
    
    cid = message.chat.id
    uid = message.from_user.id
    data = get_user_data(cid, uid)
    if data['current_number'] < amt:
        await message.reply(f"❌ Недостаточно кг! Есть: {data['current_number']}")
        return
    
    all_items = set([it["name"] for it in SHOP_ITEMS] + list(LEGENDARY_UPGRADE_PRICES.keys()))
    possible = []
    for iname in all_items:
        target = get_item_price(iname)
        if target == 0 or target < amt:
            continue
        chance = amt / target
        if chance < 0.01:
            continue
        is_case = any(case.get("name") == iname for case in CASES.values())
        case_id = next((cid2 for cid2, case in CASES.items() if case.get("name") == iname), None)
        possible.append({"name": iname, "price": target, "chance": chance, "emoji": ITEM_EMOJIS.get(iname, "🎁"), "is_case": is_case, "case_id": case_id})
    possible.sort(key=lambda x: x["price"])
    
    if not possible:
        await message.reply(f"❌ На {amt} кг нет доступных улучшений!")
        return
    
    resp = f"💱 **АПГРЕЙД {amt} КГ** 💱\n\nВыберите цель:\n"
    for i, p in enumerate(possible[:15], 1):
        resp += f"**{i}.** {p['emoji']} {p['name']} — {p['chance']*100:.1f}% (нужно: {p['price']} кг)\n"
    resp += f"\nИспользуйте `/выбрать [номер]`\n💸 {amt} кг уже списаны!"
    
    new_num = data['current_number'] - amt
    update_user_data(cid, uid, number=new_num, last_command="upgrade_kg_select", last_command_target=str(amt), last_command_use_time=datetime.now(), upgrade_active=1, upgrade_data=json.dumps({'amount': amt, 'possible': possible}))
    await message.reply(resp)

# /выбрать
async def cmd_choose(message: Message):
    parts = message.text.split() if message.text else []
    if len(parts) < 2:
        await message.reply("❌ Укажите номер!")
        return
    try:
        choice = int(parts[1]) - 1
    except:
        await message.reply("❌ Введите число!")
        return
    
    cid = message.chat.id
    uid = message.from_user.id
    data = get_user_data(cid, uid)
    
    if data.get('upgrade_active') != 1:
        await message.reply("❌ Нет активного апгрейда!")
        return
    
    last_cmd = data.get('last_command')
    last_use = data.get('last_command_use_time')
    if isinstance(last_use, str):
        last_use = datetime.fromisoformat(last_use) if last_use else None
    if not last_cmd or not last_use or (datetime.now() - last_use).total_seconds() > 300:
        await message.reply("❌ Время истекло!")
        update_user_data(cid, uid, upgrade_active=0)
        return
    
    if last_cmd == "upgrade_kg_select":
        up_data = json.loads(data.get('upgrade_data', '{}'))
        possible = up_data.get('possible', [])
        amt = up_data.get('amount', 0)
        if choice < 0 or choice >= len(possible):
            await message.reply(f"❌ Номер от 1 до {len(possible)}!")
            return
        target = possible[choice]
        
        data = get_user_data(cid, uid)
        shadow = data.get('shadow_upgrade_chance', 0)
        pb = 1 + get_prestige_luck(data.get('prestige', 0))
        lb = 1 + (data.get('luck_upgrade', 0) * LUCK_UPGRADE_BONUS_PER_LEVEL / 100)
        real = min(target['chance'] * pb * lb + shadow / 100, 1.0)
        
        if random.random() < real:
            new_shadow = max(0, shadow - 8)
            if target.get('is_case'):
                cases = data.get('cases_dict', {}).copy()
                cases[target['case_id']] = cases.get(target['case_id'], 0) + 1
                update_user_data(cid, uid, cases_dict=cases, shadow_upgrade_chance=new_shadow, upgrade_active=0)
            else:
                items = get_user_items(data['item_counts'])
                items[target['name']] = items.get(target['name'], 0) + 1
                update_user_data(cid, uid, item_counts=save_user_items(items), shadow_upgrade_chance=new_shadow, upgrade_active=0)
            add_xp(cid, uid, XP_PER_UPGRADE_KG)
            await message.reply(f"✅ **УСПЕХ!** {amt} кг → {target['emoji']} {target['name']}")
        else:
            new_shadow = min(32, shadow + 4)
            update_user_data(cid, uid, shadow_upgrade_chance=new_shadow, upgrade_active=0)
            await message.reply(f"❌ **НЕУДАЧА!** {amt} кг сгорели!")
    else:
        await message.reply("❌ Неизвестный тип апгрейда!")

# Тестерские команды
async def cmd_reset_cooldowns(message: Message):
    if not is_tester(message.from_user.id):
        await message.reply("❌ Нет прав!")
        return
    cid = message.chat.id
    db_path = get_db_path(cid)
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute('UPDATE user_fat SET fat_cooldown_time = NULL, last_case_time = NULL, daily_case_last_time = NULL')
        affected = cur.rowcount
        conn.commit()
        conn.close()
        await message.reply(f"🔄 Кулдауны сброшены для {affected} пользователей!")

async def cmd_reset_all_users(message: Message):
    if not is_tester(message.from_user.id):
        await message.reply("❌ Нет прав!")
        return
    cid = message.chat.id
    db_path = get_db_path(cid)
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute('UPDATE user_fat SET current_number = 0, consecutive_plus = 0, consecutive_minus = 0, jackpot_pity = 0, item_counts = "{}"')
        affected = cur.rowcount
        conn.commit()
        conn.close()
        await message.reply(f"⚖️ Вес всех сброшен на 0! Затронуто: {affected}")

async def cmd_fat_reset(message: Message):
    if not is_tester(message.from_user.id):
        await message.reply("❌ Нет прав!")
        return
    if not message.reply_to_message:
        await message.reply("❌ Ответьте на сообщение пользователя!")
        return
    target = message.reply_to_message.from_user
    cid = message.chat.id
    update_user_data(cid, target.id, number=0, consecutive_plus=0, consecutive_minus=0, jackpot_pity=0, item_counts='{}')
    await message.reply(f"✅ Вес {target.full_name} сброшен на 0kg")

async def cmd_give_shop_item(message: Message):
    if not is_tester(message.from_user.id):
        await message.reply("❌ Нет прав!")
        return
    parts = message.text.split() if message.text else []
    if len(parts) < 3:
        await message.reply("❌ Использование: `/выдатьпредмет количество название`")
        return
    try:
        amt = int(parts[1])
        iname = ' '.join(parts[2:])
    except:
        await message.reply("❌ Неверный формат!")
        return
    if amt <= 0 or amt > 1000:
        await message.reply("❌ Количество от 1 до 1000!")
        return
    
    cid = message.chat.id
    uid = message.from_user.id
    data = get_user_data(cid, uid)
    
    for cid2, case in CASES.items():
        if cid2 != "daily" and case["name"].lower() == iname.lower():
            cases = data.get('cases_dict', {}).copy()
            cases[cid2] = cases.get(cid2, 0) + amt
            update_user_data(cid, uid, cases_dict=cases)
            await message.reply(f"🎁 Выдан кейс: {case['name']} x{amt}")
            return
    
    for it in SHOP_ITEMS:
        if it["name"].lower() == iname.lower():
            items = get_user_items(data['item_counts'])
            items[it["name"]] = items.get(it["name"], 0) + amt
            update_user_data(cid, uid, item_counts=save_user_items(items))
            await message.reply(f"🎁 Выдан предмет: {it['name']} x{amt}")
            return
    
    await message.reply(f"❌ Предмет '{iname}' не найден!")

# /жирхелп
async def cmd_help(message: Message):
    register_chat(message.chat.id)
    resp = """🍔 **ЖИРБОТ - ПОМОЩЬ** 🍔

**Основные команды:**
/жир - изменить свой вес
/жиркейс - открыть кейс
/жиркейс_шансы - шансы в ежедневном кейсе
/жиротрясы - таблица рекордов
/профиль - профиль и прокачка
/жирзвания - список званий
/жиркулдаун - статус кулдаунов
/инвентарь - посмотреть инвентарь
/жирглобал - глобальный рейтинг чатов

**Дуэли:**
/дуэль [@username] [кг/"все"] - вызвать на дуэль (ответом на сообщение)
/отменавсё - отменить все активные действия

**Апгрейды:**
/апгрейд - улучшить предмет
/апгрейдкг [кол-во] - улучшить кг в предмет
/выбрать [номер] - выбрать цель апгрейда
/апгрейдюзер [номер] - улучшить характеристики

**Экономика:**
/магазин - магазин предметов
/купить [слот] [кол-во] - купить предмет
/продать [предмет] [кол-во] - продать предмет
/датьжир [@user] [кол-во] - передать кг (ответом на сообщение)
/датьпредмет [@user] [кол-во] [предмет] - передать предмет (ответом)

**Тестерские:**
/сброскд - сбросить кулдауны всем
/сбросвсех - сбросить вес всех на 0
/выдатьпредмет [кол-во] [предмет] - выдать предмет себе
/жир_сброс - сбросить вес пользователя (ответом)

⭐ **ХАРАКТЕРИСТИКИ** ⭐
• КД /жир — уменьшает время ожидания
• КД кейса — уменьшает время ожидания кейса
• Удача — повышает шансы в кейсах и апгрейдах
• Прибавка — +5% к доходу от предметов за уровень
• Престиж — +10% ко всем кг, +1% к шансам и +50% к опыту за уровень
• Авто-жир — автоматический /жир

🎬 **НАСТРОЙКИ АНИМАЦИЙ** 🎬
Анимации можно включить/выключить командой /анимации"""
    await message.reply(resp)

# ===== ЗАПУСК =====
async def on_startup():
    print("\n✅ TELEGRAM БОТ ЗАПУЩЕН")
    os.makedirs(DB_FOLDER, exist_ok=True)
    for cid in list(active_chats):
        try:
            await migrate_old_data(cid)
        except:
            pass
    asyncio.create_task(auto_fat_loop())
    asyncio.create_task(passive_income_loop())
    asyncio.create_task(snatcher_loop())
    asyncio.create_task(hourly_effects_loop())

async def main():
    # Регистрация всех команд
    dp.message.register(cmd_help, Command("start"))
    dp.message.register(cmd_help, Command("help"))
    dp.message.register(cmd_help, Command("жирхелп"))
    dp.message.register(cmd_fat, Command("жир"))
    dp.message.register(cmd_fat_case, Command("жиркейс"))
    dp.message.register(cmd_fat_case_chances, Command("жиркейс_шансы"))
    dp.message.register(cmd_fat_leaderboard, Command("жиротрясы"))
    dp.message.register(cmd_profile, Command("профиль"))
    dp.message.register(cmd_show_ranks, Command("жирзвания"))
    dp.message.register(cmd_cooldown_info, Command("жиркулдаун"))
    dp.message.register(cmd_show_inventory, Command("инвентарь"))
    dp.message.register(cmd_global_leaderboard, Command("жирглобал"))
    dp.message.register(cmd_shop, Command("магазин"))
    dp.message.register(cmd_buy, Command("купить"))
    dp.message.register(cmd_sell, Command("продать"))
    dp.message.register(cmd_give_fat, Command("датьжир"))
    dp.message.register(cmd_give_item, Command("датьпредмет"))
    dp.message.register(cmd_duel, Command("дуэль"))
    dp.message.register(cmd_cancel_all, Command("отменавсё"))
    dp.message.register(cmd_upgrade_user, Command("апгрейдюзер"))
    dp.message.register(cmd_upgrade, Command("апгрейд"))
    dp.message.register(cmd_upgrade_kg, Command("апгрейдкг"))
    dp.message.register(cmd_choose, Command("выбрать"))
    dp.message.register(cmd_animations, Command("анимации"))
    dp.message.register(cmd_reset_cooldowns, Command("сброскд"))
    dp.message.register(cmd_reset_all_users, Command("сбросвсех"))
    dp.message.register(cmd_fat_reset, Command("жир_сброс"))
    dp.message.register(cmd_give_shop_item, Command("выдатьпредмет"))
    
    await on_startup()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
