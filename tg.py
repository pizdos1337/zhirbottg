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
TESTER_IDS = [1776742823]  # ← ТВОЙ ID

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
JACKPOT_MIN = 500
JACKPOT_MAX = 1000

# Настройки кейса
CASE_COOLDOWN_HOURS = 24

# Призы в ежедневном кейсе
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

total_chance = sum(prize["chance"] for prize in CASE_PRIZES)
for prize in CASE_PRIZES:
    prize["normalized_chance"] = (prize["chance"] / total_chance) * 100

# Настройки Авто-жира
AUTO_FAT_INTERVALS = {1: 6, 2: 3, 3: 1, 4: 0.5, 5: 0.25, 6: 0.1}
AUTO_FAT_BASE_COST = 500
AUTO_FAT_COST_INCREMENT = 500
AUTO_FAT_MAX_LEVEL = 6

# Настройки престижа
PRESTIGE_BONUS_PER_LEVEL = 0.10
PRESTIGE_LUCK_PER_LEVEL = 0.01
PRESTIGE_BASE_COST = 2000
PRESTIGE_COST_INCREMENT = 1000

# Настройки прибавки
INCOME_BONUS_PER_LEVEL = 0.05
INCOME_BASE_COST = 250
INCOME_COST_INCREMENT = 100

# Настройки удачи
LUCK_CASE_BONUS_PER_LEVEL = 0.25
LUCK_UPGRADE_BONUS_PER_LEVEL = 0.5
LUCK_BASE_COST = 1000
LUCK_COST_INCREMENT = 500

# Настройки КД !жир
FAT_CD_REDUCTION_PER_LEVEL = 5
FAT_CD_BASE_COST = 150
FAT_CD_COST_INCREMENT = 50

# Настройки КД кейса
CASE_CD_REDUCTION_PER_LEVEL = 60
CASE_CD_BASE_COST = 100
CASE_CD_COST_INCREMENT = 100

# Настройки опыта
XP_PER_FAT = 30
XP_PER_UPGRADE = 50
XP_PER_UPGRADE_KG = 40
XP_PER_CASE = 100
XP_PER_DUEL_WIN = 100
XP_PER_SHOP_BUY = 20
LEVEL_UP_REWARD_PER_LEVEL = 15

# ===== НАСТРОЙКИ КЕЙСОВ =====
CASES = {
    "daily": {"name": "Жиркейс", "emoji": "📦", "tradable": True, "daily": True, "prizes": CASE_PRIZES},
    "chicken": {"name": "Коробка от чикенбургера", "emoji": "🍗", "tradable": True, "daily": False, "shop_chance": 0.3, "min_shop": 1, "max_shop": 3, "price": 10, "prizes": [{"value": -10, "chance": 20, "emoji": "📉"}, {"value": 0, "chance": 30, "emoji": "🔄"}, {"value": 10, "chance": 20, "emoji": "📈"}, {"value": 15, "chance": 10, "emoji": "📈"}, {"value": 20, "chance": 10, "emoji": "⬆️"}, {"value": 25, "chance": 10, "emoji": "⬆️"}]},
    "bigmac": {"name": "Коробка от Биг Мака", "emoji": "🍔", "tradable": True, "daily": False, "shop_chance": 0.25, "min_shop": 1, "max_shop": 3, "price": 15, "prizes": [{"value": -15, "chance": 20, "emoji": "📉"}, {"value": 0, "chance": 30, "emoji": "🔄"}, {"value": 15, "chance": 20, "emoji": "📈"}, {"value": 20, "chance": 10, "emoji": "⬆️"}, {"value": 25, "chance": 10, "emoji": "⬆️"}, {"value": 30, "chance": 10, "emoji": "🚀"}]},
    "whopper": {"name": "Коробка от Воппера", "emoji": "🔥", "tradable": True, "daily": False, "shop_chance": 0.23, "min_shop": 1, "max_shop": 3, "price": 25, "prizes": [{"value": -25, "chance": 20, "emoji": "📉"}, {"value": 0, "chance": 30, "emoji": "🔄"}, {"value": 25, "chance": 20, "emoji": "📈"}, {"value": 30, "chance": 10, "emoji": "🚀"}, {"value": 40, "chance": 9, "emoji": "🚀"}, {"value": 50, "chance": 1, "emoji": "💫"}]},
    "green_whopper": {"name": "Коробка от Зеленого Воппера", "emoji": "💚", "tradable": True, "daily": False, "shop_chance": 0.17, "min_shop": 1, "max_shop": 2, "price": 50, "prizes": [{"value": -25, "chance": 20, "emoji": "📉"}, {"value": 0, "chance": 10, "emoji": "🔄"}, {"value": 10, "chance": 20, "emoji": "📈"}, {"value": 30, "chance": 10, "emoji": "🚀"}, {"value": 50, "chance": 10, "emoji": "💫"}, {"value": 100, "chance": 9, "emoji": "⭐"}, {"value": 250, "chance": 1, "emoji": "💥"}]},
    "burger_pizza": {"name": "Коробка от Бургер пиццы", "emoji": "🍕", "tradable": True, "daily": False, "shop_chance": 0.15, "min_shop": 1, "max_shop": 2, "price": 100, "prizes": [{"value": -10, "chance": 20, "emoji": "📉"}, {"value": 0, "chance": 10, "emoji": "🔄"}, {"value": 30, "chance": 20, "emoji": "🚀"}, {"value": 50, "chance": 30, "emoji": "💫"}, {"value": 100, "chance": 5, "emoji": "⭐"}, {"value": 250, "chance": 5, "emoji": "⭐"}, {"value": 500, "chance": 4, "emoji": "💥"}, {"value": 1000, "chance": 1, "emoji": "💥"}]},
    "mcguffin": {"name": "Коробка от МакГаффина", "emoji": "🎁", "tradable": True, "daily": False, "shop_chance": 0.1, "min_shop": 1, "max_shop": 1, "price": 200, "prizes": [{"value": 100, "chance": 80, "emoji": "⭐"}, {"value": 200, "chance": 5, "emoji": "💥"}, {"value": 250, "chance": 5, "emoji": "💥"}, {"value": 500, "chance": 5, "emoji": "💥"}, {"value": 750, "chance": 1, "emoji": "✨"}, {"value": 1000, "chance": 1, "emoji": "✨"}, {"value": 1200, "chance": 1, "emoji": "✨"}, {"value": 1500, "chance": 1, "emoji": "✨"}]},
    "autoburger_pack": {"name": "Упаковка Автобургера", "emoji": "🍔📦", "tradable": True, "daily": False, "shop_chance": 0.05, "min_shop": 1, "max_shop": 5, "price": 250, "prizes": [{"value": 0, "chance": 98, "emoji": "🔄"}, {"value": "auto_fat", "chance": 2, "emoji": "🤖"}]},
    "rotten_pack": {"name": "Упаковка Гнилой Ножки KFC", "emoji": "💀📦", "tradable": True, "daily": False, "shop_chance": 0.1, "min_shop": 1, "max_shop": 10, "price": 100, "prizes": [{"value": 0, "chance": 90, "emoji": "🔄"}, {"value": "rotten_leg", "chance": 10, "emoji": "💀"}]},
    "water_pack": {"name": "Упаковка Стакана Воды", "emoji": "💧📦", "tradable": True, "daily": False, "shop_chance": 0.1, "min_shop": 1, "max_shop": 10, "price": 100, "prizes": [{"value": 0, "chance": 90, "emoji": "🔄"}, {"value": "water", "chance": 10, "emoji": "💧"}]}
}

# ===== НАСТРОЙКИ МАГАЗИНА =====
SHOP_SLOTS = 10
SHOP_UPDATE_HOURS = 12

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
    {"name": "Автохолестерол", "chance": 0.05, "min_amount": 1, "max_amount": 1, "price": 1000, "gain_per_24h": 0, "description": "💊 Даёт от 1кг до 10кг в час", "effect": "auto_cholesterol", "effect_value": (1, 10), "effect_type": "hourly"},
    {"name": "Холестеринимус", "chance": 0.05, "min_amount": 1, "max_amount": 1, "price": 500, "gain_per_24h": 0, "description": "💊 Даёт от 1кг до 5кг в час", "effect": "cholesterol", "effect_value": (1, 5), "effect_type": "hourly"},
    {"name": "Яблоко", "chance": 0.05, "min_amount": 1, "max_amount": 1, "price": 500, "gain_per_24h": 0, "description": "🍎 Уменьшает кулдаун /жир на 5% за штуку", "effect": "fat_cooldown_reduction", "effect_value": 0.05, "effect_type": "passive"},
    {"name": "Апельсин", "chance": 0.05, "min_amount": 1, "max_amount": 1, "price": 750, "gain_per_24h": 0, "description": "🍊 Уменьшает кулдаун /жиркейс на 5% за штуку", "effect": "case_cooldown_reduction", "effect_value": 0.05, "effect_type": "passive"},
    {"name": "Золотое Яблоко", "chance": 0.01, "min_amount": 1, "max_amount": 1, "price": 1000, "gain_per_24h": 0, "description": "🍎✨ Уменьшает кулдаун /жир на 10% за штуку", "effect": "fat_cooldown_reduction", "effect_value": 0.10, "effect_type": "passive"},
    {"name": "Золотой Апельсин", "chance": 0.01, "min_amount": 1, "max_amount": 1, "price": 1000, "gain_per_24h": 0, "description": "🍊✨ Уменьшает кулдаун /жиркейс на 10% за штуку", "effect": "case_cooldown_reduction", "effect_value": 0.10, "effect_type": "passive"},
    {"name": "Драгонфрукт", "chance": 0.01, "min_amount": 1, "max_amount": 1, "price": 1000, "gain_per_24h": 0, "description": "🐉🍈 Повышает шанс джекпота на 1% за штуку", "effect": "jackpot_boost", "effect_value": 0.01, "effect_type": "passive"},
    {"name": "Золотой Драгонфрукт", "chance": 0.005, "min_amount": 1, "max_amount": 1, "price": 3000, "gain_per_24h": 0, "description": "🐉🍈✨ Повышает шанс джекпота на 5% за штуку", "effect": "jackpot_boost", "effect_value": 0.05, "effect_type": "passive"},
    {"name": "Снатчер", "chance": 0.001, "min_amount": 1, "max_amount": 1, "price": 2000, "gain_per_24h": 0, "description": "👾 **СНАТЧЕР** 👾"},
]

ITEM_EMOJIS = {item["name"]: "📦" for item in SHOP_ITEMS}
ITEM_EMOJIS.update({"Снатчер": "👾", "Святой сэндвич": "✨", "Гнилая ножка KFC": "💀", "Стакан воды": "💧", "Автохолестерол": "💊", "Холестеринимус": "💊", "Яблоко": "🍎", "Апельсин": "🍊", "Золотое Яблоко": "🍎✨", "Золотой Апельсин": "🍊✨", "Драгонфрукт": "🐉🍈", "Золотой Драгонфрукт": "🐉🍈✨"})

# Добавляем магазинный кейс
CASES["shop_case"] = {"name": "Магазинный кейс", "emoji": "🏪", "tradable": True, "daily": False, "shop_chance": 0.2, "min_shop": 1, "max_shop": 5, "price": 100, "prizes": []}
shop_case_prizes = [{"value": item["name"], "chance": item["chance"] * 100, "emoji": ITEM_EMOJIS.get(item["name"], "🎁"), "name": item["name"]} for item in SHOP_ITEMS]
total = sum(p["chance"] for p in shop_case_prizes)
if total < 100:
    shop_case_prizes.append({"value": 0, "chance": 100 - total, "emoji": "🔄", "name": "Ничего"})
else:
    for prize in shop_case_prizes:
        prize["chance"] = (prize["chance"] / total) * 100
CASES["shop_case"]["prizes"] = shop_case_prizes

# ===== ТЕНЕВАЯ СТОИМОСТЬ ДЛЯ РАСЧЕТА ШАНСОВ АПГРЕЙДА =====
LEGENDARY_UPGRADE_PRICES = {
    "Святой сэндвич": 20000, "Гнилая ножка KFC": 5000, "Стакан воды": 3000,
    "Автохолестерол": 5000, "Холестеринимус": 2500, "Яблоко": 1500,
    "Золотое Яблоко": 3000, "Апельсин": 2000, "Золотой Апельсин": 4000,
    "Драгонфрукт": 4000, "Золотой Драгонфрукт": 8000, "Снатчер": 20000
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

# ===== ФУНКЦИИ ДЛЯ XP И УРОВНЕЙ =====
def get_xp_for_next_level(level):
    return (50 * (level + 1)) + ((level + 1) * 5)

def get_level_and_xp(total_xp):
    level = 0
    remaining_xp = total_xp
    while True:
        needed = get_xp_for_next_level(level)
        if remaining_xp < needed:
            break
        remaining_xp -= needed
        level += 1
    return level, remaining_xp

def add_xp(chat_id, user_id, xp_amount):
    data = get_user_data(chat_id, user_id)
    old_level = data.get('user_level', 0)
    new_total_xp = data.get('user_xp', 0) + xp_amount
    new_level, current_xp = get_level_and_xp(new_total_xp)
    total_kg_reward = 0
    for level in range(old_level + 1, new_level + 1):
        total_kg_reward += LEVEL_UP_REWARD_PER_LEVEL * level
    new_weight = data['current_number'] + total_kg_reward
    update_user_data(chat_id, user_id, user_xp=new_total_xp, user_level=new_level, number=new_weight)
    return new_level - old_level, total_kg_reward, new_level

def format_user_with_prestige(prestige, weight, user_name):
    if prestige > 0:
        return f"{prestige}🌟 {weight}kg {user_name}"
    return f"{weight}kg {user_name}"

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

# ===== РАБОТА С БАЗОЙ ДАННЫХ =====
def get_db_path(chat_id):
    return os.path.join(DB_FOLDER, f"chat_{chat_id}.db")

def add_missing_columns(db_path, existing_columns):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    required_columns = {
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
        'shadow_upgrade_chance': "INTEGER DEFAULT 0",
        'user_xp': "INTEGER DEFAULT 0",
        'user_level': "INTEGER DEFAULT 0",
        'fat_cd_upgrade': "INTEGER DEFAULT 0",
        'case_cd_upgrade': "INTEGER DEFAULT 0",
        'luck_upgrade': "INTEGER DEFAULT 0",
        'income_upgrade': "INTEGER DEFAULT 0",
        'prestige': "INTEGER DEFAULT 0",
        'auto_fat_level': "INTEGER DEFAULT 0",
        'next_auto_fat_time': "TIMESTAMP",
        'last_passive_income': "TIMESTAMP",
        'last_hourly_income': "TIMESTAMP"
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
        last_case_time TIMESTAMP, 
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
        duel_start_time TIMESTAMP,
        shadow_upgrade_chance INTEGER DEFAULT 0,
        user_xp INTEGER DEFAULT 0,
        user_level INTEGER DEFAULT 0,
        fat_cd_upgrade INTEGER DEFAULT 0,
        case_cd_upgrade INTEGER DEFAULT 0,
        luck_upgrade INTEGER DEFAULT 0,
        income_upgrade INTEGER DEFAULT 0,
        prestige INTEGER DEFAULT 0,
        auto_fat_level INTEGER DEFAULT 0,
        next_auto_fat_time TIMESTAMP,
        last_passive_income TIMESTAMP,
        last_hourly_income TIMESTAMP
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
        'consecutive_plus', 'consecutive_minus', 'jackpot_pity', 'last_case_time',
        'item_counts', 'last_command', 'last_command_target',
        'last_command_use_time', 'fat_cooldown_time', 'active_case_message_id',
        'daily_case_last_time', 'snatcher_last_time', 'duel_active', 'duel_opponent',
        'duel_amount', 'duel_message_id', 'duel_initiator',
        'last_case_type', 'last_case_prize', 'upgrade_active', 'upgrade_data', 'duel_start_time',
        'shadow_upgrade_chance', 'user_xp', 'user_level', 'fat_cd_upgrade',
        'case_cd_upgrade', 'luck_upgrade', 'income_upgrade', 'prestige',
        'auto_fat_level', 'next_auto_fat_time', 'last_passive_income', 'last_hourly_income'
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
            'last_case_time': None,
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
            'shadow_upgrade_chance': 0,
            'user_xp': 0,
            'user_level': 0,
            'fat_cd_upgrade': 0,
            'case_cd_upgrade': 0,
            'luck_upgrade': 0,
            'income_upgrade': 0,
            'prestige': 0,
            'auto_fat_level': 0,
            'next_auto_fat_time': None,
            'last_passive_income': None,
            'last_hourly_income': None,
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
                   'consecutive_plus', 'consecutive_minus', 'jackpot_pity', 'last_case_time',
                   'item_counts', 'last_command', 'last_command_target',
                   'last_command_use_time', 'fat_cooldown_time', 'active_case_message_id',
                   'daily_case_last_time', 'snatcher_last_time', 'duel_active', 'duel_opponent',
                   'duel_amount', 'duel_message_id', 'duel_initiator',
                   'last_case_type', 'last_case_prize', 'upgrade_active', 'upgrade_data', 'duel_start_time',
                   'shadow_upgrade_chance', 'user_xp', 'user_level', 'fat_cd_upgrade',
                   'case_cd_upgrade', 'luck_upgrade', 'income_upgrade', 'prestige',
                   'auto_fat_level', 'next_auto_fat_time', 'last_passive_income', 'last_hourly_income']
    
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

def get_all_users_sorted(chat_id):
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA table_info(user_fat)")
    columns = [col[1] for col in cursor.fetchall()]
    
    select_cols = ['user_name', 'current_number', 'last_command_time', 
                   'consecutive_plus', 'consecutive_minus', 'jackpot_pity']
    
    if 'prestige' in columns:
        select_cols.append('prestige')
    
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
    
    return {
        'total_users': total_users, 'total_weight': total_weight, 'avg_weight': avg_weight,
        'positive': positive, 'negative': negative, 'zero': zero
    }

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

# ===== ОБНОВЛЁННАЯ ФУНКЦИЯ !ЖИР С УЧЁТОМ ПРЕСТИЖА И УДАЧИ =====
def get_change_with_pity_and_jackpot(consecutive_plus, consecutive_minus, jackpot_pity, 
                                      luck_upgrade=0, prestige_bonus=1.0, items_dict=None, 
                                      current_weight=None):
    if items_dict is None:
        items_dict = {}
    
    has_rotten_leg = items_dict.get("Гнилая ножка KFC", 0) > 0
    has_holy_sandwich = items_dict.get("Святой сэндвич", 0) > 0
    has_water = items_dict.get("Стакан воды", 0) > 0
    
    minus_boost = min(consecutive_minus * CONSECUTIVE_MINUS_BOOST, MAX_CONSECUTIVE_MINUS_BOOST)
    minus_chance = BASE_MINUS_CHANCE + (consecutive_plus * PITY_INCREMENT) - minus_boost
    minus_chance = max(0.1, min(minus_chance, MAX_MINUS_CHANCE))
    
    jackpot_chance = BASE_JACKPOT_CHANCE + (jackpot_pity * JACKPOT_PITY_INCREMENT)
    
    if has_holy_sandwich:
        sandwich_count = items_dict.get("Святой сэндвич", 0)
        sandwich_bonus = 0.3 * sandwich_count
        jackpot_chance = max(jackpot_chance, sandwich_bonus)
        jackpot_chance = min(jackpot_chance, 0.9)
    else:
        jackpot_chance = min(jackpot_chance, MAX_JACKPOT_CHANCE)
    
    if has_water:
        jackpot_roll = random.random()
        if jackpot_roll < jackpot_chance:
            change = random.randint(JACKPOT_MIN, JACKPOT_MAX) // 3
            change = int(change * prestige_bonus)
            new_consecutive_plus = consecutive_plus + 1
            new_consecutive_minus = 0
            new_jackpot_pity = 0
            was_minus = False
            was_jackpot = True
            return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot
        else:
            change = random.randint(1, 20) // 3
            change = int(change * prestige_bonus)
            new_consecutive_plus = consecutive_plus + 1
            new_consecutive_minus = 0
            new_jackpot_pity = jackpot_pity + 1
            was_minus = False
            was_jackpot = False
            return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot
    
    elif has_rotten_leg:
        if random.random() < 0.6:
            if current_weight is not None:
                loss = int(current_weight * 0.5)
                change = -loss
            else:
                change = -int(consecutive_plus * 0.5)
            change = int(change * prestige_bonus)
            new_consecutive_plus = 0
            new_consecutive_minus = consecutive_minus + 1
            new_jackpot_pity = jackpot_pity + 1
            was_minus = True
            was_jackpot = False
            return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot
        else:
            change = random.randint(JACKPOT_MIN, JACKPOT_MAX)
            change = int(change * prestige_bonus)
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
            change = int(change * prestige_bonus)
            new_consecutive_plus = consecutive_plus + 1
            new_consecutive_minus = 0
            new_jackpot_pity = 0
            was_minus = False
            was_jackpot = True
            return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot
        
        roll = random.random()
        if roll < minus_chance:
            change = random.randint(-20, -1)
            change = int(change * prestige_bonus)
            new_consecutive_plus = 0
            new_consecutive_minus = consecutive_minus + 1
            new_jackpot_pity = jackpot_pity + 1
            was_minus = True
            was_jackpot = False
        else:
            change = random.randint(1, 20)
            change = int(change * prestige_bonus)
            new_consecutive_plus = consecutive_plus + 1
            new_consecutive_minus = 0
            new_jackpot_pity = jackpot_pity + 1
            was_minus = False
            was_jackpot = False
        
        return change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot

# ===== ОБНОВЛЁННАЯ ФУНКЦИЯ ОТКРЫТИЯ КЕЙСА С УЧЁТОМ УДАЧИ =====
def open_case(case_id, prestige_luck=0, luck_upgrade=0):
    case = CASES[case_id]
    prizes = case["prizes"]
    
    total_chance = sum(p["chance"] for p in prizes)
    for prize in prizes:
        prize["normalized_chance"] = (prize["chance"] / total_chance) * 100
    
    bonus = (prestige_luck * 100) + (luck_upgrade * LUCK_CASE_BONUS_PER_LEVEL)
    
    modified_prizes = []
    for prize in prizes:
        p = prize.copy()
        if (isinstance(p["value"], int) and p["value"] >= 100) or p["value"] in ["rotten_leg", "water", "auto_fat"]:
            p["normalized_chance"] = prize["normalized_chance"] + bonus
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

# ===== ФУНКЦИИ ДЛЯ МАГАЗИНА =====
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
                
                min_prize = min([p["value"] for p in case["prizes"] if isinstance(p["value"], int)] + [0])
                max_prize = max([p["value"] for p in case["prizes"] if isinstance(p["value"], int)] + [0])
                
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

# ===== ФУНКЦИИ ДЛЯ ДУЭЛЕЙ =====
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

# ===== ФУНКЦИИ ДЛЯ АПГРЕЙДОВ =====
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
    
    all_items = set([item["name"] for item in SHOP_ITEMS] + list(LEGENDARY_UPGRADE_PRICES.keys()))
    
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
    active_chats.add(chat_id)

# ===== ФУНКЦИИ ДЛЯ ФОНОВЫХ ЗАДАЧ =====
def get_users_with_auto_fat(chat_id):
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''SELECT user_id, user_name, auto_fat_level, next_auto_fat_time 
                    FROM user_fat WHERE auto_fat_level > 0 AND next_auto_fat_time IS NOT NULL''')
    users = cursor.fetchall()
    conn.close()
    return users

def get_users_with_items(chat_id):
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''SELECT user_id, user_name, current_number, item_counts, income_upgrade, prestige, last_passive_income 
                    FROM user_fat WHERE item_counts != '{}' AND item_counts IS NOT NULL''')
    users = cursor.fetchall()
    conn.close()
    return users

def get_users_with_snatcher(chat_id):
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''SELECT user_id, user_name, item_counts, snatcher_last_time 
                    FROM user_fat WHERE item_counts LIKE '%"Снатчер"%' ''')
    users = cursor.fetchall()
    conn.close()
    return users

def get_users_with_hourly_items(chat_id):
    safe_init_chat_database(chat_id, f"Chat_{chat_id}")
    db_path = get_db_path(chat_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''SELECT user_id, user_name, current_number, item_counts, income_upgrade, prestige, last_hourly_income 
                    FROM user_fat''')
    users = cursor.fetchall()
    conn.close()
    return users

async def apply_auto_fat(chat_id, user_id, user_name):
    try:
        data = get_user_data(chat_id, user_id, user_name)
        items_dict = get_user_items(data['item_counts'])
        prestige_bonus = get_prestige_bonus(data.get('prestige', 0))
        
        change, was_minus, new_plus, new_minus, new_pity, was_jackpot = get_change_with_pity_and_jackpot(
            data['consecutive_plus'], data['consecutive_minus'], data['jackpot_pity'], 
            data.get('luck_upgrade', 0), prestige_bonus, items_dict, data['current_number'])
        
        new_number = data['current_number'] + change
        
        update_data = {
            'number': new_number,
            'user_name': user_name,
            'consecutive_plus': new_plus,
            'consecutive_minus': new_minus,
            'jackpot_pity': new_pity,
            'fat_cooldown_time': datetime.now()
        }
        
        update_user_data(chat_id, user_id, **update_data)
        
        print(f"🤖 Авто-жир сработал для {user_name} в чате {chat_id}: {change:+d} кг")
    except Exception as e:
        print(f"❌ Ошибка в авто-жире: {e}")

async def auto_fat_loop():
    await asyncio.sleep(10)
    print("🤖 Авто-жир цикл запущен")
    
    while True:
        try:
            current_time = datetime.now()
            
            for chat_id in list(active_chats):
                try:
                    users = get_users_with_auto_fat(chat_id)
                    
                    for user_id, user_name, auto_fat_level, next_time_str in users:
                        try:
                            if not next_time_str:
                                continue
                            
                            if isinstance(next_time_str, str):
                                next_time = datetime.fromisoformat(next_time_str)
                            else:
                                next_time = next_time_str
                            
                            if current_time >= next_time:
                                await apply_auto_fat(chat_id, user_id, user_name)
                                
                                interval = get_auto_fat_interval(auto_fat_level)
                                if interval:
                                    new_next_time = current_time + timedelta(hours=interval)
                                    update_user_data(chat_id, user_id, next_auto_fat_time=new_next_time)
                        except Exception as e:
                            print(f"❌ Ошибка обработки авто-жира для {user_id} в чате {chat_id}: {e}")
                except Exception as e:
                    print(f"❌ Ошибка при работе с чатом {chat_id}: {e}")
        except Exception as e:
            print(f"❌ Ошибка в цикле авто-жира: {e}")
        
        await asyncio.sleep(60)

async def passive_income_loop():
    await asyncio.sleep(10)
    print("💰 Пассивный доход цикл запущен")
    
    while True:
        try:
            current_time = datetime.now()
            print(f"💰 Начисление пассивного дохода: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            for chat_id in list(active_chats):
                try:
                    users = get_users_with_items(chat_id)
                    
                    for user_id, user_name, current_number, item_counts_str, income_upgrade, prestige, last_income in users:
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
                                    income_bonus = get_income_bonus(income_upgrade or 0)
                                    prestige_bonus = get_prestige_bonus(prestige or 0)
                                    final_gain = int(total_gain * income_bonus * prestige_bonus)
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

async def apply_snatcher_effect(chat_id, user_id, user_name):
    try:
        data = get_user_data(chat_id, user_id, user_name)
        items_dict = get_user_items(data['item_counts'])
        
        snatcher_count = items_dict.get("Снатчер", 0)
        if snatcher_count == 0:
            return
        
        current_time = datetime.now()
        last_snatch = data.get('snatcher_last_time')
        if last_snatch:
            if isinstance(last_snatch, str):
                last_snatch = datetime.fromisoformat(last_snatch)
            if (current_time - last_snatch).total_seconds() < 6 * 3600:
                return
        
        if random.random() > 0.2:
            update_user_data(chat_id, user_id, snatcher_last_time=current_time)
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
            update_user_data(chat_id, user_id, snatcher_last_time=current_time)
            return
        
        items_dict[selected_item["name"]] = items_dict.get(selected_item["name"], 0) + 1
        update_user_data(
            chat_id, user_id,
            item_counts=save_user_items(items_dict),
            snatcher_last_time=current_time
        )
        
        print(f"👾 Снатчер сработал для {user_name} в чате {chat_id}: +1 {selected_item['name']} (слот {chosen_slot + 1}/10)")
    except Exception as e:
        print(f"❌ Ошибка в работе Снатчера: {e}")

async def hourly_effects_loop():
    await asyncio.sleep(10)
    print("💊 Почасовые эффекты цикл запущен")
    
    while True:
        try:
            current_time = datetime.now()
            print(f"💊 Проверка почасовых эффектов: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            for chat_id in list(active_chats):
                try:
                    users = get_users_with_hourly_items(chat_id)
                    
                    for user_id, user_name, current_number, item_counts_str, income_upgrade, prestige, last_hourly in users:
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
                                    income_bonus = get_income_bonus(income_upgrade or 0)
                                    prestige_bonus = get_prestige_bonus(prestige or 0)
                                    final_gain = int(total_gain * income_bonus * prestige_bonus)
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

# ===== КОМАНДЫ =====
async def cmd_start(message: types.Message):
    register_chat(message.chat.id)
    help_text = """
🍔 **ЖИРНЫЙ ТЕЛЕГРАМ БОТ** 🍔

**Основные команды:**
/жир - изменить свой вес
/жиркейс - открыть кейс (ежедневный или из инвентаря)
/жиркейс_шансы - шансы в ежедневном кейсе
/жиротрясы - таблица рекордов в чате
/жиринфо [@username] - информация о пользователе
/жирзвания - список всех званий
/жиркулдаун - статус кулдаунов
/инвентарь [@username] - посмотреть инвентарь

**Дуэли:**
/дуэль @username [кг/"все"] - вызвать на дуэль
/отмена - отменить текущую дуэль (только для тестеров)

**Апгрейды:**
/апгрейд - улучшить предмет
/апгрейдкг [количество] - улучшить кг в предмет
/апгрейдюзер - улучшить характеристики персонажа
/выбрать [номер] - выбрать цель апгрейда

**Экономика:**
/магазин - магазин предметов и кейсов
/купить [слот] [кол-во] - купить предмет
/продать [предмет] [кол-во] - продать предмет
/датьжир @user [кол-во] - передать кг
/датьпредмет @user [кол-во] [предмет] - передать предмет

**Тестерские команды:**
/сброскд - сбросить кулдауны всем
/сбросвсех - сбросить вес всех на 0
/выдатьпредмет [количество] [предмет] - выдать предмет себе
/жир_сброс [@user] - сбросить вес пользователя

⭐ **ХАРАКТЕРИСТИКИ** ⭐
• КД !жир — уменьшает время ожидания команды
• КД кейса — уменьшает время ожидания бесплатного кейса
• Удача — повышает шанс редких предметов в кейсах и шанс апгрейдов
• Прибавка — увеличивает получаемые кг от пассивного дохода и почасовых предметов
• Престиж — сбрасывает вес и улучшения, но даёт +10% ко всем кг и +1% к шансам за уровень (опыт и уровень сохраняются)
• Авто-жир — автоматически использует !жир каждые 6/3/1/0.5/0.25/0.1 час(ов)

🔥❄️💰 - следите за показателями!
    """
    await message.reply(help_text)

async def cmd_fat(message: types.Message):
    register_chat(message.chat.id)
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    data = get_user_data(chat_id, user_id, user_name)
    
    fat_cd_upgrade = data.get('fat_cd_upgrade', 0)
    cd_reduction_minutes = get_fat_cd_reduction(fat_cd_upgrade)
    actual_cooldown = max(0.1, COOLDOWN_HOURS * 60 - cd_reduction_minutes) / 60
    
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
    
    prestige_bonus = get_prestige_bonus(data.get('prestige', 0))
    
    change, was_minus, new_consecutive_plus, new_consecutive_minus, new_jackpot_pity, was_jackpot = get_change_with_pity_and_jackpot(
        data['consecutive_plus'], data['consecutive_minus'], data['jackpot_pity'], 
        data.get('luck_upgrade', 0), prestige_bonus, items_dict, data['current_number'])
    
    temp_number = data['current_number'] + change
    update_user_data(chat_id, user_id, number=temp_number)
    
    levels_gained, kg_reward, new_level = add_xp(chat_id, user_id, XP_PER_FAT)
    
    final_data = get_user_data(chat_id, user_id, user_name)
    final_number = final_data['current_number']
    
    update_user_data(
        chat_id, user_id,
        user_name=user_name,
        consecutive_plus=new_consecutive_plus,
        consecutive_minus=new_consecutive_minus,
        jackpot_pity=new_jackpot_pity,
        fat_cooldown_time=datetime.now()
    )
    
    rank_name, rank_emoji = get_rank(final_number)
    
    if was_jackpot:
        header = "💰 **ДЖЕКПОТ!** 💰"
    else:
        header = "🍔 Набор массы"
    
    response = f"{header}\n\n"
    response += f"**{user_name}** теперь весит **{abs(final_number)}kg**!\n\n"
    
    if was_jackpot:
        response += f"💰 Джекпот: +{change} кг\n"
    elif change > 0:
        response += f"📈 +{change} кг\n"
    elif change < 0:
        response += f"📉 {change} кг\n"
    
    response += f"🍖 Текущий вес: {final_number}kg\n"
    response += f"🎖️ Звание: {rank_emoji} {rank_name}\n"
    
    if levels_gained > 0:
        response += f"\n⭐ **ПОВЫШЕНИЕ УРОВНЯ!** ⭐\n+{kg_reward} кг за {levels_gained} уровень(ей)!\nТеперь у вас **{new_level}** уровень!\n"
    
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
        response += "\n" + "\n".join(pity_info) + "\n"
    
    if data.get('auto_fat_level', 0) > 0:
        interval = get_auto_fat_interval(data['auto_fat_level'])
        response += f"\n🤖 Авто-жир: {data['auto_fat_level']} уровень (каждые {interval} ч)\n"
    
    response += f"\n⏰ Следующая команда через {actual_cooldown*60:.0f} мин"
    
    await message.reply(response)

async def cmd_fat_case(message: types.Message):
    register_chat(message.chat.id)
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    data = get_user_data(chat_id, user_id, user_name)
    
    if data.get('active_case_message_id'):
        try:
            await bot.delete_message(chat_id, int(data['active_case_message_id']))
        except:
            pass
    
    items_dict = get_user_items(data['item_counts'])
    
    case_cd_upgrade = data.get('case_cd_upgrade', 0)
    cd_reduction_minutes = get_case_cd_reduction(case_cd_upgrade)
    actual_case_cooldown = max(1, CASE_COOLDOWN_HOURS * 60 - cd_reduction_minutes) / 60
    
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
    
    update_user_data(chat_id, user_id, active_case_message_id=None, last_case_type=case_to_open)
    
    prize_emojis = []
    for prize in case["prizes"]:
        if "emoji" in prize:
            emoji = prize["emoji"]
        elif prize["value"] == "rotten_leg":
            emoji = "💀"
        elif prize["value"] == "water":
            emoji = "💧"
        elif prize["value"] == "auto_fat":
            emoji = "🤖"
        elif isinstance(prize["value"], int):
            if prize["value"] < 0:
                emoji = "📉"
            elif prize["value"] == 0:
                emoji = "🔄"
            elif prize["value"] < 50:
                emoji = "📈"
            elif prize["value"] < 100:
                emoji = "⬆️"
            elif prize["value"] < 500:
                emoji = "🚀"
            elif prize["value"] < 1000:
                emoji = "⭐"
            else:
                emoji = "💥"
        else:
            emoji = "🎁"
        
        if emoji not in prize_emojis:
            prize_emojis.append(emoji)
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🖱️ ОТКРЫТЬ", callback_data=f"open_case_{case_to_open}"),
                InlineKeyboardButton(text="❌ ОТМЕНА", callback_data=f"cancel_case_{case_to_open}")
            ]
        ]
    )
    
    case_emoji = case["emoji"]
    case_text = f"{case_emoji} **{case['name']}** {case_emoji}\n\n"
    case_text += f"{message.from_user.full_name}, у вас есть кейс!\n\n"
    case_text += f"┌───────────────┐\n"
    case_text += f"│----{case_emoji}---{case_emoji}---{case_emoji}----│\n"
    case_text += f"│----К-Е-Й-С-------│\n"
    case_text += f"│----{case['name'][:10]}--│\n"
    case_text += f"│----{case_emoji}---{case_emoji}---{case_emoji}----│\n"
    case_text += f"└───────────────┘\n\n"
    case_text += f"⏰ У вас 30 секунд!"
    
    case_msg = await message.reply(case_text, reply_markup=keyboard)
    
    update_user_data(chat_id, user_id, active_case_message_id=str(case_msg.message_id))

@dp.callback_query(lambda c: c.data and (c.data.startswith('open_case_') or c.data.startswith('cancel_case_')))
async def process_case_callback(callback: CallbackQuery):
    register_chat(callback.message.chat.id)
    
    is_cancel = callback.data.startswith('cancel_case_')
    case_to_open = callback.data.replace('open_case_', '').replace('cancel_case_', '')
    
    chat_id = callback.message.chat.id
    user_id = callback.from_user.id
    user_name = callback.from_user.full_name
    
    data = get_user_data(chat_id, user_id, user_name)
    
    if str(user_id) != data['user_id']:
        await callback.answer("Это не ваш кейс!", show_alert=True)
        return
    
    try:
        await callback.message.delete_reply_markup()
    except:
        pass
    
    if is_cancel:
        update_user_data(chat_id, user_id, active_case_message_id=None, last_case_type=None)
        await callback.answer("❌ Отмена")
        await callback.message.edit_text(f"❌ **{callback.from_user.full_name}** отменил открытие кейса. Кейс сохранён в инвентаре!")
        return
    
    case_cd_upgrade = data.get('case_cd_upgrade', 0)
    cd_reduction_minutes = get_case_cd_reduction(case_cd_upgrade)
    actual_case_cooldown = max(1, CASE_COOLDOWN_HOURS * 60 - cd_reduction_minutes) / 60
    
    items_dict = get_user_items(data['item_counts'])
    for item_name, count in items_dict.items():
        if item_name == "Апельсин":
            actual_case_cooldown *= (1 - count * 0.05)
        elif item_name == "Золотой Апельсин":
            actual_case_cooldown *= (1 - count * 0.10)
    
    actual_case_cooldown = max(1, int(actual_case_cooldown))
    
    if case_to_open == "daily":
        can_get_daily, _ = can_get_daily_case(chat_id, user_id, actual_case_cooldown)
        if not can_get_daily:
            await callback.answer("❌ Ежедневный кейс уже использован!", show_alert=True)
            await callback.message.delete()
            update_user_data(chat_id, user_id, active_case_message_id=None, last_case_type=None)
            return
        update_daily_case_time(chat_id, user_id)
    else:
        cases_dict = data.get('cases_dict', {}).copy()
        if cases_dict.get(case_to_open, 0) <= 0:
            await callback.answer("❌ У вас больше нет этого кейса!", show_alert=True)
            await callback.message.delete()
            update_user_data(chat_id, user_id, active_case_message_id=None, last_case_type=None)
            return
        cases_dict[case_to_open] -= 1
        update_user_data(chat_id, user_id, cases_dict=cases_dict)
    
    await callback.answer()
    
    prize_emojis = []
    for prize in CASES[case_to_open]["prizes"]:
        if "emoji" in prize:
            emoji = prize["emoji"]
        elif prize["value"] == "rotten_leg":
            emoji = "💀"
        elif prize["value"] == "water":
            emoji = "💧"
        elif prize["value"] == "auto_fat":
            emoji = "🤖"
        elif isinstance(prize["value"], int):
            if prize["value"] < 0:
                emoji = "📉"
            elif prize["value"] == 0:
                emoji = "🔄"
            elif prize["value"] < 50:
                emoji = "📈"
            elif prize["value"] < 100:
                emoji = "⬆️"
            elif prize["value"] < 500:
                emoji = "🚀"
            elif prize["value"] < 1000:
                emoji = "⭐"
            else:
                emoji = "💥"
        else:
            emoji = "🎁"
        if emoji not in prize_emojis:
            prize_emojis.append(emoji)
    
    prestige_luck = get_prestige_luck(data.get('prestige', 0))
    luck_upgrade = data.get('luck_upgrade', 0)
    prize = open_case(case_to_open, prestige_luck, luck_upgrade)
    
    update_user_data(chat_id, user_id, active_case_message_id=None, last_case_type=None)
    
    levels_gained, kg_reward, new_level = add_xp(chat_id, user_id, XP_PER_CASE)
    
    line = [random.choice(prize_emojis) for _ in range(100)]
    
    if "emoji" in prize:
        prize_emoji = prize["emoji"]
    elif prize["value"] == "rotten_leg":
        prize_emoji = "💀"
    elif prize["value"] == "water":
        prize_emoji = "💧"
    elif prize["value"] == "auto_fat":
        prize_emoji = "🤖"
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
    
    line[56] = prize_emoji
    anim_msg = await callback.message.reply(f"🎰 **{CASES[case_to_open]['name']}** 🎰")
    
    animation_frames = [
        (1, 5), (2, 10), (3, 15), (4, 20), (5, 25),
        (6, 30), (7, 35), (8, 39), (9, 43), (10, 47),
        (11, 50), (12, 52), (13, 54), (14, 55), (15, 55),
        (16, 55), (17, 56), (18, 56)
    ]
    
    last_text = None
    
    for frame_num, center_pos in animation_frames:
        visible = line[center_pos-4:center_pos+5]
        display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
        current_text = f"**{display_line}**"
        
        if current_text != last_text:
            try:
                await anim_msg.edit_text(current_text)
                last_text = current_text
            except Exception as e:
                print(f"Ошибка анимации: {e}")
        
        await asyncio.sleep(0.5)
    
    visible = line[52:61]
    display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
    
    try:
        await anim_msg.edit_text(f"**{display_line}**\n\n**РЕЗУЛЬТАТ!**")
    except:
        pass
    
    await asyncio.sleep(1)
    
    items_dict = get_user_items(data['item_counts'])
    new_number = data['current_number']
    prize_value = prize["value"]
    prestige_bonus = get_prestige_bonus(data.get('prestige', 0))
    has_water = items_dict.get("Стакан воды", 0) > 0
    
    if prize_value == "rotten_leg":
        items_dict["Гнилая ножка KFC"] = items_dict.get("Гнилая ножка KFC", 0) + 1
        result_display = f"💀 **Гнилая ножка KFC!** 💀"
        result_color = "💀"
    elif prize_value == "water":
        items_dict["Стакан воды"] = items_dict.get("Стакан воды", 0) + 1
        result_display = f"💧 **Стакан воды!** 💧"
        result_color = "💧"
    elif prize_value == "auto_fat":
        new_auto_fat_level = min(data.get('auto_fat_level', 0) + 1, AUTO_FAT_MAX_LEVEL)
        interval = get_auto_fat_interval(new_auto_fat_level)
        next_time = datetime.now() + timedelta(hours=interval) if interval else None
        update_user_data(chat_id, user_id, auto_fat_level=new_auto_fat_level, next_auto_fat_time=next_time)
        result_display = f"🤖 **+1 уровень Авто-жира!** 🤖\nТеперь {new_auto_fat_level} уровень (каждые {interval} ч)"
        result_color = "🤖"
    elif isinstance(prize_value, str):
        items_dict[prize_value] = items_dict.get(prize_value, 0) + 1
        result_display = f"🎁 **{prize_value}** {prize_emoji}"
        result_color = "🎁"
    else:
        if has_water and case_to_open != "daily":
            prize_value = prize_value // 3
        prize_value = int(prize_value * prestige_bonus)
        new_number = data['current_number'] + prize_value
        result_display = f"🎉 **{prize_value:+d} кг** {prize_emoji}"
        result_color = "📈" if prize_value > 0 else "📉"
    
    update_data = {
        'number': new_number,
        'user_name': user_name,
        'item_counts': save_user_items(items_dict)
    }
    update_user_data(chat_id, user_id, **update_data)
    
    rank_name, rank_emoji = get_rank(new_number)
    
    final_text = f"{CASES[case_to_open]['emoji']} Открытие {CASES[case_to_open]['name']}\n\n"
    final_text += f"**{callback.from_user.full_name}** открыл кейс и получил:\n\n"
    final_text += f"🎁 Приз: {result_display}\n"
    
    if prize_value not in ["rotten_leg", "water", "auto_fat"] and not isinstance(prize_value, str):
        final_text += f"🍖 Новый вес: {new_number}kg\n"
        final_text += f"🎖️ Звание: {rank_emoji} {rank_name}\n"
    
    if levels_gained > 0:
        final_text += f"\n⭐ **ПОВЫШЕНИЕ УРОВНЯ!** ⭐\n+{kg_reward} кг за {levels_gained} уровень(ей)!\nТеперь у вас **{new_level}** уровень!\n"
    
    if case_to_open != "daily":
        remaining = data.get('cases_dict', {}).get(case_to_open, 0)
        if remaining > 0:
            final_text += f"\n📦 Осталось кейсов: {CASES[case_to_open]['emoji']} {CASES[case_to_open]['name']}: {remaining} шт"
    else:
        final_text += f"\n⏰ Следующий ежедневный кейс: через {actual_case_cooldown} ч"
    
    try:
        await anim_msg.reply(final_text)
    except:
        await anim_msg.reply("✅ Кейс открыт!")

async def cmd_upgrade_user(message: types.Message):
    """Улучшение характеристик персонажа"""
    register_chat(message.chat.id)
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    data = get_user_data(chat_id, user_id, user_name)
    
    fat_cd_level = data.get('fat_cd_upgrade', 0)
    case_cd_level = data.get('case_cd_upgrade', 0)
    luck_level = data.get('luck_upgrade', 0)
    income_level = data.get('income_upgrade', 0)
    prestige_level = data.get('prestige', 0)
    auto_fat_level = data.get('auto_fat_level', 0)
    
    total_xp = data.get('user_xp', 0)
    level, current_xp = get_level_and_xp(total_xp)
    next_level_xp = get_xp_for_next_level(level)
    
    fat_cd_cost = get_upgrade_cost("fat_cd", fat_cd_level)
    case_cd_cost = get_upgrade_cost("case_cd", case_cd_level)
    luck_cost = get_upgrade_cost("luck", luck_level)
    income_cost = get_upgrade_cost("income", income_level)
    prestige_cost = get_upgrade_cost("prestige", prestige_level)
    auto_fat_cost = get_upgrade_cost("auto_fat", auto_fat_level)
    
    fat_cd_bonus = get_fat_cd_reduction(fat_cd_level)
    case_cd_bonus = get_case_cd_reduction(case_cd_level)
    prestige_bonus = get_prestige_bonus(prestige_level)
    income_bonus = get_income_bonus(income_level)
    auto_fat_interval = get_auto_fat_interval(auto_fat_level)
    auto_fat_text = f"{auto_fat_interval} ч" if auto_fat_interval else "Не куплен"
    
    xp_bar_length = 20
    xp_progress = int((current_xp / next_level_xp) * xp_bar_length) if next_level_xp > 0 else 0
    xp_bar = "█" * xp_progress + "░" * (xp_bar_length - xp_progress)
    
    response = f"⭐ **ПРОКАЧКА ПЕРСОНАЖА** ⭐\n\n"
    response += f"**{user_name}**\n\n"
    response += f"📊 **УРОВЕНЬ И ОПЫТ**\n"
    response += f"Уровень: **{level}**\n"
    response += f"Опыт: {current_xp} / {next_level_xp}\n"
    response += f"`{xp_bar}`\n"
    response += f"Всего опыта: {total_xp}\n\n"
    response += f"⚡ **ХАРАКТЕРИСТИКИ** ⚡\n\n"
    
    fat_cd_color = "🟢" if data['current_number'] >= fat_cd_cost else "🔴"
    response += f"{fat_cd_color} **⏰ КД !жир** — ур.{fat_cd_level} (-{fat_cd_bonus} мин)\n"
    response += f"   Стоимость: `{fat_cd_cost} кг`\n\n"
    
    case_cd_color = "🟢" if data['current_number'] >= case_cd_cost else "🔴"
    response += f"{case_cd_color} **📦 КД кейса** — ур.{case_cd_level} (-{case_cd_bonus} мин)\n"
    response += f"   Стоимость: `{case_cd_cost} кг`\n\n"
    
    luck_color = "🟢" if data['current_number'] >= luck_cost else "🔴"
    response += f"{luck_color} **🍀 Удача** — ур.{luck_level} (+{luck_level * LUCK_CASE_BONUS_PER_LEVEL:.2f}% к редким, +{luck_level * LUCK_UPGRADE_BONUS_PER_LEVEL:.2f}% к апгрейдам)\n"
    response += f"   Стоимость: `{luck_cost} кг`\n\n"
    
    income_color = "🟢" if data['current_number'] >= income_cost else "🔴"
    response += f"{income_color} **📈 Прибавка** — ур.{income_level} (+{(income_bonus-1)*100:.0f}% к доходу от предметов)\n"
    response += f"   Стоимость: `{income_cost} кг`\n\n"
    
    prestige_color = "🟢" if data['current_number'] >= prestige_cost else "🔴"
    response += f"{prestige_color} **🌟 Престиж** — ур.{prestige_level} (+{(prestige_bonus-1)*100:.0f}% ко всему, +{prestige_level}% к шансам)\n"
    response += f"   Стоимость: `{prestige_cost} кг`\n\n"
    
    auto_fat_color = "🟢" if data['current_number'] >= auto_fat_cost else "🔴"
    response += f"{auto_fat_color} **🤖 Авто-жир** — ур.{auto_fat_level} (каждые {auto_fat_text})\n"
    response += f"   Стоимость: `{auto_fat_cost} кг`\n\n"
    
    response += f"💡 **ЧТО ДАЁТ**\n"
    response += f"• **КД !жир** — уменьшает время ожидания команды\n"
    response += f"• **КД кейса** — уменьшает время ожидания бесплатного кейса\n"
    response += f"• **Удача** — повышает шанс редких предметов в кейсах и шанс апгрейдов\n"
    response += f"• **Прибавка** — увеличивает получаемые кг от пассивного дохода и почасовых предметов\n"
    response += f"• **Престиж** — сбрасывает вес и улучшения, но даёт +10% ко всем кг и +1% к шансам за уровень (опыт и уровень сохраняются)\n"
    response += f"• **Авто-жир** — автоматически использует !жир каждые 6/3/1/0.5/0.25/0.1 час(ов)\n\n"
    response += f"💰 Для улучшения используйте `/апгрейдюзер [номер]`\n"
    response += f"1️⃣ - КД !жир\n"
    response += f"2️⃣ - КД кейса\n"
    response += f"3️⃣ - Удача\n"
    response += f"4️⃣ - Прибавка\n"
    response += f"5️⃣ - Престиж\n"
    response += f"6️⃣ - Авто-жир"
    
    await message.reply(response)

async def cmd_upgrade_user_choice(message: types.Message):
    """Обработка выбора улучшения характеристики"""
    register_chat(message.chat.id)
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    parts = message.text.split() if message.text else []
    
    if len(parts) < 2:
        await message.reply("❌ Укажите номер улучшения!\nПример: `/апгрейдюзер 1`")
        return
    
    try:
        choice = int(parts[1])
    except ValueError:
        await message.reply("❌ Введите корректный номер!")
        return
    
    if choice < 1 or choice > 6:
        await message.reply("❌ Неверный номер! Введите число от 1 до 6")
        return
    
    upgrade_map = {
        1: "fat_cd",
        2: "case_cd", 
        3: "luck",
        4: "income",
        5: "prestige",
        6: "auto_fat"
    }
    
    upgrade_type = upgrade_map[choice]
    data = get_user_data(chat_id, user_id, user_name)
    
    if upgrade_type == "fat_cd":
        current_level = data.get('fat_cd_upgrade', 0)
        cost = get_upgrade_cost("fat_cd", current_level)
    elif upgrade_type == "case_cd":
        current_level = data.get('case_cd_upgrade', 0)
        cost = get_upgrade_cost("case_cd", current_level)
    elif upgrade_type == "luck":
        current_level = data.get('luck_upgrade', 0)
        cost = get_upgrade_cost("luck", current_level)
    elif upgrade_type == "income":
        current_level = data.get('income_upgrade', 0)
        cost = get_upgrade_cost("income", current_level)
    elif upgrade_type == "prestige":
        current_level = data.get('prestige', 0)
        cost = get_upgrade_cost("prestige", current_level)
    elif upgrade_type == "auto_fat":
        current_level = data.get('auto_fat_level', 0)
        if current_level >= AUTO_FAT_MAX_LEVEL:
            await message.reply(f"❌ Авто-жир уже на максимальном ({AUTO_FAT_MAX_LEVEL}) уровне!")
            return
        cost = get_upgrade_cost("auto_fat", current_level)
    
    if data['current_number'] < cost:
        await message.reply(f"❌ Недостаточно кг! Нужно **{cost} кг**, у вас: **{data['current_number']} кг**")
        return
    
    if upgrade_type == "prestige":
        await message.reply(
            f"⚠️ **ВНИМАНИЕ! ПРЕСТИЖ** ⚠️\n\n"
            f"Вы уверены, что хотите получить престиж?\n\n"
            f"**Это действие НЕОБРАТИМО!**\n"
            f"• Вес сбросится до 0\n"
            f"• Все предметы и кейсы исчезнут\n"
            f"• Все улучшения (КД, удача, прибавка, авто-жир) обнулятся\n"
            f"• Опыт и уровень сохранятся\n"
            f"• Останется только +1 уровень престижа\n\n"
            f"Напишите `да` в течение 15 секунд для подтверждения."
        )
        
        def confirm_check(m):
            return m.from_user.id == user_id and m.text and m.text.lower() == "да"
        
        try:
            await bot.wait_for('message', timeout=15.0, check=confirm_check)
        except asyncio.TimeoutError:
            await message.reply("❌ Престиж отменён.")
            return
        
        new_prestige = current_level + 1
        current_xp = data.get('user_xp', 0)
        current_user_level = data.get('user_level', 0)
        
        update_user_data(
            chat_id, user_id,
            current_number=0,
            item_counts='{}',
            cases_dict={},
            fat_cd_upgrade=0,
            case_cd_upgrade=0,
            luck_upgrade=0,
            income_upgrade=0,
            auto_fat_level=0,
            next_auto_fat_time=None,
            prestige=new_prestige,
            consecutive_plus=0,
            consecutive_minus=0,
            jackpot_pity=0,
            shadow_upgrade_chance=0
        )
        
        response = f"🌟 **ПРЕСТИЖ ПОЛУЧЕН!** 🌟\n\n"
        response += f"**{user_name}** достиг **{new_prestige}** уровня престижа!\n\n"
        response += f"Вес сброшен до 0\n"
        response += f"Все предметы и улучшения обнулены\n"
        response += f"Опыт и уровень сохранены!\n"
        response += f"Теперь вы получаете +{new_prestige * 10}% ко всему и +{new_prestige}% к шансам!"
        
        await message.reply(response)
        return
    
    new_level = current_level + 1
    new_number = data['current_number'] - cost
    
    if upgrade_type == "auto_fat":
        interval = get_auto_fat_interval(new_level)
        next_time = datetime.now() + timedelta(hours=interval) if interval else None
        update_user_data(chat_id, user_id, number=new_number, auto_fat_level=new_level, next_auto_fat_time=next_time)
        bonus_text = f"Авто-жир повышен до {new_level} уровня! Теперь срабатывает каждые {interval} час(ов)"
    else:
        update_field = {"fat_cd": "fat_cd_upgrade", "case_cd": "case_cd_upgrade", "luck": "luck_upgrade", "income": "income_upgrade"}[upgrade_type]
        update_user_data(chat_id, user_id, number=new_number, **{update_field: new_level})
        
        if upgrade_type == "fat_cd":
            new_bonus = get_fat_cd_reduction(new_level)
            bonus_text = f"КД !жир уменьшен на {new_bonus} мин (теперь {new_level} уровень)"
        elif upgrade_type == "case_cd":
            new_bonus = get_case_cd_reduction(new_level)
            bonus_text = f"КД кейса уменьшен на {new_bonus} мин (теперь {new_level} уровень)"
        elif upgrade_type == "luck":
            bonus_text = f"Удача повышена до {new_level} уровня! +{new_level * LUCK_CASE_BONUS_PER_LEVEL:.2f}% к редким предметам, +{new_level * LUCK_UPGRADE_BONUS_PER_LEVEL:.2f}% к апгрейдам"
        elif upgrade_type == "income":
            new_bonus = get_income_bonus(new_level)
            bonus_text = f"Прибавка повышена до {new_level} уровня! +{(new_bonus-1)*100:.0f}% к доходу от предметов"
    
    response = f"✅ **УЛУЧШЕНИЕ ПОЛУЧЕНО!** ✅\n\n"
    response += f"**{user_name}** улучшил характеристику!\n\n"
    response += f"**Потрачено:** {cost} кг\n"
    response += f"**Осталось:** {new_number} кг\n\n"
    response += f"**{bonus_text}**"
    
    await message.reply(response)

async def cmd_upgrade(message: types.Message):
    """Улучшение предметов"""
    register_chat(message.chat.id)
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    data = get_user_data(chat_id, user_id, user_name)
    
    if data.get('upgrade_active', 0) == 1:
        await message.reply("⚠️ У вас уже есть активный апгрейд! Дождитесь его завершения.")
        return
    
    items_dict = get_user_items(data['item_counts'])
    
    available_items = []
    for item_name, count in items_dict.items():
        if count > 0:
            price = get_item_price(item_name)
            if price > 0:
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
    
    parts = message.text.split() if message.text else []
    
    if len(parts) < 2:
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
    items_dict[selected_item["name"]] -= 1
    if items_dict[selected_item["name"]] <= 0:
        del items_dict[selected_item["name"]]
    
    update_user_data(
        chat_id, user_id,
        item_counts=save_user_items(items_dict),
        last_command="upgrade_select",
        last_command_target=selected_item["name"],
        last_command_use_time=datetime.now(),
        upgrade_active=1,
        upgrade_data=json.dumps({'source_item': selected_item["name"]})
    )
    
    possible_upgrades = get_possible_upgrades(selected_item["name"], 1)
    
    if not possible_upgrades:
        items_dict[selected_item["name"]] = items_dict.get(selected_item["name"], 0) + 1
        update_user_data(chat_id, user_id, item_counts=save_user_items(items_dict), upgrade_active=0)
        await message.reply(f"❌ Для **{selected_item['emoji']} {selected_item['name']}** нет доступных улучшений! Предмет возвращён.")
        return
    
    response = f"🔧 **ВЫБОР ЦЕЛИ АПГРЕЙДА** 🔧\n\n"
    response += f"{user_name}, вы выбрали: **{selected_item['emoji']} {selected_item['name']}**\n\n"
    response += f"Теперь выберите цель (используйте `/выбрать [номер]`):\n\n"
    
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
    """Улучшение кг в предмет"""
    register_chat(message.chat.id)
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    data = get_user_data(chat_id, user_id, user_name)
    
    if data.get('upgrade_active', 0) == 1:
        await message.reply("⚠️ У вас уже есть активный апгрейд! Дождитесь его завершения.")
        return
    
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
    
    new_number = data['current_number'] - amount
    update_user_data(
        chat_id, user_id,
        number=new_number,
        last_command="upgrade_kg_select",
        last_command_target=str(amount),
        last_command_use_time=datetime.now(),
        upgrade_active=1,
        upgrade_data=json.dumps({'amount': amount})
    )
    
    all_items = set([item["name"] for item in SHOP_ITEMS] + list(LEGENDARY_UPGRADE_PRICES.keys()))
    possible_upgrades = []
    
    for item_name in all_items:
        target_price = get_item_price(item_name)
        if target_price == 0 or target_price < amount:
            continue
        
        chance = amount / target_price
        if chance < 0.01:
            continue
        
        is_case = any(case.get("name") == item_name for case in CASES.values())
        case_id = next((cid for cid, case in CASES.items() if case.get("name") == item_name), None)
        
        possible_upgrades.append({
            "name": item_name,
            "price": target_price,
            "chance": chance,
            "emoji": ITEM_EMOJIS.get(item_name, "🎁"),
            "is_case": is_case,
            "case_id": case_id
        })
    
    possible_upgrades.sort(key=lambda x: x["price"])
    
    if not possible_upgrades:
        update_user_data(chat_id, user_id, number=data['current_number'], upgrade_active=0)
        await message.reply(f"❌ На {amount} кг нет доступных улучшений! Кг возвращены.")
        return
    
    response = f"💱 **АПГРЕЙД КГ В ПРЕДМЕТЫ** 💱\n\n"
    response += f"{user_name}, вы потратили **{amount} кг**!\n\n"
    response += f"Выберите цель (используйте `/выбрать [номер]`):\n\n"
    
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
    response += f"\n💸 Кг уже списаны!"
    
    await message.reply(response)

async def cmd_choose(message: types.Message):
    """Выбор цели апгрейда"""
    register_chat(message.chat.id)
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    parts = message.text.split() if message.text else []
    
    if len(parts) < 2:
        await message.reply("❌ Укажите номер!")
        return
    
    try:
        choice = parts[1]
    except ValueError:
        await message.reply("❌ Номер должен быть числом!")
        return
    
    data = get_user_data(chat_id, user_id, user_name)
    
    if data.get('upgrade_active', 0) != 1:
        await message.reply("❌ У вас нет активного апгрейда! Сначала используйте `/апгрейд` или `/апгрейдкг`.")
        return
    
    last_command = data.get('last_command')
    last_use = data.get('last_command_use_time')
    
    if not last_command or not last_use:
        await message.reply("❌ Ошибка состояния апгрейда!")
        update_user_data(chat_id, user_id, upgrade_active=0, upgrade_data=None)
        return
    
    if isinstance(last_use, str):
        last_use = datetime.fromisoformat(last_use)
    
    if datetime.now() - last_use > timedelta(minutes=5):
        await message.reply("❌ Время ожидания истекло. Используйте команду заново!")
        update_user_data(chat_id, user_id, upgrade_active=0, upgrade_data=None)
        return
    
    if last_command == "upgrade_kg_select":
        try:
            amount = int(data['last_command_target'])
        except:
            await message.reply("❌ Ошибка в данных апгрейда!")
            update_user_data(chat_id, user_id, upgrade_active=0, upgrade_data=None)
            return
        
        all_items = set([item["name"] for item in SHOP_ITEMS] + list(LEGENDARY_UPGRADE_PRICES.keys()))
        possible_upgrades = []
        
        for item_name in all_items:
            target_price = get_item_price(item_name)
            if target_price == 0 or target_price < amount:
                continue
            
            chance = amount / target_price
            if chance < 0.01:
                continue
            
            is_case = any(case.get("name") == item_name for case in CASES.values())
            case_id = next((cid for cid, case in CASES.items() if case.get("name") == item_name), None)
            
            possible_upgrades.append({
                "name": item_name,
                "price": target_price,
                "chance": chance,
                "emoji": ITEM_EMOJIS.get(item_name, "🎁"),
                "is_case": is_case,
                "case_id": case_id
            })
        
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
        update_user_data(chat_id, user_id, upgrade_active=0, upgrade_data=None)
        
        # Анимация апгрейда кг (упрощённая версия)
        data = get_user_data(chat_id, user_id, user_name)
        shadow_chance = data.get('shadow_upgrade_chance', 0)
        
        prestige_bonus = 1 + get_prestige_luck(data.get('prestige', 0))
        luck_bonus = 1 + (data.get('luck_upgrade', 0) * LUCK_UPGRADE_BONUS_PER_LEVEL / 100)
        base_chance = target_item['chance']
        real_chance = min(base_chance * prestige_bonus * luck_bonus + shadow_chance / 100, 1.0)
        
        roll = random.random()
        success = roll < real_chance
        
        if success:
            new_shadow = max(0, shadow_chance - 8)
            if target_item.get("is_case", False):
                cases_dict = data.get('cases_dict', {}).copy()
                cases_dict[target_item["case_id"]] = cases_dict.get(target_item["case_id"], 0) + 1
                update_user_data(chat_id, user_id, cases_dict=cases_dict, shadow_upgrade_chance=new_shadow)
                result_text = f"✅ **УСПЕХ!** {amount} кг → {target_item['emoji']} **{target_item['name']}**\nПредмет успешно получен!"
            else:
                items_dict = get_user_items(data['item_counts'])
                items_dict[target_item["name"]] = items_dict.get(target_item["name"], 0) + 1
                update_user_data(chat_id, user_id, item_counts=save_user_items(items_dict), shadow_upgrade_chance=new_shadow)
                result_text = f"✅ **УСПЕХ!** {amount} кг → {target_item['emoji']} **{target_item['name']}**\nПредмет успешно получен!"
        else:
            new_shadow = min(32, shadow_chance + 4)
            update_user_data(chat_id, user_id, shadow_upgrade_chance=new_shadow)
            result_text = f"❌ **НЕУДАЧА!** {amount} кг сгорели в процессе улучшения!"
        
        await message.reply(result_text)
    
    elif last_command == "upgrade_select":
        source_item_name = data.get('last_command_target')
        if not source_item_name:
            await message.reply("❌ Ошибка: не выбран исходный предмет!")
            update_user_data(chat_id, user_id, upgrade_active=0, upgrade_data=None)
            return
        
        items_dict = get_user_items(data['item_counts'])
        
        if items_dict.get(source_item_name, 0) <= 0:
            await message.reply(f"❌ У вас больше нет **{source_item_name}** для улучшения!")
            update_user_data(chat_id, user_id, upgrade_active=0, upgrade_data=None)
            return
        
        possible_upgrades = get_possible_upgrades(source_item_name, items_dict[source_item_name])
        
        if not possible_upgrades:
            await message.reply("❌ Для этого предмета больше нет доступных улучшений!")
            update_user_data(chat_id, user_id, upgrade_active=0, upgrade_data=None)
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
        update_user_data(chat_id, user_id, upgrade_active=0, upgrade_data=None)
        
        # Анимация апгрейда предмета (упрощённая версия)
        data = get_user_data(chat_id, user_id, user_name)
        shadow_chance = data.get('shadow_upgrade_chance', 0)
        
        prestige_bonus = 1 + get_prestige_luck(data.get('prestige', 0))
        luck_bonus = 1 + (data.get('luck_upgrade', 0) * LUCK_UPGRADE_BONUS_PER_LEVEL / 100)
        base_chance = target_item['chance']
        real_chance = min(base_chance * prestige_bonus * luck_bonus + shadow_chance / 100, 1.0)
        
        roll = random.random()
        success = roll < real_chance
        
        if success:
            new_shadow = max(0, shadow_chance - 8)
            items_dict[target_item['name']] = items_dict.get(target_item['name'], 0) + 1
            result_text = f"✅ **УСПЕХ!** {ITEM_EMOJIS.get(source_item_name, '📦')} **{source_item_name}** → {target_item['emoji']} **{target_item['name']}**\nПредмет успешно улучшен!"
        else:
            new_shadow = min(32, shadow_chance + 4)
            result_text = f"❌ **НЕУДАЧА!** {ITEM_EMOJIS.get(source_item_name, '📦')} **{source_item_name}** был утерян в процессе улучшения!"
        
        update_user_data(chat_id, user_id, item_counts=save_user_items(items_dict), shadow_upgrade_chance=new_shadow)
        await message.reply(result_text)
    
    else:
        await message.reply("❌ Неизвестный тип апгрейда!")
        update_user_data(chat_id, user_id, upgrade_active=0, upgrade_data=None)

async def cmd_fat_case_chances(message: types.Message):
    register_chat(message.chat.id)
    embed_text = "📊 **ШАНСЫ В КЕЙСЕ** 📊\n\nВероятность выпадения каждого приза в ежедневном кейсе:\n\n"
    
    sorted_prizes = sorted(CASE_PRIZES, key=lambda x: x['chance'] if x['chance'] > 0 else 999, reverse=True)
    
    chances_text = ""
    rare_text = ""
    legendary_text = ""
    
    for prize in sorted_prizes:
        if prize["value"] == "auto_fat":
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
    embed_text += f"🍀 Бонус удачи: +{LUCK_CASE_BONUS_PER_LEVEL * 100:.0f}% к шансу редких призов за уровень"
    
    await message.reply(embed_text)

async def cmd_fat_leaderboard(message: types.Message):
    register_chat(message.chat.id)
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
        if len(user_data) >= 6:
            user_name, number, last_update, consecutive_plus, consecutive_minus, jackpot_pity = user_data[:6]
            prestige = user_data[6] if len(user_data) > 6 else 0
        else:
            user_name, number, last_update, consecutive_plus, consecutive_minus, jackpot_pity = user_data
            prestige = 0
        
        if i == 1:
            place_icon = "🥇"
        elif i == 2:
            place_icon = "🥈"
        elif i == 3:
            place_icon = "🥉"
        else:
            place_icon = "🔹"
        
        rank_name, rank_emoji = get_rank(number)
        
        display_name = f"{prestige}🌟{user_name}" if prestige > 0 else user_name
        
        pity_emojis = []
        if consecutive_plus and consecutive_plus > 0:
            pity_emojis.append(f"🔥{consecutive_plus}")
        if consecutive_minus and consecutive_minus > 0:
            pity_emojis.append(f"❄️{consecutive_minus}")
        if jackpot_pity and jackpot_pity > 0:
            pity_emojis.append(f"💰{jackpot_pity}")
        
        pity_str = f" {' '.join(pity_emojis)}" if pity_emojis else ""
        
        leaderboard_text += f"{place_icon} **{i}.** {display_name} — **{number}kg** {rank_emoji} *{rank_name}*{pity_str}\n"
        
        if len(leaderboard_text) > 3000:
            leaderboard_text += "... и ещё несколько участников"
            break
    
    response += leaderboard_text
    
    stats = get_chat_stats(chat_id)
    
    response += f"\n📊 **Статистика чата**\n"
    response += f"Участников: {stats['total_users']}\n"
    response += f"Суммарный вес: {stats['total_weight']}kg\n"
    response += f"Средний вес: {stats['avg_weight']:.1f}kg\n"
    response += f"🔼 Толстых: {stats['positive']} | 🔽 Худых: {stats['negative']} | ⚖️ Нулевых: {stats['zero']}"
    
    await message.reply(response)

async def cmd_fat_stats(message: types.Message):
    register_chat(message.chat.id)
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    data = get_user_data(chat_id, user_id, user_name)
    
    response = f"📊 **Статистика авто-жира - {user_name}**\n\n"
    
    if data.get('auto_fat_level', 0) > 0:
        interval = get_auto_fat_interval(data['auto_fat_level'])
        response += f"🤖 Уровень авто-жира: {data['auto_fat_level']} (каждые {interval} ч)\n\n"
    else:
        response += f"🤖 Авто-жир не куплен\n\n"
    
    response += f"💪 Всего срабатываний: {data.get('total_autoburger_activations', 0)}\n"
    response += f"📈 Всего набрано: {data.get('total_autoburger_gain', 0)} кг\n"
    
    if data.get('total_autoburger_activations', 0) > 0:
        avg_gain = data.get('total_autoburger_gain', 0) / data.get('total_autoburger_activations', 1)
        response += f"📊 Средний прирост: {avg_gain:.1f} кг\n"
    
    if data.get('last_autoburger_result') and data.get('last_autoburger_time'):
        try:
            last_time = data['last_autoburger_time']
            if isinstance(last_time, str):
                last_time = datetime.fromisoformat(last_time)
            time_diff = datetime.now() - last_time
            hours = time_diff.total_seconds() / 3600
            response += f"🕒 Последнее: {data['last_autoburger_result']} ({hours:.1f} ч назад)\n"
        except:
            pass
    
    if data.get('auto_fat_level', 0) > 0 and data.get('next_auto_fat_time'):
        try:
            next_time = data['next_auto_fat_time']
            if isinstance(next_time, str):
                next_time = datetime.fromisoformat(next_time)
            time_diff = next_time - datetime.now()
            if time_diff.total_seconds() > 0:
                response += f"⏰ Следующий авто-жир: через {format_time(time_diff.total_seconds())}\n"
        except:
            pass
    
    await message.reply(response)

async def cmd_fat_info(message: types.Message):
    register_chat(message.chat.id)
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    # Проверяем, указан ли пользователь
    parts = message.text.split() if message.text else []
    target_user = message.from_user
    
    if len(parts) > 1 and parts[1].startswith('@'):
        target_username = parts[1].replace('@', '')
        try:
            chat = await bot.get_chat(chat_id)
            # В Telegram сложно получить участника по username, используем другой подход
            # Пока просто ищем в тексте
            pass
        except:
            pass
    
    data = get_user_data(chat_id, user_id, user_name)
    
    rank_name, rank_emoji = get_rank(data['current_number'])
    
    fat_cd_upgrade = data.get('fat_cd_upgrade', 0)
    actual_fat_cooldown = max(0.1, COOLDOWN_HOURS * 60 - get_fat_cd_reduction(fat_cd_upgrade)) / 60
    
    case_cd_upgrade = data.get('case_cd_upgrade', 0)
    actual_case_cooldown = max(1, CASE_COOLDOWN_HOURS * 60 - get_case_cd_reduction(case_cd_upgrade)) / 60
    
    items_dict = get_user_items(data['item_counts'])
    
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
    
    income_bonus = get_income_bonus(data.get('income_upgrade', 0))
    prestige_bonus = get_prestige_bonus(data.get('prestige', 0))
    total_passive_income = int(total_passive_income * income_bonus * prestige_bonus)
    
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
    
    total_xp = data.get('user_xp', 0)
    level, current_xp = get_level_and_xp(total_xp)
    next_level_xp = get_xp_for_next_level(level)
    
    xp_bar_length = 15
    xp_progress = int((current_xp / next_level_xp) * xp_bar_length) if next_level_xp > 0 else 0
    xp_bar = "█" * xp_progress + "░" * (xp_bar_length - xp_progress)
    
    response = f"🍔 **Информация о {user_name}**\n\n"
    response += f"Текущий вес: {data['current_number']}kg\n"
    response += f"🎖️ Звание: {rank_emoji} {rank_name}\n"
    response += f"⭐ Уровень: **{level}**\n"
    response += f"📊 Опыт: {current_xp} / {next_level_xp}\n"
    response += f"`{xp_bar}`\n"
    
    if data.get('prestige', 0) > 0:
        response += f"🌟 Престиж: {data['prestige']} уровень (+{data['prestige']*10}% ко всему, +{data['prestige']}% к шансам)\n"
    
    if total_passive_income > 0:
        response += f"💰 Пассивный доход: {total_passive_income} кг/24ч\n"
    
    pity_emojis = []
    if data['consecutive_plus'] > 0:
        pity_emojis.append(f"🔥{data['consecutive_plus']}")
    if data['consecutive_minus'] > 0:
        pity_emojis.append(f"❄️{data['consecutive_minus']}")
    if data['jackpot_pity'] > 0:
        pity_emojis.append(f"💰{data['jackpot_pity']}")
    
    if pity_emojis:
        response += f"📊 Счётчики: {' '.join(pity_emojis)}\n"
    
    if data.get('auto_fat_level', 0) > 0:
        interval = get_auto_fat_interval(data['auto_fat_level'])
        response += f"🤖 Авто-жир: {data['auto_fat_level']} уровень (каждые {interval} ч)\n"
    
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
    response += f"/жир: {fat_status} (КД {actual_fat_cooldown*60:.0f} мин)\n"
    response += f"/жиркейс: {case_status} (КД {actual_case_cooldown:.1f} ч)"
    
    await message.reply(response)

async def cmd_show_ranks(message: types.Message):
    register_chat(message.chat.id)
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
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    data = get_user_data(chat_id, user_id, user_name)
    
    fat_cd_upgrade = data.get('fat_cd_upgrade', 0)
    actual_fat_cooldown = max(0.1, COOLDOWN_HOURS * 60 - get_fat_cd_reduction(fat_cd_upgrade)) / 60
    
    case_cd_upgrade = data.get('case_cd_upgrade', 0)
    actual_case_cooldown = max(1, CASE_COOLDOWN_HOURS * 60 - get_case_cd_reduction(case_cd_upgrade)) / 60
    
    items_dict = get_user_items(data['item_counts'])
    for item_name, count in items_dict.items():
        if item_name in ["Яблоко", "Золотое Яблоко"]:
            actual_fat_cooldown *= (1 - count * (0.05 if item_name == "Яблоко" else 0.10))
        elif item_name in ["Апельсин", "Золотой Апельсин"]:
            actual_case_cooldown *= (1 - count * (0.05 if item_name == "Апельсин" else 0.10))
    
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
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    # Проверяем, указан ли пользователь
    parts = message.text.split() if message.text else []
    target_user_id = user_id
    target_user_name = user_name
    
    if len(parts) > 1 and parts[1].startswith('@'):
        target_username = parts[1].replace('@', '')
        # В Telegram сложно получить пользователя по username
        # Пока просто игнорируем
    
    data = get_user_data(chat_id, target_user_id, target_user_name)
    
    if 'cases_dict' in data:
        clean_cases = {}
        for case_id, count in data['cases_dict'].items():
            if case_id in CASES:
                clean_cases[case_id] = count
            elif case_id == 'shop':
                clean_cases['shop_case'] = clean_cases.get('shop_case', 0) + count
        data['cases_dict'] = clean_cases
    
    response = f"🎒 **Инвентарь - {target_user_name}**\n\n"
    
    if data.get('auto_fat_level', 0) > 0:
        interval = get_auto_fat_interval(data['auto_fat_level'])
        response += f"🤖 Авто-жир: {data['auto_fat_level']} уровень (каждые {interval} ч)\n"
    
    response += f"⚡ Всего срабатываний: {data.get('total_autoburger_activations', 0)}\n"
    response += f"📈 Всего набрано: {data.get('total_autoburger_gain', 0)} кг\n\n"
    
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
    
    if data.get('auto_fat_level', 0) > 0 and data.get('next_auto_fat_time'):
        try:
            next_time = data['next_auto_fat_time']
            if isinstance(next_time, str):
                next_time = datetime.fromisoformat(next_time)
            time_diff = next_time - datetime.now()
            if time_diff.total_seconds() > 0:
                response += f"\n⏰ Следующий авто-жир: через {format_time(time_diff.total_seconds())}"
        except:
            pass
    
    await message.reply(response[:4000])

async def cmd_shop(message: types.Message):
    register_chat(message.chat.id)
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
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
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
    
    levels_gained, kg_reward, new_level = add_xp(chat_id, user_id, XP_PER_SHOP_BUY)
    
    response = f"✅ **Покупка совершена!**\n\n"
    response += f"📦 Предмет: {purchase_desc}\n"
    response += f"💰 Цена: {total_price} кг\n"
    response += f"💸 Осталось: {new_number} кг"
    
    if levels_gained > 0:
        response += f"\n\n⭐ **ПОВЫШЕНИЕ УРОВНЯ!** ⭐\n+{kg_reward} кг за {levels_gained} уровень(ей)!\nТеперь у вас **{new_level}** уровень!"
    
    await message.reply(response)

async def cmd_sell(message: types.Message):
    """Продажа предметов из инвентаря за 70% стоимости"""
    register_chat(message.chat.id)
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    
    parts = message.text.split() if message.text else []
    
    if len(parts) < 2:
        await message.reply(
            "❌ Использование: `/продать [название предмета] [количество]`\n"
            "Пример: `/продать Горелый бекон 5`\n"
            "Пример: `/продать всё` - продать всё сразу\n\n"
            "💰 **Цена продажи: 70% от стоимости предмета**"
        )
        return
    
    data = get_user_data(chat_id, user_id, user_name)
    items_dict = get_user_items(data['item_counts'])
    
    if parts[1].lower() in ["всё", "все"]:
        if not items_dict:
            await message.reply("📭 У вас нет предметов для продажи!")
            return
        
        total_gain = 0
        sold_items = []
        
        for item_name, count in list(items_dict.items()):
            price = get_item_price(item_name)
            if price > 0:
                sell_price = int(price * 0.7)
                item_gain = sell_price * count
                total_gain += item_gain
                sold_items.append(f"{item_name} x{count} — {item_gain} кг")
                del items_dict[item_name]
        
        if total_gain == 0:
            await message.reply("❌ Ни один из ваших предметов нельзя продать!")
            return
        
        new_number = data['current_number'] + total_gain
        update_user_data(
            chat_id, user_id,
            number=new_number,
            item_counts=save_user_items(items_dict)
        )
        
        response = f"💰 **Продажа всех предметов**\n\n"
        response += f"**{user_name}** продал всё!\n\n"
        response += "📦 **Продано:**\n" + "\n".join(sold_items[:10])
        if len(sold_items) > 10:
            response += f"\n... и ещё {len(sold_items) - 10} предметов"
        response += f"\n\n💸 **Получено:** {total_gain} кг\n"
        response += f"🍖 **Новый вес:** {new_number}kg"
        
        await message.reply(response)
        return
    
    if len(parts) < 3:
        item_name = message.text.replace('/продать', '', 1).strip()
        amount = 1
    else:
        try:
            amount = int(parts[-1])
            item_name = ' '.join(parts[1:-1]).strip()
        except ValueError:
            item_name = message.text.replace('/продать', '', 1).strip()
            amount = 1
    
    if amount <= 0:
        await message.reply("❌ Количество должно быть больше 0!")
        return
    
    found_item = None
    for key in items_dict.keys():
        if key.lower() == item_name.lower():
            found_item = key
            break
    
    if not found_item:
        for key in items_dict.keys():
            if item_name.lower() in key.lower():
                found_item = key
                break
    
    if not found_item:
        if items_dict:
            items_list = "\n".join([f"• {item}: {count} шт" for item, count in list(items_dict.items())[:10]])
            if len(items_dict) > 10:
                items_list += f"\n... и ещё {len(items_dict) - 10} предметов"
            await message.reply(f"❌ У вас нет предмета '{item_name}'!\n\n📦 **Ваши предметы:**\n{items_list}")
        else:
            await message.reply("❌ У вас нет предметов в инвентаре!")
        return
    
    if items_dict[found_item] < amount:
        await message.reply(f"❌ У вас недостаточно '{found_item}'! Есть: {items_dict[found_item]}, нужно: {amount}")
        return
    
    price = get_item_price(found_item)
    if price == 0:
        await message.reply(f"❌ Предмет '{found_item}' нельзя продать (нет цены)!")
        return
    
    sell_price = int(price * 0.7)
    total_gain = sell_price * amount
    
    items_dict[found_item] -= amount
    if items_dict[found_item] <= 0:
        del items_dict[found_item]
    
    new_number = data['current_number'] + total_gain
    
    update_user_data(
        chat_id, user_id,
        number=new_number,
        item_counts=save_user_items(items_dict)
    )
    
    response = f"💰 **Продажа предмета**\n\n"
    response += f"**{user_name}** продал:\n\n"
    response += f"📦 Предмет: **{found_item}** x{amount}\n"
    response += f"💎 Цена за шт: {price} кг\n"
    response += f"🏷️ Цена продажи (70%): {sell_price} кг/шт\n"
    response += f"💸 Всего получено: **{total_gain} кг**\n\n"
    response += f"🍖 Новый вес: **{new_number}kg**"
    
    if found_item in items_dict:
        response += f"\n📦 Осталось {found_item}: {items_dict[found_item]} шт"
    
    await message.reply(response)

async def cmd_give_fat(message: types.Message):
    register_chat(message.chat.id)
    chat_id = message.chat.id
    giver_id = message.from_user.id
    giver_name = message.from_user.full_name
    
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
    
    # В Telegram сложно получить пользователя по username в чате
    # Пока просто ищем в тексте или используем reply
    target_user = None
    if message.reply_to_message:
        target_user = message.reply_to_message.from_user
    else:
        # Пробуем найти по username (ограниченно)
        try:
            target_user = await bot.get_chat_member(chat_id, target_username)
            target_user = target_user.user
        except:
            pass
    
    if not target_user:
        await message.reply(f"❌ Пользователь @{target_username} не найден! Используйте ответ на сообщение пользователя.")
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

async def cmd_give_item(message: types.Message):
    register_chat(message.chat.id)
    
    chat_id = message.chat.id
    giver_id = message.from_user.id
    giver_name = message.from_user.full_name
    
    text = message.text
    if not text or ' ' not in text:
        await message.reply(
            "❌ Использование: `/датьпредмет @username количество \"название предмета\"`\n"
            "Пример: `/датьпредмет @user 5 Горелый бекон`\n"
            "Или ответьте на сообщение пользователя: `/датьпредмет 5 Горелый бекон`"
        )
        return
    
    # Проверяем, есть ли ответ на сообщение
    target_user = None
    if message.reply_to_message:
        target_user = message.reply_to_message.from_user
        # Убираем username из команды если он есть
        without_command = text.split(' ', 1)[1] if ' ' in text else ''
        # Пробуем распарсить без @username
        import re
        match = re.match(r'(\d+)\s+(.+)$', without_command)
        if match:
            amount = int(match.group(1))
            item_name = match.group(2).strip()
        else:
            await message.reply("❌ Неправильный формат! Используйте: `/датьпредмет количество \"название предмета\"`")
            return
    else:
        without_command = text.split(' ', 1)[1] if ' ' in text else ''
        import re
        match = re.match(r'@(\S+)\s+(\d+)\s+(.+)$', without_command)
        
        if not match:
            await message.reply(
                "❌ Неправильный формат команды!\n"
                "Использование: `/датьпредмет @username количество \"название предмета\"`\n"
                "Или ответьте на сообщение пользователя: `/датьпредмет 5 Горелый бекон`"
            )
            return
        
        target_username = match.group(1)
        amount = int(match.group(2))
        item_name = match.group(3).strip()
        
        # Пытаемся найти пользователя
        try:
            target_user = await bot.get_chat_member(chat_id, target_username)
            target_user = target_user.user
        except:
            pass
    
    if not target_user:
        await message.reply(f"❌ Пользователь не найден! Используйте ответ на сообщение пользователя.")
        return
    
    target_id = target_user.id
    target_name = target_user.full_name
    
    if amount <= 0:
        await message.reply("❌ Количество должно быть больше 0!")
        return
    
    if giver_id == target_id:
        await message.reply("❌ Нельзя передавать предметы самому себе!")
        return
    
    giver_data = get_user_data(chat_id, giver_id, giver_name)
    target_data = get_user_data(chat_id, target_id, target_name)
    
    # Проверяем кейсы
    for case_id, case in CASES.items():
        if case_id != "daily" and case["name"].lower() in item_name.lower():
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
    
    # Проверяем авто-жир
    auto_fat_keywords = ["авто-жир", "автожир", "auto_fat", "auto-fat", "🤖"]
    is_auto_fat = any(word in item_name.lower() for word in auto_fat_keywords)
    
    if is_auto_fat:
        if giver_data.get('auto_fat_level', 0) < amount:
            await message.reply(f"❌ У вас недостаточно уровней авто-жира для передачи! Есть: {giver_data.get('auto_fat_level', 0)}")
            return
        
        new_giver_level = max(0, giver_data.get('auto_fat_level', 0) - amount)
        new_target_level = target_data.get('auto_fat_level', 0) + amount
        
        new_target_next_time = None
        if new_target_level > 0:
            interval = get_auto_fat_interval(new_target_level)
            if interval:
                new_target_next_time = datetime.now() + timedelta(hours=interval)
        
        update_user_data(chat_id, giver_id, auto_fat_level=new_giver_level)
        update_user_data(chat_id, target_id, auto_fat_level=new_target_level, next_auto_fat_time=new_target_next_time)
        
        response = f"🤖 **Передача авто-жира**\n\n"
        response += f"**{giver_name}** передал уровни авто-жира **{target_name}**!\n\n"
        response += f"📦 Количество: {amount} уровень(ей)\n"
        response += f"📤 У вас осталось: {new_giver_level} уровень(ей)\n"
        response += f"📥 У получателя: {new_target_level} уровень(ей)"
        
        if new_target_level > 0:
            interval = get_auto_fat_interval(new_target_level)
            response += f"\n⏰ Интервал получателя: каждые {interval} ч"
        
        await message.reply(response)
        return
    
    # Обычные предметы
    giver_items = get_user_items(giver_data['item_counts'])
    target_items = get_user_items(target_data['item_counts'])
    
    found_item = None
    for key in giver_items.keys():
        if key.lower() == item_name.lower():
            found_item = key
            break
    
    if not found_item:
        for key in giver_items.keys():
            if item_name.lower() in key.lower():
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

async def cmd_duel(message: types.Message):
    register_chat(message.chat.id)
    chat_id = message.chat.id
    challenger = message.from_user
    
    parts = message.text.split() if message.text else []
    
    if len(parts) < 3:
        await message.reply("❌ Использование: `/дуэль @username [количество кг или \"все\"]`\nПример: `/дуэль @user 100`\nИли ответьте на сообщение пользователя: `/дуэль 100`")
        return
    
    # Проверяем, есть ли ответ на сообщение
    target_user = None
    if message.reply_to_message:
        target_user = message.reply_to_message.from_user
        amount_str = parts[1] if len(parts) > 1 else "все"
    else:
        target_username = parts[1].replace('@', '')
        amount_str = parts[2] if len(parts) > 2 else "все"
        
        try:
            target_user = await bot.get_chat_member(chat_id, target_username)
            target_user = target_user.user
        except:
            pass
    
    if not target_user:
        await message.reply(f"❌ Пользователь не найден! Используйте ответ на сообщение пользователя.")
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
        
        try:
            await callback.message.delete_reply_markup()
        except:
            pass
        
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
            
            levels_gained, kg_reward, new_level = add_xp(chat_id, winner_id, XP_PER_DUEL_WIN)
            
            result_text = f"**Победитель:** {winner.full_name}\n\n"
            result_text += f"📊 **Результаты:**\n"
            result_text += f"{winner.full_name}: {challenger_data['current_number']} → **{winner_new_weight}** (+{amount})\n"
            result_text += f"{loser.full_name}: {opponent_data['current_number']} → **{loser_new_weight}** (-{amount})"
            
            if levels_gained > 0:
                result_text += f"\n\n⭐ +{kg_reward} кг за повышение уровня!"
            
        elif result == 1:
            winner = opponent.user
            winner_id = opponent_id
            loser = challenger.user
            loser_id = challenger_id
            winner_new_weight = opponent_data['current_number'] + amount
            loser_new_weight = challenger_data['current_number'] - amount
            
            update_user_data(chat_id, winner_id, number=winner_new_weight)
            update_user_data(chat_id, loser_id, number=loser_new_weight)
            
            levels_gained, kg_reward, new_level = add_xp(chat_id, winner_id, XP_PER_DUEL_WIN)
            
            result_text = f"**Победитель:** {winner.full_name}\n\n"
            result_text += f"📊 **Результаты:**\n"
            result_text += f"{winner.full_name}: {opponent_data['current_number']} → **{winner_new_weight}** (+{amount})\n"
            result_text += f"{loser.full_name}: {challenger_data['current_number']} → **{loser_new_weight}** (-{amount})"
            
            if levels_gained > 0:
                result_text += f"\n\n⭐ +{kg_reward} кг за повышение уровня!"
            
        else:
            result_text = f"🤝 **НИЧЬЯ!** 🤝\n\n"
            result_text += f"📊 **Результаты:**\n"
            result_text += f"{challenger.user.full_name}: {challenger_data['current_number']}\n"
            result_text += f"{opponent.user.full_name}: {opponent_data['current_number']}"
        
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

async def duel_animation(message: types.Message, challenger_name, opponent_name):
    duel_emojis = ["⬆️", "⬇️", "⚔️"]
    
    line = [random.choice(duel_emojis) for _ in range(100)]
    
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
    
    last_text = None
    
    for frame_num, center_pos in animation_frames:
        visible = line[center_pos-4:center_pos+5]
        display_line = "".join(visible[:4]) + "|" + visible[4] + "|" + "".join(visible[5:])
        
        frame_text = f"**{c_name}**\n**{display_line}**\n**{o_name}**"
        
        if frame_text != last_text:
            try:
                await anim_msg.edit_text(frame_text)
                last_text = frame_text
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
    
    return result

async def cmd_cancel_duel(message: types.Message):
    register_chat(message.chat.id)
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

# ===== ТЕСТЕРСКИЕ КОМАНДЫ =====
async def cmd_reset_cooldowns(message: types.Message):
    register_chat(message.chat.id)
    if not is_tester(message.from_user.id):
        await message.reply("❌ У вас нет прав для этой команды!")
        return
    
    chat_id = message.chat.id
    
    db_path = get_db_path(chat_id)
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('UPDATE user_fat SET fat_cooldown_time = NULL')
        fat_affected = cursor.rowcount
        cursor.execute('UPDATE user_fat SET last_case_time = NULL')
        case_affected = cursor.rowcount
        cursor.execute('UPDATE user_fat SET daily_case_last_time = NULL')
        daily_affected = cursor.rowcount
        conn.commit()
        conn.close()
        
        await message.reply(f"🔄 Кулдауны сброшены!\n⏰ /жир: {fat_affected} пользователей\n📦 /жиркейс: {case_affected} пользователей\n📅 Ежедневный кейс: {daily_affected} пользователей")
    else:
        await message.reply("❌ База данных не найдена!")

async def cmd_reset_all_users(message: types.Message):
    register_chat(message.chat.id)
    if not is_tester(message.from_user.id):
        await message.reply("❌ У вас нет прав для этой команды!")
        return
    
    chat_id = message.chat.id
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ ДА", callback_data="reset_confirm")],
            [InlineKeyboardButton(text="❌ НЕТ", callback_data="reset_cancel")]
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
            item_counts = "{}"''')
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        
        await callback.message.edit_text(f"⚖️ **Глобальный сброс**\n\nЗатронуто пользователей: {affected}")
    else:
        await callback.message.edit_text("❌ База данных не найдена!")

async def cmd_fat_reset(message: types.Message):
    register_chat(message.chat.id)
    if not is_tester(message.from_user.id):
        await message.reply("❌ У вас нет прав для этой команды!")
        return
    
    chat_id = message.chat.id
    parts = message.text.split() if message.text else []
    target_user = message.from_user
    
    if len(parts) > 1 and parts[1].startswith('@'):
        target_username = parts[1].replace('@', '')
        # В Telegram сложно получить пользователя
        await message.reply("❌ Укажите пользователя через ответ на сообщение!")
        return
    
    if message.reply_to_message:
        target_user = message.reply_to_message.from_user
    
    target_id = target_user.id
    target_name = target_user.full_name
    
    data = get_user_data(chat_id, target_id, target_name)
    
    update_data = {
        'number': 0,
        'consecutive_plus': 0,
        'consecutive_minus': 0,
        'jackpot_pity': 0,
        'item_counts': '{}'
    }
    
    update_user_data(chat_id, target_id, **update_data)
    
    await message.reply(f"✅ Вес {target_name} сброшен на 0kg")

async def cmd_give_shop_item(message: types.Message):
    register_chat(message.chat.id)
    if not is_tester(message.from_user.id):
        await message.reply("❌ У вас нет прав для этой команды!")
        return
    
    text = message.text
    if not text or ' ' not in text:
        await message.reply("❌ Использование: `/выдатьпредмет количество \"название предмета\"`\nПример: `/выдатьпредмет 5 Горелый бекон`")
        return
    
    without_command = text.split(' ', 1)[1] if ' ' in text else ''
    
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
    
    # Проверяем кейсы
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
    
    # Проверяем авто-жир
    auto_fat_keywords = ["авто-жир", "автожир", "auto_fat", "auto-fat", "🤖"]
    if any(word in item_name.lower() for word in auto_fat_keywords):
        new_level = min(data.get('auto_fat_level', 0) + amount, AUTO_FAT_MAX_LEVEL)
        interval = get_auto_fat_interval(new_level)
        next_time = datetime.now() + timedelta(hours=interval) if interval else None
        
        update_user_data(chat_id, user_id, auto_fat_level=new_level, next_auto_fat_time=next_time)
        
        response = f"🤖 **Выдача авто-жира**\n\n"
        response += f"**{user_name}** выдал себе уровни авто-жира!\n\n"
        response += f"📦 Получено: +{amount} уровень(ей)\n"
        response += f"📊 Теперь {new_level} уровень (каждые {interval} ч)"
        
        await message.reply(response)
        return
    
    # Обычные предметы
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

# ===== ЗАПУСК БОТА =====
async def on_startup():
    print("\n" + "="*60)
    print("✅ TELEGRAM БОТ ЗАПУЩЕН")
    print(f"📅 Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    os.makedirs(DB_FOLDER, exist_ok=True)
    
    # Запускаем фоновые задачи
    asyncio.create_task(auto_fat_loop())
    asyncio.create_task(passive_income_loop())
    asyncio.create_task(snatcher_loop())
    asyncio.create_task(hourly_effects_loop())

async def main():
    # Регистрируем команды
    dp.message.register(cmd_start, Command(commands=["start", "help"]))
    dp.message.register(cmd_fat, Command("жир"))
    dp.message.register(cmd_fat_case, Command("жиркейс"))
    dp.message.register(cmd_fat_case_chances, Command("жиркейс_шансы"))
    dp.message.register(cmd_fat_leaderboard, Command("жиротрясы"))
    dp.message.register(cmd_fat_stats, Command("жирстат"))
    dp.message.register(cmd_fat_info, Command("жиринфо"))
    dp.message.register(cmd_show_ranks, Command("жирзвания"))
    dp.message.register(cmd_cooldown_info, Command("жиркулдаун"))
    dp.message.register(cmd_show_inventory, Command("инвентарь"))
    dp.message.register(cmd_shop, Command("магазин"))
    dp.message.register(cmd_buy, Command("купить"))
    dp.message.register(cmd_sell, Command("продать"))
    dp.message.register(cmd_give_fat, Command("датьжир"))
    dp.message.register(cmd_give_item, Command("датьпредмет"))
    dp.message.register(cmd_duel, Command("дуэль"))
    dp.message.register(cmd_cancel_duel, Command("отмена"))
    dp.message.register(cmd_upgrade_user, Command("апгрейдюзер"))
    dp.message.register(cmd_upgrade_user_choice, Command("апгрейдюзер"))  # для чисел
    dp.message.register(cmd_upgrade, Command("апгрейд"))
    dp.message.register(cmd_upgrade_kg, Command("апгрейдкг"))
    dp.message.register(cmd_choose, Command("выбрать"))
    
    # Тестерские команды
    dp.message.register(cmd_reset_cooldowns, Command("сброскд"))
    dp.message.register(cmd_reset_all_users, Command("сбросвсех"))
    dp.message.register(cmd_fat_reset, Command("жир_сброс"))
    dp.message.register(cmd_give_shop_item, Command("выдатьпредмет"))
    
    await on_startup()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
