from aiogram.types import InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder


def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="О курсе")],
            [KeyboardButton(text="Оплатить")],
        ],
        resize_keyboard=True,
    )


def back_to_main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ Главное меню")]],
        resize_keyboard=True,
    )


def month_selector() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for month in range(1, 7):
        builder.button(text=f"{month} месяц", callback_data=f"month:{month}")
    builder.button(text="Весь курс", callback_data="month:full")
    builder.adjust(3)
    return builder.as_markup()


def email_offer_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Пропустить")],
            [KeyboardButton(text="⬅️ Главное меню")],
        ],
        resize_keyboard=True,
    )
