from __future__ import annotations

import asyncio
import calendar
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Literal

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart
from aiogram.filters.command import CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from dotenv import load_dotenv

from db import Database
from keyboards import back_to_main_menu, email_offer_kb, main_menu, month_selector

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
ALLOWED_EXTENSIONS = {"txt", "pdf", "png", "jpg", "jpeg", "webp", "bmp", "gif", "tiff"}
PaymentTarget = int | Literal["full"]


class PayFlow(StatesGroup):
    waiting_for_receipt = State()
    waiting_for_email = State()


@dataclass(slots=True)
class Settings:
    bot_token: str
    admin_chat_id: int
    course_chat_id: int
    course_chat_link: str
    db_path: str
    check_interval_hours: int
    course_start_date: date
    messages_file: str


@dataclass(slots=True)
class Messages:
    welcome: str
    about_course: str
    payment_details: str
    choose_payment_target: str
    selected_month_prompt: str
    selected_full_prompt: str
    unsupported_file_type: str
    premature_upload: str
    thanks_receipt: str
    email_offer: str
    email_invalid: str
    email_saved: str
    email_skipped: str
    reminder_template: str
    removed_template: str


def load_settings() -> Settings:
    load_dotenv()
    return Settings(
        bot_token=os.environ["BOT_TOKEN"],
        admin_chat_id=int(os.environ["ADMIN_CHAT_ID"]),
        course_chat_id=int(os.environ["COURSE_CHAT_ID"]),
        course_chat_link=os.environ["COURSE_CHAT_LINK"],
        db_path=os.getenv("DB_PATH", "bot.db"),
        check_interval_hours=int(os.getenv("CHECK_INTERVAL_HOURS", "24")),
        course_start_date=date.fromisoformat(os.getenv("COURSE_START_DATE", "2026-04-04")),
        messages_file=os.getenv("MESSAGES_FILE", "messages.json"),
    )


def load_messages(path: str) -> Messages:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return Messages(**raw)


router = Router()


@router.message(CommandStart())
async def start(message: Message, command: CommandObject, db: Database, settings: Settings, messages: Messages) -> None:
    source = command.args if command and command.args else None
    await db.upsert_user(
        user_id=message.from_user.id,
        user_name=message.from_user.username,
        user_fn=message.from_user.first_name,
        user_ln=message.from_user.last_name,
        source=source,
        course_start_date=settings.course_start_date.isoformat(),
    )
    await message.answer(messages.welcome, reply_markup=main_menu())


@router.message(F.text == "⬅️ Главное меню")
async def to_main_menu(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("Вы в главном меню.", reply_markup=main_menu())


@router.message(F.text == "О курсе")
async def about_course(message: Message, messages: Messages) -> None:
    await message.answer(messages.about_course, parse_mode="HTML")


@router.message(F.text == "Оплатить")
async def pay_menu(message: Message, messages: Messages) -> None:
    await message.answer(messages.payment_details, reply_markup=back_to_main_menu(), parse_mode="HTML")
    await message.answer(messages.choose_payment_target, reply_markup=month_selector())


@router.callback_query(F.data.startswith("month:"))
async def pick_month(callback: CallbackQuery, state: FSMContext, db: Database, messages: Messages) -> None:
    target = callback.data.split(":", maxsplit=1)[1]

    if target == "full":
        await state.set_state(PayFlow.waiting_for_receipt)
        await state.update_data(target="full")
        await db.clear_selected_month(callback.from_user.id)
        await callback.message.answer(messages.selected_full_prompt)
        await callback.answer()
        return

    month = int(target)
    await db.set_selected_month(callback.from_user.id, month)
    await state.set_state(PayFlow.waiting_for_receipt)
    await state.update_data(target=month)
    await callback.message.answer(messages.selected_month_prompt.format(month=month))
    await callback.answer()


@router.message(PayFlow.waiting_for_receipt, F.document)
@router.message(PayFlow.waiting_for_receipt, F.photo)
async def upload_receipt(
    message: Message,
    state: FSMContext,
    db: Database,
    bot: Bot,
    settings: Settings,
    messages: Messages,
) -> None:
    data = await state.get_data()
    target: PaymentTarget | None = data.get("target")
    if not target:
        await message.answer("Сначала выберите месяц через меню «Оплатить».", reply_markup=main_menu())
        await state.clear()
        return

    if message.document:
        ext = (message.document.file_name or "").split(".")[-1].lower()
        if ext and ext not in ALLOWED_EXTENSIONS:
            await message.answer(messages.unsupported_file_type)
            return

    human_target = "весь курс" if target == "full" else f"{target} месяц"
    caption = (
        f"Новый чек об оплате\n"
        f"user_id: {message.from_user.id}\n"
        f"user_name: @{message.from_user.username or '-'}\n"
        f"user_FN: {message.from_user.first_name or '-'}\n"
        f"user_LN: {message.from_user.last_name or '-'}\n"
        f"target: {human_target}"
    )

    if message.document:
        await bot.send_document(chat_id=settings.admin_chat_id, document=message.document.file_id, caption=caption)
    else:
        await bot.send_photo(chat_id=settings.admin_chat_id, photo=message.photo[-1].file_id, caption=caption)

    if target == "full":
        await db.mark_full_payment(message.from_user.id)
    else:
        await db.mark_payment(message.from_user.id, target)

    try:
        await bot.unban_chat_member(settings.course_chat_id, message.from_user.id, only_if_banned=True)
    except Exception as exc:  # noqa: BLE001
        logging.warning("Could not unban user %s: %s", message.from_user.id, exc)

    await message.answer(
        f"{messages.thanks_receipt}\n"
        f"Ссылка на учебный чат: {settings.course_chat_link}\n\n"
        f"{messages.email_offer}",
        reply_markup=email_offer_kb(),
    )
    await state.set_state(PayFlow.waiting_for_email)


@router.message(F.document | F.photo)
async def premature_receipt_upload(message: Message, messages: Messages) -> None:
    await message.answer(messages.premature_upload, reply_markup=main_menu())


@router.message(PayFlow.waiting_for_email, F.text == "Пропустить")
async def skip_email(message: Message, state: FSMContext, messages: Messages) -> None:
    await state.clear()
    await message.answer(messages.email_skipped, reply_markup=main_menu())


@router.message(PayFlow.waiting_for_email, F.text)
async def save_email(message: Message, state: FSMContext, db: Database, messages: Messages) -> None:
    email = message.text.strip()
    if not EMAIL_RE.match(email):
        await message.answer(messages.email_invalid)
        return
    await db.set_email(message.from_user.id, email)
    await state.clear()
    await message.answer(messages.email_saved, reply_markup=main_menu())


def add_months(base: date, months_ahead: int) -> date:
    month0 = (base.month - 1) + months_ahead
    year = base.year + month0 // 12
    month = month0 % 12 + 1
    day = min(base.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def payment_period_bounds(course_start_date: date, month_index: int) -> tuple[date, date]:
    period_start = add_months(course_start_date, month_index - 1)
    period_end = add_months(course_start_date, month_index) - timedelta(days=1)
    return period_start, period_end


def active_payment_month_index(today: date, course_start_date: date) -> int | None:
    for month_index in range(1, 7):
        period_start, period_end = payment_period_bounds(course_start_date, month_index)
        if period_start <= today <= period_end:
            return month_index
    return None


def current_due_payment_event(today: date, course_start_date: date) -> tuple[int, date, date] | None:
    for payment_index in range(2, 7):
        removal_date = add_months(course_start_date, payment_index - 1)
        reminder_date = removal_date - timedelta(days=3)
        if today == reminder_date or today == removal_date:
            return payment_index, reminder_date, removal_date
    return None


async def payment_guard_worker(bot: Bot, db: Database, settings: Settings, messages: Messages) -> None:
    while True:
        users = await db.iter_users()
        today = date.today()
        due_event = current_due_payment_event(today, settings.course_start_date)

        for user in users:
            if due_event is not None:
                payment_index, reminder_date, removal_date = due_event
                payment_value = user.payments[f"payment_{payment_index}"]
                paid_for_period = payment_value == "да"

                if today == reminder_date and not paid_for_period and user.last_reminder_month != payment_index:
                    try:
                        await bot.send_message(
                            user.user_id,
                            messages.reminder_template.format(
                                month=payment_index,
                                removal_date=removal_date.strftime("%d.%m.%Y"),
                            ),
                        )
                        await db.set_last_reminder_month(user.user_id, payment_index)
                    except Exception as exc:  # noqa: BLE001
                        logging.warning("Could not send reminder to %s: %s", user.user_id, exc)

                if today == removal_date and not paid_for_period and user.last_removal_month != payment_index:
                    try:
                        await bot.ban_chat_member(settings.course_chat_id, user.user_id)
                        await db.set_removed_flag(user.user_id, True)
                        await db.set_last_removal_month(user.user_id, payment_index)
                        await bot.send_message(
                            user.user_id,
                            messages.removed_template.format(
                                month=payment_index,
                                removal_date=removal_date.strftime("%d.%m.%Y"),
                            ),
                        )
                    except Exception as exc:  # noqa: BLE001
                        logging.warning("Could not remove user %s: %s", user.user_id, exc)

            if user.removed_from_chat and user.last_removal_month:
                debt_paid = user.payments.get(f"payment_{user.last_removal_month}") == "да"
                if debt_paid:
                    try:
                        await bot.unban_chat_member(settings.course_chat_id, user.user_id, only_if_banned=True)
                        await db.set_removed_flag(user.user_id, False)
                    except Exception as exc:  # noqa: BLE001
                        logging.warning("Could not unban user %s in worker: %s", user.user_id, exc)

        await asyncio.sleep(settings.check_interval_hours * 3600)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = load_settings()
    messages = load_messages(settings.messages_file)
    db = Database(settings.db_path)
    await db.init()

    bot = Bot(settings.bot_token)
    dp = Dispatcher()

    dp["db"] = db
    dp["settings"] = settings
    dp["messages"] = messages
    dp.include_router(router)

    guard_task = asyncio.create_task(payment_guard_worker(bot, db, settings, messages))
    try:
        await dp.start_polling(bot)
    finally:
        guard_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
