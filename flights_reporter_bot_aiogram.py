import asyncio
import logging
import yaml
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional

import pytz
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.enums.parse_mode import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

import clickhouse_connect

# ========== НАСТРОЙКИ ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Загрузка конфигурации
CONFIG_PATH = Path(__file__).parent / "flights_reporter_config.yaml"

def load_config():
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

CONFIG = load_config()

# Инициализация бота
bot = Bot(token=CONFIG['telegram']['token'])
dp = Dispatcher()

# Константы
LEADS_CHAT_ID = CONFIG['telegram'].get('leads_chat_id')
CHAT_ID = CONFIG['telegram']['chat_id']

# Планировщик
scheduler = AsyncIOScheduler(timezone=CONFIG['report']['timezone'])


# ========== КЛАВИАТУРЫ ==========
def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Главная клавиатура с кнопками"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="📊 Weekly Report"),
                KeyboardButton(text="📆 GET DETAILED FORECAST FOR NEXT MONTH")
            ],
            [
                KeyboardButton(text="📄 Get Full Report")
            ]
        ],
        resize_keyboard=True
    )


# ========== РАБОТА С БАЗОЙ ДАННЫХ ==========
def get_clickhouse_client(config):
    """Получение клиента ClickHouse"""
    ch_config = config['clickhouse']
    return clickhouse_connect.get_client(
        host=ch_config['host'],
        port=ch_config['port'],
        username=ch_config['user'],
        password=ch_config['password'],
        database=ch_config['database']
    )


def get_week_range(now_moscow):
    """Возвращает понедельник и воскресенье текущей недели"""
    days_to_monday = now_moscow.weekday()
    start_date = now_moscow - timedelta(days=days_to_monday)
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = start_date + timedelta(days=6)
    end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=0)
    return start_date, end_date


def calculate_percent_change(current, previous):
    """Расчет процента изменения"""
    if previous == 0:
        return 0
    return round(((current - previous) / previous) * 100, 1)


def calculate_trend(current, previous):
    """Расчет тренда (растет/падает/стабильно)"""
    if previous == 0:
        return "📈 growing" if current > 0 else "➡️ stable"
    
    if current > previous:
        return "📈 growing"
    elif current < previous:
        return "📉 falling"
    else:
        return "➡️ stable"


def get_week_data(client, config, start_date: str, end_date: str) -> Dict[str, Any]:
    """Получение данных за указанную неделю"""
    ch_config = config['clickhouse']
    table = ch_config['table']
    arrival_countries = config['report']['arrival_countries']
    flight_type_filter = config['report'].get('flight_type')
    top_limit = config['report'].get('top_countries_limit', 3)
    exclude_thailand = config['report'].get('exclude_thailand_from_top', False)
    
    countries_list = ", ".join([f"'{c}'" for c in arrival_countries])
    
    if flight_type_filter:
        flight_type_condition = f"AND flight_type = '{flight_type_filter}'"
    else:
        flight_type_condition = ""
    
    # Основные метрики
    query_metrics = f"""
    SELECT 
        COUNT(*) as total_flights,
        SUM(plane_capacity) as total_capacity
    FROM {table}
    WHERE arrival_country IN ({countries_list})
        AND flight_date BETWEEN '{start_date}' AND '{end_date}'
        {flight_type_condition}
    """
    
    # Самый загруженный день недели
    query_peak_day = f"""
    SELECT 
        toDayOfWeek(flight_date) as day_of_week,
        COUNT(*) as flights_count
    FROM {table}
    WHERE arrival_country IN ({countries_list})
        AND flight_date BETWEEN '{start_date}' AND '{end_date}'
        {flight_type_condition}
    GROUP BY day_of_week
    ORDER BY flights_count DESC
    LIMIT 1
    """
    
    # Данные за последние 4 недели для прогноза
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    four_weeks_ago = start_dt - timedelta(days=28)
    four_weeks_ago_str = four_weeks_ago.strftime('%Y-%m-%d')
    
    query_last_4_weeks = f"""
    SELECT 
        COUNT(*) as total_flights
    FROM {table}
    WHERE arrival_country IN ({countries_list})
        AND flight_date BETWEEN '{four_weeks_ago_str}' AND '{end_date}'
        {flight_type_condition}
    GROUP BY toStartOfWeek(flight_date, 0)
    ORDER BY toStartOfWeek(flight_date, 0)
    """
    
    # Условие исключения Таиланда
    thailand_exclude_condition = ""
    if exclude_thailand:
        thailand_exclude_condition = "AND departure_country NOT IN ('Thailand', 'Таиланд')"
    
    # ТОП-3 страны вылета
    query_top_countries = f"""
    SELECT 
        departure_country,
        SUM(plane_capacity) as capacity
    FROM {table}
    WHERE arrival_country IN ({countries_list})
        AND flight_date BETWEEN '{start_date}' AND '{end_date}'
        {flight_type_condition}
        AND departure_country IS NOT NULL
        AND departure_country != ''
        {thailand_exclude_condition}
    GROUP BY departure_country
    ORDER BY capacity DESC
    LIMIT {top_limit}
    """
    
    # Общая вместимость международных рейсов
    query_total_international = f"""
    SELECT 
        SUM(plane_capacity) as total_international_capacity
    FROM {table}
    WHERE arrival_country IN ({countries_list})
        AND flight_date BETWEEN '{start_date}' AND '{end_date}'
        {flight_type_condition}
        AND departure_country IS NOT NULL
        AND departure_country != ''
        {thailand_exclude_condition}
    """
    
    # Данные прошлой недели
    prev_start_dt = start_dt - timedelta(days=7)
    prev_end_dt = start_dt + timedelta(days=6) - timedelta(days=7)
    prev_start = prev_start_dt.strftime('%Y-%m-%d')
    prev_end = prev_end_dt.strftime('%Y-%m-%d')
    
    query_prev_metrics = f"""
    SELECT 
        COUNT(*) as total_flights
    FROM {table}
    WHERE arrival_country IN ({countries_list})
        AND flight_date BETWEEN '{prev_start}' AND '{prev_end}'
        {flight_type_condition}
    """
    
    # Данные прошлой недели для трендов стран
    query_prev_countries = f"""
    SELECT 
        departure_country,
        SUM(plane_capacity) as capacity
    FROM {table}
    WHERE arrival_country IN ({countries_list})
        AND flight_date BETWEEN '{prev_start}' AND '{prev_end}'
        {flight_type_condition}
        AND departure_country IS NOT NULL
        AND departure_country != ''
        {thailand_exclude_condition}
    GROUP BY departure_country
    """
    
    # Выполнение запросов (синхронно, но это не блокирует бота)
    result_metrics = client.query(query_metrics)
    result_peak_day = client.query(query_peak_day)
    result_top = client.query(query_top_countries)
    result_total_international = client.query(query_total_international)
    result_prev_metrics = client.query(query_prev_metrics)
    result_prev_countries = client.query(query_prev_countries)
    result_last_4_weeks = client.query(query_last_4_weeks)
    
    # Текущие метрики
    if result_metrics.result_rows:
        row = result_metrics.result_rows[0]
        total_flights = row[0] if row[0] is not None else 0
        total_capacity = row[1] if row[1] is not None else 0
    else:
        total_flights = 0
        total_capacity = 0
    
    # Пиковый день
    days_map = {1: "Monday", 2: "Tuesday", 3: "Wednesday",
                4: "Thursday", 5: "Friday", 6: "Saturday", 7: "Sunday"}
    peak_day = None
    peak_day_flights = 0
    if result_peak_day.result_rows:
        row = result_peak_day.result_rows[0]
        peak_day = days_map.get(row[0], "Unknown")
        peak_day_flights = row[1] if row[1] is not None else 0
    
    # Общая вместимость международных рейсов
    total_international_capacity = 0
    if result_total_international.result_rows:
        row = result_total_international.result_rows[0]
        total_international_capacity = row[0] if row[0] is not None else 0
    
    # Метрики прошлой недели
    prev_flights = 0
    if result_prev_metrics.result_rows:
        row = result_prev_metrics.result_rows[0]
        prev_flights = row[0] if row[0] is not None else 0
    
    # Прогноз на следующую неделю
    forecast_next = None
    forecast_change = None
    
    if result_last_4_weeks.result_rows:
        weeks_data = []
        for row in result_last_4_weeks.result_rows:
            if row[0] is not None:
                weeks_data.append(row[0])
        
        if len(weeks_data) >= 3:
            prev_3_avg = sum(weeks_data[-3:-1]) / 2 if len(weeks_data[-3:-1]) > 0 else total_flights
            trend = (total_flights - prev_3_avg) / prev_3_avg if prev_3_avg > 0 else 0
            forecast_next = max(0, int(total_flights * (1 + trend)))
            forecast_change = round(((forecast_next - total_flights) / total_flights) * 100, 1) if total_flights > 0 else 0
    
    # Маппинг стран из конфига
    country_name_map = config.get('country_name_en', {})
    
    # ТОП-3 страны
    top_countries = []
    if result_top.result_rows:
        for row in result_top.result_rows:
            if row[0]:
                country_raw = row[0]
                country_eng = country_name_map.get(country_raw, country_raw)
                top_countries.append({
                    'country': country_eng,
                    'capacity': row[1] if row[1] is not None else 0
                })
    
    # Данные прошлой недели для трендов
    prev_countries_capacity = {}
    if result_prev_countries.result_rows:
        for row in result_prev_countries.result_rows:
            if row[0]:
                country_raw = row[0]
                country_eng = country_name_map.get(country_raw, country_raw)
                prev_countries_capacity[country_eng] = row[1] if row[1] is not None else 0
    
    return {
        'flights': total_flights,
        'capacity': total_capacity,
        'prev_flights': prev_flights,
        'top_countries': top_countries,
        'prev_countries_capacity': prev_countries_capacity,
        'total_international_capacity': total_international_capacity,
        'peak_day': peak_day,
        'peak_day_flights': peak_day_flights,
        'forecast_next': forecast_next,
        'forecast_change': forecast_change
    }


def format_week_report(week_data: Dict[str, Any], start_date_str: str, end_date_str: str,
                       next_week_start_str: str, next_week_end_str: str, config: Dict) -> str:
    """Форматирование отчета за одну неделю"""
    
    flights = week_data['flights']
    capacity = week_data['capacity']
    top_countries = week_data['top_countries']
    prev_countries = week_data['prev_countries_capacity']
    exclude_thailand = config['report'].get('exclude_thailand_from_top', False)
    peak_day = week_data.get('peak_day')
    peak_day_flights = week_data.get('peak_day_flights', 0)
    forecast_next = week_data.get('forecast_next')
    forecast_change = week_data.get('forecast_change')
    
    # Сезонные нормы
    seasonal = config['report'].get('seasonal_averages', {})
    high_season_avg = seasonal.get('high_season', 850)
    low_season_avg = seasonal.get('low_season', 620)
    
    high_season_diff = calculate_percent_change(flights, high_season_avg)
    low_season_diff = calculate_percent_change(flights, low_season_avg)
    
    # Формирование отчета
    report = (
        f"🗓 Forecast for the week: {start_date_str} - {end_date_str}\n"
        f"<b>{flights}</b> flights arriving in Phuket with total capacity of <b>{capacity:,}</b> seats.\n\n"
    )
    
    # Пиковый день
    if peak_day and peak_day_flights > 0:
        report += f"📅 Busiest day: <b>{peak_day}</b> ({peak_day_flights} flights)\n"
    
    # Пустая строка между пиковым днем и прогнозом
    if peak_day and peak_day_flights > 0 and forecast_next and forecast_change is not None:
        report += f"\n"
    
    # Прогноз с периодом
    if forecast_next and forecast_change is not None:
        forecast_symbol = "📈" if forecast_change > 0 else "📉" if forecast_change < 0 else "➡️"
        if forecast_change > 0:
            forecast_text = f"+{forecast_change}%"
        elif forecast_change < 0:
            forecast_text = f"{forecast_change}%"
        else:
            forecast_text = "0%"
        report += f"🔮 Next week forecast ({next_week_start_str} - {next_week_end_str}): <b>{forecast_next}</b> flights ({forecast_symbol} {forecast_text})\n"
    
    report += f"\n📈 Comparison with seasonal norms:\n"
    report += f"— Deviation from high season average: <b>{high_season_diff}%</b>\n"
    report += f"— Deviation from low season average: <b>{low_season_diff}%</b>\n\n"
    
    # Заголовок ТОП-3
    if exclude_thailand:
        report += f"✈️ TOP-3 destinations (excluding Thailand):\n"
    else:
        report += f"✈️ TOP-3 destinations (where guests come from):\n"
    
    # Общая вместимость для расчета долей
    total_international_capacity = week_data.get('total_international_capacity', 0)
    if total_international_capacity == 0:
        total_international_capacity = sum(c['capacity'] for c in top_countries) if top_countries else 1
    
    # Эмодзи стран из конфига
    country_emoji_map = config.get('country_emoji', {})
    
    for country_data in top_countries:
        country = country_data['country']
        capacity_country = country_data['capacity']
        
        # Доля в процентах
        share = round((capacity_country / total_international_capacity) * 100, 1)
        
        # Тренд по стране
        prev_capacity = prev_countries.get(country, 0)
        trend = calculate_trend(capacity_country, prev_capacity)
        
        # Эмодзи из конфига
        country_emoji = country_emoji_map.get(country, '🌏')
        
        report += f"{country_emoji} <b>{country}</b> — {share}% ({trend})\n"
    
    return report


def get_weekly_report(config: Dict) -> str:
    """Получение недельного отчета"""
    client = get_clickhouse_client(config)
    try:
        moscow_tz = pytz.timezone(config['report']['timezone'])
        now_moscow = datetime.now(moscow_tz)
        start_date, end_date = get_week_range(now_moscow)
        
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        # Даты следующей недели
        next_week_start = start_date + timedelta(days=7)
        next_week_end = end_date + timedelta(days=7)
        next_week_start_str = next_week_start.strftime('%Y-%m-%d')
        next_week_end_str = next_week_end.strftime('%Y-%m-%d')
        
        week_data = get_week_data(client, config, start_date_str, end_date_str)
        report = f"📊 Weekly FORECAST: Phuket\n\n"
        report += format_week_report(week_data, start_date_str, end_date_str, 
                                      next_week_start_str, next_week_end_str, config)
        return report
    finally:
        client.close()


def get_monthly_report(config: Dict) -> str:
    """Получение месячного отчета (4 недели)"""
    client = get_clickhouse_client(config)
    try:
        moscow_tz = pytz.timezone(config['report']['timezone'])
        now_moscow = datetime.now(moscow_tz)
        
        # Находим понедельник текущей недели
        days_to_monday = now_moscow.weekday()
        current_monday = now_moscow - timedelta(days=days_to_monday)
        current_monday = current_monday.replace(hour=0, minute=0, second=0, microsecond=0)
        
        report = f"📊 Monthly Forecast: Phuket\n\n"
        report += f"🗓 Detailed forecast for next 4 weeks\n"
        report += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        
        for i in range(4):
            week_start = current_monday + timedelta(days=i * 7)
            week_end = week_start + timedelta(days=6)
            
            week_start_str = week_start.strftime('%Y-%m-%d')
            week_end_str = week_end.strftime('%Y-%m-%d')
            
            # Даты следующей недели для прогноза
            next_week_start = week_start + timedelta(days=7)
            next_week_end = week_end + timedelta(days=7)
            next_week_start_str = next_week_start.strftime('%Y-%m-%d')
            next_week_end_str = next_week_end.strftime('%Y-%m-%d')
            
            week_data = get_week_data(client, config, week_start_str, week_end_str)
            report += format_week_report(week_data, week_start_str, week_end_str,
                                          next_week_start_str, next_week_end_str, config)
            report += f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
        
        return report
    finally:
        client.close()


# ========== ПЛАНОВАЯ ОТПРАВКА ЧЕРЕЗ APSCHEDULER ==========
async def scheduled_weekly_report():
    """Плановая отправка отчета"""
    logger.info("Running scheduled weekly report...")
    try:
        report_text = get_weekly_report(CONFIG)
        await bot.send_message(chat_id=CHAT_ID, text=report_text, parse_mode=ParseMode.HTML)
        logger.info("Scheduled report sent successfully")
    except Exception as e:
        logger.error(f"Error in scheduled report: {e}")


def setup_scheduler():
    """Настройка планировщика"""
    report_config = CONFIG['report']
    weekday = report_config['weekday']  # 0 = понедельник
    time_str = report_config['time']     # "07:00"
    
    hour, minute = map(int, time_str.split(':'))
    
    # Дни недели для apscheduler: mon=1, tue=2, wed=3, thu=4, fri=5, sat=6, sun=7
    weekday_map = {
        0: 'mon',  # понедельник
        1: 'tue',  # вторник
        2: 'wed',  # среда
        3: 'thu',  # четверг
        4: 'fri',  # пятница
        5: 'sat',  # суббота
        6: 'sun'   # воскресенье
    }
    
    day_of_week = weekday_map.get(weekday, 'mon')
    
    scheduler.add_job(
        scheduled_weekly_report,
        trigger=CronTrigger(day_of_week=day_of_week, hour=hour, minute=minute, timezone=report_config['timezone']),
        id='weekly_report',
        replace_existing=True
    )
    
    logger.info(f"Scheduled weekly report: every {day_of_week} at {time_str} {report_config['timezone']}")


# ========== ОБРАБОТЧИКИ СООБЩЕНИЙ ==========
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Обработчик команды /start"""
    # Проверяем, не является ли чат чатом для лидов
    if LEADS_CHAT_ID and message.chat.id == LEADS_CHAT_ID:
        await message.answer("✈️ Bot is active. New leads will appear here.")
    else:
        await message.answer(
            "✈️ Welcome!\n\nI am a bot for tracking flights to Thailand.\nChoose a report:",
            reply_markup=get_main_keyboard()
        )


@dp.message(F.text == "📊 Weekly Report")
async def weekly_report(message: types.Message):
    """Обработчик кнопки Weekly Report"""
    # Игнорируем в чате лидов
    if LEADS_CHAT_ID and message.chat.id == LEADS_CHAT_ID:
        logger.info(f"Ignoring Weekly Report in leads chat {message.chat.id}")
        return
    
    logger.info("Generating weekly report...")
    await bot.send_chat_action(message.chat.id, action="typing")
    
    try:
        report_text = get_weekly_report(CONFIG)
        await message.answer(report_text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
        logger.info("Weekly report sent")
    except Exception as e:
        logger.error(f"Error generating weekly report: {e}")
        await message.answer(f"❌ Error generating report: {str(e)[:200]}", reply_markup=get_main_keyboard())


@dp.message(F.text == "📆 GET DETAILED FORECAST FOR NEXT MONTH")
async def monthly_report(message: types.Message):
    """Обработчик кнопки Monthly Report"""
    # Игнорируем в чате лидов
    if LEADS_CHAT_ID and message.chat.id == LEADS_CHAT_ID:
        logger.info(f"Ignoring Monthly Report in leads chat {message.chat.id}")
        return
    
    logger.info("Generating monthly forecast...")
    await bot.send_chat_action(message.chat.id, action="typing")
    
    try:
        report_text = get_monthly_report(CONFIG)
        await message.answer(report_text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
        logger.info("Monthly forecast sent")
    except Exception as e:
        logger.error(f"Error generating monthly report: {e}")
        await message.answer(f"❌ Error generating report: {str(e)[:200]}", reply_markup=get_main_keyboard())


@dp.message(F.text == "📄 Get Full Report")
async def full_report(message: types.Message):
    """Обработчик кнопки Get Full Report"""
    logger.info(f"Full report requested by user {message.chat.id}")
    
    # Получаем информацию о пользователе
    user = message.from_user
    user_name = user.full_name if user.full_name else "Unknown"
    user_link = f"@{user.username}" if user.username else f"ID: {user.id}"
    
    # Текущее время
    moscow_tz = pytz.timezone('Europe/Moscow')
    time_str = datetime.now(moscow_tz).strftime("%Y-%m-%d %H:%M:%S")
    
    # Формируем сообщение о лиде
    lead_message = (
        f"🆕 <b>NEW LEAD!</b>\n\n"
        f"👤 Name: {user_name}\n"
        f"🔗 {user_link}\n"
        f"🆔 Chat ID: <code>{user.id}</code>\n"
        f"📅 Time: {time_str}\n"
        f"📄 Requested: Full Report\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 <i>Lead captured. Follow up required.</i>"
    )
    
    # Отправляем лид в чат для лидов
    if LEADS_CHAT_ID:
        await bot.send_message(chat_id=LEADS_CHAT_ID, text=lead_message, parse_mode=ParseMode.HTML)
        logger.info(f"Lead sent to leads chat {LEADS_CHAT_ID}")
    else:
        # Если чат не указан, отправляем в тот же чат
        await message.answer(lead_message, parse_mode=ParseMode.HTML)
        logger.info("Lead sent to same chat (no leads_chat_id configured)")
    
    # Подтверждение пользователю
    confirm_text = (
        "✅ Thank you for your interest!\n\n"
        "Your request has been sent to our team. "
        "We will contact you shortly."
    )
    await message.answer(confirm_text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
    logger.info("Lead captured and notification sent")


@dp.message()
async def unknown_message(message: types.Message):
    """Обработчик неизвестных сообщений"""
    # Игнорируем в чате лидов
    if LEADS_CHAT_ID and message.chat.id == LEADS_CHAT_ID:
        logger.info(f"Ignoring unknown message in leads chat {message.chat.id}")
        return
    
    await message.answer("Choose a report:", reply_markup=get_main_keyboard())


# ========== ЗАПУСК ==========
async def main():
    """Главная функция запуска бота"""
    logger.info("Starting bot...")
    
    # Настройка планировщика
    setup_scheduler()
    
    # Запуск планировщика
    scheduler.start()
    
    logger.info("Bot started")
    logger.info("Send any message to the bot in Telegram")
    logger.info("Press Ctrl+C to stop")
    
    # Запускаем поллинг
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")