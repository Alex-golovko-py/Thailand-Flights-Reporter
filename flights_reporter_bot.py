import clickhouse_connect
import schedule
import time
import yaml
from datetime import datetime, timedelta
import pytz
import logging
import requests
import json
import signal
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

running = True


def signal_handler(signum, frame):
    global running
    logger.info("Received stop signal. Shutting down...")
    running = False


def load_config(config_path='flights_reporter_config.yaml'):
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config


def get_clickhouse_client(config):
    ch_config = config['clickhouse']
    client = clickhouse_connect.get_client(
        host=ch_config['host'],
        port=ch_config['port'],
        username=ch_config['user'],
        password=ch_config['password'],
        database=ch_config['database']
    )
    return client


def get_week_range(now_moscow):
    days_to_monday = now_moscow.weekday()
    start_date = now_moscow - timedelta(days=days_to_monday)
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = start_date + timedelta(days=6)
    end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=0)
    return start_date, end_date


def get_week_data(client, config, start_date, end_date):
    """Get data for specified week"""
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
    
    # Main metrics
    query_metrics = f"""
    SELECT 
        COUNT(*) as total_flights,
        SUM(plane_capacity) as total_capacity
    FROM {table}
    WHERE arrival_country IN ({countries_list})
        AND flight_date BETWEEN '{start_date}' AND '{end_date}'
        {flight_type_condition}
    """
    
    # Peak day of week
    query_peak_day = f"""
    SELECT 
        toDayOfWeek(flight_date) as day_of_week,
        COUNT(*) as flights_count,
        SUM(plane_capacity) as capacity
    FROM {table}
    WHERE arrival_country IN ({countries_list})
        AND flight_date BETWEEN '{start_date}' AND '{end_date}'
        {flight_type_condition}
    GROUP BY day_of_week
    ORDER BY flights_count DESC
    LIMIT 1
    """
    
    # Data for last 4 weeks for forecast
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    four_weeks_ago = start_dt - timedelta(days=28)
    four_weeks_ago_str = four_weeks_ago.strftime('%Y-%m-%d')
    
    query_last_4_weeks = f"""
    SELECT 
        toStartOfWeek(flight_date, 0) as week_start,
        COUNT(*) as total_flights
    FROM {table}
    WHERE arrival_country IN ({countries_list})
        AND flight_date BETWEEN '{four_weeks_ago_str}' AND '{end_date}'
        {flight_type_condition}
    GROUP BY week_start
    ORDER BY week_start
    """
    
    # Exclude Thailand condition
    thailand_exclude_condition = ""
    if exclude_thailand:
        thailand_exclude_condition = "AND departure_country NOT IN ('Thailand', 'Таиланд')"
    
    # TOP-3 departure countries
    query_top_countries = f"""
    SELECT 
        departure_country,
        COUNT(*) as flights,
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
    
    # Total international capacity
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
    
    # Previous week data for forecast
    prev_start_dt = start_dt - timedelta(days=7)
    prev_end_dt = start_dt + timedelta(days=6) - timedelta(days=7)
    prev_start = prev_start_dt.strftime('%Y-%m-%d')
    prev_end = prev_end_dt.strftime('%Y-%m-%d')
    
    query_prev_metrics = f"""
    SELECT 
        COUNT(*) as total_flights,
        SUM(plane_capacity) as total_capacity
    FROM {table}
    WHERE arrival_country IN ({countries_list})
        AND flight_date BETWEEN '{prev_start}' AND '{prev_end}'
        {flight_type_condition}
    """
    
    # Previous week countries data for trends
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
    
    result_metrics = client.query(query_metrics)
    result_peak_day = client.query(query_peak_day)
    result_top = client.query(query_top_countries)
    result_total_international = client.query(query_total_international)
    result_prev_metrics = client.query(query_prev_metrics)
    result_prev_countries = client.query(query_prev_countries)
    result_last_4_weeks = client.query(query_last_4_weeks)
    
    # Current metrics
    if result_metrics.result_rows:
        row = result_metrics.result_rows[0]
        total_flights = row[0] if row[0] is not None else 0
        total_capacity = row[1] if row[1] is not None else 0
    else:
        total_flights = 0
        total_capacity = 0
    
    # Peak day
    peak_day = None
    peak_day_flights = 0
    days_map = {
        1: "Monday", 2: "Tuesday", 3: "Wednesday",
        4: "Thursday", 5: "Friday", 6: "Saturday", 7: "Sunday"
    }
    if result_peak_day.result_rows:
        row = result_peak_day.result_rows[0]
        peak_day = days_map.get(row[0], "Unknown")
        peak_day_flights = row[1] if row[1] is not None else 0
    
    # Total international capacity
    total_international_capacity = 0
    if result_total_international.result_rows:
        row = result_total_international.result_rows[0]
        total_international_capacity = row[0] if row[0] is not None else 0
    
    # Previous week metrics
    if result_prev_metrics.result_rows:
        row = result_prev_metrics.result_rows[0]
        prev_flights = row[0] if row[0] is not None else 0
        prev_capacity = row[1] if row[1] is not None else 0
    else:
        prev_flights = 0
        prev_capacity = 0
    
    # Forecast for next week
    forecast_next = None
    forecast_change = None
    
    if result_last_4_weeks.result_rows:
        weeks_data = []
        for row in result_last_4_weeks.result_rows:
            if row[1] is not None:
                weeks_data.append(row[1])
        
        if len(weeks_data) >= 3:
            prev_3_avg = sum(weeks_data[-3:-1]) / 2 if len(weeks_data[-3:-1]) > 0 else total_flights
            trend = (total_flights - prev_3_avg) / prev_3_avg if prev_3_avg > 0 else 0
            forecast_next = max(0, int(total_flights * (1 + trend)))
            forecast_change = round(((forecast_next - total_flights) / total_flights) * 100, 1) if total_flights > 0 else 0
    
    # Получаем маппинг названий стран из конфига
    country_name_map = config.get('country_name_en', {})
    
    # TOP-3 countries with English names
    top_countries = []
    if result_top.result_rows:
        for row in result_top.result_rows:
            if row[0]:
                country_raw = row[0]
                # Convert to English using config mapping
                country_eng = country_name_map.get(country_raw, country_raw)
                top_countries.append({
                    'country': country_eng,
                    'flights': row[1],
                    'capacity': row[2] if row[2] is not None else 0
                })
    
    # Previous week countries data with English names
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
        'prev_capacity': prev_capacity,
        'top_countries': top_countries,
        'prev_countries_capacity': prev_countries_capacity,
        'total_international_capacity': total_international_capacity,
        'peak_day': peak_day,
        'peak_day_flights': peak_day_flights,
        'forecast_next': forecast_next,
        'forecast_change': forecast_change
    }


def calculate_trend(current, previous):
    """Calculate trend (growing/falling/stable)"""
    if previous == 0:
        return "📈 growing" if current > 0 else "➡️ stable"
    
    if current > previous:
        return "📈 growing"
    elif current < previous:
        return "📉 falling"
    else:
        return "➡️ stable"


def calculate_percent_change(current, previous):
    """Calculate percent change"""
    if previous == 0:
        return 0
    return round(((current - previous) / previous) * 100, 1)


def format_week_report(week_data, start_date_str, end_date_str, next_week_start_str, next_week_end_str, config):
    """Format single week report - dates are strings already"""
    
    flights = week_data['flights']
    capacity = week_data['capacity']
    top_countries = week_data['top_countries']
    prev_countries = week_data['prev_countries_capacity']
    exclude_thailand = config['report'].get('exclude_thailand_from_top', False)
    peak_day = week_data.get('peak_day')
    peak_day_flights = week_data.get('peak_day_flights', 0)
    forecast_next = week_data.get('forecast_next')
    forecast_change = week_data.get('forecast_change')
    
    # Seasonal norms
    seasonal = config['report'].get('seasonal_averages', {})
    high_season_avg = seasonal.get('high_season', 850)
    low_season_avg = seasonal.get('low_season', 620)
    
    high_season_diff = calculate_percent_change(flights, high_season_avg)
    low_season_diff = calculate_percent_change(flights, low_season_avg)
    
    # Format report
    report = (
        f"🗓 Forecast for the week: {start_date_str} - {end_date_str}\n"
        f"<b>{flights}</b> flights arriving in Phuket with total capacity of <b>{capacity:,}</b> seats.\n\n"
    )
    
    # Peak day
    if peak_day and peak_day_flights > 0:
        report += f"📅 Busiest day: <b>{peak_day}</b> ({peak_day_flights} flights)\n"
    
    # Empty line between busiest day and forecast
    if peak_day and peak_day_flights > 0 and forecast_next and forecast_change is not None:
        report += f"\n"
    
    # Forecast with period
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
    
    # TOP-3 header
    if exclude_thailand:
        report += f"✈️ TOP-3 destinations (excluding Thailand):\n"
    else:
        report += f"✈️ TOP-3 destinations (where guests come from):\n"
    
    # Calculate total international capacity for shares
    total_international_capacity = week_data.get('total_international_capacity', 0)
    
    if total_international_capacity == 0:
        total_international_capacity = sum(c['capacity'] for c in top_countries) if top_countries else 1
    
    # Получаем маппинг эмодзи из конфига
    country_emoji_map = config.get('country_emoji', {})
    
    for country_data in top_countries:
        country = country_data['country']
        capacity_country = country_data['capacity']
        
        # Share percentage
        share = round((capacity_country / total_international_capacity) * 100, 1)
        
        # Country trend
        prev_capacity = prev_countries.get(country, 0)
        trend = calculate_trend(capacity_country, prev_capacity)
        
        # Country emoji from config
        country_emoji = country_emoji_map.get(country, '🌏')
        
        report += f"{country_emoji} <b>{country}</b> — {share}% ({trend})\n"
    
    return report


def get_weekly_report(client, config):
    """Weekly detailed report"""
    moscow_tz = pytz.timezone(config['report']['timezone'])
    now_moscow = datetime.now(moscow_tz)
    start_date, end_date = get_week_range(now_moscow)
    
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    # Calculate next week dates
    next_week_start = start_date + timedelta(days=7)
    next_week_end = end_date + timedelta(days=7)
    next_week_start_str = next_week_start.strftime('%Y-%m-%d')
    next_week_end_str = next_week_end.strftime('%Y-%m-%d')
    
    week_data = get_week_data(client, config, start_date_str, end_date_str)
    report = f"📊 Weekly FORECAST: Phuket\n\n"
    report += format_week_report(week_data, start_date_str, end_date_str, next_week_start_str, next_week_end_str, config)
    
    return report


def get_weekly_report_simple(client, config):
    """Simple weekly report for scheduled sending"""
    moscow_tz = pytz.timezone(config['report']['timezone'])
    now_moscow = datetime.now(moscow_tz)
    start_date, end_date = get_week_range(now_moscow)
    
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    # Calculate next week dates
    next_week_start = start_date + timedelta(days=7)
    next_week_end = end_date + timedelta(days=7)
    next_week_start_str = next_week_start.strftime('%Y-%m-%d')
    next_week_end_str = next_week_end.strftime('%Y-%m-%d')
    
    week_data = get_week_data(client, config, start_date_str, end_date_str)
    report = f"📊 Weekly FORECAST: Phuket\n\n"
    report += format_week_report(week_data, start_date_str, end_date_str, next_week_start_str, next_week_end_str, config)
    
    return report


def get_monthly_weekly_report(client, config):
    """Monthly report - next 4 weeks from current week"""
    moscow_tz = pytz.timezone(config['report']['timezone'])
    now_moscow = datetime.now(moscow_tz)
    
    # Get current week start (Monday)
    days_to_monday = now_moscow.weekday()
    current_monday = now_moscow - timedelta(days=days_to_monday)
    current_monday = current_monday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Generate next 4 weeks
    report = f"📊 Monthly Forecast: Phuket\n\n"
    report += f"🗓 Detailed forecast for next 4 weeks\n"
    report += f"━━━━━━━━━━━━━━━━━━━━━━\n"
    
    for i in range(4):
        week_start = current_monday + timedelta(days=i * 7)
        week_end = week_start + timedelta(days=6)
        
        week_start_str = week_start.strftime('%Y-%m-%d')
        week_end_str = week_end.strftime('%Y-%m-%d')
        
        # Calculate next week dates for forecast (i+1 week)
        next_week_start = week_start + timedelta(days=7)
        next_week_end = week_end + timedelta(days=7)
        next_week_start_str = next_week_start.strftime('%Y-%m-%d')
        next_week_end_str = next_week_end.strftime('%Y-%m-%d')
        
        # Get data for this week
        week_data = get_week_data(client, config, week_start_str, week_end_str)
        
        # Format week report with all required arguments
        report += format_week_report(week_data, week_start_str, week_end_str, next_week_start_str, next_week_end_str, config)
        report += f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
    
    return report


def send_telegram_message(bot_token, chat_id, text, reply_markup=None):
    """Send message to Telegram"""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': 'HTML'
    }
    if reply_markup:
        payload['reply_markup'] = json.dumps(reply_markup)
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        logger.info("Message sent successfully")
        return True
    except Exception as e:
        logger.error(f"Error sending message: {e}")
        return False


def get_main_keyboard():
    """Main keyboard with buttons - English only"""
    keyboard = {
        "keyboard": [
            [
                {"text": "📊 Weekly Report"},
                {"text": "📆 GET DETAILED FORECAST FOR NEXT MONTH"}
            ],
            [
                {"text": "📄 Get Full Report"}
            ]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }
    return keyboard


def handle_updates(bot_token, last_update_id, config):
    """Handle incoming messages"""
    url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
    params = {
        'offset': last_update_id + 1,
        'timeout': 30
    }
    
    try:
        response = requests.get(url, params=params, timeout=35)
        data = response.json()
        
        if data['ok'] and data['result']:
            processed_ids = set()
            
            for update in data['result']:
                update_id = update['update_id']
                
                if update_id in processed_ids:
                    continue
                processed_ids.add(update_id)
                
                message = update.get('message')
                
                if message:
                    chat_id = message['chat']['id']
                    text = message.get('text', '')
                    
                    logger.info(f"Received message: {text} from chat_id: {chat_id}")
                    
                    # Get leads chat ID from config
                    leads_chat_id = config['telegram'].get('leads_chat_id')
                    
                    # Check if this is the leads chat (convert to int for comparison)
                    is_leads_chat = False
                    if leads_chat_id is not None:
                        try:
                            if int(chat_id) == int(leads_chat_id):
                                is_leads_chat = True
                        except:
                            pass
                    
                    # Rate limiting
                    current_time = time.time()
                    if not hasattr(handle_updates, 'last_request_time'):
                        handle_updates.last_request_time = {}
                    
                    last_time = handle_updates.last_request_time.get(chat_id, 0)
                    if current_time - last_time < 2:
                        logger.info(f"Too frequent requests, ignoring")
                        continue
                    handle_updates.last_request_time[chat_id] = current_time
                    
                    client = None
                    try:
                        client = get_clickhouse_client(config)
                        
                        # /start command - works in any chat, but without keyboard in leads chat
                        if text == '/start':
                            if is_leads_chat:
                                # In leads chat - only send simple response, no keyboard
                                send_telegram_message(
                                    bot_token, chat_id, "✈️ Bot is active. New leads will appear here."
                                )
                            else:
                                # In user chat - send welcome with keyboard
                                welcome_text = (
                                    "✈️ Welcome!\n\n"
                                    "I am a bot for tracking flights to Thailand.\n"
                                    "Choose a report:"
                                )
                                send_telegram_message(
                                    bot_token, chat_id, welcome_text, get_main_keyboard()
                                )
                        
                        # Weekly Report
                        elif text == '📊 Weekly Report':
                            if is_leads_chat:
                                logger.info(f"Ignoring Weekly Report in leads chat {chat_id}")
                            else:
                                logger.info("Generating weekly report...")
                                report_text = get_weekly_report(client, config)
                                send_telegram_message(bot_token, chat_id, report_text, get_main_keyboard())
                                logger.info("Weekly report sent")
                        
                        # Monthly Report (new button name)
                        elif text == '📆 GET DETAILED FORECAST FOR NEXT MONTH':
                            if is_leads_chat:
                                logger.info(f"Ignoring Monthly Report in leads chat {chat_id}")
                            else:
                                logger.info("Generating monthly forecast...")
                                report_text = get_monthly_weekly_report(client, config)
                                send_telegram_message(bot_token, chat_id, report_text, get_main_keyboard())
                                logger.info("Monthly forecast sent")
                        
                        # Get Full Report
                        elif text == '📄 Get Full Report':
                            logger.info(f"Full report requested by user {chat_id}")
                            
                            # Get user info
                            username = message.get('from', {}).get('username', '')
                            first_name = message.get('from', {}).get('first_name', '')
                            last_name = message.get('from', {}).get('last_name', '')
                            user_id = message.get('from', {}).get('id', chat_id)
                            
                            # Current time
                            moscow_tz = pytz.timezone('Europe/Moscow')
                            now = datetime.now(moscow_tz)
                            time_str = now.strftime("%Y-%m-%d %H:%M:%S")
                            
                            # Format lead message
                            user_name = f"{first_name} {last_name}".strip() if first_name else "Unknown"
                            user_link = f"@{username}" if username else f"ID: {user_id}"
                            
                            lead_message = (
                                f"🆕 <b>NEW LEAD!</b>\n\n"
                                f"👤 Name: {user_name}\n"
                                f"🔗 {user_link}\n"
                                f"🆔 Chat ID: <code>{user_id}</code>\n"
                                f"📅 Time: {time_str}\n"
                                f"📄 Requested: Full Report\n"
                                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                                f"📌 <i>Lead captured. Follow up required.</i>"
                            )
                            
                            # Send lead notification to leads chat
                            if leads_chat_id:
                                send_telegram_message(bot_token, leads_chat_id, lead_message)
                                logger.info(f"Lead sent to leads chat {leads_chat_id}")
                            else:
                                # Fallback to same chat if leads_chat_id not set
                                send_telegram_message(bot_token, chat_id, lead_message)
                                logger.info("Lead sent to same chat (no leads_chat_id configured)")
                            
                            # Send confirmation to user
                            confirm_text = (
                                "✅ Thank you for your interest!\n\n"
                                "Your request has been sent to our team. "
                                "We will contact you shortly."
                            )
                            # Only send keyboard to non-leads chat
                            if is_leads_chat:
                                send_telegram_message(bot_token, chat_id, confirm_text)
                            else:
                                send_telegram_message(bot_token, chat_id, confirm_text, get_main_keyboard())
                            
                            logger.info("Lead captured and notification sent")
                        
                        else:
                            # Unknown command - only respond in user chat, not in leads chat
                            if not is_leads_chat:
                                send_telegram_message(
                                    bot_token, chat_id, "Choose a report:", get_main_keyboard()
                                )
                            else:
                                # In leads chat - just ignore any other message
                                logger.info(f"Ignoring unknown command '{text}' in leads chat {chat_id}")
                    
                    except Exception as e:
                        logger.error(f"Error: {e}")
                        if not is_leads_chat:
                            send_telegram_message(
                                bot_token, chat_id, f"❌ Error: {str(e)[:200]}", get_main_keyboard()
                            )
                        else:
                            logger.error(f"Error in leads chat: {e}")
                    
                    finally:
                        if client:
                            client.close()
                
                last_update_id = update_id
        
        return last_update_id
    
    except Exception as e:
        logger.error(f"Error receiving updates: {e}")
        return last_update_id


def scheduled_job():
    """Scheduled job for weekly report"""
    logger.info("Starting scheduled job")
    config = load_config()
    chat_id = config['telegram']['chat_id']
    
    client = None
    try:
        client = get_clickhouse_client(config)
        report_text = get_weekly_report_simple(client, config)
        send_telegram_message(config['telegram']['token'], chat_id, report_text)
        logger.info("Scheduled report sent")
    except Exception as e:
        logger.error(f"Error in scheduled_job: {e}")
        send_telegram_message(
            config['telegram']['token'],
            chat_id,
            f"❌ Error sending scheduled report: {str(e)[:200]}"
        )
    finally:
        if client:
            client.close()


def setup_schedule(config):
    """Setup schedule for weekly report"""
    report_config = config['report']
    schedule_time = report_config['time']
    schedule_weekday = report_config['weekday']
    
    schedule.clear()
    
    if schedule_weekday == 0:
        schedule.every().monday.at(schedule_time).do(scheduled_job)
    elif schedule_weekday == 1:
        schedule.every().tuesday.at(schedule_time).do(scheduled_job)
    elif schedule_weekday == 2:
        schedule.every().wednesday.at(schedule_time).do(scheduled_job)
    elif schedule_weekday == 3:
        schedule.every().thursday.at(schedule_time).do(scheduled_job)
    elif schedule_weekday == 4:
        schedule.every().friday.at(schedule_time).do(scheduled_job)
    elif schedule_weekday == 5:
        schedule.every().saturday.at(schedule_time).do(scheduled_job)
    elif schedule_weekday == 6:
        schedule.every().sunday.at(schedule_time).do(scheduled_job)
    
    logger.info(f"Schedule: every {schedule_weekday} at {schedule_time} Moscow time")


def main():
    global running
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        config = load_config()
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        sys.exit(1)
    
    bot_token = config['telegram']['token']
    
    if not config['telegram'].get('chat_id'):
        logger.error("chat_id not found in config")
        sys.exit(1)
    
    setup_schedule(config)
    
    last_update_id = 0
    logger.info("Bot started")
    logger.info("Send any message to the bot in Telegram")
    logger.info("Press Ctrl+C to stop")
    
    error_counter = 0
    
    while running:
        try:
            config = load_config()
            
            new_update_id = handle_updates(bot_token, last_update_id, config)
            
            if new_update_id != last_update_id:
                last_update_id = new_update_id
                error_counter = 0
            
            schedule.run_pending()
            time.sleep(1)
            
        except KeyboardInterrupt:
            logger.info("Interrupt signal received")
            break
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            error_counter += 1
            
            if error_counter > 10:
                logger.critical("Too many consecutive errors. Bot stopping.")
                break
            
            time.sleep(5)
    
    logger.info("Bot stopped")


if __name__ == "__main__":
    main()