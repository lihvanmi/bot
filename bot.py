from telegram import Update
from telegram import ChatPermissions
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    JobQueue,
)
import logging
import re
import os
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
from datetime import timedelta,  datetime as dt, time as dt_time
import time
import asyncio

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

# Получение переменных окружения
DATABASE_URL = os.getenv('DATABASE_URL')
BOT_TOKEN = os.getenv('BOT_TOKEN')
TARGET_GROUP_ID = int(os.getenv('TARGET_GROUP_ID'))

# Время в секундах (45 минут = 2700 секунд)
PINNED_DURATION = 2700  # Изменено на 45 минут

# Разрешенный пользователь для сброса таймера
ALLOWED_USER = "@Muzikant1429"

# Список запрещенных слов (антимат)
BANNED_WORDS = ["бляд", "хуй", "пизд", "наху", "гандон", "пидр", "пидорас","пидар", "шалав", "шлюх", "мразь", "мразо", "ебат"]

# Ключевые слова для мессенджеров и ссылок
MESSENGER_KEYWORDS = [
    "t.me", "telegram", "whatsapp", "viber", "discord", "vk.com", "instagram",
    "facebook", "twitter", "youtube", "http", "www", ".com", ".ru", ".net", "tiktok"
]

# Лимиты для антиспама
SPAM_LIMIT = 4  # Максимальное количество сообщений
SPAM_INTERVAL = 30  # Интервал в секундах
MUTE_DURATION = 360  # Время мута в секундах (5 минут)

# Глобальные переменные
last_pinned_times = {}  # {chat_id: timestamp}
last_user_username = {}  # {chat_id: username}
last_zch_times = {}  # {chat_id: timestamp}
last_thanks_times = {}  # {chat_id: timestamp}
pinned_messages = {}  # {chat_id: message_id}  # Добавлено
db_initialized = False  # Глобальный флаг
# Бан-лист
banned_users = set()

# Функция для подключения к базе данных
def get_db_connection():
    conn = psycopg2.connect(DATABASE_URL)
    conn.cursor_factory = DictCursor
    return conn


# Инициализация базы данных
def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS pinned_messages (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            user_id BIGINT,
            username TEXT,
            message_text TEXT,
            timestamp BIGINT
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS active_users (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            username TEXT,
            delete_count INTEGER,
            timestamp BIGINT
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS birthdays (
            id SERIAL PRIMARY KEY,
            user_id BIGINT UNIQUE,
            username TEXT,
            birth_date TEXT,
            last_congratulated_year INTEGER
        )
    ''')
    cur.execute('''
            CREATE TABLE IF NOT EXISTS ban_list (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            username TEXT,
            phone TEXT,
            ban_time BIGINT
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS ban_history (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            username TEXT,
            reason TEXT,
            timestamp BIGINT
        )
    ''')
    conn.commit()
    cur.close()
    conn.close()


init_db()


# Проверка прав администратора
async def is_admin_or_musician(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user = update.message.from_user
    chat_id = update.message.chat.id

    try:
        chat_member = await context.bot.get_chat_member(chat_id, user.id)
        if chat_member.status in ["administrator", "creator"]:
            return True
    except Exception as e:
        logger.error(f"Ошибка при проверке прав пользователя {user.id}: {e}")

    if user.username == ALLOWED_USER[1:]:
        return True

    return False


# Удаление системных сообщений через указанное время
async def delete_system_message(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    try:
        await context.bot.delete_message(chat_id=job.chat_id, message_id=job.data)
    except Exception as e:
        logger.error(f"Ошибка при удалении системного сообщения: {e}")

# Команда /reset_pin_timer
async def reset_pin_timer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat.id
    user = update.message.from_user

    if not await is_admin_or_musician(update, context):
        response = await update.message.reply_text("У вас нет прав для выполнения этой команды.")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=chat_id)
        await update.message.delete()  # Удаляем команду
        return

    last_pinned_times[chat_id] = 0

    try:
        await context.bot.unpin_all_chat_messages(chat_id=chat_id)
        logger.info(f"Откреплены все сообщения в группе {chat_id}.")
    except Exception as e:
        logger.error(f"Ошибка при откреплении сообщений в группе {chat_id}: {e}")

    success_message = await update.message.reply_text("Таймер закрепа успешно сброшен.")
    context.job_queue.run_once(delete_system_message, 10, data=success_message.message_id, chat_id=chat_id)
    await update.message.delete()  # Удаляем команду

# Функция для добавления нарушителей в банлист_ХИСТОРИ:
async def add_to_ban_history(user_id: int, username: str, reason: str):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute('''
        INSERT INTO ban_history (user_id, username, reason, timestamp)
        VALUES (%s, %s, %s, %s)
    ''', (user_id, username, reason, int(time.time())))
        conn.commit()
    except Exception as e:
        logger.error(f"Ошибка при добавлении в ban_history: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

# Команда /ban_history:
async def ban_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat.id
    user = update.message.from_user

    # Проверка прав администратора или музыканта
    if not await is_admin_or_musician(update, context):
        response = await update.message.reply_text("У вас нет прав для выполнения этой команды.")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=chat_id)
        await update.message.delete()  # Удаляем команду
        return

    # Получаем период из аргументов команды
    days = int(context.args[0]) if context.args else 1

    # Получаем данные из базы
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute('''
        SELECT user_id, username, reason, timestamp 
        FROM ban_history 
        WHERE timestamp >= %s
    ''', (int(time.time()) - days * 86400,))
        results = cur.fetchall()
    except Exception as e:
        logger.error(f"Ошибка при получении ban_history: {e}")
        results = []
    finally:
        cur.close()
        conn.close()

    if not results:
        response = await update.message.reply_text(f"Нет нарушителей за последние {days} дней.")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=chat_id)
        await update.message.delete()  # Удаляем команду
        return

    # Формируем сообщение
    text = f"Нарушители за последние {days} дней:\n"
    for idx, row in enumerate(results, start=1):
        text += (
            f"{idx}. ID: {row['user_id']} | "
            f"Имя: {row['username']} | "
            f"Причина: {row['reason']} | "
            f"Дата: {datetime.fromtimestamp(row['timestamp']).strftime('%d.%m.%Y %H:%M')}\n"
        )

    await update.message.reply_text(text)
    context.job_queue.run_once(delete_system_message, 60, data=response.message_id, chat_id=chat_id)
    await update.message.delete()  # Удаляем команду

# Команда /del
async def delete_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat.id
    user = update.message.from_user

    if not await is_admin_or_musician(update, context):
        success_message = await update.message.reply_text("У вас нет прав для выполнения этой команды.")
        context.job_queue.run_once(delete_system_message, 10, data=success_message.message_id, chat_id=chat_id)
        await update.message.delete()  # Удаляем команду
        return

    if not update.message.reply_to_message:
        success_message = await update.message.reply_text("Ответьте на сообщение, которое нужно удалить.")
        context.job_queue.run_once(delete_system_message, 10, data=success_message.message_id, chat_id=chat_id)
        await update.message.delete()  # Удаляем команду
        return

    try:
        await update.message.reply_to_message.delete()
        logger.info(f"Сообщение удалено пользователем {user.username} в чате {chat_id}.")
        await update.message.delete()  # Удаляем команду
    except Exception as e:
        logger.error(f"Ошибка при удалении сообщения: {e}")
        success_message = await update.message.reply_text("Не удалось удалить сообщение. Проверьте права бота.")
        context.job_queue.run_once(delete_system_message, 10, data=success_message.message_id, chat_id=chat_id)
        await update.message.delete()  # Удаляем команду


# Обработчик новых сообщений
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    user = message.from_user
    chat_id = message.chat.id
    text = message.text
    current_time = int(time.time())

    # Проверка на бан в базе бота
    if user.id in banned_users:
        try:
            await message.delete()
        except Exception as e:
            logger.error(f"Ошибка удаления: {e}")
        return

    # Игнорируем сообщения не из групп/супергрупп
    if message.chat.type not in ['group', 'supergroup']:
        return

    # Проверка на маркер "зч" или "🌟"
    if not text.lower().startswith(("звезда", "зч")) and "🌟" not in text:
        return

    # Проверка на антимат и антирекламу
    if not await is_admin_or_musician(update, context): # Исключаем администраторов и музыкантов из ограничений
        # Антимат
        if any(word in text.lower() for word in BANNED_WORDS):
            await message.delete()
            warning_message = await context.bot.send_message(
                chat_id=chat_id,
                text="Использование нецензурных выражений недопустимо!"
            )
            context.job_queue.run_once(delete_system_message, 10, data=warning_message.message_id, chat_id=chat_id)
            # Добавляем нарушителя в базу данных
            await add_to_ban_history(user.id, user.username or user.first_name, "Использование нецензурных выражений")
            return

        # Антиреклама
        if any(re.search(rf"\b{re.escape(keyword)}\b", text.lower()) for keyword in MESSENGER_KEYWORDS):
            await message.delete()
            warning_message = await context.bot.send_message(
                chat_id=chat_id,
                text="Отправка ссылок и упоминаний мессенджеров недопустима!"
            )
            context.job_queue.run_once(delete_system_message, 10, data=warning_message.message_id, chat_id=chat_id)
            # Добавляем нарушителя в базу данных
            await add_to_ban_history(user.id, user.username or user.first_name, "Отправка ссылок или упоминание мессенджеров")
            return

        # Антиспам
        user_id = user.id
        if user_id in last_zch_times:
            if current_time - last_zch_times[user_id] < SPAM_INTERVAL:
                await message.delete()
                warning_message = await context.bot.send_message(
                    chat_id=chat_id,
                    text="Слишком частое отправление сообщений! Вы замьючены на 5 минут."
                )
                context.job_queue.run_once(delete_system_message, 10, data=warning_message.message_id, chat_id=chat_id)

                # Мут пользователя на 5 минут
                try:
                    await context.bot.restrict_chat_member(
                        chat_id=chat_id,
                        user_id=user_id,
                        permissions=ChatPermissions(can_send_messages=False),
                        until_date=int(time.time()) + MUTE_DURATION
                    )
                    logger.info(f"Пользователь {user_id} замьючен на {MUTE_DURATION} секунд.")
                except Exception as e:
                    logger.error(f"Ошибка при мьюте пользователя {user_id}: {e}")

                # Добавляем нарушителя в историю
                await add_to_ban_history(user_id, user.username or user.first_name, "Спам")
                return
        last_zch_times[user_id] = current_time

    # Проверка наличия закрепленного сообщения в группе
    try:
        chat = await context.bot.get_chat(chat_id)
        pinned_message = chat.pinned_message
    except Exception as e:
        logger.error(f"Ошибка при получении информации о закрепленном сообщении: {e}")
        pinned_message = None

    # Если закрепленного сообщения нет, разрешаем закрепление
    if pinned_message is None:
        try:
            await message.pin()
            last_pinned_times[chat_id] = current_time
            last_user_username[chat_id] = user.username if user.username else None

            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute('''
                    INSERT INTO pinned_messages (chat_id, user_id, username, message_text, timestamp)
                    VALUES (%s, %s, %s, %s, %s)
            ''', (chat_id, user.id, user.username, text, current_time))
                conn.commit()
            except Exception as e:
                logger.error(f"Ошибка при добавлении закрепленного сообщения в базу данных: {e}")
                conn.rollback()
            finally:
                cur.close()
                conn.close()

            # Автопоздравление именинников
            await auto_birthdays(context, chat_id)
            context.job_queue.run_once(unpin_last_message, PINNED_DURATION, chat_id=chat_id)

            if chat_id != TARGET_GROUP_ID:
                new_text = text.replace("🌟 ", "").strip()
                forwarded_message = await context.bot.send_message(chat_id=TARGET_GROUP_ID, text=new_text)
                await forwarded_message.pin()
        except Exception as e:
            logger.error(f"Ошибка при закреплении сообщения: {e}")
        return

    last_pinned_time = last_pinned_times.get(chat_id, 0)
    if current_time - last_pinned_time < PINNED_DURATION:
        if not await is_admin_or_musician(update, context):
            await message.delete()

            # Сохраняем информацию о удаленном сообщении
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute('SELECT id FROM active_users WHERE user_id = %s', (user.id,))
                result = cur.fetchone()

                if result:
                    # Если пользователь уже есть, увеличиваем счетчик delete_count
                    cur.execute('UPDATE active_users SET delete_count = delete_count + 1 WHERE user_id = %s', (user.id,))
                else:
                    # Если пользователя нет, добавляем новую запись
                    cur.execute('''
                        INSERT INTO active_users (user_id, username, delete_count, timestamp)
                        VALUES (%s, %s, %s, %s)
                    ''', (user.id, user.username or user.first_name, 1, current_time))
                # Фиксируем изменения в базе данных
                conn.commit()
            except Exception as e:
                # Логируем ошибку, если что-то пошло не так
                logger.error(f"Ошибка при работе с базой данных: {e}")
                conn.rollback()  # Откатываем изменения в случае ошибки
            finally:
                # Закрываем курсор и соединение - гарантирует освобождение ресурсов даже в случае ошибки.
                cur.close()
                conn.close()

            # Отправляем благодарность за повторное сообщение
            if current_time - last_pinned_time < 180:
                last_thanks_time = last_thanks_times.get(chat_id, 0)
                if current_time - last_thanks_time >= 180:
                    thanks_message = await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"Спасибо за вашу бдительность! Звезда часа уже замечена пользователем "
                             f"{'@' + (last_user_username.get(chat_id, 'неизвестным') or user.first_name)} и вошел в рейтинг - /lider"
                             f"Но Вы также попали в рейтинг аивности - /active"
                    )
                    context.job_queue.run_once(delete_system_message, 180, data=thanks_message.message_id, chat_id=chat_id)
                    last_thanks_times[chat_id] = current_time
            return
        else:
            try:
                await message.pin()
                last_pinned_times[chat_id] = current_time
                last_user_username[chat_id] = user.username if user.username else None
                
                # Работа с базой данных PostgreSQL
                conn = get_db_connection()
                cur = conn.cursor()
                try:
                    cur.execute('''
                        INSERT INTO pinned_messages (chat_id, user_id, user.username, message_text, timestamp)
                        VALUES (%s, %s, %s, %s, %s)
                    ''', (chat_id, user.id, user.username or user.first_name, text, current_time))
                    conn.commit()
                except Exception as e:
                    # Логируем ошибку, если что-то пошло не так
                    logger.error(f"Ошибка при работе с базой данных: {e}")
                    conn.rollback()  # Откатываем изменения в случае ошибки
                finally:
                    cur.close()
                    conn.close()

                correction_message = await context.bot.send_message(
                    chat_id=chat_id,
                    text="Корректировка звезды часа от Админа."
                )
                context.job_queue.run_once(delete_system_message, 10, data=correction_message.message_id, chat_id=chat_id)
            except Exception as e:
                logger.error(f"Ошибка при закреплении сообщения: {e}")
            return

    # Если время закрепления истекло, закрепляем сообщение
    try:
        await message.pin()
        last_pinned_times[chat_id] = current_time
        last_user_username[chat_id] = user.username if user.username else None

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute('''
                INSERT INTO pinned_messages (chat_id, user_id, username, message_text, timestamp)
                VALUES (%s, %s, %s, %s, %s)
            ''', (chat_id, user.id, user.username or user.first_name, text, current_time))
            conn.commit()
        except Exception as e:
            logger.error(f"Ошибка при добавлении закрепленного сообщения в базу данных: {e}")
            conn.rollback()  # Откатываем изменения в случае ошибки
        finally:
            cur.close()
            conn.close()

        # Автопоздравление именинников
        await auto_birthdays(context, chat_id)
        context.job_queue.run_once(unpin_last_message, PINNED_DURATION, chat_id=chat_id)
        
        # Пересылка сообщения в целевую группу
        if chat_id != TARGET_GROUP_ID:
            try:
                new_text = text.replace("🌟 ", "").strip()
                forwarded_message = await context.bot.send_message(chat_id=TARGET_GROUP_ID, text=new_text)
                await forwarded_message.pin()
            except Exception as e:
                logger.error(f"Ошибка при пересылке сообщения в целевую группу: {e}")
    except Exception as e:
        logger.error(f"Ошибка при закреплении сообщения: {e}")

    # Если время закрепления истекло, закрепляем новое сообщение
    try:
        await message.pin()
        last_pinned_times[chat_id] = current_time
        last_user_username[chat_id] = user.username if user.username else None

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute('''
                INSERT INTO pinned_messages (chat_id, user_id, username, message_text, timestamp)
                VALUES (%s, %s, %s, %s, %s)
            ''', (chat_id, user.id, user.username or user.first_name, text, current_time))
            conn.commit()
        except Exception as e:
            logger.error(f"Ошибка при добавлении закрепленного сообщения в базу данных: {e}")
            conn.rollback()  # Откатываем изменения в случае ошибки
        finally:
            cur.close()
            conn.close()

        # Пересылка сообщения в целевую группу
        if chat_id != TARGET_GROUP_ID:
            try:
                new_text = text.replace("🌟 ", "").strip()
                forwarded_message = await context.bot.send_message(chat_id=TARGET_GROUP_ID, text=new_text)
                await forwarded_message.pin()
            except Exception as e:
                logger.error(f"Ошибка при пересылке сообщения в целевую группу: {e}")

        # Автопоздравление именинников
        await auto_birthdays(context, chat_id)
        context.job_queue.run_once(unpin_last_message, PINNED_DURATION, chat_id=chat_id)
        
    except Exception as e:
        logger.error(f"Ошибка при закреплении сообщения: {e}")
    
async def check_all_birthdays(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cur = conn.cursor()
    results = None  # Инициализируем переменную
    try:
        cur.execute('SELECT user_id, username, birth_date FROM birthdays')
        results = cur.fetchall()
    except Exception as e:
        logger.error(f"Ошибка при добавлении закрепленного сообщения в базу данных: {e}")
    finally:
        cur.close()   
        conn.close()

    if not results:
        response = await update.message.reply_text("В базе данных нет записей о днях рождения.")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
        await update.message.delete()
        return

    text = "Все дни рождения:\n"
    for row in results:
        text += f"• @{row['username']} — {row['birth_date']}\n"

    await update.message.reply_text(text)
    await update.message.delete()

# Команда /liderX
async def lider(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Получаем количество дней из аргументов команды, по умолчанию 1 день
    days = int(context.args[0]) if context.args else 1

    # Подключаемся к базе данных и выполняем запрос
    conn = get_db_connection()
    cur = conn.cursor()
    results = None  # Инициализируем переменную
    try:
        cur.execute('''
            SELECT user_id, username, COUNT(*) as count
            FROM pinned_messages
            WHERE timestamp >= %s
            GROUP BY user_id
            ORDER BY count DESC
            LIMIT 3
        ''', (int(time.time()) - days * 86400,))
        results = cur.fetchall()
    except Exception as e:
        # Логируем ошибку, если что-то пошло не так
        logger.error(f"Ошибка при получении данных из базы данных: {e}")
    finally:
        cur.close()
        conn.close()

    # Если результаты отсутствуют, отправляем сообщение об отсутствии данных
    if not results:
        response = await update.message.reply_text("Нет данных за указанный период.")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
        await update.message.delete()  # Удаляем команду
        return

    # Формируем текст для вывода ТОП-участников
    text = f"ТОП участников по закрепам за {days} д.:\n"
    for i, row in enumerate(results, start=1):
        user_id = row['user_id']
        # Определяем способ обращения к пользователю
        try:
            chat_member = await context.bot.get_chat_member(update.message.chat.id, user_id)
            user_name = (
                chat_member.user.first_name or  # Используем имя, если оно есть
                (f"@{chat_member.user.username}" if chat_member.user.username else None) or  # Или логин с собакой
                f"ID: {chat_member.user.id}"  # Или ID
            )
        except Exception as e:
            logger.error(f"Ошибка при получении информации о пользователе {user_id}: {e}")
            user_name = f"ID: {user_id}"

        # Добавляем строку в текст
        text += f"{i}. {user_name} — {row['count']} 🌟\n"

    # Отправляем сообщение с результатами
    await update.message.reply_text(text)
    await update.message.delete()  # Удаляем команду


# Команда /zhX
async def zh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    count = int(context.args[0]) if context.args else 10
    conn = get_db_connection()
    cur = conn.cursor()
    results = None  # Инициализируем переменную
    try:
        cur = conn.cursor()
        cur.execute('''
            SELECT user_id, username, message_text
            FROM pinned_messages
            ORDER BY timestamp DESC
            LIMIT %s
        ''', (count,))
        results = cur.fetchall()
    except Exception as e:
        # Логируем ошибку, если что-то пошло не так
        logger.error(f"Ошибка при получении данных из базы данных: {e}")
    finally:
        cur.close()
        conn.close()

    if not results:
        await update.message.reply_text("Нет закрепленных сообщений.")
        await update.message.delete()  # Удаляем команду
        return

    text = f"Последние {count} ⭐️🕐:\n"
    for i, row in enumerate(results, start=1):
        text += f"{i}. @{row['username']}: {row['message_text']}\n"

    await update.message.reply_text(text)
    await update.message.delete()  # Удаляем команду


# Команда /activeX
async def active(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Получаем количество дней из аргументов команды, по умолчанию 1 день
    days = int(context.args[0]) if context.args else 1

    # Подключаемся к базе данных и выполняем запрос
    conn = get_db_connection()
    cur = conn.cursor()
    results = None  # Инициализируем переменную
    try:
        cur.execute('''
            SELECT user_id, username, SUM(delete_count) as total_deletes
            FROM active_users
            WHERE timestamp >= %s
            GROUP BY user_id
            ORDER BY total_deletes DESC
            LIMIT 3
        ''', (int(time.time()) - days * 86400,))
        results = cur.fetchall()
    except Exception as e:
        # Логируем ошибку, если что-то пошло не так
        logger.error(f"Ошибка при получении данных из базы данных: {e}")
    finally:
        cur.close()
        conn.close()

    # Если результаты отсутствуют, отправляем сообщение об отсутствии данных
    if not results:
        response = await update.message.reply_text("Нет активных пользователей за указанный период.")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
        await update.message.delete()  # Удаляем команду
        return

    # Формируем текст для вывода ТОП-участников
    text = f"Самые активные участники за период - {days} д.:\n"
    for i, row in enumerate(results, start=1):
        user_id = row['user_id']
        # Определяем способ обращения к пользователю
        try:
            chat_member = await context.bot.get_chat_member(update.message.chat.id, user_id)
            user_mention = (
                chat_member.user.first_name or  # Используем имя, если оно есть
                (f"@{chat_member.user.username}" if chat_member.user.username else None) or  # Или логин с собакой
                f"ID: {chat_member.user.id}"  # Или ID
            )
        except Exception as e:
            logger.error(f"Ошибка при получении информации о пользователе {user_id}: {e}")
            user_mention = f"ID: {user_id}"

        # Добавляем строку в текст
        text += f"{i}. {user_mention} — {row['total_deletes']} раз(а) написал(а) ⭐\n"

    # Отправляем сообщение с результатами
    await update.message.reply_text(text)
    await update.message.delete()  # Удаляем команду


# Команда /dr
async def dr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    if context.args:
        birth_date = context.args[0]
        if re.match(r"\d{2}\.\d{2}\.\d{4}", birth_date):
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute('''
                    INSERT INTO birthdays (user_id, username, birth_date, last_congratulated_year)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET
                        username = EXCLUDED.username,
                        birth_date = EXCLUDED.birth_date,
                        last_congratulated_year = EXCLUDED.last_congratulated_year
                ''', (user.id, user.username or user.first_name, birth_date, 0))  # 0 означает, что пользователь еще не был поздравлен
                conn.commit()
                response = await update.message.reply_text(f"Дата рождения сохранена: {birth_date}")
                context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
                await update.message.delete()  # Удаляем команду
            except Exception as e:
                logger.error(f"Ошибка при сохранении даты рождения: {e}")
                response = await update.message.reply_text("Произошла ошибка при сохранении даты рождения.")
                context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
                await update.message.delete()  # Удаляем команду
            finally:
                cur.close()
                conn.close()
        else:
            response = await update.message.reply_text("Неверный формат даты. Напишите одним сообщением  /dr ДД.ММ.ГГГГ")
            context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
            await update.message.delete()  # Удаляем команду
    else:
        response = await update.message.reply_text("Напишите свою дату рождения в формате ДД.ММ.ГГГГ")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
        await update.message.delete()  # Удаляем команду


async def birthday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Получаем сегодняшнюю дату в формате ДД.ММ
    today = datetime.now().strftime("%d.%m")
    
    # Подключаемся к базе данных
    conn = get_db_connection()
    cur = conn.cursor()
    results = None  # Инициализируем переменную
    try:
        # Логируем запрос и данные
        logger.info(f"Ищем именинников на дату: {today}")
    
        # Выполняем запрос к базе данных для поиска сегодняшних именинников
        cur.execute('SELECT user_id, username FROM birthdays WHERE substr(birth_date, 1, 5) = %s', (today,))
        results = cur.fetchall()
    except Exception as e:
        # Логируем ошибку, если что-то пошло не так
        logger.error(f"Ошибка при поиске именинников: {e}")
    finally:
        cur.close()
        conn.close()

    # Если именинников нет
    if not results:
        response = await update.message.reply_text(f"Сегодня ({today}) нет именинников. Чтобы добавить свою дату рождения, напишите /dr и дату рождения одним сообщением в формате /dr ДД.ММ.ГГГГ")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
        await update.message.delete()
        return

    # Формируем сообщение с именинниками
    text = f"Сегодня ({today}) день рождения у:\n"
    for row in results:
        user_id = row['user_id']
        username = row['username']
        # Определяем способ обращения к пользователю
        try:
            chat_member = await context.bot.get_chat_member(update.message.chat.id, user_id)
            user_mention = (
                chat_member.user.first_name or  # Используем имя, если оно есть
                (f"@{chat_member.user.username}" if chat_member.user.username else None) or  # Или логин с собакой
                f"ID: {chat_member.user.id}"  # Или ID
            )
        except Exception as e:
            logger.error(f"Ошибка при получении информации о пользователе {user_id}: {e}")
            user_mention = f"ID: {user_id}"

        # Добавляем строку в текст
        text += f"• {user_mention}\n"
    # Отправляем сообщение
    await update.message.reply_text(text)
    await update.message.delete()

# Автопоздравление именинников
async def auto_birthdays(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    today = time.strftime("%d.%m")  # Сегодняшняя дата в формате ДД.ММ
    current_year = datetime.now().year  # Текущий год
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Ищем именинников
        cur.execute('''
            SELECT user_id, username 
            FROM birthdays 
            WHERE substr(birth_date, 1, 5) = %s AND (last_congratulated_year IS NULL OR last_congratulated_year < %s)
        ''', (today, current_year))
        results = cur.fetchall()

        for row in results:
            user_id = row['user_id']
            username = row['username']

            # Получаем информацию о пользователе
            try:
                user = await context.bot.get_chat_member(chat_id, user_id)
                user_name = (
                    user.user.first_name or  # Используем имя, если оно есть
                    (f"@{user.user.username}" if user.user.username else None) or  # Или логин с собакой
                    f"ID: {user.user.id}"  # Или ID
                )
            except Exception as e:
                logger.error(f"Ошибка при получении информации о пользователе {user_id}: {e}")
                user_name = f"ID: {user_id}"
            # Поздравляем пользователя
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🎉{user_name} 🎊 - Поздравляю тебя с днем рождения! 🍀Желаю умножить свой cash🎁back x10 раз 🎉. ____________________________ Чтобы добавить свою дату рождения в базу, напишите команду с датой в формате /dr ДД.ММ.ГГГГ"
            )

            # Обновляем год последнего поздравления
            cur.execute('''
                UPDATE birthdays 
                SET last_congratulated_year = %s 
                WHERE user_id = %s
            ''', (current_year, user_id))
            conn.commit()
    except Exception as e:
        # Логируем ошибку, если что-то пошло не так
        logger.error(f"Ошибка при работе с базой данных: {e}")
        conn.rollback()  # Откатываем изменения в случае ошибки
    finally:
        cur.close()
        conn.close()

async def druser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat.id
    user = update.message.from_user

    # Проверка прав администратора или музыканта
    if not await is_admin_or_musician(update, context):
        response = await update.message.reply_text("У вас нет прав для выполнения этой команды.")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=chat_id)
        await update.message.delete()  # Удаляем команду
        return

    # Проверка, является ли команда ответом на сообщение
    if update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
        user_id = target_user.id
        username = target_user.username or f"ID: {target_user.id}"
        birth_date = " ".join(context.args) if context.args else None
    else:
        # Если команда не является ответом на сообщение, обрабатываем как обычно
        if not context.args or len(context.args) < 2:
            response = await update.message.reply_text(
                "Используйте команду в формате: /druser @username dd.mm.yyyy, /druser ID dd.mm.yyyy или ответьте на сообщение пользователя с командой /druser dd.mm.yyyy"
            )
            context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=chat_id)
            await update.message.delete()  # Удаляем команду
            return

        user_identifier = context.args[0]  # @username или ID
        birth_date = context.args[1]  # Дата рождения

        # Получаем user_id
        user_id = None
        username = None

        if user_identifier.startswith("@"):  # Если указан @username
            username = user_identifier[1:]  # Убираем @
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute('SELECT user_id FROM birthdays WHERE username = %s', (username,))
                result = cur.fetchone()
                if result:
                    user_id = result['user_id']
            except Exception as e:
                logger.error(f"Ошибка при поиске пользователя в базе данных: {e}")
            finally:
                cur.close()
                conn.close()

            # Если user_id не найден в базе, пытаемся получить его через get_chat_member
            if not user_id:
                try:
                    chat_member = await context.bot.get_chat_member(chat_id, username)
                    user_id = chat_member.user.id
                    username = chat_member.user.username or username
                except Exception as e:
                    logger.error(f"Ошибка при получении информации о пользователе {username}: {e}")
                    response = await update.message.reply_text(f"Пользователь @{username} не найден.")
                    context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=chat_id)
                    await update.message.delete()  # Удаляем команду
                    return
        else:  # Если указан ID
            try:
                user_id = int(user_identifier)
            except ValueError:
                response = await update.message.reply_text("Неверный формат ID. Используйте числовой ID.")
                context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=chat_id)
                await update.message.delete()  # Удаляем команду
                return

    # Проверка формата даты
    if not birth_date or not re.match(r"\d{2}\.\d{2}\.\d{4}", birth_date):
        response = await update.message.reply_text("Неверный формат даты. Используйте ДД.ММ.ГГГГ.")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=chat_id)
        await update.message.delete()  # Удаляем команду
        return

    # Сохраняем дату рождения в базу данных
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute('''
            INSERT INTO birthdays (user_id, username, birth_date, last_congratulated_year)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                username = EXCLUDED.username,
                birth_date = EXCLUDED.birth_date,
                last_congratulated_year = EXCLUDED.last_congratulated_year
        ''', (user_id, username, birth_date, 0))  # 0 означает, что пользователь еще не был поздравлен
        conn.commit()

        response = await update.message.reply_text(f"Дата рождения для пользователя {username or f'ID: {user_id}'} сохранена: {birth_date}")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=chat_id)
        await update.message.delete()  # Удаляем команду
    except Exception as e:
        logger.error(f"Ошибка при сохранении даты рождения: {e}")
        response = await update.message.reply_text("Произошла ошибка при сохранении даты рождения.")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=chat_id)
        await update.message.delete()  # Удаляем команду
    finally:
        cur.close()
        conn.close()

async def get_user_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat.id
    user = update.message.from_user

    # Проверка прав администратора или музыканта
    if not await is_admin_or_musician(update, context):
        response = await update.message.reply_text("У вас нет прав для выполнения этой команды.")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=chat_id)
        await update.message.delete()  # Удаляем команду
        return

    # Проверка, является ли команда ответом на сообщение
    if not update.message.reply_to_message:
        response = await update.message.reply_text("Ответьте на сообщение пользователя, чтобы узнать его ID.")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=chat_id)
        await update.message.delete()  # Удаляем команду
        return

    # Получаем информацию о пользователе
    target_user = update.message.reply_to_message.from_user
    user_id = target_user.id
    username = target_user.username or "без username"
    first_name = target_user.first_name or "без имени"

    # Формируем текст сообщения
    try:
        response_text = f"ID пользователя {first_name} (@{username}): {user_id}"
        response = await update.message.reply_text(response_text)
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=chat_id)
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения: {e}")
        response = await update.message.reply_text("Произошла ошибка при отправке сообщения.")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=chat_id)
    finally:
        await update.message.delete()  # Удаляем команду

# Команда /ban_list
async def ban_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cur = conn.cursor()
    results = None  # Инициализируем переменную
    try:
        # Выполняем запрос к базе данных
        cur.execute('SELECT user_id, username FROM ban_list')
        results = cur.fetchall()

        if not results:
            response = await update.message.reply_text("Бан-лист пуст.")
            context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
            await update.message.delete()  # Удаляем команду
            return

        text = "Бан-лист:\n"
        for idx, row in enumerate(results, start=1):
            user_id = row['user_id']
            username = row['username'] or "без username"  # Если username равен None, используем "без username"
            text += f"{idx}. ID: {user_id} | Username: @{username}\n"
    
        response = await update.message.reply_text(text)
        context.job_queue.run_once(delete_system_message, 60, data=response.message_id, chat_id=update.message.chat.id)
    except Exception as e:
        # Логируем ошибку, если что-то пошло не так
        logger.error(f"Ошибка при получении данных из базы данных: {e}")
        response = await update.message.reply_text("Произошла ошибка при получении данных из базы данных.")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
    finally:
        cur.close()
        conn.close()
        await update.message.delete()  # Удаляем команду


# Команда /ban
async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin_or_musician(update, context):
        response = await update.message.reply_text("❌ Только админы могут банить!")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
        await update.message.delete()  # Удаляем команду
        return
    
    if update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
        
        try:
            await update.message.delete()  # Удаляем команду
        except Exception as e:
            logger.error(f"Ошибка при удалении сообщения пользователя {target_user.id}: {e}")

        if target_user.id in banned_users:
            response = await update.message.reply_text(f"@{target_user.username} уже забанен.")
            context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
            await update.message.delete()  # Удаляем команду
            return

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            # Выполняем запрос к базе данных
            cur.execute('''
                INSERT INTO ban_list (user_id, username, ban_time)
                VALUES (%s, %s, %s)
            ''', (target_user.id, target_user.username, int(time.time())))
            conn.commit()
        except Exception as e:
            logger.error(f"Ошибка при выполнении запроса: {e}")
            conn.rollback()
        finally:
            # Закрываем курсор и соединение
            cur.close()
            conn.close()

        banned_users.add(target_user.id)

        try:
            await context.bot.ban_chat_member(chat_id=update.message.chat.id, user_id=target_user.id)
        except Exception as e:
            logger.error(f"Ошибка при бане пользователя {target_user.id}: {e}")     
            response = await update.message.reply_text("Не удалось забанить пользователя. Проверьте права бота.")
            context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
            await update.message.delete()  # Удаляем команду
            return

        
        response = await update.message.reply_text(f"@{target_user.username} забанен.")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
        await update.message.delete()  # Удаляем команду
    elif context.args:
        user_id = context.args[0]
        try:
            user_id = int(user_id)
        except ValueError:
            response = await update.message.reply_text("Введите корректный ID пользователя.")
            context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
            await update.message.delete()  # Удаляем команду
            return

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            # Выполняем запрос к базе данных
            cur.execute('''
                INSERT INTO ban_list (user_id, username, ban_time)
                VALUES (%s, %s, %s)
                ''', (user_id, "Unknown", int(time.time())))
            conn.commit()
        except Exception as e:
            logger.error(f"Ошибка при выполнении запроса: {e}")
            conn.rollback()
        finally:
            # Закрываем курсор и соединение
            cur.close()
            conn.close()

        banned_users.add(user_id) # Обновляем кэш

        try:
            await context.bot.ban_chat_member(chat_id=update.message.chat.id, user_id=user_id)
        except Exception as e:
            logger.error(f"Ошибка при бане пользователя {user_id}: {e}")
            response = await update.message.reply_text("Не удалось забанить пользователя. Проверьте права бота.")
            context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
            await update.message.delete()  # Удаляем команду
            return

        response = await update.message.reply_text(f"Пользователь с ID {user_id} забанен.")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
        await update.message.delete()  # Удаляем команду
    else:
        response = await update.message.reply_text("Ответьте на сообщение пользователя или укажите его ID.")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
        await update.message.delete()  # Удаляем команду


# Команда /deban
async def deban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin_or_musician(update, context):
        response = await update.message.reply_text("У вас нет прав для выполнения этой команды.")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
        await update.message.delete()  # Удаляем команду
        return
    if update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
        if target_user.id not in banned_users:
            response = await update.message.reply_text(f"@{target_user.username} не находится в бане.")
            context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
            await update.message.delete()  # Удаляем команду
            return

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            # Выполняем запрос к базе данных
            cur.execute('''
                DELETE FROM ban_list WHERE user_id = %s
                ''', (target_user.id,))
            conn.commit()
        except Exception as e:
            logger.error(f"Ошибка при выполнении запроса: {e}")
        finally:
            # Закрываем курсор и соединение
            cur.close()
            conn.close()

        banned_users.discard(target_user.id)

        try:
            await context.bot.unban_chat_member(chat_id=update.message.chat.id, user_id=target_user.id)
        except Exception as e:
            logger.error(f"Ошибка при разбане пользователя {target_user.id}: {e}")
            response = await update.message.reply_text("Не удалось разбанить пользователя. Проверьте права бота.")
            context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
            await update.message.delete()  # Удаляем команду
            return

        response = await update.message.reply_text(f"@{target_user.username} разбанен.")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
        await update.message.delete()  # Удаляем команду
    elif context.args:
        user_id = context.args[0]
        try:
            user_id = int(user_id)
        except ValueError:
            response = await update.message.reply_text("Введите корректный ID пользователя.")
            context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
            await update.message.delete()  # Удаляем команду
            return

        if user_id not in banned_users:
            response = await update.message.reply_text(f"Пользователь с ID {user_id} не находится в бане.")
            context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
            await update.message.delete()  # Удаляем команду
            return

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            # Выполняем запрос к базе данных
            cur.execute('''
                DELETE FROM ban_list WHERE user_id = %s
            ''', (user_id,))
            conn.commit()
        except Exception as e:
            logger.error(f"Ошибка при удалении пользователя из базы данных: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

        banned_users.discard(user_id)
        
        try:
            await context.bot.unban_chat_member(chat_id=update.message.chat.id, user_id=user_id)
        except Exception as e:
            logger.error(f"Ошибка при разбане пользователя {user_id}: {e}")
            response = await update.message.reply_text("Не удалось разбанить пользователя. Проверьте права бота.")
            context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
            await update.message.delete()  # Удаляем команду
            return

        response = await update.message.reply_text(f"Пользователь с ID {user_id} разбанен.")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
        await update.message.delete()  # Удаляем команду
    else:
        response = await update.message.reply_text("Ответьте на сообщение пользователя или укажите его ID.")
        context.job_queue.run_once(delete_system_message, 10, data=response.message_id, chat_id=update.message.chat.id)
        await update.message.delete()  # Удаляем команду

# Основная функция
def main():
    global db_initialized
    if not db_initialized:
        init_db()  # Инициализация базы данных
        db_initialized = True
    global banned_users
    banned_users = set()

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Выполняем запрос к базе данных
        cur.execute('SELECT user_id FROM ban_list')
        # Получаем все строки результата
        rows = cur.fetchall()
        banned_users = {row['user_id'] for row in rows}
    except Exception as e:
        logger.error(f"Ошибка при выполнении запроса: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

    application = Application.builder().token(BOT_TOKEN).build()
    job_queue = application.job_queue  # Инициализация JobQueue

    # Добавляем обработчики команд
    application.add_handler(CommandHandler("timer", reset_pin_timer))
    application.add_handler(CommandHandler("del", delete_message))
    application.add_handler(CommandHandler("lider", lider))
    application.add_handler(CommandHandler("zh", zh))
    application.add_handler(CommandHandler("active", active))
    application.add_handler(CommandHandler("dr", dr))
    application.add_handler(CommandHandler("druser", druser))
    application.add_handler(CommandHandler("id", get_user_id))
    application.add_handler(CommandHandler("birthday", birthday))
    application.add_handler(CommandHandler("check_birthdays", check_all_birthdays))
    application.add_handler(CommandHandler("ban_list", ban_list))
    application.add_handler(CommandHandler("ban", ban_user))
    application.add_handler(CommandHandler("deban", deban_user))
    application.add_handler(CommandHandler("ban_history", ban_history))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Запуск бота
    while True:  # Используем цикл для повторного запуска
        try:
            application.run_polling()
            logger.info("Бот запущен. Ожидание сообщений...")
        except Exception as e:
            logger.error(f"Ошибка при запуске бота: {e}")
            # Повторный запуск через 10 секунд
            time.sleep(10)
        finally:
            logger.info("Бот остановлен.")

if __name__ == '__main__':
    main()
