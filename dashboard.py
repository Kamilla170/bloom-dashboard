import asyncio
import asyncpg
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Bloom AI Dashboard")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database URL из переменной окружения
def get_database_url():
    """Получить корректный DATABASE_URL"""
    
    # ПРИОРИТЕТ 1: Пробуем собрать из отдельных переменных (надёжнее для Railway)
    pg_host = os.getenv("PGHOST")
    pg_port = os.getenv("PGPORT")
    pg_user = os.getenv("PGUSER")
    pg_password = os.getenv("PGPASSWORD")
    pg_database = os.getenv("PGDATABASE")
    
    if pg_host and pg_password:
        # Используем значения по умолчанию если не указаны
        pg_port = pg_port or "5432"
        pg_user = pg_user or "postgres"
        pg_database = pg_database or "railway"
        
        database_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}"
        logger.info("✅ DATABASE_URL собран из отдельных переменных")
        logger.info(f"🔗 Подключение: postgresql://{pg_user}:***@{pg_host}:{pg_port}/{pg_database}")
        return database_url
    
    # ПРИОРИТЕТ 2: Пробуем DATABASE_PRIVATE_URL
    private_url = os.getenv("DATABASE_PRIVATE_URL")
    if private_url:
        logger.info("✅ Использую DATABASE_PRIVATE_URL")
        return private_url
    
    # ПРИОРИТЕТ 3: Последняя попытка с DATABASE_URL
    public_url = os.getenv("DATABASE_URL")
    if public_url:
        logger.info("⚠️ Использую DATABASE_URL (может быть некорректным)")
        logger.info(f"🔍 Первые 50 символов: {public_url[:50]}...")
        return public_url
    
    logger.error("❌ Не найдены переменные для подключения к БД")
    logger.error("💡 Установите переменные: PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE")
    return None

DATABASE_URL = get_database_url()

# Database pool
db_pool = None

async def init_db():
    """Инициализация пула подключений"""
    global db_pool
    
    if not DATABASE_URL:
        logger.error("❌ DATABASE_URL не установлен! Проверьте переменные окружения.")
        return False
    
    try:
        logger.info(f"🔌 Подключаюсь к БД...")
        db_pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1,
            max_size=3,
            timeout=30
        )
        logger.info("✅ Подключение к БД установлено")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к БД: {e}")
        logger.error(f"💡 Проверьте переменные: DATABASE_URL, DATABASE_PRIVATE_URL или PGHOST, PGPASSWORD")
        return False

@app.on_event("startup")
async def startup():
    """Запуск приложения"""
    logger.info("🚀 Запуск дашборда...")
    success = await init_db()
    if success:
        logger.info("✅ Дашборд готов к работе")
    else:
        logger.error("❌ Не удалось подключиться к БД")

@app.on_event("shutdown")
async def shutdown():
    """Остановка приложения"""
    global db_pool
    if db_pool:
        await db_pool.close()
        logger.info("✅ Соединение с БД закрыто")

@app.get("/", response_class=HTMLResponse)
async def root():
    """Главная страница"""
    # Получаем путь относительно текущего файла
    current_dir = Path(__file__).parent
    html_path = current_dir / "static" / "index.html"
    
    if html_path.exists():
        return FileResponse(html_path)
    else:
        logger.error(f"❌ Файл не найден: {html_path}")
        return HTMLResponse("<h1>Dashboard</h1><p>Error: index.html not found</p>")

@app.get("/api/stats/today")
async def get_today_stats():
    """Статистика за сегодня"""
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database not connected")
    
    try:
        async with db_pool.acquire() as conn:
            today = datetime.now().date()
            
            # Общее количество пользователей
            total_users = await conn.fetchval("SELECT COUNT(*) FROM users")
            
            # Новые пользователи за сегодня
            new_users_today = await conn.fetchval("""
                SELECT COUNT(*) FROM users 
                WHERE created_at::date = $1
            """, today)
            
            # Пользователи которые полили сегодня
            watered_today = await conn.fetchval("""
                SELECT COUNT(DISTINCT plant_id) FROM care_history 
                WHERE action_type = 'watered' 
                AND action_date::date = $1
            """, today)
            
            # Количество уникальных пользователей которые полили
            users_watered_today = await conn.fetchval("""
                SELECT COUNT(DISTINCT p.user_id) 
                FROM care_history ch
                JOIN plants p ON ch.plant_id = p.id
                WHERE ch.action_type = 'watered' 
                AND ch.action_date::date = $1
            """, today)
            
            # Пользователи которые добавили растение сегодня
            added_plants_today = await conn.fetchval("""
                SELECT COUNT(DISTINCT user_id) FROM plants 
                WHERE saved_date::date = $1
            """, today)
            
            # Активные пользователи сегодня (last_activity)
            active_today = await conn.fetchval("""
                SELECT COUNT(*) FROM users 
                WHERE last_activity IS NOT NULL 
                AND last_activity::date = $1
            """, today)
            
            # Неактивные пользователи сегодня
            inactive_today = total_users - active_today if active_today else total_users
            
            # Проценты
            watered_percent = round((users_watered_today / total_users * 100), 1) if total_users > 0 else 0
            added_plants_percent = round((added_plants_today / total_users * 100), 1) if total_users > 0 else 0
            active_percent = round((active_today / total_users * 100), 1) if total_users > 0 else 0
            inactive_percent = round((inactive_today / total_users * 100), 1) if total_users > 0 else 0
            
            return {
                "date": today.isoformat(),
                "total_users": total_users,
                "new_users": new_users_today,
                "watered": {
                    "count": users_watered_today,
                    "percent": watered_percent
                },
                "added_plants": {
                    "count": added_plants_today,
                    "percent": added_plants_percent
                },
                "active": {
                    "count": active_today,
                    "percent": active_percent
                },
                "inactive": {
                    "count": inactive_today,
                    "percent": inactive_percent
                }
            }
    except Exception as e:
        logger.error(f"Ошибка получения статистики: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats/yesterday")
async def get_yesterday_stats():
    """Статистика за вчера"""
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database not connected")
    
    try:
        async with db_pool.acquire() as conn:
            yesterday = (datetime.now() - timedelta(days=1)).date()
            
            # Общее количество пользователей на вчера
            total_users = await conn.fetchval("""
                SELECT COUNT(*) FROM users 
                WHERE created_at::date <= $1
            """, yesterday)
            
            # Новые пользователи вчера
            new_users = await conn.fetchval("""
                SELECT COUNT(*) FROM users 
                WHERE created_at::date = $1
            """, yesterday)
            
            # Пользователи которые полили вчера
            watered = await conn.fetchval("""
                SELECT COUNT(DISTINCT p.user_id) 
                FROM care_history ch
                JOIN plants p ON ch.plant_id = p.id
                WHERE ch.action_type = 'watered' 
                AND ch.action_date::date = $1
            """, yesterday)
            
            # Пользователи которые добавили растение вчера
            added_plants = await conn.fetchval("""
                SELECT COUNT(DISTINCT user_id) FROM plants 
                WHERE saved_date::date = $1
            """, yesterday)
            
            # Активные пользователи вчера
            active = await conn.fetchval("""
                SELECT COUNT(*) FROM users 
                WHERE last_activity IS NOT NULL
                AND last_activity::date = $1
            """, yesterday)
            
            inactive = total_users - active if active else total_users
            
            # Проценты
            watered_percent = round((watered / total_users * 100), 1) if total_users > 0 else 0
            added_plants_percent = round((added_plants / total_users * 100), 1) if total_users > 0 else 0
            active_percent = round((active / total_users * 100), 1) if total_users > 0 else 0
            inactive_percent = round((inactive / total_users * 100), 1) if total_users > 0 else 0
            
            return {
                "date": yesterday.isoformat(),
                "total_users": total_users,
                "new_users": new_users,
                "watered": {
                    "count": watered,
                    "percent": watered_percent
                },
                "added_plants": {
                    "count": added_plants,
                    "percent": added_plants_percent
                },
                "active": {
                    "count": active,
                    "percent": active_percent
                },
                "inactive": {
                    "count": inactive,
                    "percent": inactive_percent
                }
            }
    except Exception as e:
        logger.error(f"Ошибка получения статистики: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats/week")
async def get_week_stats():
    """Статистика за последние 7 дней"""
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database not connected")
    
    try:
        async with db_pool.acquire() as conn:
            days = []
            
            for i in range(7):
                day = (datetime.now() - timedelta(days=i)).date()
                
                # Новые пользователи
                new_users = await conn.fetchval("""
                    SELECT COUNT(*) FROM users WHERE created_at::date = $1
                """, day)
                
                # Поливы
                watered = await conn.fetchval("""
                    SELECT COUNT(DISTINCT p.user_id) 
                    FROM care_history ch
                    JOIN plants p ON ch.plant_id = p.id
                    WHERE ch.action_type = 'watered' 
                    AND ch.action_date::date = $1
                """, day)
                
                # Добавленные растения
                added_plants = await conn.fetchval("""
                    SELECT COUNT(DISTINCT user_id) FROM plants WHERE saved_date::date = $1
                """, day)
                
                # Активные
                active = await conn.fetchval("""
                    SELECT COUNT(*) FROM users 
                    WHERE last_activity IS NOT NULL 
                    AND last_activity::date = $1
                """, day)
                
                days.append({
                    "date": day.isoformat(),
                    "new_users": new_users,
                    "watered": watered,
                    "added_plants": added_plants,
                    "active": active
                })
            
            return {"days": list(reversed(days))}
    except Exception as e:
        logger.error(f"Ошибка получения недельной статистики: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats/month")
async def get_month_stats():
    """Статистика за последние 30 дней"""
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database not connected")
    
    try:
        async with db_pool.acquire() as conn:
            days = []
            
            for i in range(30):
                day = (datetime.now() - timedelta(days=i)).date()
                
                # Новые пользователи
                new_users = await conn.fetchval("""
                    SELECT COUNT(*) FROM users WHERE created_at::date = $1
                """, day)
                
                # Поливы
                watered = await conn.fetchval("""
                    SELECT COUNT(DISTINCT p.user_id) 
                    FROM care_history ch
                    JOIN plants p ON ch.plant_id = p.id
                    WHERE ch.action_type = 'watered' 
                    AND ch.action_date::date = $1
                """, day)
                
                # Добавленные растения
                added_plants = await conn.fetchval("""
                    SELECT COUNT(DISTINCT user_id) FROM plants WHERE saved_date::date = $1
                """, day)
                
                # Активные
                active = await conn.fetchval("""
                    SELECT COUNT(*) FROM users 
                    WHERE last_activity IS NOT NULL 
                    AND last_activity::date = $1
                """, day)
                
                days.append({
                    "date": day.isoformat(),
                    "new_users": new_users,
                    "watered": watered,
                    "added_plants": added_plants,
                    "active": active
                })
            
            return {"days": list(reversed(days))}
    except Exception as e:
        logger.error(f"Ошибка получения месячной статистики: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/health")
async def health_check():
    """Проверка здоровья"""
    db_status = "connected" if db_pool else "disconnected"
    return {
        "status": "healthy",
        "database": db_status,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/stats/additional")
async def get_additional_stats():
    """Дополнительные метрики"""
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database not connected")
    
    try:
        async with db_pool.acquire() as conn:
            today = datetime.now().date()
            week_ago = today - timedelta(days=7)
            
            # Вопросы
            questions_today = await conn.fetchval("""
                SELECT COUNT(*) FROM plant_qa_history 
                WHERE question_date::date = $1
            """, today)
            
            questions_week = await conn.fetchval("""
                SELECT COUNT(*) FROM plant_qa_history 
                WHERE question_date::date >= $1
            """, week_ago)
            
            # Feedback
            feedback_today = await conn.fetchval("""
                SELECT COUNT(*) FROM feedback 
                WHERE created_at::date = $1
            """, today)
            
            feedback_week = await conn.fetchval("""
                SELECT COUNT(*) FROM feedback 
                WHERE created_at::date >= $1
            """, week_ago)
            
            # Выращивание
            growing_active = await conn.fetchval("""
                SELECT COUNT(*) FROM growing_plants 
                WHERE status = 'active'
            """)
            
            growing_completed = await conn.fetchval("""
                SELECT COUNT(*) FROM growing_plants 
                WHERE status = 'completed'
            """)
            
            # Всего растений
            total_plants = await conn.fetchval("""
                SELECT COUNT(*) FROM plants
            """)
            
            total_users = await conn.fetchval("""
                SELECT COUNT(*) FROM users
            """)
            
            avg_plants_per_user = round(total_plants / total_users, 1) if total_users > 0 else 0
            
            # Топ-5 растений
            top_plants = await conn.fetch("""
                SELECT plant_name, COUNT(*) as count
                FROM plants
                WHERE plant_name IS NOT NULL 
                AND plant_name != ''
                AND NOT plant_name ILIKE '%неизвестн%'
                AND NOT plant_name ILIKE '%неопознан%'
                GROUP BY plant_name
                ORDER BY count DESC
                LIMIT 5
            """)
            
            return {
                "questions": {
                    "today": questions_today or 0,
                    "week": questions_week or 0
                },
                "feedback": {
                    "today": feedback_today or 0,
                    "week": feedback_week or 0
                },
                "growing": {
                    "active": growing_active or 0,
                    "completed": growing_completed or 0,
                    "total": (growing_active or 0) + (growing_completed or 0)
                },
                "plants": {
                    "total": total_plants or 0,
                    "avg_per_user": avg_plants_per_user
                },
                "top_plants": [
                    {"name": row["plant_name"], "count": row["count"]}
                    for row in top_plants
                ]
            }
    except Exception as e:
        logger.error(f"Ошибка получения дополнительных метрик: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats/retention-flexible")
async def get_retention_flexible_stats(
    retention_type: str = Query("classic", regex="^(classic|functional|rolling)$"),
    granularity: str = Query("day", regex="^(day|week|month)$"),
    period: int = Query(7, ge=1, le=365)
):
    """
    Гибкий расчет retention метрик с выбором гранулярности
    
    Параметры:
    - retention_type: тип retention (classic/functional/rolling)
    - granularity: гранулярность (day/week/month)
    - period: период в единицах гранулярности (1-365 для дней, 1-52 для недель, 1-12 для месяцев)
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database not connected")
    
    try:
        async with db_pool.acquire() as conn:
            cohorts = []
            
            if granularity == "day":
                # По дням
                for i in range(min(365, period * 5)):  # Анализируем последние N*5 дней когорт
                    cohort_date = (datetime.now() - timedelta(days=i + period)).date()
                    target_date = cohort_date + timedelta(days=period)
                    
                    cohort_size = await conn.fetchval("""
                        SELECT COUNT(*) FROM users WHERE created_at::date = $1
                    """, cohort_date)
                    
                    if cohort_size == 0:
                        continue
                    
                    returned = await get_returned_users(
                        conn, retention_type, cohort_date, cohort_date, target_date, target_date, granularity
                    )
                    
                    retention_percent = round((returned / cohort_size * 100), 1) if cohort_size > 0 else 0
                    
                    cohorts.append({
                        "cohort_label": cohort_date.isoformat(),
                        "target_label": target_date.isoformat(),
                        "registered": cohort_size,
                        "returned": returned or 0,
                        "retention_percent": retention_percent
                    })
            
            elif granularity == "week":
                # По неделям
                for i in range(min(52, period * 5)):
                    cohort_start = (datetime.now() - timedelta(weeks=i + period)).date()
                    # Начало недели (понедельник)
                    cohort_start = cohort_start - timedelta(days=cohort_start.weekday())
                    cohort_end = cohort_start + timedelta(days=6)
                    
                    target_start = cohort_start + timedelta(weeks=period)
                    target_end = target_start + timedelta(days=6)
                    
                    cohort_size = await conn.fetchval("""
                        SELECT COUNT(*) FROM users 
                        WHERE created_at::date >= $1 AND created_at::date <= $2
                    """, cohort_start, cohort_end)
                    
                    if cohort_size == 0:
                        continue
                    
                    returned = await get_returned_users(
                        conn, retention_type, cohort_start, cohort_end, target_start, target_end, granularity
                    )
                    
                    retention_percent = round((returned / cohort_size * 100), 1) if cohort_size > 0 else 0
                    
                    cohorts.append({
                        "cohort_label": f"{cohort_start.strftime('%d.%m')}-{cohort_end.strftime('%d.%m')}",
                        "target_label": f"{target_start.strftime('%d.%m')}-{target_end.strftime('%d.%m')}",
                        "registered": cohort_size,
                        "returned": returned or 0,
                        "retention_percent": retention_percent
                    })
            
            elif granularity == "month":
                # По месяцам
                for i in range(min(12, period * 3)):
                    cohort_date = (datetime.now() - relativedelta(months=i + period)).date()
                    cohort_start = cohort_date.replace(day=1)
                    cohort_end = (cohort_start + relativedelta(months=1) - timedelta(days=1))
                    
                    target_start = (cohort_start + relativedelta(months=period))
                    target_end = (target_start + relativedelta(months=1) - timedelta(days=1))
                    
                    cohort_size = await conn.fetchval("""
                        SELECT COUNT(*) FROM users 
                        WHERE created_at::date >= $1 AND created_at::date <= $2
                    """, cohort_start, cohort_end)
                    
                    if cohort_size == 0:
                        continue
                    
                    returned = await get_returned_users(
                        conn, retention_type, cohort_start, cohort_end, target_start, target_end, granularity
                    )
                    
                    retention_percent = round((returned / cohort_size * 100), 1) if cohort_size > 0 else 0
                    
                    cohorts.append({
                        "cohort_label": cohort_start.strftime('%b %Y'),
                        "target_label": target_start.strftime('%b %Y'),
                        "registered": cohort_size,
                        "returned": returned or 0,
                        "retention_percent": retention_percent
                    })
            
            return {
                "retention_type": retention_type,
                "granularity": granularity,
                "period": period,
                "cohorts": cohorts
            }
    
    except Exception as e:
        logger.error(f"Ошибка получения flexible retention метрик: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def get_returned_users(conn, retention_type, cohort_start, cohort_end, target_start, target_end, granularity):
    """Вспомогательная функция для подсчета вернувшихся пользователей"""
    
    if retention_type == "classic":
        # Classic retention - активность в целевом периоде
        returned = await conn.fetchval("""
            SELECT COUNT(DISTINCT u.user_id) FROM users u
            WHERE u.created_at::date >= $1 AND u.created_at::date <= $2
            AND u.last_activity IS NOT NULL
            AND u.last_activity::date >= $3 AND u.last_activity::date <= $4
        """, cohort_start, cohort_end, target_start, target_end)
    
    elif retention_type == "functional":
        # Functional retention - полезные действия в целевом периоде
        watered_users = await conn.fetch("""
            SELECT DISTINCT p.user_id
            FROM care_history ch
            JOIN plants p ON ch.plant_id = p.id
            JOIN users u ON p.user_id = u.user_id
            WHERE u.created_at::date >= $1 AND u.created_at::date <= $2
            AND ch.action_type = 'watered'
            AND ch.action_date::date >= $3 AND ch.action_date::date <= $4
        """, cohort_start, cohort_end, target_start, target_end)
        
        added_plant_users = await conn.fetch("""
            SELECT DISTINCT p.user_id
            FROM plants p
            JOIN users u ON p.user_id = u.user_id
            WHERE u.created_at::date >= $1 AND u.created_at::date <= $2
            AND p.saved_date::date >= $3 AND p.saved_date::date <= $4
        """, cohort_start, cohort_end, target_start, target_end)
        
        asked_question_users = await conn.fetch("""
            SELECT DISTINCT qa.user_id
            FROM plant_qa_history qa
            JOIN users u ON qa.user_id = u.user_id
            WHERE u.created_at::date >= $1 AND u.created_at::date <= $2
            AND qa.question_date::date >= $3 AND qa.question_date::date <= $4
        """, cohort_start, cohort_end, target_start, target_end)
        
        functional_users = set()
        functional_users.update(row['user_id'] for row in watered_users)
        functional_users.update(row['user_id'] for row in added_plant_users)
        functional_users.update(row['user_id'] for row in asked_question_users)
        
        returned = len(functional_users)
    
    elif retention_type == "rolling":
        # Rolling retention - активность ЗА период
        rolling_start = cohort_end + timedelta(days=1)
        rolling_end = target_end
        
        returned = await conn.fetchval("""
            SELECT COUNT(DISTINCT u.user_id) FROM users u
            WHERE u.created_at::date >= $1 AND u.created_at::date <= $2
            AND u.last_activity IS NOT NULL
            AND u.last_activity::date >= $3 AND u.last_activity::date <= $4
        """, cohort_start, cohort_end, rolling_start, rolling_end)
    
    return returned or 0

@app.get("/api/stats/timeseries")
async def get_timeseries_stats(
    granularity: str = Query("day", regex="^(day|week|month)$"),
    date_from: str = Query(...),
    date_to: str = Query(...)
):
    """
    Гибкая статистика с выбором периода и гранулярности
    
    Параметры:
    - granularity: гранулярность данных (day, week, month)
    - date_from: начальная дата (YYYY-MM-DD)
    - date_to: конечная дата (YYYY-MM-DD)
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database not connected")
    
    try:
        from_date = datetime.strptime(date_from, "%Y-%m-%d").date()
        to_date = datetime.strptime(date_to, "%Y-%m-%d").date()
        
        if from_date > to_date:
            raise HTTPException(status_code=400, detail="date_from must be before date_to")
        
        async with db_pool.acquire() as conn:
            data_points = []
            
            if granularity == "day":
                # По дням
                current_date = from_date
                while current_date <= to_date:
                    # Новые пользователи
                    new_users = await conn.fetchval("""
                        SELECT COUNT(*) FROM users WHERE created_at::date = $1
                    """, current_date)
                    
                    # Поливы (уникальные пользователи)
                    watered = await conn.fetchval("""
                        SELECT COUNT(DISTINCT p.user_id) 
                        FROM care_history ch
                        JOIN plants p ON ch.plant_id = p.id
                        WHERE ch.action_type = 'watered' 
                        AND ch.action_date::date = $1
                    """, current_date)
                    
                    # Добавили растения
                    added_plants = await conn.fetchval("""
                        SELECT COUNT(DISTINCT user_id) FROM plants WHERE saved_date::date = $1
                    """, current_date)
                    
                    # Добавили рост с нуля
                    added_growing = await conn.fetchval("""
                        SELECT COUNT(DISTINCT user_id) FROM growing_plants WHERE created_at::date = $1
                    """, current_date) or 0
                    
                    # Задали вопрос
                    asked_question = await conn.fetchval("""
                        SELECT COUNT(DISTINCT user_id) FROM plant_qa_history WHERE question_date::date = $1
                    """, current_date) or 0
                    
                    # Оставили отзыв
                    left_feedback = await conn.fetchval("""
                        SELECT COUNT(DISTINCT user_id) FROM feedback WHERE created_at::date = $1
                    """, current_date) or 0
                    
                    # Открыли бота (last_activity)
                    opened_bot = await conn.fetchval("""
                        SELECT COUNT(*) FROM users 
                        WHERE last_activity IS NOT NULL 
                        AND last_activity::date = $1
                    """, current_date)
                    
                    data_points.append({
                        "date": current_date.isoformat(),
                        "label": current_date.strftime("%d.%m"),
                        "new_users": new_users or 0,
                        "watered": watered or 0,
                        "added_plants": added_plants or 0,
                        "added_growing": added_growing,
                        "asked_question": asked_question,
                        "left_feedback": left_feedback,
                        "opened_bot": opened_bot or 0
                    })
                    
                    current_date += timedelta(days=1)
            
            elif granularity == "week":
                # По неделям
                current_date = from_date
                while current_date <= to_date:
                    week_end = min(current_date + timedelta(days=6), to_date)
                    
                    # Новые пользователи за неделю
                    new_users = await conn.fetchval("""
                        SELECT COUNT(*) FROM users 
                        WHERE created_at::date >= $1 AND created_at::date <= $2
                    """, current_date, week_end)
                    
                    # Поливы за неделю
                    watered = await conn.fetchval("""
                        SELECT COUNT(DISTINCT p.user_id) 
                        FROM care_history ch
                        JOIN plants p ON ch.plant_id = p.id
                        WHERE ch.action_type = 'watered' 
                        AND ch.action_date::date >= $1 AND ch.action_date::date <= $2
                    """, current_date, week_end)
                    
                    # Добавили растения за неделю
                    added_plants = await conn.fetchval("""
                        SELECT COUNT(DISTINCT user_id) FROM plants 
                        WHERE saved_date::date >= $1 AND saved_date::date <= $2
                    """, current_date, week_end)
                    
                    # Добавили рост с нуля за неделю
                    added_growing = await conn.fetchval("""
                        SELECT COUNT(DISTINCT user_id) FROM growing_plants 
                        WHERE created_at::date >= $1 AND created_at::date <= $2
                    """, current_date, week_end) or 0
                    
                    # Задали вопрос за неделю
                    asked_question = await conn.fetchval("""
                        SELECT COUNT(DISTINCT user_id) FROM plant_qa_history 
                        WHERE question_date::date >= $1 AND question_date::date <= $2
                    """, current_date, week_end) or 0
                    
                    # Оставили отзыв за неделю
                    left_feedback = await conn.fetchval("""
                        SELECT COUNT(DISTINCT user_id) FROM feedback 
                        WHERE created_at::date >= $1 AND created_at::date <= $2
                    """, current_date, week_end) or 0
                    
                    # Открыли бота за неделю (уникальные)
                    opened_bot = await conn.fetchval("""
                        SELECT COUNT(DISTINCT user_id) FROM users 
                        WHERE last_activity IS NOT NULL 
                        AND last_activity::date >= $1 AND last_activity::date <= $2
                    """, current_date, week_end)
                    
                    data_points.append({
                        "date": current_date.isoformat(),
                        "label": f"{current_date.strftime('%d.%m')}-{week_end.strftime('%d.%m')}",
                        "new_users": new_users or 0,
                        "watered": watered or 0,
                        "added_plants": added_plants or 0,
                        "added_growing": added_growing,
                        "asked_question": asked_question,
                        "left_feedback": left_feedback,
                        "opened_bot": opened_bot or 0
                    })
                    
                    current_date += timedelta(days=7)
            
            elif granularity == "month":
                # По месяцам
                current_date = from_date.replace(day=1)
                while current_date <= to_date:
                    month_end = (current_date + relativedelta(months=1) - timedelta(days=1))
                    if month_end > to_date:
                        month_end = to_date
                    
                    # Новые пользователи за месяц
                    new_users = await conn.fetchval("""
                        SELECT COUNT(*) FROM users 
                        WHERE created_at::date >= $1 AND created_at::date <= $2
                    """, current_date, month_end)
                    
                    # Поливы за месяц
                    watered = await conn.fetchval("""
                        SELECT COUNT(DISTINCT p.user_id) 
                        FROM care_history ch
                        JOIN plants p ON ch.plant_id = p.id
                        WHERE ch.action_type = 'watered' 
                        AND ch.action_date::date >= $1 AND ch.action_date::date <= $2
                    """, current_date, month_end)
                    
                    # Добавили растения за месяц
                    added_plants = await conn.fetchval("""
                        SELECT COUNT(DISTINCT user_id) FROM plants 
                        WHERE saved_date::date >= $1 AND saved_date::date <= $2
                    """, current_date, month_end)
                    
                    # Добавили рост с нуля за месяц
                    added_growing = await conn.fetchval("""
                        SELECT COUNT(DISTINCT user_id) FROM growing_plants 
                        WHERE created_at::date >= $1 AND created_at::date <= $2
                    """, current_date, month_end) or 0
                    
                    # Задали вопрос за месяц
                    asked_question = await conn.fetchval("""
                        SELECT COUNT(DISTINCT user_id) FROM plant_qa_history 
                        WHERE question_date::date >= $1 AND question_date::date <= $2
                    """, current_date, month_end) or 0
                    
                    # Оставили отзыв за месяц
                    left_feedback = await conn.fetchval("""
                        SELECT COUNT(DISTINCT user_id) FROM feedback 
                        WHERE created_at::date >= $1 AND created_at::date <= $2
                    """, current_date, month_end) or 0
                    
                    # Открыли бота за месяц (уникальные)
                    opened_bot = await conn.fetchval("""
                        SELECT COUNT(DISTINCT user_id) FROM users 
                        WHERE last_activity IS NOT NULL 
                        AND last_activity::date >= $1 AND last_activity::date <= $2
                    """, current_date, month_end)
                    
                    data_points.append({
                        "date": current_date.isoformat(),
                        "label": current_date.strftime("%b %Y"),
                        "new_users": new_users or 0,
                        "watered": watered or 0,
                        "added_plants": added_plants or 0,
                        "added_growing": added_growing,
                        "asked_question": asked_question,
                        "left_feedback": left_feedback,
                        "opened_bot": opened_bot or 0
                    })
                    
                    current_date += relativedelta(months=1)
            
            return {
                "granularity": granularity,
                "date_from": date_from,
                "date_to": date_to,
                "data": data_points
            }
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except Exception as e:
        logger.error(f"Ошибка получения timeseries данных: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    logger.info("=" * 70)
    logger.info("🌱 BLOOM AI DASHBOARD")
    logger.info("=" * 70)
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")
