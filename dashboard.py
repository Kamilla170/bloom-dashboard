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

@app.get("/api/stats/retention")
async def get_retention_stats(
    retention_type: str = Query("classic", regex="^(classic|functional|rolling)$"),
    days: int = Query(7, ge=1, le=365)
):
    """
    Гибкий расчет retention метрик (по дням)
    
    Параметры:
    - retention_type: тип retention
      * classic - любая активность (last_activity на день N)
      * functional - полезные действия (полив/добавление/вопрос на день N)
      * rolling - вернулся хоть раз за N дней после регистрации
    - days: количество дней для измерения (1-365)
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database not connected")
    
    try:
        async with db_pool.acquire() as conn:
            cohorts = []
            
            # Анализируем последние 365 дней
            for i in range(365):
                cohort_date = (datetime.now() - timedelta(days=i + days)).date()
                target_date = cohort_date + timedelta(days=days)
                
                # Сколько зарегистрировалось в cohort_date
                cohort_size = await conn.fetchval("""
                    SELECT COUNT(*) FROM users 
                    WHERE created_at::date = $1
                """, cohort_date)
                
                if cohort_size == 0:
                    continue
                
                returned = 0
                
                if retention_type == "classic":
                    # Classic retention - любая активность на день N
                    returned = await conn.fetchval("""
                        SELECT COUNT(*) FROM users 
                        WHERE created_at::date = $1
                        AND last_activity IS NOT NULL
                        AND last_activity::date = $2
                    """, cohort_date, target_date)
                
                elif retention_type == "functional":
                    # Functional retention - полезные действия на день N
                    watered_users = await conn.fetch("""
                        SELECT DISTINCT p.user_id
                        FROM care_history ch
                        JOIN plants p ON ch.plant_id = p.id
                        JOIN users u ON p.user_id = u.user_id
                        WHERE u.created_at::date = $1
                        AND ch.action_type = 'watered'
                        AND ch.action_date::date = $2
                    """, cohort_date, target_date)
                    
                    added_plant_users = await conn.fetch("""
                        SELECT DISTINCT p.user_id
                        FROM plants p
                        JOIN users u ON p.user_id = u.user_id
                        WHERE u.created_at::date = $1
                        AND p.saved_date::date = $2
                    """, cohort_date, target_date)
                    
                    asked_question_users = await conn.fetch("""
                        SELECT DISTINCT qa.user_id
                        FROM plant_qa_history qa
                        JOIN users u ON qa.user_id = u.user_id
                        WHERE u.created_at::date = $1
                        AND qa.question_date::date = $2
                    """, cohort_date, target_date)
                    
                    # Объединяем уникальных пользователей
                    functional_users = set()
                    functional_users.update(row['user_id'] for row in watered_users)
                    functional_users.update(row['user_id'] for row in added_plant_users)
                    functional_users.update(row['user_id'] for row in asked_question_users)
                    
                    returned = len(functional_users)
                
                elif retention_type == "rolling":
                    # Rolling retention - вернулся хоть раз за N дней
                    start_date = cohort_date + timedelta(days=1)
                    end_date = cohort_date + timedelta(days=days)
                    
                    returned = await conn.fetchval("""
                        SELECT COUNT(*) FROM users 
                        WHERE created_at::date = $1
                        AND last_activity IS NOT NULL
                        AND last_activity::date >= $2
                        AND last_activity::date <= $3
                    """, cohort_date, start_date, end_date)
                
                retention_percent = round((returned / cohort_size * 100), 1) if cohort_size > 0 else 0
                
                cohorts.append({
                    "cohort_date": cohort_date.isoformat(),
                    "target_date": target_date.isoformat(),
                    "registered": cohort_size,
                    "returned": returned or 0,
                    "retention_percent": retention_percent
                })
            
            return {
                "retention_type": retention_type,
                "days": days,
                "cohorts": cohorts
            }
    
    except Exception as e:
        logger.error(f"Ошибка получения retention метрик: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats/retention-monthly")
async def get_retention_monthly_stats(
    retention_type: str = Query("classic", regex="^(classic|functional|rolling)$"),
    months: int = Query(1, ge=1, le=12)
):
    """
    Retention метрики по месяцам
    
    Параметры:
    - retention_type: тип retention
      * classic - любая активность через N месяцев
      * functional - полезные действия через N месяцев
      * rolling - вернулся хоть раз за N месяцев
    - months: количество месяцев для измерения (1-12)
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database not connected")
    
    try:
        async with db_pool.acquire() as conn:
            cohorts = []
            
            # Анализируем последние 12 месяцев когорт
            for i in range(12):
                cohort_date = (datetime.now() - relativedelta(months=i + months)).date()
                target_date_start = (datetime.combine(cohort_date, datetime.min.time()) + relativedelta(months=months)).date()
                target_date_end = (datetime.combine(cohort_date, datetime.min.time()) + relativedelta(months=months + 1) - timedelta(days=1)).date()
                
                # Сколько зарегистрировалось в этом месяце
                cohort_month_start = cohort_date.replace(day=1)
                cohort_month_end = (cohort_month_start + relativedelta(months=1) - timedelta(days=1))
                
                cohort_size = await conn.fetchval("""
                    SELECT COUNT(*) FROM users 
                    WHERE created_at::date >= $1 
                    AND created_at::date <= $2
                """, cohort_month_start, cohort_month_end)
                
                if cohort_size == 0:
                    continue
                
                returned = 0
                
                if retention_type == "classic":
                    # Classic retention - активность в целевом месяце
                    returned = await conn.fetchval("""
                        SELECT COUNT(DISTINCT u.user_id) FROM users u
                        WHERE u.created_at::date >= $1 
                        AND u.created_at::date <= $2
                        AND u.last_activity IS NOT NULL
                        AND u.last_activity::date >= $3
                        AND u.last_activity::date <= $4
                    """, cohort_month_start, cohort_month_end, target_date_start, target_date_end)
                
                elif retention_type == "functional":
                    # Functional retention - полезные действия в целевом месяце
                    watered_users = await conn.fetch("""
                        SELECT DISTINCT p.user_id
                        FROM care_history ch
                        JOIN plants p ON ch.plant_id = p.id
                        JOIN users u ON p.user_id = u.user_id
                        WHERE u.created_at::date >= $1 
                        AND u.created_at::date <= $2
                        AND ch.action_type = 'watered'
                        AND ch.action_date::date >= $3
                        AND ch.action_date::date <= $4
                    """, cohort_month_start, cohort_month_end, target_date_start, target_date_end)
                    
                    added_plant_users = await conn.fetch("""
                        SELECT DISTINCT p.user_id
                        FROM plants p
                        JOIN users u ON p.user_id = u.user_id
                        WHERE u.created_at::date >= $1 
                        AND u.created_at::date <= $2
                        AND p.saved_date::date >= $3
                        AND p.saved_date::date <= $4
                    """, cohort_month_start, cohort_month_end, target_date_start, target_date_end)
                    
                    asked_question_users = await conn.fetch("""
                        SELECT DISTINCT qa.user_id
                        FROM plant_qa_history qa
                        JOIN users u ON qa.user_id = u.user_id
                        WHERE u.created_at::date >= $1 
                        AND u.created_at::date <= $2
                        AND qa.question_date::date >= $3
                        AND qa.question_date::date <= $4
                    """, cohort_month_start, cohort_month_end, target_date_start, target_date_end)
                    
                    # Объединяем уникальных пользователей
                    functional_users = set()
                    functional_users.update(row['user_id'] for row in watered_users)
                    functional_users.update(row['user_id'] for row in added_plant_users)
                    functional_users.update(row['user_id'] for row in asked_question_users)
                    
                    returned = len(functional_users)
                
                elif retention_type == "rolling":
                    # Rolling retention - активность за N месяцев
                    rolling_start = cohort_month_end + timedelta(days=1)
                    rolling_end = target_date_end
                    
                    returned = await conn.fetchval("""
                        SELECT COUNT(DISTINCT u.user_id) FROM users u
                        WHERE u.created_at::date >= $1 
                        AND u.created_at::date <= $2
                        AND u.last_activity IS NOT NULL
                        AND u.last_activity::date >= $3
                        AND u.last_activity::date <= $4
                    """, cohort_month_start, cohort_month_end, rolling_start, rolling_end)
                
                retention_percent = round((returned / cohort_size * 100), 1) if cohort_size > 0 else 0
                
                cohorts.append({
                    "cohort_month": cohort_month_start.isoformat(),
                    "target_month": target_date_start.isoformat(),
                    "registered": cohort_size,
                    "returned": returned or 0,
                    "retention_percent": retention_percent
                })
            
            return {
                "retention_type": retention_type,
                "months": months,
                "cohorts": cohorts
            }
    
    except Exception as e:
        logger.error(f"Ошибка получения monthly retention метрик: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    logger.info("=" * 70)
    logger.info("🌱 BLOOM AI DASHBOARD")
    logger.info("=" * 70)
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")
