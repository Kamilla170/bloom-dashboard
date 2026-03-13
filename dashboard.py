import asyncio
import asyncpg
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from fastapi import FastAPI, HTTPException, Query, Response
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
    current_dir = Path(__file__).parent
    html_path = current_dir / "static" / "index.html"
    
    if html_path.exists():
        return FileResponse(
            html_path,
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )
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

@app.get("/api/debug/date/{date}")
async def debug_date_data(date: str):
    """Диагностика данных за конкретную дату"""
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database not connected")
    
    try:
        target_date = datetime.strptime(date, "%Y-%m-%d").date()
        
        async with db_pool.acquire() as conn:
            # Проверяем вопросы
            questions = await conn.fetch("""
                SELECT user_id, question_date, question_text
                FROM plant_qa_history 
                WHERE question_date::date = $1
                ORDER BY question_date
            """, target_date)
            
            # Проверяем поливы
            waterings = await conn.fetch("""
                SELECT ch.id, ch.plant_id, ch.action_date, p.user_id, p.plant_name
                FROM care_history ch
                JOIN plants p ON ch.plant_id = p.id
                WHERE ch.action_type = 'watered'
                AND ch.action_date::date = $1
                ORDER BY ch.action_date
            """, target_date)
            
            # Проверяем активность пользователей
            active_users = await conn.fetch("""
                SELECT user_id, username, last_activity
                FROM users 
                WHERE last_activity IS NOT NULL 
                AND last_activity::date = $1
                ORDER BY last_activity
            """, target_date)
            
            # Проверяем новых пользователей
            new_users = await conn.fetch("""
                SELECT user_id, username, created_at
                FROM users 
                WHERE created_at::date = $1
                ORDER BY created_at
            """, target_date)
            
            # Проверяем добавленные растения
            added_plants = await conn.fetch("""
                SELECT id, user_id, plant_name, saved_date
                FROM plants 
                WHERE saved_date::date = $1
                ORDER BY saved_date
            """, target_date)
            
            return {
                "date": date,
                "server_timezone": str(datetime.now().astimezone()),
                "questions": {
                    "count": len(questions),
                    "data": [
                        {
                            "user_id": q["user_id"],
                            "question_date": str(q["question_date"]),
                            "question_text": q["question_text"][:100] if q["question_text"] else None
                        }
                        for q in questions
                    ]
                },
                "waterings": {
                    "count": len(waterings),
                    "unique_users": len(set(w["user_id"] for w in waterings)),
                    "data": [
                        {
                            "id": w["id"],
                            "user_id": w["user_id"],
                            "plant_id": w["plant_id"],
                            "plant_name": w["plant_name"],
                            "action_date": str(w["action_date"])
                        }
                        for w in waterings
                    ]
                },
                "active_users": {
                    "count": len(active_users),
                    "data": [
                        {
                            "user_id": u["user_id"],
                            "username": u["username"],
                            "last_activity": str(u["last_activity"])
                        }
                        for u in active_users
                    ]
                },
                "new_users": {
                    "count": len(new_users),
                    "data": [
                        {
                            "user_id": u["user_id"],
                            "username": u["username"],
                            "created_at": str(u["created_at"])
                        }
                        for u in new_users
                    ]
                },
                "added_plants": {
                    "count": len(added_plants),
                    "unique_users": len(set(p["user_id"] for p in added_plants)),
                    "data": [
                        {
                            "id": p["id"],
                            "user_id": p["user_id"],
                            "plant_name": p["plant_name"],
                            "saved_date": str(p["saved_date"])
                        }
                        for p in added_plants
                    ]
                }
            }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format. Use YYYY-MM-DD: {e}")
    except Exception as e:
        logger.error(f"Ошибка диагностики данных: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats/actions-per-user")
async def get_actions_per_user_stats(
    granularity: str = Query("day", regex="^(day|week|month)$"),
    date_from: str = Query(...),
    date_to: str = Query(...)
):
    """Статистика полезных действий на одного активного пользователя"""
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
                current_date = from_date
                while current_date <= to_date:
                    active_users = await conn.fetchval("""
                        SELECT COUNT(DISTINCT user_id) FROM users 
                        WHERE last_activity IS NOT NULL 
                        AND last_activity::date = $1
                    """, current_date) or 0
                    
                    users_with_plants = await conn.fetchval("""
                        SELECT COUNT(DISTINCT user_id) FROM plants
                        WHERE saved_date::date <= $1
                    """, current_date) or 0
                    
                    total_plants_count = await conn.fetchval("""
                        SELECT COUNT(*) FROM plants
                        WHERE saved_date::date <= $1
                    """, current_date) or 0
                    
                    plants_per_user = round(total_plants_count / users_with_plants, 2) if users_with_plants > 0 else 0
                    
                    if active_users == 0:
                        data_points.append({
                            "date": current_date.isoformat(),
                            "label": current_date.strftime("%d.%m"),
                            "watered_per_user": 0,
                            "added_plants_per_user": 0,
                            "added_growing_per_user": 0,
                            "asked_question_per_user": 0,
                            "left_feedback_per_user": 0,
                            "plants_per_user": plants_per_user,
                            "active_users": 0
                        })
                        current_date += timedelta(days=1)
                        continue
                    
                    total_watered = await conn.fetchval("""
                        SELECT COUNT(*) FROM care_history ch
                        WHERE ch.action_type = 'watered' 
                        AND ch.action_date::date = $1
                    """, current_date) or 0
                    
                    total_added_plants = await conn.fetchval("""
                        SELECT COUNT(*) FROM plants WHERE saved_date::date = $1
                    """, current_date) or 0
                    
                    try:
                        total_added_growing = await conn.fetchval("""
                            SELECT COUNT(*) FROM growing_plants 
                            WHERE started_date::date = $1
                        """, current_date) or 0
                    except Exception:
                        total_added_growing = 0
                    
                    try:
                        total_asked_question = await conn.fetchval("""
                            SELECT COUNT(*) FROM plant_qa_history WHERE question_date::date = $1
                        """, current_date) or 0
                    except Exception:
                        total_asked_question = 0
                    
                    try:
                        total_left_feedback = await conn.fetchval("""
                            SELECT COUNT(*) FROM feedback WHERE created_at::date = $1
                        """, current_date) or 0
                    except Exception:
                        total_left_feedback = 0
                    
                    data_points.append({
                        "date": current_date.isoformat(),
                        "label": current_date.strftime("%d.%m"),
                        "watered_per_user": round(total_watered / active_users, 2),
                        "added_plants_per_user": round(total_added_plants / active_users, 2),
                        "added_growing_per_user": round(total_added_growing / active_users, 2),
                        "asked_question_per_user": round(total_asked_question / active_users, 2),
                        "left_feedback_per_user": round(total_left_feedback / active_users, 2),
                        "plants_per_user": plants_per_user,
                        "active_users": active_users
                    })
                    
                    current_date += timedelta(days=1)
            
            elif granularity == "week":
                current_date = from_date
                while current_date <= to_date:
                    week_end = min(current_date + timedelta(days=6), to_date)
                    
                    active_users = await conn.fetchval("""
                        SELECT COUNT(DISTINCT user_id) FROM users 
                        WHERE last_activity IS NOT NULL 
                        AND last_activity::date >= $1 AND last_activity::date <= $2
                    """, current_date, week_end) or 0
                    
                    users_with_plants = await conn.fetchval("""
                        SELECT COUNT(DISTINCT user_id) FROM plants
                        WHERE saved_date::date <= $1
                    """, week_end) or 0
                    
                    total_plants_count = await conn.fetchval("""
                        SELECT COUNT(*) FROM plants
                        WHERE saved_date::date <= $1
                    """, week_end) or 0
                    
                    plants_per_user = round(total_plants_count / users_with_plants, 2) if users_with_plants > 0 else 0
                    
                    if active_users == 0:
                        data_points.append({
                            "date": current_date.isoformat(),
                            "label": f"{current_date.strftime('%d.%m')}-{week_end.strftime('%d.%m')}",
                            "watered_per_user": 0,
                            "added_plants_per_user": 0,
                            "added_growing_per_user": 0,
                            "asked_question_per_user": 0,
                            "left_feedback_per_user": 0,
                            "plants_per_user": plants_per_user,
                            "active_users": 0
                        })
                        current_date += timedelta(days=7)
                        continue
                    
                    total_watered = await conn.fetchval("""
                        SELECT COUNT(*) FROM care_history ch
                        WHERE ch.action_type = 'watered' 
                        AND ch.action_date::date >= $1 AND ch.action_date::date <= $2
                    """, current_date, week_end) or 0
                    
                    total_added_plants = await conn.fetchval("""
                        SELECT COUNT(*) FROM plants 
                        WHERE saved_date::date >= $1 AND saved_date::date <= $2
                    """, current_date, week_end) or 0
                    
                    try:
                        total_added_growing = await conn.fetchval("""
                            SELECT COUNT(*) FROM growing_plants 
                            WHERE started_date::date >= $1 AND started_date::date <= $2
                        """, current_date, week_end) or 0
                    except Exception:
                        total_added_growing = 0
                    
                    try:
                        total_asked_question = await conn.fetchval("""
                            SELECT COUNT(*) FROM plant_qa_history 
                            WHERE question_date::date >= $1 AND question_date::date <= $2
                        """, current_date, week_end) or 0
                    except Exception:
                        total_asked_question = 0
                    
                    try:
                        total_left_feedback = await conn.fetchval("""
                            SELECT COUNT(*) FROM feedback 
                            WHERE created_at::date >= $1 AND created_at::date <= $2
                        """, current_date, week_end) or 0
                    except Exception:
                        total_left_feedback = 0
                    
                    data_points.append({
                        "date": current_date.isoformat(),
                        "label": f"{current_date.strftime('%d.%m')}-{week_end.strftime('%d.%m')}",
                        "watered_per_user": round(total_watered / active_users, 2),
                        "added_plants_per_user": round(total_added_plants / active_users, 2),
                        "added_growing_per_user": round(total_added_growing / active_users, 2),
                        "asked_question_per_user": round(total_asked_question / active_users, 2),
                        "left_feedback_per_user": round(total_left_feedback / active_users, 2),
                        "plants_per_user": plants_per_user,
                        "active_users": active_users
                    })
                    
                    current_date += timedelta(days=7)
            
            elif granularity == "month":
                current_date = from_date.replace(day=1)
                while current_date <= to_date:
                    month_end = (current_date + relativedelta(months=1) - timedelta(days=1))
                    if month_end > to_date:
                        month_end = to_date
                    
                    active_users = await conn.fetchval("""
                        SELECT COUNT(DISTINCT user_id) FROM users 
                        WHERE last_activity IS NOT NULL 
                        AND last_activity::date >= $1 AND last_activity::date <= $2
                    """, current_date, month_end) or 0
                    
                    users_with_plants = await conn.fetchval("""
                        SELECT COUNT(DISTINCT user_id) FROM plants
                        WHERE saved_date::date <= $1
                    """, month_end) or 0
                    
                    total_plants_count = await conn.fetchval("""
                        SELECT COUNT(*) FROM plants
                        WHERE saved_date::date <= $1
                    """, month_end) or 0
                    
                    plants_per_user = round(total_plants_count / users_with_plants, 2) if users_with_plants > 0 else 0
                    
                    if active_users == 0:
                        data_points.append({
                            "date": current_date.isoformat(),
                            "label": current_date.strftime("%b %Y"),
                            "watered_per_user": 0,
                            "added_plants_per_user": 0,
                            "added_growing_per_user": 0,
                            "asked_question_per_user": 0,
                            "left_feedback_per_user": 0,
                            "plants_per_user": plants_per_user,
                            "active_users": 0
                        })
                        current_date += relativedelta(months=1)
                        continue
                    
                    total_watered = await conn.fetchval("""
                        SELECT COUNT(*) FROM care_history ch
                        WHERE ch.action_type = 'watered' 
                        AND ch.action_date::date >= $1 AND ch.action_date::date <= $2
                    """, current_date, month_end) or 0
                    
                    total_added_plants = await conn.fetchval("""
                        SELECT COUNT(*) FROM plants 
                        WHERE saved_date::date >= $1 AND saved_date::date <= $2
                    """, current_date, month_end) or 0
                    
                    try:
                        total_added_growing = await conn.fetchval("""
                            SELECT COUNT(*) FROM growing_plants 
                            WHERE started_date::date >= $1 AND started_date::date <= $2
                        """, current_date, month_end) or 0
                    except Exception:
                        total_added_growing = 0
                    
                    try:
                        total_asked_question = await conn.fetchval("""
                            SELECT COUNT(*) FROM plant_qa_history 
                            WHERE question_date::date >= $1 AND question_date::date <= $2
                        """, current_date, month_end) or 0
                    except Exception:
                        total_asked_question = 0
                    
                    try:
                        total_left_feedback = await conn.fetchval("""
                            SELECT COUNT(*) FROM feedback 
                            WHERE created_at::date >= $1 AND created_at::date <= $2
                        """, current_date, month_end) or 0
                    except Exception:
                        total_left_feedback = 0
                    
                    data_points.append({
                        "date": current_date.isoformat(),
                        "label": current_date.strftime("%b %Y"),
                        "watered_per_user": round(total_watered / active_users, 2),
                        "added_plants_per_user": round(total_added_plants / active_users, 2),
                        "added_growing_per_user": round(total_added_growing / active_users, 2),
                        "asked_question_per_user": round(total_asked_question / active_users, 2),
                        "left_feedback_per_user": round(total_left_feedback / active_users, 2),
                        "plants_per_user": plants_per_user,
                        "active_users": active_users
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
        logger.error(f"Ошибка получения actions-per-user данных: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats/retention-flexible")
async def get_retention_flexible_stats(
    retention_type: str = Query("classic", regex="^(classic|functional|rolling)$"),
    granularity: str = Query("day", regex="^(day|week|month)$"),
    period: int = Query(7, ge=1, le=365)
):
    """Гибкий расчет retention метрик с выбором гранулярности"""
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database not connected")
    
    try:
        async with db_pool.acquire() as conn:
            cohorts = []
            
            if granularity == "day":
                for i in range(min(365, period * 5)):
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
                for i in range(min(52, period * 5)):
                    cohort_start = (datetime.now() - timedelta(weeks=i + period)).date()
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
        returned = await conn.fetchval("""
            SELECT COUNT(DISTINCT u.user_id) FROM users u
            WHERE u.created_at::date >= $1 AND u.created_at::date <= $2
            AND u.last_activity IS NOT NULL
            AND u.last_activity::date >= $3 AND u.last_activity::date <= $4
        """, cohort_start, cohort_end, target_start, target_end)
    
    elif retention_type == "functional":
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
    """Гибкая статистика с выбором периода и гранулярности"""
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
                current_date = from_date
                while current_date <= to_date:
                    new_users = await conn.fetchval("SELECT COUNT(*) FROM users WHERE created_at::date = $1", current_date)
                    watered = await conn.fetchval("""
                        SELECT COUNT(DISTINCT p.user_id) FROM care_history ch
                        JOIN plants p ON ch.plant_id = p.id
                        WHERE ch.action_type = 'watered' AND ch.action_date::date = $1
                    """, current_date)
                    added_plants = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM plants WHERE saved_date::date = $1", current_date)
                    try:
                        added_growing = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM growing_plants WHERE started_date::date = $1", current_date) or 0
                    except Exception:
                        added_growing = 0
                    try:
                        asked_question = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM plant_qa_history WHERE question_date::date = $1", current_date) or 0
                    except Exception:
                        asked_question = 0
                    try:
                        left_feedback = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM feedback WHERE created_at::date = $1", current_date) or 0
                    except Exception:
                        left_feedback = 0
                    opened_bot = await conn.fetchval("SELECT COUNT(*) FROM users WHERE last_activity IS NOT NULL AND last_activity::date = $1", current_date)
                    
                    data_points.append({
                        "date": current_date.isoformat(), "label": current_date.strftime("%d.%m"),
                        "new_users": new_users or 0, "watered": watered or 0,
                        "added_plants": added_plants or 0, "added_growing": added_growing,
                        "asked_question": asked_question, "left_feedback": left_feedback,
                        "opened_bot": opened_bot or 0
                    })
                    current_date += timedelta(days=1)
            
            elif granularity == "week":
                current_date = from_date
                while current_date <= to_date:
                    week_end = min(current_date + timedelta(days=6), to_date)
                    new_users = await conn.fetchval("SELECT COUNT(*) FROM users WHERE created_at::date >= $1 AND created_at::date <= $2", current_date, week_end)
                    watered = await conn.fetchval("""
                        SELECT COUNT(DISTINCT p.user_id) FROM care_history ch
                        JOIN plants p ON ch.plant_id = p.id
                        WHERE ch.action_type = 'watered' AND ch.action_date::date >= $1 AND ch.action_date::date <= $2
                    """, current_date, week_end)
                    added_plants = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM plants WHERE saved_date::date >= $1 AND saved_date::date <= $2", current_date, week_end)
                    try:
                        added_growing = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM growing_plants WHERE started_date::date >= $1 AND started_date::date <= $2", current_date, week_end) or 0
                    except Exception:
                        added_growing = 0
                    try:
                        asked_question = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM plant_qa_history WHERE question_date::date >= $1 AND question_date::date <= $2", current_date, week_end) or 0
                    except Exception:
                        asked_question = 0
                    try:
                        left_feedback = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM feedback WHERE created_at::date >= $1 AND created_at::date <= $2", current_date, week_end) or 0
                    except Exception:
                        left_feedback = 0
                    opened_bot = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM users WHERE last_activity IS NOT NULL AND last_activity::date >= $1 AND last_activity::date <= $2", current_date, week_end)
                    
                    data_points.append({
                        "date": current_date.isoformat(), "label": f"{current_date.strftime('%d.%m')}-{week_end.strftime('%d.%m')}",
                        "new_users": new_users or 0, "watered": watered or 0,
                        "added_plants": added_plants or 0, "added_growing": added_growing,
                        "asked_question": asked_question, "left_feedback": left_feedback,
                        "opened_bot": opened_bot or 0
                    })
                    current_date += timedelta(days=7)
            
            elif granularity == "month":
                current_date = from_date.replace(day=1)
                while current_date <= to_date:
                    month_end = (current_date + relativedelta(months=1) - timedelta(days=1))
                    if month_end > to_date:
                        month_end = to_date
                    new_users = await conn.fetchval("SELECT COUNT(*) FROM users WHERE created_at::date >= $1 AND created_at::date <= $2", current_date, month_end)
                    watered = await conn.fetchval("""
                        SELECT COUNT(DISTINCT p.user_id) FROM care_history ch
                        JOIN plants p ON ch.plant_id = p.id
                        WHERE ch.action_type = 'watered' AND ch.action_date::date >= $1 AND ch.action_date::date <= $2
                    """, current_date, month_end)
                    added_plants = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM plants WHERE saved_date::date >= $1 AND saved_date::date <= $2", current_date, month_end)
                    try:
                        added_growing = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM growing_plants WHERE started_date::date >= $1 AND started_date::date <= $2", current_date, month_end) or 0
                    except Exception:
                        added_growing = 0
                    try:
                        asked_question = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM plant_qa_history WHERE question_date::date >= $1 AND question_date::date <= $2", current_date, month_end) or 0
                    except Exception:
                        asked_question = 0
                    try:
                        left_feedback = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM feedback WHERE created_at::date >= $1 AND created_at::date <= $2", current_date, month_end) or 0
                    except Exception:
                        left_feedback = 0
                    opened_bot = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM users WHERE last_activity IS NOT NULL AND last_activity::date >= $1 AND last_activity::date <= $2", current_date, month_end)
                    
                    data_points.append({
                        "date": current_date.isoformat(), "label": current_date.strftime("%b %Y"),
                        "new_users": new_users or 0, "watered": watered or 0,
                        "added_plants": added_plants or 0, "added_growing": added_growing,
                        "asked_question": asked_question, "left_feedback": left_feedback,
                        "opened_bot": opened_bot or 0
                    })
                    current_date += relativedelta(months=1)
            
            return {"granularity": granularity, "date_from": date_from, "date_to": date_to, "data": data_points}
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except Exception as e:
        logger.error(f"Ошибка получения timeseries данных: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats/funnel")
async def get_funnel_stats(
    granularity: str = Query("day", regex="^(day|week|month)$"),
    date_from: str = Query(...),
    date_to: str = Query(...)
):
    """Воронка пользователей"""
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
                current_date = from_date
                while current_date <= to_date:
                    funnel_data = await calculate_funnel_for_period(conn, current_date, current_date)
                    funnel_data["date"] = current_date.isoformat()
                    funnel_data["label"] = current_date.strftime("%d.%m")
                    data_points.append(funnel_data)
                    current_date += timedelta(days=1)
            
            elif granularity == "week":
                current_date = from_date
                while current_date <= to_date:
                    week_end = min(current_date + timedelta(days=6), to_date)
                    funnel_data = await calculate_funnel_for_period(conn, current_date, week_end)
                    funnel_data["date"] = current_date.isoformat()
                    funnel_data["label"] = f"{current_date.strftime('%d.%m')}-{week_end.strftime('%d.%m')}"
                    data_points.append(funnel_data)
                    current_date += timedelta(days=7)
            
            elif granularity == "month":
                current_date = from_date.replace(day=1)
                while current_date <= to_date:
                    month_end = (current_date + relativedelta(months=1) - timedelta(days=1))
                    if month_end > to_date:
                        month_end = to_date
                    funnel_data = await calculate_funnel_for_period(conn, current_date, month_end)
                    funnel_data["date"] = current_date.isoformat()
                    funnel_data["label"] = current_date.strftime("%b %Y")
                    data_points.append(funnel_data)
                    current_date += relativedelta(months=1)
            
            return {"granularity": granularity, "date_from": date_from, "date_to": date_to, "data": data_points}
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except Exception as e:
        logger.error(f"Ошибка получения funnel данных: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def calculate_funnel_for_period(conn, period_start, period_end):
    """Расчет ПОСЛЕДОВАТЕЛЬНОЙ воронки для заданного периода"""
    
    opened_bot_users = await conn.fetch("""
        SELECT DISTINCT user_id FROM users 
        WHERE last_activity IS NOT NULL 
        AND last_activity::date >= $1 AND last_activity::date <= $2
    """, period_start, period_end)
    opened_bot_ids = set(row['user_id'] for row in opened_bot_users)
    opened_bot = len(opened_bot_ids)
    
    if opened_bot > 0:
        added_plant_users = await conn.fetch("""
            SELECT DISTINCT user_id FROM plants WHERE user_id = ANY($1::bigint[])
        """, list(opened_bot_ids))
        added_plant_ids = set(row['user_id'] for row in added_plant_users)
        added_plant = len(added_plant_ids)
    else:
        added_plant_ids = set()
        added_plant = 0
    
    if added_plant > 0:
        watered_users = await conn.fetch("""
            SELECT DISTINCT p.user_id FROM care_history ch
            JOIN plants p ON ch.plant_id = p.id
            WHERE p.user_id = ANY($1::bigint[]) AND ch.action_type = 'watered'
        """, list(added_plant_ids))
        watered_ids = set(row['user_id'] for row in watered_users)
        watered = len(watered_ids)
    else:
        watered_ids = set()
        watered = 0
    
    if watered > 0:
        try:
            asked_question_users = await conn.fetch("""
                SELECT DISTINCT user_id FROM plant_qa_history WHERE user_id = ANY($1::bigint[])
            """, list(watered_ids))
            asked_question_ids = set(row['user_id'] for row in asked_question_users)
            asked_question = len(asked_question_ids)
        except Exception:
            asked_question_ids = set()
            asked_question = 0
    else:
        asked_question_ids = set()
        asked_question = 0
    
    registered_users = await conn.fetch("""
        SELECT DISTINCT user_id FROM users 
        WHERE created_at::date >= $1 AND created_at::date <= $2
        AND last_activity IS NOT NULL
        AND last_activity::date >= created_at::date 
        AND last_activity::date <= created_at::date + 14
    """, period_start, period_end)
    active_14days = len(registered_users)
    
    opened_bot_percent = 100.0
    added_plant_percent = round((added_plant / opened_bot * 100), 1) if opened_bot > 0 else 0
    watered_percent = round((watered / opened_bot * 100), 1) if opened_bot > 0 else 0
    asked_question_percent = round((asked_question / opened_bot * 100), 1) if opened_bot > 0 else 0
    
    registered_in_period = await conn.fetchval("""
        SELECT COUNT(*) FROM users WHERE created_at::date >= $1 AND created_at::date <= $2
    """, period_start, period_end) or 1
    active_14days_percent = round((active_14days / registered_in_period * 100), 1) if registered_in_period > 0 else 0
    
    return {
        "opened_bot": {"count": opened_bot, "percent": opened_bot_percent},
        "added_plant": {"count": added_plant, "percent": added_plant_percent},
        "watered": {"count": watered, "percent": watered_percent},
        "asked_question": {"count": asked_question, "percent": asked_question_percent},
        "active_14days": {"count": active_14days, "percent": active_14days_percent}
    }


# =============================================
# НОВЫЕ ЭНДПОИНТЫ: ОПЛАТЫ И UTM
# =============================================

@app.get("/api/stats/payments")
async def get_payment_stats():
    """Общая статистика по оплатам"""
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database not connected")
    
    try:
        async with db_pool.acquire() as conn:
            today = datetime.now().date()
            week_ago = today - timedelta(days=7)
            month_ago = today - timedelta(days=30)
            
            # Всего успешных оплат
            total_payments = await conn.fetchval(
                "SELECT COUNT(*) FROM payments WHERE status = 'succeeded'"
            ) or 0
            
            # Общая выручка
            total_revenue = await conn.fetchval(
                "SELECT COALESCE(SUM(amount), 0) FROM payments WHERE status = 'succeeded'"
            ) or 0
            
            # Выручка за сегодня
            revenue_today = await conn.fetchval("""
                SELECT COALESCE(SUM(amount), 0) FROM payments 
                WHERE status = 'succeeded' AND created_at::date = $1
            """, today) or 0
            
            payments_today = await conn.fetchval("""
                SELECT COUNT(*) FROM payments 
                WHERE status = 'succeeded' AND created_at::date = $1
            """, today) or 0
            
            # Выручка за неделю
            revenue_week = await conn.fetchval("""
                SELECT COALESCE(SUM(amount), 0) FROM payments 
                WHERE status = 'succeeded' AND created_at::date >= $1
            """, week_ago) or 0
            
            payments_week = await conn.fetchval("""
                SELECT COUNT(*) FROM payments 
                WHERE status = 'succeeded' AND created_at::date >= $1
            """, week_ago) or 0
            
            # Выручка за месяц
            revenue_month = await conn.fetchval("""
                SELECT COALESCE(SUM(amount), 0) FROM payments 
                WHERE status = 'succeeded' AND created_at::date >= $1
            """, month_ago) or 0
            
            payments_month = await conn.fetchval("""
                SELECT COUNT(*) FROM payments 
                WHERE status = 'succeeded' AND created_at::date >= $1
            """, month_ago) or 0
            
            # Средний чек
            avg_check = await conn.fetchval(
                "SELECT COALESCE(AVG(amount), 0) FROM payments WHERE status = 'succeeded'"
            ) or 0
            
            # Активные подписки
            active_subs = await conn.fetchval(
                "SELECT COUNT(*) FROM subscriptions WHERE plan = 'pro' AND (expires_at IS NULL OR expires_at > NOW())"
            ) or 0
            
            # Подписки с автопродлением
            auto_pay_subs = await conn.fetchval(
                "SELECT COUNT(*) FROM subscriptions WHERE plan = 'pro' AND auto_pay_method_id IS NOT NULL AND (expires_at IS NULL OR expires_at > NOW())"
            ) or 0
            
            # Уникальные платящие пользователи
            unique_payers = await conn.fetchval(
                "SELECT COUNT(DISTINCT user_id) FROM payments WHERE status = 'succeeded'"
            ) or 0
            
            # Рекуррентные платежи
            recurring_payments = await conn.fetchval(
                "SELECT COUNT(*) FROM payments WHERE status = 'succeeded' AND is_recurring = TRUE"
            ) or 0
            
            # Конверсия в оплату (всего пользователей vs платящих)
            total_users = await conn.fetchval("SELECT COUNT(*) FROM users") or 1
            conversion_rate = round((unique_payers / total_users * 100), 2)
            
            return {
                "total_payments": total_payments,
                "total_revenue": total_revenue,
                "today": {
                    "revenue": revenue_today,
                    "payments": payments_today
                },
                "week": {
                    "revenue": revenue_week,
                    "payments": payments_week
                },
                "month": {
                    "revenue": revenue_month,
                    "payments": payments_month
                },
                "avg_check": round(avg_check),
                "active_subscriptions": active_subs,
                "auto_pay_subscriptions": auto_pay_subs,
                "unique_payers": unique_payers,
                "recurring_payments": recurring_payments,
                "conversion_rate": conversion_rate,
                "total_users": total_users
            }
    except Exception as e:
        logger.error(f"Ошибка получения статистики оплат: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats/payments/timeseries")
async def get_payment_timeseries(
    granularity: str = Query("day", regex="^(day|week|month)$"),
    date_from: str = Query(...),
    date_to: str = Query(...)
):
    """Выручка и оплаты по времени"""
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
                current_date = from_date
                while current_date <= to_date:
                    revenue = await conn.fetchval("""
                        SELECT COALESCE(SUM(amount), 0) FROM payments 
                        WHERE status = 'succeeded' AND created_at::date = $1
                    """, current_date) or 0
                    
                    count = await conn.fetchval("""
                        SELECT COUNT(*) FROM payments 
                        WHERE status = 'succeeded' AND created_at::date = $1
                    """, current_date) or 0
                    
                    new_subs = await conn.fetchval("""
                        SELECT COUNT(*) FROM payments 
                        WHERE status = 'succeeded' AND is_recurring = FALSE AND created_at::date = $1
                    """, current_date) or 0
                    
                    renewals = await conn.fetchval("""
                        SELECT COUNT(*) FROM payments 
                        WHERE status = 'succeeded' AND is_recurring = TRUE AND created_at::date = $1
                    """, current_date) or 0
                    
                    data_points.append({
                        "date": current_date.isoformat(),
                        "label": current_date.strftime("%d.%m"),
                        "revenue": revenue,
                        "payments": count,
                        "new_subscriptions": new_subs,
                        "renewals": renewals
                    })
                    current_date += timedelta(days=1)
            
            elif granularity == "week":
                current_date = from_date
                while current_date <= to_date:
                    week_end = min(current_date + timedelta(days=6), to_date)
                    
                    revenue = await conn.fetchval("""
                        SELECT COALESCE(SUM(amount), 0) FROM payments 
                        WHERE status = 'succeeded' AND created_at::date >= $1 AND created_at::date <= $2
                    """, current_date, week_end) or 0
                    
                    count = await conn.fetchval("""
                        SELECT COUNT(*) FROM payments 
                        WHERE status = 'succeeded' AND created_at::date >= $1 AND created_at::date <= $2
                    """, current_date, week_end) or 0
                    
                    new_subs = await conn.fetchval("""
                        SELECT COUNT(*) FROM payments 
                        WHERE status = 'succeeded' AND is_recurring = FALSE AND created_at::date >= $1 AND created_at::date <= $2
                    """, current_date, week_end) or 0
                    
                    renewals = await conn.fetchval("""
                        SELECT COUNT(*) FROM payments 
                        WHERE status = 'succeeded' AND is_recurring = TRUE AND created_at::date >= $1 AND created_at::date <= $2
                    """, current_date, week_end) or 0
                    
                    data_points.append({
                        "date": current_date.isoformat(),
                        "label": f"{current_date.strftime('%d.%m')}-{week_end.strftime('%d.%m')}",
                        "revenue": revenue,
                        "payments": count,
                        "new_subscriptions": new_subs,
                        "renewals": renewals
                    })
                    current_date += timedelta(days=7)
            
            elif granularity == "month":
                current_date = from_date.replace(day=1)
                while current_date <= to_date:
                    month_end = (current_date + relativedelta(months=1) - timedelta(days=1))
                    if month_end > to_date:
                        month_end = to_date
                    
                    revenue = await conn.fetchval("""
                        SELECT COALESCE(SUM(amount), 0) FROM payments 
                        WHERE status = 'succeeded' AND created_at::date >= $1 AND created_at::date <= $2
                    """, current_date, month_end) or 0
                    
                    count = await conn.fetchval("""
                        SELECT COUNT(*) FROM payments 
                        WHERE status = 'succeeded' AND created_at::date >= $1 AND created_at::date <= $2
                    """, current_date, month_end) or 0
                    
                    new_subs = await conn.fetchval("""
                        SELECT COUNT(*) FROM payments 
                        WHERE status = 'succeeded' AND is_recurring = FALSE AND created_at::date >= $1 AND created_at::date <= $2
                    """, current_date, month_end) or 0
                    
                    renewals = await conn.fetchval("""
                        SELECT COUNT(*) FROM payments 
                        WHERE status = 'succeeded' AND is_recurring = TRUE AND created_at::date >= $1 AND created_at::date <= $2
                    """, current_date, month_end) or 0
                    
                    data_points.append({
                        "date": current_date.isoformat(),
                        "label": current_date.strftime("%b %Y"),
                        "revenue": revenue,
                        "payments": count,
                        "new_subscriptions": new_subs,
                        "renewals": renewals
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
        logger.error(f"Ошибка получения payment timeseries: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats/utm")
async def get_utm_stats():
    """Статистика по UTM-источникам с воронкой"""
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database not connected")
    
    try:
        async with db_pool.acquire() as conn:
            # Все источники с количеством регистраций
            sources = await conn.fetch("""
                SELECT 
                    COALESCE(u.utm_source, '(organic)') as source,
                    COUNT(*) as registered,
                    COUNT(CASE WHEN u.last_activity IS NOT NULL 
                          AND u.last_activity::date > u.created_at::date THEN 1 END) as returned,
                    MIN(u.created_at) as first_user,
                    MAX(u.created_at) as last_user
                FROM users u
                GROUP BY COALESCE(u.utm_source, '(organic)')
                ORDER BY registered DESC
            """)
            
            result = []
            
            for src in sources:
                source_name = src['source']
                registered = src['registered']
                
                if source_name == '(organic)':
                    # Для органики — пользователи без utm
                    user_filter = "u.utm_source IS NULL"
                else:
                    user_filter = f"u.utm_source = '{source_name}'"
                
                # Добавили растение
                added_plant = await conn.fetchval(f"""
                    SELECT COUNT(DISTINCT p.user_id) FROM plants p
                    JOIN users u ON p.user_id = u.user_id
                    WHERE {user_filter}
                """) or 0
                
                # Полили
                watered = await conn.fetchval(f"""
                    SELECT COUNT(DISTINCT p2.user_id) 
                    FROM care_history ch
                    JOIN plants p2 ON ch.plant_id = p2.id
                    JOIN users u ON p2.user_id = u.user_id
                    WHERE ch.action_type = 'watered' AND {user_filter}
                """) or 0
                
                # Оплатили
                paid = await conn.fetchval(f"""
                    SELECT COUNT(DISTINCT pay.user_id) FROM payments pay
                    JOIN users u ON pay.user_id = u.user_id
                    WHERE pay.status = 'succeeded' AND {user_filter}
                """) or 0
                
                # Выручка
                revenue = await conn.fetchval(f"""
                    SELECT COALESCE(SUM(pay.amount), 0) FROM payments pay
                    JOIN users u ON pay.user_id = u.user_id
                    WHERE pay.status = 'succeeded' AND {user_filter}
                """) or 0
                
                result.append({
                    "source": source_name,
                    "registered": registered,
                    "returned": src['returned'] or 0,
                    "added_plant": added_plant,
                    "watered": watered,
                    "paid": paid,
                    "revenue": revenue,
                    "conversion_to_plant": round(added_plant / registered * 100, 1) if registered > 0 else 0,
                    "conversion_to_payment": round(paid / registered * 100, 1) if registered > 0 else 0,
                    "arpu": round(revenue / registered) if registered > 0 else 0,
                    "arppu": round(revenue / paid) if paid > 0 else 0,
                    "first_user": src['first_user'].isoformat() if src['first_user'] else None,
                    "last_user": src['last_user'].isoformat() if src['last_user'] else None,
                })
            
            return {"sources": result}
    
    except Exception as e:
        logger.error(f"Ошибка получения UTM статистики: {e}")
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

if __name__ == "__main__":
    import uvicorn
    logger.info("=" * 70)
    logger.info("🌱 BLOOM AI DASHBOARD")
    logger.info("=" * 70)
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")
