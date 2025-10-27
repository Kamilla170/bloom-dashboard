import asyncio
import asyncpg
import os
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Bloom AI Dashboard")

# Database URL из переменной окружения
def get_database_url():
    """Получить корректный DATABASE_URL с агрессивной очисткой"""
    import re
    
    # Пробуем разные варианты переменных Railway
    raw_url = os.getenv("DATABASE_URL") or os.getenv("DATABASE_PRIVATE_URL")
    
    logger.info(f"📋 Сырой DATABASE_URL найден: {bool(raw_url)}")
    
    if not raw_url:
        # Пробуем собрать из отдельных переменных
        pg_host = os.getenv("PGHOST")
        pg_port = os.getenv("PGPORT", "5432")
        pg_user = os.getenv("PGUSER", "postgres")
        pg_password = os.getenv("PGPASSWORD")
        pg_database = os.getenv("PGDATABASE", "railway")
        
        if pg_host and pg_password:
            database_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}"
            logger.info("✅ DATABASE_URL собран из отдельных переменных")
            return database_url
        else:
            logger.error("❌ Не найдены переменные для подключения к БД")
            return None
    
    # Агрессивная очистка URL
    cleaned_url = raw_url
    
    # Логируем исходный формат (первые 30 символов)
    logger.info(f"🔍 Исходный формат: {cleaned_url[:50]}...")
    
    # Удаляем все варианты неправильных префиксов
    prefixes_to_remove = [
        "railwaypostgresql://",
        "railway://",
        "railway",
    ]
    
    for prefix in prefixes_to_remove:
        if cleaned_url.startswith(prefix):
            cleaned_url = cleaned_url[len(prefix):]
            logger.info(f"🧹 Удалён префикс: {prefix}")
    
    # Убираем лишние слеши в начале
    cleaned_url = cleaned_url.lstrip('/')
    
    # Если URL не начинается с postgresql://, добавляем
    if not cleaned_url.startswith("postgresql://"):
        cleaned_url = "postgresql://" + cleaned_url
        logger.info("➕ Добавлен префикс postgresql://")
    
    # Проверяем формат итогового URL
    # Должен быть: postgresql://user:password@host:port/database
    url_pattern = r'^postgresql://[^:]+:[^@]+@[^:]+:\d+/\w+$'
    
    if re.match(url_pattern, cleaned_url):
        # Логируем безопасную версию (скрываем пароль)
        safe_parts = cleaned_url.split('@')
        if len(safe_parts) == 2:
            user_part = safe_parts[0].split('://')[1].split(':')[0]
            host_part = safe_parts[1]
            logger.info(f"✅ URL очищен: postgresql://{user_part}:***@{host_part}")
    else:
        logger.warning(f"⚠️ URL может быть некорректным: {cleaned_url[:30]}...")
    
    return cleaned_url

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
    html_path = Path("/home/claude/static/index.html")
    if html_path.exists():
        return FileResponse(html_path)
    return HTMLResponse("<h1>Dashboard</h1><p>Loading...</p>")

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
                WHERE DATE(created_at) = $1
            """, today)
            
            # Пользователи которые полили сегодня
            watered_today = await conn.fetchval("""
                SELECT COUNT(DISTINCT user_id) FROM care_history 
                WHERE action_type = 'watered' 
                AND DATE(action_date) = $1
            """, today)
            
            # Пользователи которые добавили растение сегодня
            added_plants_today = await conn.fetchval("""
                SELECT COUNT(DISTINCT user_id) FROM plants 
                WHERE DATE(saved_date) = $1
            """, today)
            
            # Активные пользователи сегодня (last_activity)
            active_today = await conn.fetchval("""
                SELECT COUNT(*) FROM users 
                WHERE DATE(last_activity) = $1
            """, today)
            
            # Неактивные пользователи сегодня
            inactive_today = total_users - active_today if active_today else total_users
            
            # Проценты
            watered_percent = round((watered_today / total_users * 100), 1) if total_users > 0 else 0
            added_plants_percent = round((added_plants_today / total_users * 100), 1) if total_users > 0 else 0
            active_percent = round((active_today / total_users * 100), 1) if total_users > 0 else 0
            inactive_percent = round((inactive_today / total_users * 100), 1) if total_users > 0 else 0
            
            return {
                "date": today.isoformat(),
                "total_users": total_users,
                "new_users": new_users_today,
                "watered": {
                    "count": watered_today,
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
                WHERE DATE(created_at) <= $1
            """, yesterday)
            
            # Новые пользователи вчера
            new_users = await conn.fetchval("""
                SELECT COUNT(*) FROM users 
                WHERE DATE(created_at) = $1
            """, yesterday)
            
            # Пользователи которые полили вчера
            watered = await conn.fetchval("""
                SELECT COUNT(DISTINCT user_id) FROM care_history 
                WHERE action_type = 'watered' 
                AND DATE(action_date) = $1
            """, yesterday)
            
            # Пользователи которые добавили растение вчера
            added_plants = await conn.fetchval("""
                SELECT COUNT(DISTINCT user_id) FROM plants 
                WHERE DATE(saved_date) = $1
            """, yesterday)
            
            # Активные пользователи вчера
            active = await conn.fetchval("""
                SELECT COUNT(*) FROM users 
                WHERE DATE(last_activity) = $1
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
                    SELECT COUNT(*) FROM users WHERE DATE(created_at) = $1
                """, day)
                
                # Поливы
                watered = await conn.fetchval("""
                    SELECT COUNT(DISTINCT user_id) FROM care_history 
                    WHERE action_type = 'watered' AND DATE(action_date) = $1
                """, day)
                
                # Добавленные растения
                added_plants = await conn.fetchval("""
                    SELECT COUNT(DISTINCT user_id) FROM plants WHERE DATE(saved_date) = $1
                """, day)
                
                # Активные
                active = await conn.fetchval("""
                    SELECT COUNT(*) FROM users WHERE DATE(last_activity) = $1
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
                    SELECT COUNT(*) FROM users WHERE DATE(created_at) = $1
                """, day)
                
                # Поливы
                watered = await conn.fetchval("""
                    SELECT COUNT(DISTINCT user_id) FROM care_history 
                    WHERE action_type = 'watered' AND DATE(action_date) = $1
                """, day)
                
                # Добавленные растения
                added_plants = await conn.fetchval("""
                    SELECT COUNT(DISTINCT user_id) FROM plants WHERE DATE(saved_date) = $1
                """, day)
                
                # Активные
                active = await conn.fetchval("""
                    SELECT COUNT(*) FROM users WHERE DATE(last_activity) = $1
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

if __name__ == "__main__":
    import uvicorn
    logger.info("=" * 70)
    logger.info("🌱 BLOOM AI DASHBOARD")
    logger.info("=" * 70)
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")
