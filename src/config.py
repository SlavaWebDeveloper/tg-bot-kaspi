"""
Конфигурация приложения
"""
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()


class Config:
    """Основные настройки приложения"""
    
    # Telegram
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
    TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
    
    # Kaspi API
    KASPI_API_TOKEN = os.getenv('KASPI_API_TOKEN')
    KASPI_API_URL = os.getenv('KASPI_API_URL', 'https://kaspi.kz/shop/api/v2')
    
    # Настройки опроса
    POLL_INTERVAL_MINUTES = int(os.getenv('POLL_INTERVAL_MINUTES', '10'))
    
    # Период запроса заказов (максимум 14 дней для Kaspi API)
    ORDERS_DAYS_BACK = int(os.getenv('ORDERS_DAYS_BACK', '14'))
    
    # База данных
    DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///kaspi_orders.db')
    
    @classmethod
    def validate(cls):
        """Проверка наличия обязательных переменных"""
        required = {
            'TELEGRAM_BOT_TOKEN': cls.TELEGRAM_BOT_TOKEN,
            'KASPI_API_TOKEN': cls.KASPI_API_TOKEN,
            'TELEGRAM_CHAT_ID': cls.TELEGRAM_CHAT_ID,
        }
        
        missing = [key for key, value in required.items() if not value]
        
        if missing:
            raise ValueError(
                f"Отсутствуют обязательные переменные окружения: {', '.join(missing)}\n"
                f"Скопируйте .env.example в .env и заполните необходимые значения"
            )
