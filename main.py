"""
Главный файл приложения - точка входа
"""
import logging
import colorlog
from src.config import Config
from src.kaspi.api_client import KaspiAPIClient
from src.kaspi.order_service import OrderService
from src.database.models import Database
from src.bot.telegram_bot import TelegramBot


def setup_logging():
    """Настройка логирования с цветным выводом"""
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        }
    ))
    
    logging.basicConfig(
        level=logging.INFO,
        handlers=[handler]
    )
    
    # Уменьшаем уровень логирования для сторонних библиотек
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('telegram').setLevel(logging.WARNING)
    logging.getLogger('apscheduler').setLevel(logging.WARNING)


def main():
    """Главная функция приложения"""
    # Настраиваем логирование
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Валидируем конфигурацию
        Config.validate()
        logger.info("✓ Конфигурация загружена успешно")
        
        # Инициализируем базу данных
        database = Database(Config.DATABASE_URL)
        logger.info("✓ База данных инициализирована")
        
        # Инициализируем Kaspi API клиент
        kaspi_client = KaspiAPIClient(
            api_token=Config.KASPI_API_TOKEN,
            base_url=Config.KASPI_API_URL
        )
        logger.info("✓ Kaspi API клиент создан")
        
        # Инициализируем сервис заказов
        order_service = OrderService(kaspi_client, database)
        logger.info("✓ Сервис заказов создан")
        
        # Инициализируем Telegram бота
        bot = TelegramBot(
            token=Config.TELEGRAM_BOT_TOKEN,
            chat_id=Config.TELEGRAM_CHAT_ID,
            order_service=order_service
        )
        bot.setup()
        logger.info("✓ Telegram бот настроен")
        
        # Добавляем задачу периодической проверки заказов
        bot.add_job_check_orders(Config.POLL_INTERVAL_MINUTES)
        logger.info(f"✓ Настроена проверка заказов каждые {Config.POLL_INTERVAL_MINUTES} минут")
        
        # Запускаем бота
        logger.info("="*60)
        logger.info("🚀 Бот запущен и готов к работе!")
        logger.info("="*60)
        bot.run()
        
    except ValueError as e:
        logger.error(f"❌ Ошибка конфигурации: {e}")
        logger.error("Проверьте файл .env и убедитесь, что все переменные заполнены")
        return 1
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit(main())
