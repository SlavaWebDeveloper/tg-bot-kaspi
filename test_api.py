"""
Тестовый скрипт для проверки подключения к Kaspi API
Запустите этот скрипт чтобы убедиться, что API токен работает
"""
import asyncio
import logging
from src.config import Config
from src.kaspi.api_client import KaspiAPIClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_api():
    """Тест подключения к Kaspi API"""
    try:
        # Загружаем конфигурацию
        Config.validate()
        logger.info("✓ Конфигурация загружена")
        
        # Создаем клиент
        client = KaspiAPIClient(
            api_token=Config.KASPI_API_TOKEN,
            base_url=Config.KASPI_API_URL
        )
        logger.info("✓ API клиент создан")
        
        # Пробуем получить список заказов
        logger.info("Запрашиваем список заказов...")
        response = await client.get_orders(
            status=['APPROVED_BY_BANK', 'ACCEPTED_BY_MERCHANT'],
            page_size=5
        )
        
        orders = response.get('data', [])
        total = response.get('meta', {}).get('totalCount', 0)
        
        logger.info(f"✓ Успешно получены заказы!")
        logger.info(f"Всего заказов: {total}")
        logger.info(f"Получено в ответе: {len(orders)}")
        
        if orders:
            logger.info("\nПример первого заказа:")
            first_order = orders[0]['attributes']
            logger.info(f"  Код: {first_order.get('code')}")
            logger.info(f"  Статус: {first_order.get('status')}")
            logger.info(f"  Состояние: {first_order.get('state')}")
            logger.info(f"  Сумма: {first_order.get('totalPrice')} ₸")
        else:
            logger.info("Заказов не найдено (это нормально, если их нет)")
        
        return True
        
    except ValueError as e:
        logger.error(f"❌ Ошибка конфигурации: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка при тестировании API: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    print("="*60)
    print("Тест подключения к Kaspi API")
    print("="*60)
    
    success = asyncio.run(test_api())
    
    print("="*60)
    if success:
        print("✅ Тест успешно пройден!")
        print("Вы можете запускать основной бот: python main.py")
    else:
        print("❌ Тест не пройден. Проверьте настройки в .env файле")
    print("="*60)
