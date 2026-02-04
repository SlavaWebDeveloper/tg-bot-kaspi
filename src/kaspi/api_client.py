"""
Клиент для работы с Kaspi API
"""
import httpx
import logging
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class KaspiAPIClient:
    """Клиент для взаимодействия с Kaspi Merchant API"""
    
    def __init__(self, api_token: str, base_url: str):
        self.api_token = api_token
        self.base_url = base_url

        self.headers = {
            'Content-Type': 'application/vnd.api+json',
            'Accept': 'application/vnd.api+json',
            'X-Auth-Token': api_token,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8'
        }
    
    async def get_orders(
        self,
        status: Optional[List[str]] = None,
        state: Optional[List[str]] = None,
        page_number: int = 0,
        page_size: int = 100,
        creation_date_from: Optional[int] = None,
        creation_date_to: Optional[int] = None
    ) -> Dict:
        """
        Получить список заказов
        
        Args:
            status: Статусы заказов (APPROVED_BY_BANK, ACCEPTED_BY_MERCHANT и т.д.)
            state: Состояния заказов (NEW, PICKUP, DELIVERY, KASPI_DELIVERY)
            page_number: Номер страницы
            page_size: Количество заказов на странице (макс 100)
            creation_date_from: Начальная дата создания заказа в миллисекундах (обязательный параметр!)
            creation_date_to: Конечная дата создания заказа в миллисекундах
        
        Returns:
            Словарь с данными о заказах
        """
        from datetime import datetime, timedelta
        
        # Если дата не указана, берем заказы за последние 14 дней (максимум для Kaspi API)
        if creation_date_from is None:
            days_ago = datetime.now() - timedelta(days=14)
            creation_date_from = int(days_ago.timestamp() * 1000)
        
        # Верхняя граница - текущее время
        if creation_date_to is None:
            creation_date_to = int(datetime.now().timestamp() * 1000)
        
        params = {
            'page[number]': page_number,
            'page[size]': min(page_size, 100),
            'filter[orders][creationDate][$ge]': creation_date_from,
            'filter[orders][creationDate][$le]': creation_date_to
        }
        
        if status:
            params['filter[orders][status]'] = ','.join(status)
        
        if state:
            params['filter[orders][state]'] = ','.join(state)
        
        url = f"{self.base_url}/orders"
        
        logger.info(f"Запрос к API: {url}")
        logger.info(f"Параметры: {params}")
        logger.info(f"Период: последние 14 дней")
        
        try:
            async with httpx.AsyncClient(timeout=60.0, verify=True) as client:
                response = await client.get(
                    url,
                    headers=self.headers,
                    params=params
                )
                
                logger.info(f"Статус ответа: {response.status_code}")
                
                response.raise_for_status()
                
                data = response.json()
                logger.info(f"Получено заказов: {len(data.get('data', []))}")
                logger.info(f"Всего заказов (meta): {data.get('meta', {}).get('totalCount', 'N/A')}")
                
                return data
                
        except httpx.HTTPStatusError as e:
            logger.error(f"❌ Ошибка HTTP при получении заказов: {e.response.status_code}")
            logger.error(f"URL: {e.request.url}")
            logger.error(f"Заголовки запроса: {dict(e.request.headers)}")
            logger.error(f"Тело ответа: {e.response.text[:500]}")  # Первые 500 символов
            raise
        except httpx.TimeoutException as e:
            logger.error(f"❌ Таймаут при запросе к API: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка при получении заказов: {type(e).__name__}: {e}")
            raise
    
    async def get_order_by_code(self, order_code: str) -> Dict:
        """
        Получить информацию о заказе по его коду
        
        Args:
            order_code: Код заказа
        
        Returns:
            Словарь с данными о заказе
        """
        params = {
            'filter[orders][code]': order_code
        }
        
        try:
            async with httpx.AsyncClient(timeout=60.0, verify=True) as client:
                response = await client.get(
                    f"{self.base_url}/orders",
                    headers=self.headers,
                    params=params
                )
                response.raise_for_status()
                return response.json()
        except Exception as e:
            logger.error(f"Ошибка при получении заказа {order_code}: {e}")
            raise
    
    async def get_order_by_id(self, order_id: str) -> Dict:
        """
        Получить информацию о заказе по его ID
        
        Args:
            order_id: ID заказа
        
        Returns:
            Словарь с данными о заказе
        """
        try:
            async with httpx.AsyncClient(timeout=60.0, verify=True) as client:
                response = await client.get(
                    f"{self.base_url}/orders/{order_id}",
                    headers=self.headers
                )
                response.raise_for_status()
                return response.json()
        except Exception as e:
            logger.error(f"Ошибка при получении заказа по ID {order_id}: {e}")
            raise
    
    async def get_order_items(self, order_id: str) -> Dict:
        """
        Получить товары в заказе
        
        Args:
            order_id: ID заказа
        
        Returns:
            Словарь с данными о товарах
        """
        try:
            async with httpx.AsyncClient(timeout=60.0, verify=True) as client:
                response = await client.get(
                    f"{self.base_url}/orders/{order_id}/entries",
                    headers=self.headers
                )
                response.raise_for_status()
                return response.json()
        except Exception as e:
            logger.error(f"Ошибка при получении товаров заказа {order_id}: {e}")
            raise
    
    async def get_product_description(self, entry_id: str) -> Dict:
        """
        Получить описание товара по ID позиции заказа
        
        Args:
            entry_id: ID позиции заказа
        
        Returns:
            Словарь с данными о товаре (code, name, manufacturer, category)
        """
        try:
            async with httpx.AsyncClient(timeout=60.0, verify=True) as client:
                response = await client.get(
                    f"{self.base_url}/orderentries/{entry_id}/product",
                    headers=self.headers
                )
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.debug(f"Описание товара недоступно для {entry_id} (404)")
            else:
                logger.warning(f"Ошибка {e.response.status_code} при получении описания товара для {entry_id}")
            # Возвращаем пустой результат вместо ошибки
            return {'data': {'attributes': {}}}
        except Exception as e:
            logger.warning(f"Не удалось получить описание товара для {entry_id}: {e}")
            # Возвращаем пустой результат вместо ошибки
            return {'data': {'attributes': {}}}
    
    async def get_order_entry_details(self, entry_id: str) -> Dict:
        """
        Получить детальную информацию о товаре в заказе
        (количество, цена, вес и т.д.)
        
        Args:
            entry_id: ID позиции заказа
        
        Returns:
            Словарь с детальными данными о товаре
        """
        try:
            async with httpx.AsyncClient(timeout=60.0, verify=True) as client:
                response = await client.get(
                    f"{self.base_url}/orderentries/{entry_id}",
                    headers=self.headers
                )
                response.raise_for_status()
                return response.json()
        except Exception as e:
            logger.warning(f"Не удалось получить детали товара для {entry_id}: {e}")
            return {'data': {'attributes': {}}}
    
    async def get_delivery_point(self, entry_id: str) -> Dict:
        """
        Получить информацию о складе отправки
        
        Args:
            entry_id: ID позиции заказа
        
        Returns:
            Словарь с данными о складе
        """
        try:
            async with httpx.AsyncClient(timeout=60.0, verify=True) as client:
                response = await client.get(
                    f"{self.base_url}/orderentries/{entry_id}/deliveryPointOfService",
                    headers=self.headers
                )
                response.raise_for_status()
                return response.json()
        except Exception as e:
            logger.error(f"Ошибка при получении склада для {entry_id}: {e}")
            raise
    
    async def get_point_of_service(self, point_id: str) -> Dict:
        """
        Получить информацию о складе по ID
        
        Args:
            point_id: ID склада
        
        Returns:
            Словарь с данными о складе
        """
        try:
            async with httpx.AsyncClient(timeout=60.0, verify=True) as client:
                response = await client.get(
                    f"{self.base_url}/pointofservices/{point_id}",
                    headers=self.headers
                )
                response.raise_for_status()
                return response.json()
        except Exception as e:
            logger.error(f"Ошибка при получении информации о складе {point_id}: {e}")
            raise
    
    async def accept_order(self, order_id: str, order_code: str) -> Dict:
        """
        Принять заказ (изменить статус на ACCEPTED_BY_MERCHANT)
        
        Args:
            order_id: ID заказа
            order_code: Код заказа
        
        Returns:
            Словарь с обновленными данными заказа
        """
        payload = {
            "data": {
                "type": "orders",
                "id": order_id,
                "attributes": {
                    "code": order_code,
                    "status": "ACCEPTED_BY_MERCHANT"
                }
            }
        }
        
        try:
            async with httpx.AsyncClient(timeout=60.0, verify=True) as client:
                response = await client.post(
                    f"{self.base_url}/orders",
                    headers=self.headers,
                    json=payload
                )
                response.raise_for_status()
                logger.info(f"Заказ {order_code} принят успешно")
                return response.json()
        except Exception as e:
            logger.error(f"Ошибка при принятии заказа {order_id}: {e}")
            raise
    
    async def change_order_status(self, order_id: str, status: str, number_of_space: int = 1) -> Dict:
        """
        Изменить статус заказа (например, для формирования накладной)
        
        Args:
            order_id: ID заказа
            status: Новый статус (например, ASSEMBLE для формирования накладной)
            number_of_space: Количество мест в заказе
        
        Returns:
            Словарь с обновленными данными заказа
        """
        payload = {
            "data": {
                "type": "orders",
                "id": order_id,
                "attributes": {
                    "status": status,
                    "numberOfSpace": number_of_space
                }
            }
        }
        
        try:
            async with httpx.AsyncClient(timeout=60.0, verify=True) as client:
                response = await client.post(
                    f"{self.base_url}/orders",
                    headers=self.headers,
                    json=payload
                )
                response.raise_for_status()
                logger.info(f"Статус заказа {order_id} изменен на {status}")
                return response.json()
        except Exception as e:
            logger.error(f"Ошибка при изменении статуса заказа {order_id}: {e}")
            raise