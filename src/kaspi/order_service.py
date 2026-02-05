"""
Сервис для обработки заказов Kaspi
"""
import logging
import httpx
from datetime import datetime
from typing import List, Dict, Optional
from src.kaspi.api_client import KaspiAPIClient
from src.database.models import Database

logger = logging.getLogger(__name__)


class OrderService:
    """Сервис для работы с заказами"""
    
    def __init__(self, kaspi_client: KaspiAPIClient, database: Database):
        self.kaspi = kaspi_client
        self.db = database
    
    def _format_timestamp(self, timestamp_ms: Optional[int]) -> Optional[datetime]:
        """Конвертировать timestamp в миллисекундах в datetime"""
        if timestamp_ms:
            return datetime.fromtimestamp(timestamp_ms / 1000)
        return None
    
    def _get_delivery_type_text(self, delivery_mode: str, is_kaspi_delivery: bool) -> str:
        """Получить текстовое описание типа доставки"""
        # Определяем тип доставки согласно документации Kaspi
        if delivery_mode == 'DELIVERY_LOCAL':
            if is_kaspi_delivery:
                return 'Kaspi Доставка (по городу)'
            else:
                return 'Доставка по городу (своими силами)'
        
        elif delivery_mode == 'DELIVERY_PICKUP':
            if is_kaspi_delivery:
                return 'Kaspi Postomat'
            else:
                return 'Самовывоз'
        
        elif delivery_mode == 'DELIVERY_REGIONAL_TODOOR':
            if is_kaspi_delivery:
                return 'Kaspi Доставка (по области)'
            else:
                return 'Доставка по области'
        
        elif delivery_mode == 'DELIVERY_REGIONAL_PICKUP':
            return '🏪 Самовывоз (доставка по области до склада)'
        
        # Если неизвестный тип
        return f'📍 {delivery_mode}'
    
    async def _download_waybill_pdf(self, waybill_url: str) -> Optional[bytes]:
        """
        Скачать PDF накладной по URL
        
        Args:
            waybill_url: URL накладной
        
        Returns:
            Содержимое PDF как bytes или None при ошибке
        """
        try:
            logger.info(f"Скачиваю PDF накладной из {waybill_url}")
            
            # Используем те же заголовки что и для API, включая токен авторизации
            headers = {
                'X-Auth-Token': self.kaspi.api_token,
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/pdf,*/*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8'
            }
            
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                response = await client.get(waybill_url, headers=headers)
                response.raise_for_status()
                
                pdf_content = response.content
                
                # Проверяем что это действительно PDF
                if not pdf_content.startswith(b'%PDF'):
                    logger.error(f"Полученный файл не является PDF. Первые 100 байт: {pdf_content[:100]}")
                    return None
                
                logger.info(f"PDF накладной успешно скачан, размер: {len(pdf_content)} байт")
                return pdf_content
                
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP ошибка при скачивании PDF накладной: {e.response.status_code}")
            logger.error(f"Ответ сервера: {e.response.text[:500]}")
            return None
        except Exception as e:
            logger.error(f"Ошибка при скачивании PDF накладной: {type(e).__name__}: {e}")
            return None
    
    async def get_new_orders(self) -> List[Dict]:
        """
        Получить новые заказы, о которых еще не было уведомления
        
        Returns:
            Список словарей с полной информацией о заказах
        """
        try:
            logger.info("🔍 Запрашиваем заказы у Kaspi API...")
            logger.info("Фильтры: status=['APPROVED_BY_BANK', 'ACCEPTED_BY_MERCHANT']")
            logger.info("Это автоматически исключает: COMPLETED, CANCELLED, ARCHIVE")
            
            response = await self.kaspi.get_orders(
                status=['APPROVED_BY_BANK', 'ACCEPTED_BY_MERCHANT']
            )
            
            orders_data = response.get('data', [])
            total_count = response.get('meta', {}).get('totalCount', 0)
            
            logger.info(f"📊 Получено заказов из API: {len(orders_data)} (всего в системе: {total_count})")
            
            if not orders_data:
                logger.info("ℹ️  Заказов с указанными фильтрами не найдено")
                logger.info("💡 Возможные причины:")
                logger.info("   - Нет заказов с нужными статусами")
                logger.info("   - Все заказы уже обработаны")
                logger.info("   - Проверьте статусы заказов в личном кабинете Kaspi")
                return []
            
            new_orders = []
            
            for idx, order in enumerate(orders_data, 1):
                order_code = order['attributes']['code']
                order_status = order['attributes']['status']
                order_state = order['attributes']['state']
                
                logger.info(f"  [{idx}/{len(orders_data)}] Заказ #{order_code} - статус: {order_status}, состояние: {order_state}")
                
                # Проверяем, отправляли ли уже уведомление
                if self.db.is_order_notified(order_code):
                    logger.info(f"    ⏭️  Пропускаем - уже обработан ранее")
                    continue
                
                # Определяем завершен ли заказ
                is_completed = order_status in ['COMPLETED', 'CANCELLED', 'CANCELLING'] or order_state == 'ARCHIVE'
                
                # Получаем полную информацию о заказе
                logger.info(f"    ✅ Получаем информацию о заказе...")
                order_info = await self._get_full_order_info(order)
                
                if not order_info:
                    logger.warning(f"    ⚠️  Не удалось получить полную информацию")
                    continue
                
                # Сохраняем ВСЕ заказы в БД
                self.save_order_to_db(order_info)
                self.mark_order_notified(order_code)
                
                # Но в список для УВЕДОМЛЕНИЙ добавляем только активные
                if is_completed:
                    logger.info(f"    📝 Сохранен в БД без уведомления - заказ завершен")
                else:
                    logger.info(f"    ✓ Сохранен в БД, будет отправлено уведомление")
                    new_orders.append(order_info)
            
            logger.info(f"🎯 Итого новых заказов для обработки: {len(new_orders)}")
            return new_orders
            
        except Exception as e:
            logger.error(f"❌ Ошибка при получении новых заказов: {type(e).__name__}: {e}", exc_info=True)
            return []
    
    async def _get_full_order_info(self, order: Dict) -> Optional[Dict]:
        """
        Получить полную информацию о заказе включая товары и склад
        
        Args:
            order: Базовые данные заказа из API
        
        Returns:
            Словарь с полной информацией о заказе
        """
        try:
            order_id = order['id']
            attributes = order['attributes']
            
            # Получаем товары
            items_response = await self.kaspi.get_order_items(order_id)
            items_data = items_response.get('data', [])
            
            # Формируем список товаров
            items = []
            warehouse_info = None
            
            for item in items_data:
                item_attrs = item['attributes']
                
                # Получаем описание товара через правильный endpoint
                product_description = ""
                try:
                    product_info = await self.kaspi.get_product_description(item['id'])
                    product_attrs = product_info.get('data', {}).get('attributes', {})
                    
                    # Формируем описание из product endpoint
                    desc_parts = []
                    
                    # Название товара
                    if product_attrs.get('name'):
                        desc_parts.append(product_attrs['name'])
                    
                    # Бренд
                    if product_attrs.get('manufacturer'):
                        desc_parts.append(f"Бренд: {product_attrs['manufacturer']}")
                    
                    # Код товара в Kaspi (БЕЗ префикса "Код:")
                    if product_attrs.get('code'):
                        desc_parts.append(product_attrs['code'])
                    
                    product_description = " | ".join(desc_parts) if desc_parts else ""
                    
                except Exception as e:
                    logger.debug(f"Описание товара недоступно: {e}")
                    product_description = ""
                
                # Получаем информацию о складе для первого товара
                if warehouse_info is None:
                    try:
                        warehouse_response = await self.kaspi.get_delivery_point(item['id'])
                        warehouse_data = warehouse_response.get('data', {})
                        warehouse_attrs = warehouse_data.get('attributes', {})
                        warehouse_info = {
                            'id': warehouse_data.get('id', ''),
                            'name': warehouse_attrs.get('displayName', 'Не указан'),
                            'address': warehouse_attrs.get('address', {}).get('formattedAddress', 'Адрес не указан')
                        }
                    except Exception as e:
                        logger.warning(f"Не удалось получить информацию о складе: {e}")
                        warehouse_info = {
                            'id': '',
                            'name': 'Не указан',
                            'address': 'Адрес не указан'
                        }
                
                items.append({
                    'name': item_attrs.get('category', {}).get('title', 'Товар'),
                    'description': product_description,
                    'quantity': item_attrs.get('quantity', 1),
                    'price': item_attrs.get('basePrice', 0),
                    'total_price': item_attrs.get('totalPrice', 0)
                })
            
            # Формируем полную информацию о заказе
            customer = attributes.get('customer', {})
            delivery_address = attributes.get('deliveryAddress', {})
            
            # Проверяем экспресс-доставку
            is_express = attributes.get('express', False)
            
            # Получаем URL накладной
            waybill_url = attributes.get('waybill', '')
            
            # Скачиваем PDF накладной если есть URL
            waybill_pdf_data = None
            if waybill_url:
                waybill_pdf_data = await self._download_waybill_pdf(waybill_url)
            
            order_info = {
                'id': order_id,
                'code': attributes['code'],
                'status': attributes['status'],
                'state': attributes['state'],
                'total_price': attributes.get('totalPrice', 0),
                'customer_name': f"{customer.get('firstName', '')} {customer.get('lastName', '')}".strip(),
                'customer_phone': customer.get('cellPhone', 'Не указан'),
                'delivery_mode': attributes.get('deliveryMode', ''),
                'delivery_type_text': self._get_delivery_type_text(
                    attributes.get('deliveryMode', ''),
                    attributes.get('isKaspiDelivery', False)
                ),
                'delivery_address': delivery_address.get('formattedAddress', 'Самовывоз'),
                'is_kaspi_delivery': attributes.get('isKaspiDelivery', False),
                'is_express': is_express,
                'planned_delivery_date': self._format_timestamp(attributes.get('plannedDeliveryDate')),
                'creation_date': self._format_timestamp(attributes.get('creationDate')),
                'warehouse_id': warehouse_info['id'] if warehouse_info else '',
                'warehouse_name': warehouse_info['name'] if warehouse_info else 'Не указан',
                'warehouse_address': warehouse_info['address'] if warehouse_info else 'Адрес не указан',
                'items': items,
                'waybill_url': waybill_url,
                'waybill_pdf_data': waybill_pdf_data  # Добавляем PDF данные
            }
            
            return order_info
            
        except Exception as e:
            logger.error(f"Ошибка при получении полной информации о заказе: {e}")
            return None
    
    def save_order_to_db(self, order_info: Dict):
        """Сохранить заказ в базу данных"""
        try:
            order_data = {
                'id': order_info['id'],
                'code': order_info['code'],
                'status': order_info['status'],
                'state': order_info['state'],
                'total_price': order_info['total_price'],
                'customer_name': order_info['customer_name'],
                'customer_phone': order_info['customer_phone'],
                'delivery_mode': order_info['delivery_mode'],
                'delivery_address': order_info['delivery_address'],
                'warehouse_id': order_info['warehouse_id'],
                'warehouse_name': order_info['warehouse_name'],
                'warehouse_address': order_info['warehouse_address'],
                'planned_delivery_date': order_info['planned_delivery_date'],
                'is_kaspi_delivery': order_info['is_kaspi_delivery'],
                'is_express': order_info.get('is_express', False),
                'waybill_url': order_info.get('waybill_url', ''),
                'waybill_pdf_data': order_info.get('waybill_pdf_data'),  # Добавляем PDF данные
                'items': order_info.get('items', [])
            }
            
            self.db.save_order(order_data)
            logger.info(f"Заказ {order_info['code']} сохранен в БД")
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении заказа в БД: {e}")
    
    def mark_order_notified(self, order_code: str):
        """Отметить заказ как обработанный"""
        try:
            self.db.mark_as_notified(order_code)
            logger.info(f"Заказ {order_code} отмечен как обработанный")
        except Exception as e:
            logger.error(f"Ошибка при отметке заказа как обработанного: {e}")
    
    async def get_active_orders(self) -> List[Dict]:
        """
        Получить список активных заказов (не переданных в доставку)
        
        Returns:
            Список заказов из базы данных
        """
        try:
            orders = self.db.get_active_orders()
            return [
                {
                    'id': order.id,
                    'code': order.code,
                    'status': order.status,
                    'state': order.state,
                    'customer_name': order.customer_name,
                    'customer_phone': order.customer_phone,
                    'total_price': order.total_price,
                    'warehouse_name': order.warehouse_name,
                    'warehouse_address': order.warehouse_address,
                    'delivery_address': order.delivery_address,
                    'planned_delivery_date': order.planned_delivery_date,
                    'creation_date': order.created_at,
                    'is_kaspi_delivery': order.is_kaspi_delivery,
                    'is_express': getattr(order, 'is_express', False),
                    'waybill_url': order.waybill_url,
                    'items': order.items 
                }
                for order in orders
            ]
        except Exception as e:
            logger.error(f"Ошибка при получении активных заказов: {e}")
            return []
    
    async def accept_order(self, order_id: str, order_code: str) -> bool:
        """
        Принять заказ через API
        
        Args:
            order_id: ID заказа
            order_code: Код заказа
        
        Returns:
            True если успешно, False при ошибке
        """
        try:
            result = await self.kaspi.accept_order(order_id, order_code)
            
            # Обновляем статус в БД
            self.db.update_order_status(order_code, 'ACCEPTED_BY_MERCHANT')
            
            logger.info(f"✅ Заказ {order_code} успешно принят")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка при принятии заказа {order_code}: {e}")
            return False
    
    async def create_waybill(self, order_id: str, number_of_spaces: int = 1) -> Dict:
        """
        Сформировать накладную для заказа (изменить статус на ASSEMBLE)
        
        Args:
            order_id: ID заказа
            number_of_spaces: Количество мест в заказе
        
        Returns:
            Словарь с результатом (waybill_url если есть) или False при ошибке
        """
        try:
            # Шаг 1: Изменяем статус на ASSEMBLE
            import base64
            order_code = base64.b64decode(order_id).decode('utf-8')

            result = await self.kaspi.change_order_status(
                order_code=order_code,  
                status='ASSEMBLE',
                number_of_space=number_of_spaces
            )
            
            logger.info(f"Статус заказа {order_id} изменен на ASSEMBLE")
            
            # Шаг 2: Получаем информацию о заказе для получения URL накладной
            # Kaspi API не возвращает waybill сразу, нужно запросить заказ отдельно
            import asyncio
            await asyncio.sleep(2)  # Даем время Kaspi сгенерировать накладную
            
            order_info = await self.kaspi.get_order_by_id(order_id)
            attributes = order_info.get('data', {}).get('attributes', {})
            
            # Получаем код заказа
            order_code = attributes.get('code')
            
            # Получаем URL накладной
            waybill_url = attributes.get('waybill')
            
            # Если URL накладной еще не готов, пробуем еще раз через 3 секунды
            if not waybill_url:
                logger.info(f"Накладная еще не готова, ожидаю 3 секунды...")
                await asyncio.sleep(3)
                order_info = await self.kaspi.get_order_by_id(order_id)
                attributes = order_info.get('data', {}).get('attributes', {})
                waybill_url = attributes.get('waybill')
            
            # Скачиваем PDF накладной если есть URL
            waybill_pdf_data = None
            if waybill_url:
                logger.info(f"Скачиваю PDF накладной по URL: {waybill_url}")
                waybill_pdf_data = await self._download_waybill_pdf(waybill_url)
            
            # Обновляем статус и URL накладной в БД
            if order_code:
                self.db.update_order_status(order_code, 'ASSEMBLE')
                if waybill_url:
                    self.db.update_order_waybill(order_code, waybill_url, waybill_pdf_data)
                    logger.info(f"✅ Накладная для заказа {order_code} сформирована и сохранена")
                else:
                    logger.warning(f"⚠️ Накладная для заказа {order_code} сформирована, но URL еще не доступен")
            
            return {
                'success': True,
                'waybill_url': waybill_url
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка при формировании накладной для заказа {order_id}: {e}")
            return False
    
    async def check_order_status(self, order_id: str, order_code: str) -> Dict:
        """
        Проверить текущий статус заказа
        
        Args:
            order_id: ID заказа
            order_code: Код заказа
        
        Returns:
            Словарь с информацией о статусе заказа или None при ошибке
        """
        try:
            # Получаем заказ по коду
            response = await self.kaspi.get_order_by_code(order_code)
            orders = response.get('data', [])
            
            if not orders:
                logger.warning(f"Заказ {order_code} не найден")
                return None
            
            order = orders[0]
            attributes = order.get('attributes', {})
            
            # Проверяем наличие накладной для Kaspi Доставки
            waybill_url = None
            if attributes.get('isKaspiDelivery'):
                kaspi_delivery = attributes.get('kaspiDelivery', {})
                waybill_url = kaspi_delivery.get('waybill')
            
            # Скачиваем PDF если есть URL и его еще нет в БД
            if waybill_url and not self.db.get_order_waybill_pdf(order_code):
                waybill_pdf_data = await self._download_waybill_pdf(waybill_url)
                if waybill_pdf_data:
                    self.db.update_order_waybill(order_code, waybill_url, waybill_pdf_data)
            
            return {
                'status': attributes.get('status'),
                'state': attributes.get('state'),
                'waybill_url': waybill_url,
                'is_kaspi_delivery': attributes.get('isKaspiDelivery', False)
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке статуса заказа {order_code}: {e}")
            return None
    
    def clear_database(self) -> int:
        """
        Очистить все данные из базы данных
        
        Returns:
            Количество удаленных записей
        """
        return self.db.clear_all_orders()