"""
Сервис для обработки заказов Kaspi
"""
import logging
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
        delivery_types = {
            'DELIVERY_LOCAL': 'По городу',
            'DELIVERY_PICKUP': 'Самовывоз' if not is_kaspi_delivery else 'Kaspi Postomat',
            'DELIVERY_REGIONAL_TODOOR': 'Kaspi Доставка',
            'DELIVERY_REGIONAL_PICKUP': 'Доставка по области (самовывоз)'
        }
        
        delivery_text = delivery_types.get(delivery_mode, delivery_mode)
        
        if is_kaspi_delivery and delivery_mode == 'DELIVERY_LOCAL':
            delivery_text += ' (Kaspi Доставка)'
        
        return delivery_text
    
    async def get_new_orders(self) -> List[Dict]:
        """
        Получить новые заказы, о которых еще не было уведомления
        
        Returns:
            Список словарей с полной информацией о заказах
        """
        try:
            logger.info("🔍 Запрашиваем заказы у Kaspi API...")
            logger.info("Фильтры: status=['APPROVED_BY_BANK', 'ACCEPTED_BY_MERCHANT'], state=['NEW', 'PICKUP', 'DELIVERY', 'KASPI_DELIVERY']")
            
            # Получаем заказы со статусами, которые нужно обработать
            response = await self.kaspi.get_orders(
                status=['APPROVED_BY_BANK', 'ACCEPTED_BY_MERCHANT'],
                state=['NEW', 'PICKUP', 'DELIVERY', 'KASPI_DELIVERY']
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
                
                logger.info(f"    ✅ Новый заказ! Получаем детальную информацию...")
                
                # Получаем полную информацию о заказе
                order_info = await self._get_full_order_info(order)
                
                if order_info:
                    new_orders.append(order_info)
                    logger.info(f"    ✓ Информация получена")
                else:
                    logger.warning(f"    ⚠️  Не удалось получить полную информацию")
            
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
                    'quantity': item_attrs.get('quantity', 1),
                    'price': item_attrs.get('basePrice', 0),
                    'total_price': item_attrs.get('totalPrice', 0)
                })
            
            # Формируем полную информацию о заказе
            customer = attributes.get('customer', {})
            delivery_address = attributes.get('deliveryAddress', {})
            
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
                'planned_delivery_date': self._format_timestamp(attributes.get('plannedDeliveryDate')),
                'creation_date': self._format_timestamp(attributes.get('creationDate')),
                'warehouse_id': warehouse_info['id'] if warehouse_info else '',
                'warehouse_name': warehouse_info['name'] if warehouse_info else 'Не указан',
                'warehouse_address': warehouse_info['address'] if warehouse_info else 'Адрес не указан',
                'items': items,
                'waybill_url': attributes.get('waybill', '')
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
                'planned_delivery_date': order_info['planned_delivery_date'],
                'is_kaspi_delivery': order_info['is_kaspi_delivery']
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
                    'code': order.code,
                    'status': order.status,
                    'state': order.state,
                    'customer_name': order.customer_name,
                    'customer_phone': order.customer_phone,
                    'total_price': order.total_price,
                    'warehouse_name': order.warehouse_name,
                    'delivery_address': order.delivery_address,
                    'planned_delivery_date': order.planned_delivery_date
                }
                for order in orders
            ]
        except Exception as e:
            logger.error(f"Ошибка при получении активных заказов: {e}")
            return []
