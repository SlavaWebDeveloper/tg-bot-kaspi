"""
Модели базы данных
"""
import logging
import json
import base64
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Float, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
logger = logging.getLogger(__name__)


class Order(Base):
    """Модель заказа"""
    __tablename__ = 'orders'
    
    id = Column(String, primary_key=True)
    code = Column(String, unique=True, nullable=False)
    status = Column(String, nullable=False)
    state = Column(String, nullable=False)
    total_price = Column(Float)
    customer_name = Column(String)
    customer_phone = Column(String)
    delivery_mode = Column(String)
    delivery_address = Column(String)
    warehouse_id = Column(String)
    warehouse_name = Column(String)
    warehouse_address = Column(String)
    planned_delivery_date = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    notified_at = Column(DateTime)
    is_kaspi_delivery = Column(Boolean, default=False)
    is_express = Column(Boolean, default=False)  # Экспресс-доставка
    waybill_url = Column(String)  # URL накладной
    waybill_pdf = Column(Text)  # PDF накладной в base64
    items_json = Column(Text)  # JSON с товарами
    
    def __repr__(self):
        return f"<Order(code={self.code}, status={self.status})>"
    
    @property
    def items(self):
        """Получить товары из JSON"""
        if self.items_json:
            try:
                return json.loads(self.items_json)
            except:
                return []
        return []
    
    @items.setter
    def items(self, value):
        """Сохранить товары в JSON"""
        if value:
            self.items_json = json.dumps(value, ensure_ascii=False)
        else:
            self.items_json = None
    
    @property
    def waybill_pdf_bytes(self):
        """Получить PDF как bytes"""
        if self.waybill_pdf:
            try:
                return base64.b64decode(self.waybill_pdf)
            except:
                return None
        return None
    
    @waybill_pdf_bytes.setter
    def waybill_pdf_bytes(self, value):
        """Сохранить PDF из bytes"""
        if value:
            self.waybill_pdf = base64.b64encode(value).decode('utf-8')
        else:
            self.waybill_pdf = None


class Database:
    """Класс для работы с базой данных"""
    
    def __init__(self, database_url: str):
        self.engine = create_engine(database_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
    
    def get_session(self):
        """Получить сессию БД"""
        return self.Session()
    
    def is_order_notified(self, order_code: str) -> bool:
        """Проверить, было ли отправлено уведомление о заказе"""
        session = self.get_session()
        try:
            order = session.query(Order).filter_by(code=order_code).first()
            return order is not None and order.notified_at is not None
        finally:
            session.close()
    
    def save_order(self, order_data: dict):
        """Сохранить заказ в БД"""
        session = self.get_session()
        try:
            order = session.query(Order).filter_by(code=order_data['code']).first()
            
            # Извлекаем items отдельно для обработки
            items = order_data.pop('items', None)
            # Извлекаем waybill_pdf_data если есть
            waybill_pdf_data = order_data.pop('waybill_pdf_data', None)
            
            if order is None:
                order = Order(**order_data)
                if items:
                    order.items = items
                if waybill_pdf_data:
                    order.waybill_pdf_bytes = waybill_pdf_data
                session.add(order)
            else:
                # Обновляем существующий заказ
                for key, value in order_data.items():
                    setattr(order, key, value)
                if items:
                    order.items = items
                if waybill_pdf_data:
                    order.waybill_pdf_bytes = waybill_pdf_data
            
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def mark_as_notified(self, order_code: str):
        """Отметить заказ как обработанный (уведомление отправлено)"""
        session = self.get_session()
        try:
            order = session.query(Order).filter_by(code=order_code).first()
            if order:
                order.notified_at = datetime.utcnow()
                session.commit()
        finally:
            session.close()
    
    def get_active_orders(self) -> list:
        """Получить активные заказы (не переданные в доставку)"""
        session = self.get_session()
        try:
            orders = session.query(Order).filter(
                Order.status.in_(['APPROVED_BY_BANK', 'ACCEPTED_BY_MERCHANT', 'ASSEMBLE']),
                Order.state.in_(['NEW', 'PICKUP', 'DELIVERY', 'KASPI_DELIVERY'])
            ).all()
            return orders
        finally:
            session.close()
    
    def update_order_status(self, order_code: str, new_status: str):
        """Обновить статус заказа в БД"""
        session = self.get_session()
        try:
            order = session.query(Order).filter_by(code=order_code).first()
            if order:
                order.status = new_status
                session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Ошибка при обновлении статуса заказа {order_code}: {e}")
            raise e
        finally:
            session.close()
    
    def update_order_waybill(self, order_code: str, waybill_url: str, waybill_pdf_data: bytes = None):
        """Обновить URL накладной и PDF для заказа"""
        session = self.get_session()
        try:
            order = session.query(Order).filter_by(code=order_code).first()
            if order:
                order.waybill_url = waybill_url
                if waybill_pdf_data:
                    order.waybill_pdf_bytes = waybill_pdf_data
                session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Ошибка при обновлении накладной {order_code}: {e}")
            raise e
        finally:
            session.close()
    
    def get_order_waybill_pdf(self, order_code: str) -> bytes:
        """Получить PDF накладной из БД"""
        session = self.get_session()
        try:
            order = session.query(Order).filter_by(code=order_code).first()
            if order:
                return order.waybill_pdf_bytes
            return None
        finally:
            session.close()
    
    def clear_all_orders(self) -> int:
        """Удалить все заказы из БД"""
        session = self.get_session()
        try:
            count = session.query(Order).count()
            session.query(Order).delete()
            session.commit()
            return count
        except Exception as e:
            session.rollback()
            logger.error(f"Ошибка при очистке БД: {e}")
            raise e
        finally:
            session.close()