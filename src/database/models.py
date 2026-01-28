"""
Модели базы данных
"""
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


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
    planned_delivery_date = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    notified_at = Column(DateTime)
    is_kaspi_delivery = Column(Boolean, default=False)
    
    def __repr__(self):
        return f"<Order(code={self.code}, status={self.status})>"


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
            
            if order is None:
                order = Order(**order_data)
                session.add(order)
            else:
                # Обновляем существующий заказ
                for key, value in order_data.items():
                    setattr(order, key, value)
            
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
                Order.status.in_(['APPROVED_BY_BANK', 'ACCEPTED_BY_MERCHANT']),
                Order.state.in_(['NEW', 'PICKUP', 'DELIVERY', 'KASPI_DELIVERY'])
            ).all()
            return orders
        finally:
            session.close()
