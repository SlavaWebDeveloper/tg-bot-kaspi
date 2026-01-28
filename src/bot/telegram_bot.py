"""
Telegram бот для уведомлений о заказах
"""
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes
)
from typing import Dict
from datetime import datetime

logger = logging.getLogger(__name__)


class TelegramBot:
    """Класс для управления Telegram ботом"""
    
    def __init__(self, token: str, chat_id: str, order_service):
        self.token = token
        self.chat_id = chat_id
        self.order_service = order_service
        self.application = None
    
    def format_order_message(self, order: Dict) -> str:
        """
        Форматировать сообщение о заказе для Telegram
        
        Args:
            order: Словарь с данными о заказе
        
        Returns:
            Отформатированный текст сообщения
        """
        message_parts = [
            f"🆕 <b>Новый заказ #{order['code']}</b>\n",
            f"📦 <b>Что отправить:</b>"
        ]
        
        # Добавляем товары
        for item in order['items']:
            message_parts.append(
                f"• {item['name']} x {item['quantity']} шт — {item['total_price']:,.0f} ₸"
            )
        
        message_parts.append(f"\n<b>Итого:</b> {order['total_price']:,.0f} ₸\n")
        
        # Информация о складе
        message_parts.extend([
            f"📍 <b>Склад отправки:</b> {order['warehouse_name']}",
            f"{order['warehouse_address']}\n"
        ])
        
        # Информация о клиенте
        message_parts.extend([
            f"👤 <b>Клиент:</b>",
            f"{order['customer_name']}",
            f"+{order['customer_phone']}\n"
        ])
        
        # Информация о доставке
        message_parts.extend([
            f"🚚 <b>Доставка:</b>",
            f"{order['delivery_type_text']}",
            f"{order['delivery_address']}\n"
        ])
        
        # Срок доставки
        if order['planned_delivery_date']:
            delivery_date = order['planned_delivery_date'].strftime('%d.%m.%Y')
            message_parts.append(f"📅 <b>Срок доставки:</b> {delivery_date}")
        
        return '\n'.join(message_parts)
    
    def format_active_orders_message(self, orders: list) -> str:
        """
        Форматировать сообщение со списком активных заказов
        
        Args:
            orders: Список активных заказов
        
        Returns:
            Отформатированный текст сообщения
        """
        if not orders:
            return "📋 Нет активных заказов"
        
        message_parts = [f"📋 <b>Активные заказы ({len(orders)}):</b>\n"]
        
        for order in orders:
            delivery_date = ""
            if order['planned_delivery_date']:
                delivery_date = order['planned_delivery_date'].strftime('%d.%m.%Y')
            
            message_parts.extend([
                f"🔹 <b>Заказ #{order['code']}</b>",
                f"Сумма: {order['total_price']:,.0f} ₸",
                f"Клиент: {order['customer_name']} (+{order['customer_phone']})",
                f"Склад: {order['warehouse_name']}",
                f"Доставка: {order['delivery_address']}",
                f"Срок: {delivery_date}\n"
            ])
        
        return '\n'.join(message_parts)
    
    async def send_order_notification(self, order: Dict):
        """
        Отправить уведомление о новом заказе
        
        Args:
            order: Словарь с данными о заказе
        """
        try:
            message = self.format_order_message(order)
            
            # Добавляем кнопку для скачивания накладной если это Kaspi Доставка
            keyboard = None
            if order.get('is_kaspi_delivery') and order.get('waybill_url'):
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📄 Скачать накладную", url=order['waybill_url'])]
                ])
            
            await self.application.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode='HTML',
                reply_markup=keyboard
            )
            
            logger.info(f"Отправлено уведомление о заказе {order['code']}")
            
        except Exception as e:
            logger.error(f"Ошибка при отправке уведомления о заказе: {e}")
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start"""
        await update.message.reply_text(
            "🤖 Бот для уведомлений о заказах Kaspi запущен!\n\n"
            "Доступные команды:\n"
            "/active - Показать активные заказы\n"
            "/help - Помощь"
        )
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /help"""
        help_text = (
            "📖 <b>Помощь по боту</b>\n\n"
            "Бот автоматически отправляет уведомления о новых заказах каждые 30 минут.\n\n"
            "<b>Команды:</b>\n"
            "/active - Показать все активные заказы (не переданные в доставку)\n"
            "/debug - Отладочная информация и проверка API\n"
            "/help - Показать это сообщение\n\n"
            "<b>Информация в уведомлениях:</b>\n"
            "• Номер заказа\n"
            "• Список товаров и сумма\n"
            "• Склад отправки\n"
            "• Контакты клиента\n"
            "• Адрес доставки\n"
            "• Срок доставки\n"
            "• Ссылка на накладную (для Kaspi Доставки)"
        )
        await update.message.reply_text(help_text, parse_mode='HTML')
    
    async def active_orders_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /active - показать активные заказы"""
        try:
            orders = await self.order_service.get_active_orders()
            message = self.format_active_orders_message(orders)
            await update.message.reply_text(message, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Ошибка при получении активных заказов: {e}")
            await update.message.reply_text(
                "❌ Произошла ошибка при получении списка заказов"
            )
    
    async def debug_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /debug - отладочная информация об API"""
        try:
            await update.message.reply_text("🔍 Проверяю подключение к Kaspi API...", parse_mode='HTML')
            
            # Пробуем получить заказы без фильтров
            from src.kaspi.api_client import KaspiAPIClient
            from src.config import Config
            
            client = KaspiAPIClient(Config.KASPI_API_TOKEN, Config.KASPI_API_URL)
            
            # Запрос без фильтров - первые 5 заказов
            response = await client.get_orders(page_size=5)
            
            data = response.get('data', [])
            meta = response.get('meta', {})
            
            debug_info = [
                "📊 <b>Результат запроса к API:</b>\n",
                f"Всего заказов в системе: {meta.get('totalCount', 'N/A')}",
                f"Получено в ответе: {len(data)}",
                f"Страниц: {meta.get('pageCount', 'N/A')}\n"
            ]
            
            if data:
                debug_info.append("<b>Примеры заказов:</b>")
                for idx, order in enumerate(data[:3], 1):
                    attrs = order['attributes']
                    debug_info.append(
                        f"{idx}. Заказ #{attrs.get('code')} - "
                        f"статус: {attrs.get('status')}, "
                        f"состояние: {attrs.get('state')}"
                    )
            else:
                debug_info.append("⚠️ Заказов не найдено")
            
            await update.message.reply_text('\n'.join(debug_info), parse_mode='HTML')
            
        except Exception as e:
            logger.error(f"Ошибка в debug команде: {e}", exc_info=True)
            await update.message.reply_text(
                f"❌ Ошибка при подключении к API:\n{type(e).__name__}: {str(e)}"
            )
    
    async def send_startup_message(self):
        """Отправить приветственное сообщение при запуске бота"""
        try:
            startup_message = (
                "🤖 <b>Бот запущен!</b>\n\n"
                "Мониторинг заказов Kaspi активирован.\n"
                "Проверка новых заказов каждые 30 минут.\n\n"
                f"Дата и время запуска: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
            )
            await self.application.bot.send_message(
                chat_id=self.chat_id,
                text=startup_message,
                parse_mode='HTML'
            )
            logger.info("Приветственное сообщение отправлено")
        except Exception as e:
            logger.error(f"Ошибка при отправке приветственного сообщения: {e}")
    
    async def check_new_orders(self, context: ContextTypes.DEFAULT_TYPE):
        """
        Периодическая проверка новых заказов (запускается по расписанию)
        """
        try:
            logger.info("Проверка новых заказов...")
            new_orders = await self.order_service.get_new_orders()
            
            if new_orders:
                logger.info(f"Найдено новых заказов: {len(new_orders)}")
                
                for order in new_orders:
                    # Отправляем уведомление
                    await self.send_order_notification(order)
                    
                    # Сохраняем в БД
                    self.order_service.save_order_to_db(order)
                    
                    # Отмечаем как обработанный
                    self.order_service.mark_order_notified(order['code'])
            else:
                logger.info("Новых заказов не найдено")
                
        except Exception as e:
            logger.error(f"Ошибка при проверке новых заказов: {e}", exc_info=True)
    
    def setup(self):
        """Настроить бота и обработчики команд"""
        self.application = Application.builder().token(self.token).build()
        
        # Добавляем обработчики команд
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("active", self.active_orders_command))
        self.application.add_handler(CommandHandler("debug", self.debug_command))
        
        logger.info("Telegram бот настроен")
    
    def add_job_check_orders(self, interval_minutes: int):
        """
        Добавить задачу периодической проверки заказов
        
        Args:
            interval_minutes: Интервал проверки в минутах
        """
        self.application.job_queue.run_repeating(
            self.check_new_orders,
            interval=interval_minutes * 60,
            first=10  # Первая проверка через 10 секунд после запуска
        )
        logger.info(f"Настроена периодическая проверка заказов каждые {interval_minutes} минут")
    
    def run(self):
        """Запустить бота"""
        logger.info("Запуск Telegram бота...")
        
        # Отправляем приветственное сообщение при старте
        async def send_startup():
            await self.send_startup_message()
        
        # Планируем отправку приветственного сообщения
        self.application.job_queue.run_once(send_startup, when=2)
        
        self.application.run_polling(allowed_updates=Update.ALL_TYPES)
