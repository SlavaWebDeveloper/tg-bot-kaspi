"""
Telegram бот для уведомлений о заказах
"""
import logging
import httpx
import io
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
        
        # ID админов с особыми полномочиями
        self.admin_ids = [554076618, 773205112]
        
        # Состояния для подтверждений
        self.pending_confirmations = {}  # {user_id: {'action': 'clear_db', 'data': {...}}}
    
    def format_order_message(self, order: Dict) -> str:
        """
        Форматировать сообщение о заказе для Telegram
        
        Args:
            order: Словарь с данными о заказе
        
        Returns:
            Отформатированный текст сообщения
        """
        message_parts = []
        
        # Если экспресс-доставка, выделяем это в начале
        if order.get('is_express'):
            message_parts.append("⚡️ <b>EXPRESS ДОСТАВКА</b> ⚡️")
        
        message_parts.append(f"🆕 <b>Новый заказ #{order['code']}</b>")
        
        # Добавляем дату создания заказа
        if order.get('creation_date'):
            creation_date = order['creation_date'].strftime('%d.%m.%Y %H:%M')
            message_parts.append(f"📅 <b>Создан:</b> {creation_date}\n")
        else:
            message_parts.append("")
        
        message_parts.append(f"📦 <b>Что отправить:</b>")
        
        # Добавляем товары - всё в одном блоке <code> для удобного копирования
        items_text = []
        for item in order['items']:
            # Формируем строку: Название | Код: XXX
            item_line = item['name']
            if item.get('description'):
                item_line += f" | Код: {item['description']}"
            items_text.append(item_line)
            
            # Количество и цена
            price_line = f"{item['quantity']} шт × {item['price']:,.0f} ₸ = {item['total_price']:,.0f} ₸"
            items_text.append(price_line)
        
        # Весь список товаров в одном <code> блоке
        message_parts.append(f"<code>{chr(10).join(items_text)}</code>")
        
        message_parts.append(f"\n<b>Итого:</b> {order['total_price']:,.0f} ₸\n")
        
        # Информация о складе
        message_parts.extend([
            f"📍 <b>Склад отправки:</b> {order['warehouse_name']}",
            f"{order['warehouse_address']}\n"
        ])
        
        # Информация о клиенте (без номера телефона)
        message_parts.extend([
            f"👤 <b>Клиент:</b>",
            f"{order['customer_name']}\n"
        ])
        
        # Информация о доставке - теперь с иконками и четким указанием типа
        message_parts.extend([
            f"🚚 <b>Доставка:</b>",
            f"{order['delivery_type_text']}",
            f"📍 {order['delivery_address']}"
        ])
        
        # Срок доставки
        if order['planned_delivery_date']:
            delivery_date = order['planned_delivery_date'].strftime('%d.%m.%Y')
            message_parts.append(f"⏰ <b>Срок доставки:</b> {delivery_date}")
        
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
        
        message_parts = [f"📋 <b>Активные заказы ({len(orders)}):</b>"]
        
        for order in orders:
            creation_date = ""
            if order.get('creation_date'):
                creation_date = order['creation_date'].strftime('%d.%m.%Y %H:%M')
            
            delivery_date = ""
            if order['planned_delivery_date']:
                delivery_date = order['planned_delivery_date'].strftime('%d.%m.%Y')
            
            # Заголовок заказа с пометкой экспресс если нужно
            order_header = f"🔹 <b>Заказ #{order['code']}</b> • {creation_date}"
            if order.get('is_express'):
                order_header = f"⚡️ {order_header}"
            message_parts.append(order_header)
            
            # Товары - компактный формат
            if order.get('items'):
                items_list = []
                for item in order['items']:
                    item_name = item['name']
                    # Если название слишком длинное, берем только первые 30 символов
                    if len(item_name) > 30:
                        item_name = item_name[:30] + "..."
                    
                    # Добавляем код если есть в description
                    if item.get('description'):
                        # Извлекаем только код (последняя часть после последнего |)
                        parts = item['description'].split('|')
                        code = parts[-1].strip() if parts else item['description']
                        # Если код слишком длинный, берем только последние 15 символов
                        if len(code) > 15:
                            code = "..." + code[-15:]
                        item_text = f"{item_name} (Код: {code}, {item['quantity']} шт)"
                    else:
                        item_text = f"{item_name} ({item['quantity']} шт)"
                    
                    items_list.append(item_text)
                
                # Если товаров больше 2, показываем только первые 2 и "+N еще"
                if len(items_list) > 2:
                    shown_items = items_list[:2]
                    remaining = len(items_list) - 2
                    message_parts.append(f"Товары: {'; '.join(shown_items)} +{remaining} еще")
                else:
                    message_parts.append(f"Товары: {'; '.join(items_list)}")
            
            # Получаем текстовое описание доставки (без адреса для компактности)
            delivery_type = order.get('delivery_type_text', 'Не указан')
            
            # Остальная информация
            message_parts.extend([
                f"Сумма: {order['total_price']:,.0f} ₸",
                f"Клиент: {order['customer_name']}",
                f"Склад: {order['warehouse_name']}",
                f"Доставка: {delivery_type}",
                f"Адрес: {order['delivery_address']}",
                f"Срок: {delivery_date}" if delivery_date else ""
            ])
            
            # Добавляем ссылку на накладную если есть
            if order.get('is_kaspi_delivery') and order.get('waybill_url'):
                message_parts.append(f"📄 <a href=\"{order['waybill_url']}\">Скачать накладную</a>")
            
            message_parts.append("")  # Пустая строка между заказами
        
        # Убираем пустые строки
        message_parts = [part for part in message_parts if part]
        
        return '\n'.join(message_parts)
    
    async def send_waybill_from_db(self, order_code: str, chat_id: str):
        """
        Отправить PDF накладную из БД
        
        Args:
            order_code: Код заказа
            chat_id: ID чата для отправки
        """
        try:
            logger.info(f"Отправляю накладную для заказа {order_code} из БД")
            
            # Получаем PDF из БД
            pdf_data = self.order_service.db.get_order_waybill_pdf(order_code)
            
            if not pdf_data:
                await self.application.bot.send_message(
                    chat_id=chat_id,
                    text=f"❌ PDF накладной для заказа #{order_code} не найден в базе данных",
                    parse_mode='HTML'
                )
                return
            
            # Отправляем как документ
            await self.application.bot.send_document(
                chat_id=chat_id,
                document=io.BytesIO(pdf_data),
                filename=f"Накладная_{order_code}.pdf",
                caption=f"📄 Накладная для заказа #{order_code}"
            )
            
            logger.info(f"Накладная для заказа {order_code} успешно отправлена из БД")
            
        except Exception as e:
            logger.error(f"Ошибка при отправке накладной из БД: {e}")
            await self.application.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Не удалось отправить накладную для заказа #{order_code}",
                parse_mode='HTML'
            )
    
    async def download_and_send_waybill(self, waybill_url: str, order_code: str, chat_id: str):
        """
        Скачать PDF накладную и отправить её в чат
        
        Args:
            waybill_url: URL накладной
            order_code: Код заказа
            chat_id: ID чата для отправки
        """
        try:
            logger.info(f"Скачиваю накладную для заказа {order_code} из {waybill_url}")
            
            # Скачиваем PDF
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(waybill_url)
                response.raise_for_status()
                
                pdf_content = response.content
                
                # Сохраняем PDF в БД
                self.order_service.db.update_order_waybill(
                    order_code=order_code,
                    waybill_url=waybill_url,
                    waybill_pdf_data=pdf_content
                )
                logger.info(f"PDF накладной для заказа {order_code} сохранен в БД")
                
                # Отправляем как документ
                await self.application.bot.send_document(
                    chat_id=chat_id,
                    document=io.BytesIO(pdf_content),
                    filename=f"Накладная_{order_code}.pdf",
                    caption=f"📄 Накладная для заказа #{order_code}"
                )
                
                logger.info(f"Накладная для заказа {order_code} успешно отправлена")
                
        except Exception as e:
            logger.error(f"Ошибка при скачивании/отправке накладной: {e}")

            await self.application.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Не удалось скачать накладную для заказа #{order_code}.\n"
                     f"Попробуйте скачать по <a href=\"{waybill_url}\">прямой ссылке</a>",
                parse_mode='HTML'
            )
    
    async def send_order_notification(self, order: Dict):
        """
        Отправить уведомление о новом заказе
        
        Args:
            order: Словарь с данными о заказе
        """
        try:
            message = self.format_order_message(order)
            
            # Создаем кнопки для нового заказа
            keyboard = []
            
            # Кнопка "Принять заказ" только для заказов со статусом APPROVED_BY_BANK
            if order.get('status') == 'APPROVED_BY_BANK':
                keyboard.append([
                    InlineKeyboardButton(
                        "✅ Принять заказ", 
                        callback_data=f"accept_order:{order['id']}:{order['code']}"
                    )
                ])
            
            # Кнопка "Сформировать накладную" для заказов которые приняты
            if order.get('status') in ['ACCEPTED_BY_MERCHANT', 'PICKUP'] and not order.get('waybill_url'):
                keyboard.append([
                    InlineKeyboardButton(
                        "📋 Сформировать накладную", 
                        callback_data=f"waybill:{order['id']}:{order['code']}"
                    )
                ])
            
            # Кнопки для скачивания накладной если это Kaspi Доставка и накладная уже есть
            if order.get('is_kaspi_delivery') and order.get('waybill_url'):
                keyboard.append([
                    InlineKeyboardButton("📄 Скачать онлайн", url=order['waybill_url']),
                    InlineKeyboardButton("📥 Получить PDF", callback_data=f"download_waybill:{order['code']}")
                ])
            
            reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
            
            await self.application.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode='HTML',
                reply_markup=reply_markup
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
            "/waybills - Сформировать накладные\n"
            "/help - Помощь"
        )
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /help"""
        user_id = update.effective_user.id
        
        help_text = (
            "📖 <b>Помощь по боту</b>\n\n"
            "Бот автоматически отправляет уведомления о новых заказах каждые 30 минут.\n\n"
            "<b>Команды:</b>\n"
            "/active - Показать все активные заказы (не переданные в доставку)\n"
            "/waybills - Сформировать накладные для активных заказов\n"
            "/help - Показать это сообщение\n\n"
            "<b>Информация в уведомлениях:</b>\n"
            "• Номер заказа\n"
            "• Дата и время создания заказа\n"
            "• Список товаров с описанием и сумма\n"
            "• Склад отправки\n"
            "• Имя клиента\n"
            "• Тип доставки (с иконками)\n"
            "• Адрес доставки\n"
            "• Срок доставки\n"
            "• Накладная (для Kaspi Доставки)\n\n"
            "<b>Кнопки:</b>\n"
            "✅ Принять заказ - принять новый заказ в обработку\n"
            "📋 Сформировать накладную - создать накладную для передачи в Kaspi Доставку\n"
        )
        
        # Добавляем админские команды если это админ
        if user_id in self.admin_ids:
            help_text += (
                "\n\n<b>⚙️ Команды администратора:</b>\n"
                "/cancel_order - Отменить заказ\n"
                "/clear_db - Очистить базу данных"
            )
        
        await update.message.reply_text(help_text, parse_mode='HTML')
    
    async def active_orders_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /active - показать активные заказы"""
        try:
            orders = await self.order_service.get_active_orders()
            
            # Добавляем delivery_type_text для каждого заказа
            for order in orders:
                if 'delivery_type_text' not in order:
                    order['delivery_type_text'] = self.order_service._get_delivery_type_text(
                        order.get('delivery_mode', ''),
                        order.get('is_kaspi_delivery', False)
                    )
            
            message = self.format_active_orders_message(orders)
            await update.message.reply_text(message, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Ошибка при получении активных заказов: {e}")
            await update.message.reply_text(
                "❌ Произошла ошибка при получении списка заказов"
            )
    
    async def cancel_order_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /cancel_order - отменить заказ (только для админа)"""
        user_id = update.effective_user.id
        
        # Проверяем права админа
        if user_id not in self.admin_ids:
            return  # Просто игнорируем команду для обычных пользователей
        
        try:
            orders = await self.order_service.get_active_orders()
            
            if not orders:
                await update.message.reply_text(
                    "📋 Нет активных заказов для отмены",
                    parse_mode='HTML'
                )
                return
            
            # Создаем кнопки для каждого заказа
            keyboard = []
            for order in orders:
                keyboard.append([
                    InlineKeyboardButton(
                        f"❌ Заказ #{order['code']} - {order['total_price']:,.0f} ₸",
                        callback_data=f"cancel_order_select:{order['id']}:{order['code']}"
                    )
                ])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                f"⚠️ <b>Выберите заказ для отмены:</b>\n\n"
                f"Найдено активных заказов: {len(orders)}",
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            
        except Exception as e:
            logger.error(f"Ошибка при получении списка для отмены: {e}")
            await update.message.reply_text(
                "❌ Произошла ошибка при получении списка заказов"
            )
    
    async def clear_db_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /clear_db - очистить БД (только для админа)"""
        user_id = update.effective_user.id
        
        # Проверяем права админа
        if user_id not in self.admin_ids:
            return  # Просто игнорируем команду для обычных пользователей
        
        # Проверяем есть ли уже pending подтверждение
        if user_id in self.pending_confirmations and self.pending_confirmations[user_id].get('action') == 'clear_db':
            # Это второе подтверждение
            await self._execute_clear_db(update)
            self.pending_confirmations.pop(user_id, None)
        else:
            # Первое подтверждение
            self.pending_confirmations[user_id] = {'action': 'clear_db'}
            
            keyboard = [
                [
                    InlineKeyboardButton("✅ Да, очистить", callback_data="confirm_clear_db"),
                    InlineKeyboardButton("❌ Отмена", callback_data="cancel_clear_db")
                ]
            ]
            
            await update.message.reply_text(
                "⚠️ <b>ВНИМАНИЕ!</b>\n\n"
                "Вы действительно хотите очистить базу данных?\n\n"
                "Это действие:\n"
                "• Удалит ВСЕ записи о заказах\n"
                "• Удалит историю уведомлений\n"
                "• Удалит все сохраненные PDF накладные\n"
                "• НЕОБРАТИМО\n\n"
                "Для подтверждения нажмите кнопку ниже, затем отправьте команду /clear_db повторно.",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    
    async def waybills_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /waybills - сформировать накладные"""
        try:
            orders = await self.order_service.get_active_orders()
            
            if not orders:
                await update.message.reply_text(
                    "📋 Нет активных заказов для формирования накладных",
                    parse_mode='HTML'
                )
                return
            
            # Создаем кнопки для каждого заказа
            keyboard = []
            for order in orders:
                keyboard.append([
                    InlineKeyboardButton(
                        f"📋 Заказ #{order['code']} - {order['total_price']:,.0f} ₸",
                        callback_data=f"waybill:{order['id']}:{order['code']}"
                    )
                ])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                f"📋 <b>Выберите заказ для формирования накладной:</b>\n\n"
                f"Найдено активных заказов: {len(orders)}",
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            
        except Exception as e:
            logger.error(f"Ошибка при получении списка для накладных: {e}")
            await update.message.reply_text(
                "❌ Произошла ошибка при получении списка заказов"
            )
    
    async def _execute_clear_db(self, update: Update):
        """Выполнить очистку базы данных"""
        try:
            await update.message.reply_text("⏳ Очищаю базу данных...", parse_mode='HTML')
            
            count = self.order_service.clear_database()
            
            await update.message.reply_text(
                f"✅ <b>База данных очищена</b>\n\n"
                f"Удалено записей: {count}",
                parse_mode='HTML'
            )
            logger.info(f"База данных очищена администратором")
            
        except Exception as e:
            logger.error(f"Ошибка при очистке БД: {e}")
            await update.message.reply_text(
                f"❌ Ошибка при очистке базы данных:\n{str(e)}",
                parse_mode='HTML'
            )
    
    async def button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик нажатий на inline кнопки"""
        query = update.callback_query
        await query.answer()
        
        callback_data = query.data
        user_id = query.from_user.id
        
        # Скачать и отправить PDF накладной
        if callback_data.startswith("download_waybill:"):
            order_code = callback_data.split(":")[1]
            
            await query.edit_message_text(
                f"⏳ Получаю накладную для заказа #{order_code}...",
                parse_mode='HTML'
            )
            
            # Сначала пробуем из БД
            await self.send_waybill_from_db(order_code, query.message.chat_id)
            
            await query.edit_message_text(
                f"✅ Накладная для заказа #{order_code} отправлена",
                parse_mode='HTML'
            )
        
        # Обработка принятия заказа
        elif callback_data.startswith("accept_order:"):
            _, order_id, order_code = callback_data.split(":")
            await self.handle_accept_order(query, order_id, order_code)
        
        # Обработка формирования накладной - с подтверждением
        elif callback_data.startswith("waybill:"):
            _, order_id, order_code = callback_data.split(":")
            # Сохраняем для подтверждения
            self.pending_confirmations[user_id] = {
                'action': 'create_waybill',
                'order_id': order_id,
                'order_code': order_code
            }
            
            keyboard = [
                [
                    InlineKeyboardButton("✅ Да, сформировать", callback_data=f"confirm_waybill:{order_id}:{order_code}"),
                    InlineKeyboardButton("❌ Отмена", callback_data="cancel_action")
                ]
            ]
            
            await query.edit_message_text(
                f"⚠️ <b>Подтверждение формирования накладной</b>\n\n"
                f"Заказ: #{order_code}\n\n"
                f"После формирования накладной:\n"
                f"• Статус заказа изменится на ASSEMBLE (Передача)\n"
                f"• Заказ будет готов к отправке в Kaspi Доставку\n"
                f"• Накладная станет доступна для скачивания\n\n"
                f"Продолжить?",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        
        # Подтверждение формирования накладной
        elif callback_data.startswith("confirm_waybill:"):
            _, order_id, order_code = callback_data.split(":")
            self.pending_confirmations.pop(user_id, None)
            await self.handle_create_waybill(query, order_id, order_code)
        
        # Выбор заказа для отмены (только админ)
        elif callback_data.startswith("cancel_order_select:"):
            if user_id not in self.admin_ids:
                await query.answer("❌ Доступ запрещен", show_alert=True)
                return
            
            _, order_id, order_code = callback_data.split(":")
            
            # Сохраняем для подтверждения
            self.pending_confirmations[user_id] = {
                'action': 'cancel_order',
                'order_id': order_id,
                'order_code': order_code
            }
            
            # Показываем причины отмены
            keyboard = [
                [InlineKeyboardButton("👤 Отказ покупателя", callback_data=f"cancel_reason:BUYER_CANCELLATION_BY_MERCHANT:{order_id}:{order_code}")],
                [InlineKeyboardButton("📞 Не удалось связаться", callback_data=f"cancel_reason:BUYER_NOT_REACHABLE:{order_id}:{order_code}")],
                [InlineKeyboardButton("📦 Нет в наличии", callback_data=f"cancel_reason:MERCHANT_OUT_OF_STOCK:{order_id}:{order_code}")],
                [InlineKeyboardButton("❌ Отмена", callback_data="cancel_action")]
            ]
            
            await query.edit_message_text(
                f"⚠️ <b>Отмена заказа #{order_code}</b>\n\n"
                f"Выберите причину отмены:",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        
        # Подтверждение отмены заказа с причиной
        elif callback_data.startswith("cancel_reason:"):
            if user_id not in self.admin_ids:
                await query.answer("❌ Доступ запрещен", show_alert=True)
                return
            
            parts = callback_data.split(":")
            reason = parts[1]
            order_id = parts[2]
            order_code = parts[3]
            
            await self.handle_cancel_order(query, order_id, order_code, reason)
            self.pending_confirmations.pop(user_id, None)
        
        # Подтверждение очистки БД
        elif callback_data == "confirm_clear_db":
            if user_id not in self.admin_ids:
                await query.answer("❌ Доступ запрещен", show_alert=True)
                return
            
            await query.edit_message_text(
                "✅ Первое подтверждение получено.\n\n"
                "Отправьте команду /clear_db еще раз для окончательного подтверждения.",
                parse_mode='HTML'
            )
        
        # Отмена действия
        elif callback_data in ["cancel_action", "cancel_clear_db"]:
            self.pending_confirmations.pop(user_id, None)
            await query.edit_message_text(
                "❌ Действие отменено",
                parse_mode='HTML'
            )
    
    async def handle_accept_order(self, query, order_id: str, order_code: str):
        """Обработка принятия заказа"""
        try:
            await query.edit_message_text(
                f"⏳ Принимаю заказ #{order_code}...",
                parse_mode='HTML'
            )
            
            # Принимаем заказ через API
            result = await self.order_service.accept_order(order_id, order_code)
            
            if result:
                await query.edit_message_text(
                    f"✅ <b>Заказ #{order_code} успешно принят!</b>\n\n"
                    f"Статус изменен на: ACCEPTED_BY_MERCHANT",
                    parse_mode='HTML'
                )
                logger.info(f"Заказ {order_code} принят через бота")
            else:
                await query.edit_message_text(
                    f"❌ Ошибка при принятии заказа #{order_code}\n"
                    f"Попробуйте позже или проверьте статус в личном кабинете",
                    parse_mode='HTML'
                )
        except Exception as e:
            logger.error(f"Ошибка при принятии заказа {order_code}: {e}")
            await query.edit_message_text(
                f"❌ Произошла ошибка при принятии заказа #{order_code}:\n{str(e)}",
                parse_mode='HTML'
            )
    
    async def handle_cancel_order(self, query, order_id: str, order_code: str, reason: str):
        """Обработка отмены заказа (только для админа)"""
        try:
            await query.edit_message_text(
                f"⏳ Отменяю заказ #{order_code}...",
                parse_mode='HTML'
            )
            
            # Отменяем заказ через API
            result = await self.order_service.cancel_order(order_id, order_code, reason)
            
            if result:
                reason_text = {
                    'BUYER_CANCELLATION_BY_MERCHANT': 'Отказ покупателя',
                    'BUYER_NOT_REACHABLE': 'Не удалось связаться с покупателем',
                    'MERCHANT_OUT_OF_STOCK': 'Товара нет в наличии'
                }.get(reason, reason)
                
                await query.edit_message_text(
                    f"✅ <b>Заказ #{order_code} отменен</b>\n\n"
                    f"Причина: {reason_text}\n"
                    f"Статус изменен на: CANCELLED",
                    parse_mode='HTML'
                )
                logger.info(f"Заказ {order_code} отменен администратором. Причина: {reason}")
            else:
                await query.edit_message_text(
                    f"❌ Ошибка при отмене заказа #{order_code}\n"
                    f"Возможно заказ уже в другом статусе",
                    parse_mode='HTML'
                )
        except Exception as e:
            logger.error(f"Ошибка при отмене заказа {order_code}: {e}")
            await query.edit_message_text(
                f"❌ Произошла ошибка при отмене заказа #{order_code}:\n{str(e)}",
                parse_mode='HTML'
            )
    
    async def handle_create_waybill(self, query, order_id: str, order_code: str):
        """Обработка формирования накладной"""
        try:
            await query.edit_message_text(
                f"⏳ Проверяю статус заказа #{order_code}...",
                parse_mode='HTML'
            )
            
            # Проверяем текущий статус заказа
            current_status = await self.order_service.check_order_status(order_id, order_code)
            
            if not current_status:
                await query.edit_message_text(
                    f"❌ Не удалось получить информацию о заказе #{order_code}",
                    parse_mode='HTML'
                )
                return
            
            # Проверяем можно ли формировать накладную
            status = current_status.get('status')
            state = current_status.get('state')
            waybill_url = current_status.get('waybill_url')
            
            # Если накладная уже сформирована
            if status == 'ASSEMBLE' or waybill_url:
                message = f"ℹ️ <b>Накладная для заказа #{order_code} уже сформирована</b>\n\n"
                message += f"Статус: {status}\n"
                
                if waybill_url:
                    message += "\nНакладная доступна:"
                    keyboard = [[
                        InlineKeyboardButton("📄 Скачать онлайн", url=waybill_url),
                        InlineKeyboardButton("📥 Получить PDF", callback_data=f"download_waybill:{order_code}")
                    ]]
                    await query.edit_message_text(
                        message,
                        parse_mode='HTML',
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                    
                    # Скачиваем и сохраняем PDF если его еще нет в БД
                    if not self.order_service.db.get_order_waybill_pdf(order_code):
                        await self.download_and_send_waybill(waybill_url, order_code, query.message.chat_id)
                else:
                    message += "\nНакладная будет доступна в личном кабинете Kaspi."
                    await query.edit_message_text(message, parse_mode='HTML')
                
                logger.info(f"Накладная для заказа {order_code} уже была сформирована")
                return
            
            # Проверяем что заказ принят
            if status == 'APPROVED_BY_BANK':
                await query.edit_message_text(
                    f"⚠️ <b>Заказ #{order_code} еще не принят</b>\n\n"
                    f"Сначала примите заказ, затем можно формировать накладную.",
                    parse_mode='HTML'
                )
                return
            
            # Проверяем что заказ не завершен
            if status in ['COMPLETED', 'CANCELLED', 'CANCELLING']:
                await query.edit_message_text(
                    f"❌ <b>Заказ #{order_code} уже завершен</b>\n\n"
                    f"Статус: {status}\n"
                    f"Формирование накладной невозможно.",
                    parse_mode='HTML'
                )
                return
            
            # Формируем накладную
            await query.edit_message_text(
                f"⏳ Формирую накладную для заказа #{order_code}...",
                parse_mode='HTML'
            )
            
            # Запрашиваем количество мест
            number_of_spaces = 1
            
            # Формируем накладную через API
            result = await self.order_service.create_waybill(order_id, number_of_spaces)
            
            if result:
                # Получаем URL накладной
                waybill_url = result.get('waybill_url')
                
                success_message = (
                    f"✅ <b>Накладная для заказа #{order_code} сформирована!</b>\n\n"
                    f"Количество мест: {number_of_spaces}\n"
                    f"Статус изменен на: ASSEMBLE (Передача)\n\n"
                )
                
                # Если есть URL накладной, добавляем кнопки и скачиваем PDF
                if waybill_url:
                    success_message += "Накладная доступна:"
                    keyboard = [[
                        InlineKeyboardButton("📄 Скачать онлайн", url=waybill_url),
                        InlineKeyboardButton("📥 Получить PDF", callback_data=f"download_waybill:{order_code}")
                    ]]
                    await query.edit_message_text(
                        success_message,
                        parse_mode='HTML',
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                    
                    # Скачиваем и сохраняем PDF
                    await self.download_and_send_waybill(waybill_url, order_code, query.message.chat_id)
                else:
                    success_message += "Накладная будет доступна в личном кабинете Kaspi через несколько минут."
                    await query.edit_message_text(
                        success_message,
                        parse_mode='HTML'
                    )
                
                logger.info(f"Накладная для заказа {order_code} сформирована через бота")
            else:
                await query.edit_message_text(
                    f"❌ Ошибка при формировании накладной для заказа #{order_code}\n"
                    f"Проверьте статус в личном кабинете",
                    parse_mode='HTML'
                )
        except Exception as e:
            logger.error(f"Ошибка при формировании накладной {order_code}: {e}")
            await query.edit_message_text(
                f"❌ Произошла ошибка при формировании накладной #{order_code}:\n{str(e)}",
                parse_mode='HTML'
            )
    
    async def send_startup_message(self, context: ContextTypes.DEFAULT_TYPE):
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
                
                # Фильтруем заказы - отправляем уведомления только для активных
                active_statuses = [
                    'APPROVED_BY_BANK',      # Новый заказ, ждет принятия
                    'ACCEPTED_BY_MERCHANT',  # Принят продавцом
                    'ASSEMBLE',              # Передача в доставку
                    'PICKUP'                 # Готов к выдаче
                ]
                
                orders_to_notify = [
                    order for order in new_orders 
                    if order.get('status') in active_statuses
                ]
                
                orders_archived = len(new_orders) - len(orders_to_notify)
                
                logger.info(f"Заказов для уведомления: {len(orders_to_notify)}")
                if orders_archived > 0:
                    logger.info(f"Пропущено архивных/завершенных заказов: {orders_archived}")
                
                # СНАЧАЛА сохраняем ВСЕ заказы и отмечаем как обработанные
                for order in new_orders:
                    self.order_service.save_order_to_db(order)
                    self.order_service.mark_order_notified(order['code'])
                
                # ПОТОМ отправляем уведомления ТОЛЬКО для активных заказов
                for order in orders_to_notify:
                    try:
                        await self.send_order_notification(order)
                        logger.info(f"Уведомление отправлено для заказа {order['code']}")
                        # Небольшая задержка между сообщениями чтобы избежать flood control
                        import asyncio
                        await asyncio.sleep(1)
                    except Exception as e:
                        logger.error(f"Ошибка при отправке уведомления о заказе {order['code']}: {e}")
                
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
        self.application.add_handler(CommandHandler("waybills", self.waybills_command))
        
        # Админские команды
        self.application.add_handler(CommandHandler("cancel_order", self.cancel_order_command))
        self.application.add_handler(CommandHandler("clear_db", self.clear_db_command))
        
        # Добавляем обработчик callback кнопок
        self.application.add_handler(CallbackQueryHandler(self.button_callback))
        
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
        
        # Планируем отправку приветственного сообщения
        self.application.job_queue.run_once(self.send_startup_message, when=2)
        
        self.application.run_polling(allowed_updates=Update.ALL_TYPES)