# Метрики для мониторинга

Необходимо собирать самые разнообразные метрики:

- Стандартные метрики Kafka
- Общие метрики микросервисов
- Метрики прохождения Saga
- Метрики Stream Processor
- Метрики инфраструктуры
- Бизнес-метрики

Подробно рассмотрим бизнес-метрики.

Бронирования и поездки:

- Созданные бронирования: bookings_created_total{region, status}
- Активные поездки: bookings_active_count{region} (gauge)
- Завершенные поездки: bookings_completed_total{region}
- Отмененные бронирования: bookings_cancelled_total{region, reason}
- Просроченные бронирования: bookings_expired_total{region}
- Средняя длительность поездки: booking_duration_seconds{region} (histogram)
- Время ожидания водителя: booking_driver_wait_time_seconds{region} (histogram)
- Время от создания до начала поездки: booking_to_start_duration_seconds{region} (histogram)

Водители:

- Онлайн водители: drivers_online_count{region} (gauge)
- Доступные водители: drivers_available_count{region} (gauge)
- Занятые водители: drivers_busy_count{region} (gauge)
- Новые регистрации: drivers_registered_total{region}
- Изменения статуса: drivers_status_changes_total{region, status_from, status_to}
- Обновления геолокации: drivers_location_updates_total{region} (высокая частота)
- Плотность водителей по регионам: drivers_density_per_km2{region} (gauge, из Flink)

Платежи:

- Инициированные платежи: payments_initiated_total{region, payment_method}
- Успешные платежи: payments_processed_total{region, payment_method}
- Неудачные платежи: payments_failed_total{region, payment_method, error_type}
- Возвраты: payments_refunded_total{region, reason}
- Сумма платежей: payments_amount_total{region, currency} (counter)
- Средний чек: payments_average_amount{region, currency} (gauge)
- Время обработки платежа: payment_processing_duration_seconds{region} (histogram)

Выплаты водителям:

- Инициированные выплаты: payouts_initiated_total{region, driver_id}
- Успешные выплаты: payouts_completed_total{region}
- Неудачные выплаты: payouts_failed_total{region, error_type}
- Сумма выплат: payouts_amount_total{region, currency} (counter)
- Средняя выплата: payouts_average_amount{region, currency} (gauge)
- Время обработки выплаты: payout_processing_duration_seconds{region} (histogram)

Ценообразование:

- Рассчитанные цены: prices_calculated_total{region}
- Активации surge pricing: surge_pricing_activated_total{region, surge_multiplier}
- Средний коэффициент surge: surge_pricing_multiplier_avg{region} (gauge)
- Средняя цена поездки: trip_price_avg{region, currency} (gauge)
- Максимальная цена поездки: trip_price_max{region, currency} (gauge)
- Минимальная цена поездки: trip_price_min{region, currency} (gauge)

Геолокация и маршрутизация:

- Рассчитанные маршруты: routes_calculated_total{region}
- Обновления ETA: eta_updates_total{region}
- Среднее время в пути: route_duration_avg_seconds{region} (gauge)
- Средняя длина маршрута: route_distance_avg_km{region} (gauge)
- Время расчета маршрута: route_calculation_duration_seconds{region} (histogram)

Уведомления:

- Отправленные уведомления: notifications_sent_total{region, notification_type, channel}
- Доставленные уведомления: notifications_delivered_total{region, notification_type, channel}
- Неудачные отправки: notifications_failed_total{region, notification_type, channel, error_type}
- Открытые уведомления: notifications_opened_total{region, notification_type} (если доступно)
- Время доставки: notification_delivery_duration_seconds{region, channel} (histogram)

Обнаружение мошенничества:

- Запрошенные проверки: fraud_checks_requested_total{region, check_type}
- Обнаруженные случаи фрода: fraud_detected_total{region, fraud_type, severity}
- Успешные проверки: fraud_checks_passed_total{region}
- Заблокированные транзакции: fraud_transactions_blocked_total{region, fraud_type}
- Ложные срабатывания: fraud_false_positives_total{region} (если доступно)
- Время проверки на фрод: fraud_check_duration_seconds{region} (histogram)

Аналитика и агрегации:

- Активные поездки по регионам: analytics_active_trips_by_region{region} (gauge, из Flink)
- Средняя загрузка водителей: analytics_driver_utilization_avg{region} (gauge, из Flink)
- Коэффициент конверсии бронирований: analytics_booking_conversion_rate{region} (gauge, из Flink)
- Средний рейтинг водителей: analytics_driver_rating_avg{region} (gauge)
- Средний рейтинг пассажиров: analytics_passenger_rating_avg{region} (gauge)
