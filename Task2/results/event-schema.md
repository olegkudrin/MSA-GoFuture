# Схема доменных событий и топиков Kafka

## Доменные события по сервисам

Booking Service:

- BookingCreated - создано новое бронирование
- BookingAccepted - водитель принял заказ
- BookingStarted - поездка началась
- BookingCompleted - поездка завершена
- BookingCancelled - бронирование отменено
- BookingExpired - бронирование просрочено (тайм-аут)

Driver Service:

- DriverRegistered - новый водитель зарегистрирован
- DriverStatusChanged - изменился статус водителя (online, offline, busy)
- DriverLocationUpdated - обновлена геолокация водителя
- DriverAssigned - водитель назначен на заказ
- DriverArrived - водитель прибыл к пассажиру

Pricing Service:

- PriceCalculated - рассчитана и зафиксирована цена поездки
- SurgePriceActivated - активировано повышение цены (surge pricing)

Payments Service:

- PaymentInitiated - инициирован платеж
- PaymentProcessed - платеж успешно обработан
- PaymentFailed - платеж не прошел
- PaymentRefunded - платеж возвращен

Payouts Service:

- PayoutInitiated - инициирована выплата водителю
- PayoutCompleted - выплата успешно завершена
- PayoutFailed - выплата не прошла

Geography Service:

- RouteCalculated - рассчитан маршрут
- ETAUpdated - обновлено расчетное время прибытия

Fraud Service:

- FraudCheckRequested - запрошена проверка на мошенничество
- FraudDetected - обнаружено мошенничество
- FraudCheckPassed - проверка пройдена

Notification Service:

- NotificationSent - уведомление отправлено
- NotificationFailed - отправка уведомления не удалась

## Схема топиков Kafka

Принципы именования:

- Формат: domain.event-type.region
- Регионы: europe, asia, south-america, etc.
- Для глобальных событий: domain.event-type

### Топики по доменам

Booking Domain:

- booking.created.{region} - создание бронирований
- booking.lifecycle.{region} - жизненный цикл (accepted, started, completed, cancelled, expired)
- Партиционирование: по bookingId (гарантия порядка обработки)

Driver Domain:

- driver.registration.{region} - регистрация новых водителей
- driver.status.{region} - изменения статуса водителей
- driver.location.{region} - обновление геолокации (высокая частота)
- Партиционирование: по driverId для registration и status, по region для location

Pricing Domain:

- pricing.calculated.{region} - расчеты и фиксация цены при создании бронирования
- pricing.surge.{region} - активация surge pricing в регионе
- Партиционирование: по region (географическое распределение)

Payments Domain:

- payments.transactions.{region} - все транзакции платежей
- Партиционирование: по paymentId (гарантия порядка)

Payouts Domain:

- payouts.transactions.{region} - все транзакции выплат
- Партиционирование: по driverId (гарантия порядка выплкат водителю)

Geography Domain:

- geography.routes.{region} - расчеты маршрутов
- Партиционирование: по region

Fraud Domain:

- fraud.checks.{region} - проверка на мошенничество
- Партиционирование: по userId (анализ поведения пользователя)

Notification Domain:

- notifications.events - события уведомления (глобальный топик)
- Партиционирование: по userId

Analytics Domain:

- Analytics Service подписывается на все доменные топики для сбора аналитики

## Stream Processing (Apache Flink)

Stream Processor используется для обработки событий в реальном времени с использованием Apache Flink.

### Обрабатываемые события и задачи

Агрегация DriverLocationUpdated:

- Входной топик: driver.location.{region}
- Задача: Агрегация обновлений геолокации водителей для расчета плотности водителей в регионах
- Результат: Публикует агрегированные данные о плотности водителей (для использования Pricing Service при расчете surge pricing)

Расчет динамических цен:

- Входные топики: driver.location.{region}, booking.created.{region}, pricing.surge.{region}
- Задача: Анализ спроса и предложения в реальном времени для расчета динамических коэффициентов цеообразования
- Результат: Публикует обновленные коэффициенты для Pricing Service

Обнаружение аномалий:

- Входные топики: Все доменные топики
- Задача: Обнаружение аномальных паттернов в потоках событий (подозрительная активность)
- Результат: Публикует события об аномалиях для Fraud Service и мониторинга

Агрегации в реальном времени:

- Входные топики: Все доменные топики
- Задача: Создание агрегация в реальном времени для других сервисов (статистика по регионам, метрики производительности)
- Результат: Публикует агрегированные события в соответствующие топики

### Технология

- Apache Flink (PyFlink) для обработки потоков событий
- Python API для разработки stream processing приложений
- Подписывается на доменные топики Kafka через Flink Kafka Connector
- Публикует обработанные события обратно в Kafka
- Обработка в реальном времени с нихкой задержкой (< 100 ms v 95%)

### Преимущества выбора Flink

- Команда знает Python (используется Django в текущем стеке)
- Единый язык для stream processing упрощает разработку и поддержку
- Хорошая производительность для задач обработки событий
- Поддерка сложных операций: windowing, joins, stateful processing
- Горизонтальное масштабирование для обработки 500 тыс. поездок

### Примеры использования

- Windowed aggregations для расчета метрик за временные окна (tumbling, sliding, session windows)
- Join операций между потоками событий (например, объединение данных о водителях и брнированиях)
- Stateful processing для отслеживания состояния (например, текущая плотность водителей в регионе)
- Event time processing с watermarking для корректной обработки задержанных событий
- Геопростанственные агрегации для расчета плотности водителей по регионам

## Saga Events (Orchestration Pattern)

### Booking Service (Saga Orchestrator)

Booking Service выполняет оркестрацию процесса бронирования (Parallel Saga).

События для оркестрации:

- SagaBookingStarted - начало процесса бронирования (публикует Booking Service)
- SagaBookingDriverFound - найден подходящий водитель (публикует Booking Service после получения DriverAssigned)
- SagaBookingPriceCalculated - рассчитана цена (публикует Booking Service после получения PriceCalculated)
- SagaBookingPaymentProcessed - обработан платеж (публикует Booking Service после получения PaymentProcessed)
- SagaBookingCompleted - процесс завершен успешно (публикует Booking Service)
- SagaBookingCompensated - выполнена компенсация (откат) (публикует Booking Service при ошибках)

Публикует: Booking Service (встроенная оркестрация Saga)

Подписывается: Booking Service на события от Driver, Pricing, Payments для координации Saga.

- Топик: saga.booking.{region}
- Партиционирование: по bookingId (гарантия порядка шагов Saga)
