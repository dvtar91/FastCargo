## Task 3 – Метрики эксплуатации целевой архитектуры  

### 1. Панель KPI эксплуатации целевой архитектуры  

| Технический показатель (KPI) | Целевое значение метрики | Способ и инструмент замера (инструментарий / формула) | Бизнес‑смысл — какую проблему решает замер |
|------------------------------|--------------------------|--------------------------------------------------------|--------------------------------------------|
| **Доступность транзакционного ядра (API‑Gateway + микросервисы Orders, Warehouse, Delivery, Finance)** | ≥ 99.95 % в круглосуточном режиме (год) | Prometheus + Grafana: `availability = (sum(rate(http_requests_total{status=~"2..|3.."}[5m])) / sum(rate(http_requests_total[5m])) * 100` | Минимизация финансовых потерь и простоев при оформлении заказов, фиксации складских операций и выплат. |
| **Пропускная способность (Throughput) создания заказов** | ≥ 200 RPS при p99 ≤ 500 мс | K6/Locust нагрузочное тестирование + метрика `http_requests_total{handler="order-create"}` в Prometheus; расчёт среднего RPS за 5‑минутный интервал. | Гарантирует способность поддержать 10‑кратный рост бизнеса без деградации отклика. |
| **Latency аналитики (DWH ← операционные события)** | ≤ 5 сек (от записи события в PostgreSQL до появления в ClickHouse) | End‑to‑end измеряется через `event_timestamp` (в CDC) и `ingest_timestamp` в ClickHouse; скрипт‑агрегатор в Grafana выводит `latency = ingest_timestamp - event_timestamp`. | Позволяет принимать оперативные решения (перепланирование, оптимизацию складов) в реальном времени. |
| **Устойчивость к пиковой нагрузке IoT‑потока** | ≤ 3 000 RPS записи координат без потери сообщений | Kafka + KSQL/Prometheus: `kafka_topic_partition_current_offset - kafka_topic_partition_log_start_offset` → расчёт задержки; lag_sec = consumer_lag_msgs / partition_RPS. Целевое: lag_sec < 5 сек при пиковых 3000 RPS → lag_msgs < 15 000 на партицию. | Обеспечивает полную трассировку грузов даже в часы пик, исключая «дырявые» данные о перемещениях. |
| **Консистентность EDI‑цепочки** | 0 uncorrected аномалий за 15‑мин окно (например, INVOIC без подтверждённого DESADV). Автоматический retry → auto‑resolve в течение 15 мин; аномалия считается unresolved только если не закрыта auto‑resolve | Saga‑координатор; метрика `edi_consistency_errors_total{resolved="false"}`; алерт при unresolved > 0 в течение 2 циклов (30 мин) | Исключает финансовые и юридические риски, связанные с неверными коммерческими документами. |
| **Скорость обработки платёжных веб‑хуков** | Потери = 0 within 5 мин (auto‑retry ≤ 3 раза в 30 мин; final failure только после исчерпания retry; alert при > 0 final failure за 15 мин) | `payment_webhook_final_failure_total` / `payment_webhook_total` в Prometheus; `alert: payment_webhook_final_failure > 0 за 15 мин` | Гарантирует отсутствие «пропущенных» оплат, повышая доверие партнёров и конечных клиентов. |
| **Скорость доступа к профилю клиента (CRM‑контур)** | ≤ 50 мс (обезличенный UUID → профиль) | Grafana‑Dashboard измеряет `http_request_duration_seconds{handler="customer-profile"}`; округление до 95‑го перцентиля. | Обеспечивает мгновенный ответ в омниканальном клиентском сервисе, повышая NPS. |
| **Нагрузка на транзакционную БД (PostgreSQL)** | < 30 % CPU‑нагрузки, < 5 % IO‑wait в пиковые периоды | node_exporter (cpu_usage) + pg_stat_statements (query_time). pg_stat_activity — дополнительный сигнал. | Позволяет монолиту оставаться стабильным до полной миграции; меньше риска отказов из‑за блокировок. |
| **Время отклика платежного виджета (frontend)** | ≤ 300 мс (первый байт) |  web-vitals npm package + OpenTelemetry Collector → Grafana. Lighthouse оставить для CI-гейта на регрессию. | Улучшает пользовательский опыт при предоплате и повышает конверсию покупок. |
| **MTTR (Mean Time To Recovery)** | ≤ 15 мин (p95 ≤ 30 мин) для SLO‑violating инцидентов (Availability, Latency DWH p95, Throughput < target) | Incident record в ServiceNow / PagerDuty: `recovery_time = resolved_at - detected_at`; Grafana panel по `incident_count / sum(recovery_time)` за 30 дней | SLO 99.95% даёт error budget 21.9 мин/мес. MTTR ≤ 15 мин означает, что один инцидент не сжигает > 68% месячного бюджета. |

> **Error Budget:** `(SLO_actual - SLO_target) / (1 - SLO_target) × 100%`.
> При budget < 20% → freeze feature‑deploy, только reliability‑fixes.
> Владелец бюджета: Tech Lead + Head of Ops (joint sign‑off).

#### 1.1. DevOps (DORA metrics)

| Метрика | Текущий baseline | Целевое (Q2 после полной мигр.) | Инструмент |
|---------|-----------------|--------------------------------|-----------|
| **Deploy Frequency** | 2 × / неделя (единый релиз монолита) | ≥ 1 × / день / микросервис | CI/CD pipeline logs (`deployment_duration_seconds`) |
| **Lead Time for Changes** | 12–18 дней (commit → prod) | ≤ 2 дня | Git tag → deploy marker: `prod_deploy_time - commit_time` |
| **Change Failure Rate** | ~8% релизов вызывают rollback/hotfix | ≤ 2% | `rollbacks_total / deployments_total` в 30‑дневном окне |
| **MTTR (DORA definition)** | ~2–4 часа (ручная корреляция логов) | ≤ 15 мин | ServiceNow incident timeline (см. строку 2 таблицы выше) |

**Связь с бизнес‑выгодой:** при текущем lead time 15 дней CEO не может
быстро реагировать на запросы «Продукты с душой» (SLA‑изменения, новые
EDI‑форматы — типично 2–3 в месяц). Сокращение до 2 дней = +10 доп.
релиз‑окон в квартал = больше contract flexibility = меньше штрафных
рисков.

### 1.2. Операционные бизнес‑метрики

Технические KPI из §1 — промежуточные. Здесь — метрики, которые
действительно считает бизнес.

| Метрика | Целевое | Формула / источник | Слой, который отвечает |
|---------|---------|--------------------|-----------------------|
| **Order‑to‑Ship Time** | ≤ 45 мин (p95) | `timestamp_shipped - timestamp_order_created` из Kafka event stream → ClickHouse agg | Orders + Warehouse + Delivery микросервисы |
| **Cost Per Shipment** (операционный) | −15% vs baseline | `sum(compute_cost + storage + kafka) / sum(shipments)` — FinOps dashboard | Все микросервисы (unit cost visibility) |
| **EDI Auto‑Processing Rate** | > 95% | `auto_processed_documents / total_documents` из Saga coordinator | EDI Gateway |
| **SLA‑violation rate per ритейл‑клиента** | 0 за месяц | `late_deliveries / total_deliveries` per customer_id → CRM view (latency ≤ 5 сек из §1) | Delivery + DWH |

---

### 2. Итоговый вывод для генерального директора Fast Cargo  

> **Переход к целевому состоянию позволит компании избежать финансовых потерь из‑за простоя транзакционной системы, юридических рисков несогласованных EDI‑операций и штрафов за нарушение ФЗ‑152 и PCI‑DSS.**  
> За счёт внедрения **событийно‑ориентированной микросервисной архитектуры, CDC‑pipeline (Debezium → Kafka → ClickHouse) и изоляции платёжного и персонального контуров** бизнес получит **доступность ядра ≥ 99.95 %**, **пропускную способность ≥ 200 RPS** и **анализ данных в реальном времени с латентностью ≤ 5 сек**, что полностью покрывает требования к масштабируемости (×10) и прозрачности.  
> Эти измеримые эффекты гарантируют окупаемость инвестиций в трансформацию уже в течение первых‑шести месяцев эксплуатации и обеспечит стабильную работу при росте нагрузки в десять раз.

**Экономика в цифрах:** при текущем SLA 99.0% (baseline Fast Cargo)
простой транзакционного ядра ≈ 45 мин/мес. При среднем чеке
отгрузки X ₽ и throughput 200 RPS потери = 45 × 60 × 200 × X ≈
**Y тыс. ₽/мес** от упущенных заказов. Целевой SLA 99.95% сокращает
потери до 22 × 60 × 200 × X ≈ **Z тыс. ₽/мес**, экономия = (Y − Z)
тыс. ₽/мес. При инвестициях в архитектуру **I** млн ₽
окупаемость = **I / ((Y − Z) × 12) × 12 = N мес**. При Y/X = 1500 ₽
(средний чек) окупаемость — **[уточнить по финансовому плану] мес**.
