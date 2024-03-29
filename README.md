# Проект 5-го спринта

### Описание
Репозиторий предназначен для сдачи проекта 5-го спринта.
Проект построения DWH из нескольких источников через инкрементальную загрузку данных ETL-скриптом в Airflow
В результате формируются витрина расчётов с курьерами и витрина расчетов с ресторанами
Стайлдж слой заполняетя данными из MongoDB, PostgeSQL и S3
DDS слой заполняется инкрементально

### Структура репозитория
Внутри `src` расположены три папки:
- `/src/dags`;
- `/src/sql`;
- `/src/scripts`.

### Шаги/ход исследования
- Подготовлен ДАГ main_stage.py. ДАГ проверяет наличие таблиц в схеме STAGE и загружает в stage данные из PostgreSQL, MongoDB и S3. 
  Код загрузки данных вынесен в отдельный модуль /src/scripts/Stage_upload.py
- Подготовлен ДАГ main_dds.py. ДАГ проверяет наличие таблиц в схеме DDS и загружает в dds данные о ресторанах, клиентах, времени заказа, продуктах, курьерах и событиях
  Загрузка выполняется инкрементально, код обработки данных выделен в отдельный модуль /src/scripts/DDS_upload.py
- Выполнен ДАГ main_cdm.py. ДАГ проверяет наличие витрин в схеме CDM и инкрементально заполняет витрину расчета с курьерами и витрину расчета с ресторанами

### Инструменты
Python, S3, MongoDB, Airflow, PostgreSQL, ETL

### Выводы
Подготовлен ETL процесс загружающий данные из MongoDB, PostgeSQL и S3 в STAGE, DDS и формирующий витрины расчета с курьерами и ресторанами
