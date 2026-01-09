#!/usr/bin/env python3
"""
Приложение для генерации случайных данных из PostgreSQL и отправки в Logstash
Работает в cluster1 с PostgreSQL
"""

import os
import time
import json
import random
import string
import requests
from datetime import datetime
from decimal import Decimal
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация из переменных окружения
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres-a-svc.dbs.svc.cluster.local')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
POSTGRES_DB = os.getenv('POSTGRES_DB', 'db_a')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'user_a')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'user_a_pass')
POSTGRES_SOURCE = os.getenv('POSTGRES_SOURCE', 'postgres-a')  # Источник: postgres-a или postgres-b

LOGSTASH_HOST = os.getenv('LOGSTASH_HOST', '192.168.0.42')  # IP хоста
LOGSTASH_PORT = int(os.getenv('LOGSTASH_PORT', '8080'))
LOGSTASH_URL = f'http://{LOGSTASH_HOST}:{LOGSTASH_PORT}'

INTERVAL_SECONDS = int(os.getenv('INTERVAL_SECONDS', '60'))  # Интервал отправки данных

def connect_postgres():
    """Подключается к PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            connect_timeout=5
        )
        logger.info(f"Успешное подключение к PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Ошибка подключения к PostgreSQL: {e}")
        raise

def get_table_names():
    """Определяет имена таблиц в зависимости от базы данных"""
    if POSTGRES_DB == 'db_a':
        return ('a_table1', 'a_table2')
    elif POSTGRES_DB == 'db_b':
        return ('b_table1', 'b_table2')
    else:
        # По умолчанию для db_a
        return ('a_table1', 'a_table2')

def generate_and_insert_data(conn):
    """Генерирует и вставляет случайные данные в PostgreSQL"""
    try:
        cursor = conn.cursor()
        table1_name, table2_name = get_table_names()
        
        # Генерация случайных данных
        random_info = ''.join(random.choices(string.ascii_letters + string.digits, k=15))
        random_value = random.randint(1, 10000)
        
        # Вставка в первую таблицу (a_table1 или b_table1)
        if POSTGRES_DB == 'db_a':
            cursor.execute(
                f"INSERT INTO {table1_name} (info, created_at) VALUES (%s, %s) RETURNING id, info, created_at",
                (random_info, datetime.utcnow())
            )
            result1 = cursor.fetchone()
            
            # Вставка во вторую таблицу (a_table2)
            cursor.execute(
                f"INSERT INTO {table2_name} (value, created_at) VALUES (%s, %s) RETURNING id, value, created_at",
                (random_value, datetime.utcnow())
            )
            result2 = cursor.fetchone()
        else:  # db_b
            cursor.execute(
                f"INSERT INTO {table1_name} (note, created_at) VALUES (%s, %s) RETURNING id, note, created_at",
                (random_info, datetime.utcnow())
            )
            result1 = cursor.fetchone()
            
            # Вставка во вторую таблицу (b_table2)
            cursor.execute(
                f"INSERT INTO {table2_name} (amount, created_at) VALUES (%s, %s) RETURNING id, amount, created_at",
                (float(random_value) / 100, datetime.utcnow())
            )
            result2 = cursor.fetchone()
        
        conn.commit()
        
        # Формируем данные в зависимости от базы
        if POSTGRES_DB == 'db_a':
            data = {
                table1_name: {
                    'id': result1[0],
                    'info': result1[1],
                    'created_at': result1[2].isoformat() if result1[2] else None
                },
                table2_name: {
                    'id': result2[0],
                    'value': result2[1],
                    'created_at': result2[2].isoformat() if result2[2] else None
                }
            }
        else:  # db_b
            data = {
                table1_name: {
                    'id': result1[0],
                    'note': result1[1],
                    'created_at': result1[2].isoformat() if result1[2] else None
                },
                table2_name: {
                    'id': result2[0],
                    'amount': result2[1],
                    'created_at': result2[2].isoformat() if result2[2] else None
                }
            }
        
        logger.info(f"Данные вставлены в PostgreSQL: {table1_name}_id={result1[0]}, {table2_name}_id={result2[0]}")
        return data
        
    except psycopg2.Error as e:
        logger.error(f"Ошибка работы с PostgreSQL: {e}")
        conn.rollback()
        raise

def read_recent_data(conn):
    """Читает последние данные из PostgreSQL"""
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        table1_name, table2_name = get_table_names()
        
        # Читаем последние записи из первой таблицы
        if POSTGRES_DB == 'db_a':
            cursor.execute(f"""
                SELECT id, info, created_at 
                FROM {table1_name} 
                ORDER BY created_at DESC 
                LIMIT 5
            """)
            table1_data = [dict(row) for row in cursor.fetchall()]
            
            cursor.execute(f"""
                SELECT id, value, created_at 
                FROM {table2_name} 
                ORDER BY created_at DESC 
                LIMIT 5
            """)
            table2_data = [dict(row) for row in cursor.fetchall()]
        else:  # db_b
            cursor.execute(f"""
                SELECT id, note, created_at 
                FROM {table1_name} 
                ORDER BY created_at DESC 
                LIMIT 5
            """)
            table1_data = [dict(row) for row in cursor.fetchall()]
            
            cursor.execute(f"""
                SELECT id, amount, created_at 
                FROM {table2_name} 
                ORDER BY created_at DESC 
                LIMIT 5
            """)
            table2_data = [dict(row) for row in cursor.fetchall()]
        
        # Преобразуем datetime и Decimal в строки
        for row in table1_data:
            if row.get('created_at'):
                row['created_at'] = row['created_at'].isoformat()
            # Преобразуем Decimal в float для JSON сериализации
            for key, value in row.items():
                if isinstance(value, Decimal):
                    row[key] = float(value)
        
        for row in table2_data:
            if row.get('created_at'):
                row['created_at'] = row['created_at'].isoformat()
            # Преобразуем Decimal в float для JSON сериализации
            for key, value in row.items():
                if isinstance(value, Decimal):
                    row[key] = float(value)
        
        return {
            table1_name: table1_data,
            table2_name: table2_data
        }
        
    except psycopg2.Error as e:
        logger.error(f"Ошибка чтения из PostgreSQL: {e}")
        table1_name, table2_name = get_table_names()
        return {table1_name: [], table2_name: []}

def send_to_logstash(data):
    """Отправляет данные в Logstash"""
    try:
        # Отправляем данные из каждой таблицы отдельно с метаданными
        for table_name, table_data in data.items():
            if table_data:  # Отправляем только если есть данные
                log_entry = {
                    '@timestamp': datetime.utcnow().isoformat(),
                    'cluster': 'cluster1',
                    'database': 'postgresql',
                    'db_name': POSTGRES_DB,
                    'table_name': table_name,  # Название таблицы
                    'source': POSTGRES_SOURCE,  # Источник данных (postgres-a или postgres-b)
                    'data': table_data,
                    'message': f"PostgreSQL data from {POSTGRES_SOURCE}/{POSTGRES_DB}.{table_name}"
                }
                
                response = requests.post(
                    LOGSTASH_URL,
                    json=log_entry,
                    headers={'Content-Type': 'application/json'},
                    timeout=5
                )
                response.raise_for_status()
                logger.info(f"Данные отправлены в Logstash из таблицы {table_name}")
        
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка отправки в Logstash: {e}")
        return False

def ensure_connection(conn):
    """Проверяет и переподключается к PostgreSQL при необходимости"""
    try:
        # Проверяем, живо ли соединение
        if conn.closed:
            logger.warning("Соединение с PostgreSQL закрыто, переподключаемся...")
            return connect_postgres()
        # Проверяем соединение простым запросом
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        return conn
    except (psycopg2.Error, AttributeError) as e:
        logger.warning(f"Соединение с PostgreSQL не работает: {e}, переподключаемся...")
        try:
            if conn and not conn.closed:
                conn.close()
        except:
            pass
        return connect_postgres()

def main():
    """Основной цикл приложения"""
    logger.info("Запуск приложения для генерации данных из PostgreSQL")
    logger.info(f"Конфигурация:")
    logger.info(f"  PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    logger.info(f"  Logstash: {LOGSTASH_URL}")
    logger.info(f"  Интервал: {INTERVAL_SECONDS} секунд")
    
    # Подключение к PostgreSQL
    conn = None
    try:
        conn = connect_postgres()
    except Exception as e:
        logger.error(f"Не удалось подключиться к PostgreSQL: {e}")
        return
    
    # Основной цикл
    iteration = 0
    while True:
        try:
            iteration += 1
            logger.info(f"Итерация {iteration}")
            
            # Проверяем и восстанавливаем соединение при необходимости
            try:
                conn = ensure_connection(conn)
            except Exception as e:
                logger.error(f"Не удалось восстановить соединение с PostgreSQL: {e}")
                time.sleep(5)  # Ждём перед повторной попыткой
                continue
            
            # Генерация и вставка новых данных
            try:
                new_data = generate_and_insert_data(conn)
            except Exception as e:
                logger.error(f"Ошибка генерации данных: {e}")
                # Сбрасываем соединение при ошибке
                try:
                    conn.close()
                except:
                    pass
                conn = None
                continue
            
            # Чтение последних данных
            try:
                recent_data = read_recent_data(conn)
                table1_name, table2_name = get_table_names()
                logger.info(f"Прочитано {len(recent_data.get(table1_name, []))} записей из {table1_name}, {len(recent_data.get(table2_name, []))} из {table2_name}")
                
                # Отправка данных в Logstash
                send_to_logstash(recent_data)
                
            except Exception as e:
                logger.error(f"Ошибка чтения данных: {e}")
                # Сбрасываем соединение при ошибке
                try:
                    conn.close()
                except:
                    pass
                conn = None
                continue
            
            # Ожидание перед следующей итерацией
            time.sleep(INTERVAL_SECONDS)
            
        except KeyboardInterrupt:
            logger.info("Получен сигнал остановки")
            break
        except Exception as e:
            logger.error(f"Неожиданная ошибка: {e}")
            time.sleep(INTERVAL_SECONDS)
    
    # Закрытие подключения
    if conn and not conn.closed:
        try:
            conn.close()
        except:
            pass
    logger.info("Приложение остановлено")

if __name__ == '__main__':
    main()

