#!/usr/bin/env python3
"""
Приложение для генерации случайных данных из MongoDB и отправки в Logstash
Работает в cluster2 с MongoDB
"""

import os
import time
import json
import random
import string
import requests
from datetime import datetime
from bson import ObjectId
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация из переменных окружения
MONGO_HOST = os.getenv('MONGO_HOST', 'svc-mongo-a.mongo.svc.cluster.local')
MONGO_PORT = int(os.getenv('MONGO_PORT', '27017'))
MONGO_DB = os.getenv('MONGO_DB', 'dbA')
MONGO_USER = os.getenv('MONGO_USER', 'userA')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'passwordA')
MONGO_SOURCE = os.getenv('MONGO_SOURCE', 'mongo-a')  # Источник: mongo-a или mongo-b

LOGSTASH_HOST = os.getenv('LOGSTASH_HOST', '192.168.0.42')  # IP хоста
LOGSTASH_PORT = int(os.getenv('LOGSTASH_PORT', '8080'))
LOGSTASH_URL = f'http://{LOGSTASH_HOST}:{LOGSTASH_PORT}'

INTERVAL_SECONDS = int(os.getenv('INTERVAL_SECONDS', '60'))  # Интервал отправки данных (60 секунд = 1 минута)

def generate_random_data():
    """Генерирует случайные данные для вставки в MongoDB (соответствует структуре из practice4)"""
    return {
        'key': ''.join(random.choices(string.ascii_letters + string.digits, k=10)),
        'createdAt': datetime.utcnow()
    }

def connect_mongo():
    """Подключается к MongoDB"""
    try:
        # Для репликасета добавляем параметры, для standalone - обычное подключение
        if 'mongo-b' in MONGO_SOURCE.lower():
            # Репликасет: указываем replicaSet и все хосты
            connection_string = f'mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource={MONGO_DB}&replicaSet=rs-b'
        else:
            # Standalone
            connection_string = f'mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource={MONGO_DB}'
        
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        # Проверка подключения
        client.admin.command('ping')
        db = client[MONGO_DB]
        logger.info(f"Успешное подключение к MongoDB: {MONGO_HOST}:{MONGO_PORT}/{MONGO_DB} (source: {MONGO_SOURCE})")
        return client, db
    except ConnectionFailure as e:
        logger.error(f"Ошибка подключения к MongoDB: {e}")
        raise
    except OperationFailure as e:
        logger.error(f"Ошибка аутентификации MongoDB: {e}")
        raise

def serialize_mongo_doc(doc):
    """Сериализует документ MongoDB для JSON (преобразует ObjectId и datetime)"""
    if isinstance(doc, dict):
        result = {}
        for key, value in doc.items():
            if isinstance(value, ObjectId):
                result[key] = str(value)
            elif isinstance(value, datetime):
                result[key] = value.isoformat()
            elif isinstance(value, dict):
                result[key] = serialize_mongo_doc(value)
            elif isinstance(value, list):
                result[key] = [serialize_mongo_doc(item) for item in value]
            else:
                result[key] = value
        return result
    elif isinstance(doc, ObjectId):
        return str(doc)
    elif isinstance(doc, datetime):
        return doc.isoformat()
    elif isinstance(doc, list):
        return [serialize_mongo_doc(item) for item in doc]
    else:
        return doc

def send_to_logstash(data):
    """Отправляет данные в Logstash"""
    try:
        # Сериализуем данные для JSON
        serialized_data = serialize_mongo_doc(data)
        
        # Добавляем метаданные для Logstash
        log_entry = {
            '@timestamp': datetime.utcnow().isoformat(),
            'cluster': 'cluster2',
            'database': 'mongodb',
            'db_name': MONGO_DB,
            'collection_name': 'sample',  # Название коллекции
            'source': MONGO_SOURCE,  # Источник: mongo-a или mongo-b
            'data': serialized_data,
            'message': f"MongoDB data from {MONGO_SOURCE}/{MONGO_DB}.sample: key={serialized_data.get('key', 'unknown')}"
        }
        
        response = requests.post(
            LOGSTASH_URL,
            json=log_entry,
            headers={'Content-Type': 'application/json'},
            timeout=5
        )
        response.raise_for_status()
        logger.info(f"Данные отправлены в Logstash из {MONGO_SOURCE}: key={serialized_data.get('key', 'unknown')}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка отправки в Logstash: {e}")
        return False

def main():
    """Основной цикл приложения"""
    logger.info("Запуск приложения для генерации данных из MongoDB")
    logger.info(f"Конфигурация:")
    logger.info(f"  MongoDB: {MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}")
    logger.info(f"  Источник: {MONGO_SOURCE}")
    logger.info(f"  Logstash: {LOGSTASH_URL}")
    logger.info(f"  Интервал: {INTERVAL_SECONDS} секунд (5 записей за раз)")
    
    # Подключение к MongoDB
    try:
        mongo_client, db = connect_mongo()
    except Exception as e:
        logger.error(f"Не удалось подключиться к MongoDB: {e}")
        return
    
    collection = db.sample
    
    # Основной цикл
    iteration = 0
    while True:
        try:
            iteration += 1
            logger.info(f"Итерация {iteration}")
            
            # Генерация случайных данных
            new_data = generate_random_data()
            
            # Вставка в MongoDB
            try:
                result = collection.insert_one(new_data)
                logger.info(f"Данные вставлены в MongoDB: {result.inserted_id}")
            except Exception as e:
                logger.error(f"Ошибка вставки в MongoDB: {e}")
            
            # Чтение последних 5 документов из коллекции
            try:
                recent_docs = list(collection.find().sort('_id', -1).limit(5))
                logger.info(f"Найдено {len(recent_docs)} документов в коллекции")
                
                # Отправка последних документов в Logstash
                for doc in recent_docs:
                    send_to_logstash(doc)
                    
            except Exception as e:
                logger.error(f"Ошибка чтения из MongoDB: {e}")
            
            # Ожидание перед следующей итерацией
            time.sleep(INTERVAL_SECONDS)
            
        except KeyboardInterrupt:
            logger.info("Получен сигнал остановки")
            break
        except Exception as e:
            logger.error(f"Неожиданная ошибка: {e}")
            time.sleep(INTERVAL_SECONDS)
    
    # Закрытие подключения
    mongo_client.close()
    logger.info("Приложение остановлено")

if __name__ == '__main__':
    main()

