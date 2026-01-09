#!/bin/bash
# Скрипт для сборки и загрузки образа postgres-data-generator в minikube cluster1

set -e

CLUSTER_NAME="cluster1"
IMAGE_NAME="postgres-data-generator"
IMAGE_TAG="latest"

echo "=== Сборка и загрузка образа ${IMAGE_NAME}:${IMAGE_TAG} в minikube ${CLUSTER_NAME} ==="

# Проверка, что minikube cluster1 существует
if ! minikube profile list | grep -q "${CLUSTER_NAME}"; then
    echo "Ошибка: minikube профиль ${CLUSTER_NAME} не найден"
    exit 1
fi

# Сборка образа локально
echo "Сборка образа ${IMAGE_NAME}:${IMAGE_TAG}..."
docker build -t ${IMAGE_NAME}:${IMAGE_TAG} .

# Загрузка образа в minikube
echo "Загрузка образа в minikube ${CLUSTER_NAME}..."
minikube -p ${CLUSTER_NAME} image load ${IMAGE_NAME}:${IMAGE_TAG}

# Проверка, что образ собран
if ! docker images | grep -q "${IMAGE_NAME}"; then
    echo "Ошибка: образ ${IMAGE_NAME}:${IMAGE_TAG} не найден после сборки"
    exit 1
fi

echo "Образ ${IMAGE_NAME}:${IMAGE_TAG} успешно собран и загружен в minikube ${CLUSTER_NAME}"
echo ""
echo "Для применения манифестов используйте:"
echo "  kubectl --context=${CLUSTER_NAME} apply -f ../k8s/postgres-a-data-generator.yaml"
echo "  kubectl --context=${CLUSTER_NAME} apply -f ../k8s/postgres-b-data-generator.yaml"
