#!/bin/bash
# Скрипт для развертывания ELK стека в cluster3

set -e

echo "=== Развертывание ELK стека в cluster3 ==="

# Проверка текущего контекста
echo "Текущий контекст Kubernetes:"
kubectl config current-context
read -p "Продолжить? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
fi

# Применение манифестов в правильном порядке
echo ""
echo "1. Создание namespace elk..."
kubectl apply -f namespace.yaml

echo ""
echo "2. Создание Secrets..."
kubectl apply -f secrets.yaml

echo ""
echo "3. Создание ConfigMap для Logstash..."
kubectl apply -f configmap-logstash.yaml

echo ""
echo "4. Развертывание Elasticsearch (StatefulSet с автоматическим созданием PVC)..."
kubectl apply -f elasticsearch.yaml

echo ""
echo "5. Ожидание готовности Elasticsearch (максимум 5 минут)..."
kubectl wait --for=condition=ready pod -l app=elasticsearch -n elk --timeout=300s || {
    echo "Elasticsearch не готов. Проверьте логи:"
    kubectl logs -l app=elasticsearch -n elk --tail=50
    exit 1
}

echo ""
echo "6. Развертывание Logstash..."
kubectl apply -f logstash.yaml

echo ""
echo "7. Развертывание Kibana..."
kubectl apply -f kibana.yaml

echo ""
echo "=== Развертывание завершено ==="
echo ""
echo "Проверка статуса подов в namespace elk:"
kubectl get pods -n elk

echo ""
echo "Проверка сервисов в namespace elk:"
kubectl get svc -n elk

echo ""
echo "Для доступа к Kibana используйте:"
NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
echo "  http://${NODE_IP}:30601"
echo ""
echo "Логин: elastic"
echo "Пароль: elasticadmin"
echo ""
echo "Все компоненты развернуты в namespace 'elk'"

