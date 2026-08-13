#!/usr/bin/env sh
# Сборка Linux-бинаря компоненты в Docker. Запускать из корня репозитория.
# Работает на macOS, Linux и в Git Bash под Windows.
# Результат: build/linux/libSimpleKafka1C_Static.so
set -e

IMAGE=kafka_1c_ubuntu
CONTAINER=temp_container_ubuntu

if [ ! -f scripts/Dockerfile.ubuntu20 ]; then
    echo "Запускать из корня репозитория (не вижу scripts/Dockerfile.ubuntu20)" >&2
    exit 1
fi

docker build -f scripts/Dockerfile.ubuntu20 -t "$IMAGE" .

docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
docker create --name "$CONTAINER" "$IMAGE" >/dev/null

mkdir -p build/linux
# ВНИМАНИЕ: внутри образа файл называется libSimpleKafka1C.so (без _Static)
docker cp "$CONTAINER":/src/build/libSimpleKafka1C.so build/linux/libSimpleKafka1C_Static.so
docker rm "$CONTAINER" >/dev/null

echo "Готово: build/linux/libSimpleKafka1C_Static.so"
