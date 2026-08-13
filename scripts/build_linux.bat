REM Сборка Linux-бинаря компоненты в Docker. Запускать из корня репозитория.
REM Результат: build\linux\libSimpleKafka1C_Static.so
docker build -f scripts/Dockerfile.ubuntu20 -t kafka_1c_ubuntu . || exit /b 1
docker rm -f temp_container_ubuntu 2>nul
docker create --name temp_container_ubuntu kafka_1c_ubuntu || exit /b 1
if not exist build\linux mkdir build\linux
REM ВНИМАНИЕ: внутри образа файл называется libSimpleKafka1C.so (без _Static)
docker cp temp_container_ubuntu:/src/build/libSimpleKafka1C.so build/linux/libSimpleKafka1C_Static.so || exit /b 1
docker rm temp_container_ubuntu
echo Done: build\linux\libSimpleKafka1C_Static.so
