# Simple Kafka Connector 1C

Внешняя компонента, адаптер к 1С, позволяющий писать, а так же получать сообщения из топиков Apache Kafka. Объем поддержки Kafka достаточный для поддержки витрин данных СМЭВ 4.

## Проект входит в ТОП-500 репозиториев OpenYellow
[![OpenYellow](https://img.shields.io/endpoint?url=https://openyellow.neocities.org/badges/2/609486812.json)](https://openyellow.notion.site/openyellow/24727888daa641af95514b46bee4d6f2?p=f9171a9a4a3045f6ab39730271af15a5&amp;pm=s)

## Использование

Версия компоненты 1.1.0 и выше, имеет значительные, не совместимые изменения от более ранних версий. Документация по использованию ранних версий находится [здесь](./OldReleases.md).

[Сборка](./building.md)

[Подключение внешней компоненты](./connection.md)

[Публикация сообщений](./producer.md)

[Чтение сообщений](./consumer.md)

[Админ API](./admin.md)

[Логирование](./logging.md)

[Статистика](./statistic.md)

[Формирование сообщений в формате AVRO](./avro.md)

## Известные проблемы и пути их решения

Большинство кейсов, которые связаны с работой компоненты, решаются либо установкой нужных параметров либо периодическим перезапуском компоненты, если она работает продолжительное время.

[Разбор кейсов](./problems.md)

## Отказ от ответственности

Внешняя компонента Simple Kafka — это бесплатный инструмент, созданный и поддерживаемый сообществом открытого исходного кода. Инструмент не содержит каких-либо платных функций или планов подписки, которые будут добавлены в будущем.

## Поддерживаемые платформы

✔ Windows 32/64

✔ Linux 64
