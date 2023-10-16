# Публикация сообщений

## Последовательность использования методов компоненты и их описание

### Установка параметров

При необходимости можно устанавить параметры. Доступные параметры для продюсера помечены в колонке "C/P" символами "P" или "*" (см. https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)

По умолчанию для продюсера можно не устанавливать никаких параметров.

```1c
УстановитьПараметр(ИмяПараметра, ЗначениеПараметра);
```

**ИмяПараметра** - Строка. Имя параметра, согласно колонке *Property* в [данной таблице](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) 

**ЗначениеПараметра** - Строка. Доступные значения приведены в колонках *Range* и *Default* в [данной таблице](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) 


## Инициализация подключения к брокерам 

```1c
ИнициализироватьПродюсера(Брокеры)
```

**Брокеры** - Строка. Список брокеров кластера, разделенные запятой. 

Если в кластере несколько брокеров, то достаточно указать одного основного брокера. Брокер содержит все необходимую информацию об других связанных с ним брокерах в кластере.


## Публикация сообщений. Асинхронная и синхронная отправка сообщений

Библиотека, на которой основана компонента, по своей природе имеет асинхронную архитектуру взаимодействия с брокерами.

### Асинхронная отправка

Для **асинхронной** отправки сообщения необходимо использовать метод **ОтправитьСообщение**. Метод возвращает булево значение "Истина" практически во всех случаях, кроме следующих кейсов:
+ Размер сообщения превышает установленный максимальный размер: message.max.bytes
+ Запрошенный раздел (партиция) неизвестен в кластере Kafka
+ Указанная тема не найдена в кластере Kafka (только если отключено автосоздание топиков на брокере)

```1c
ОтправитьСообщение(Сообщение, Топик, Партиция, Ключ, Заголовки)
```

**Сообщение** - Строка. Тело сообщения, например json строка.

**Топик** - Строка.

**Партиция** - Число. Если не указано значение, то по умолчанию запись производится в 0 раздел.

**Ключ** - Строка. Необязательный параметр. Ключ сообщения, который может использоваться в темах с уплотнением сообщений по ключу.

**Заголовки** - Строка. Перечень заголовков, состоящих из ключа и значения в следующем формате "ключ1,значение1;ключ2,значение2"

В случае асинхронной отправки - результаты доставки можно получить с помощью следующих архитектурных подходов:
+ При создании экземпляра продюсера, создаем так же экземпляр консьюмера, подписываемся на тему, куда производится отправка. После отправки каждого сообщения - получаем с помощью экземпляра консьюмера сообщение, вычисляем хэш отправленного и полученного сообщения (или же используем ключи сообщений). Сравниваем хэши или ключи - в случае совпадения - считаем сообщение доставленным.
+ Настраиваем считывание из файла лога. Если при отправке был указан key сообщения, в лог будет выведен key и результат доставки. Если key не был указан - в лог будет выведен хэш (md5) сообщения.
    
Порядок полей в логе: Дата время: Хэш или ключ сообщения, Статус доставки:Причина, Размер сообщения в байтах, Топик, Смещение (-1001 если все плохо), Партиция (-1 если все плохо), Брокер (-1 если все плохо).

Примеры строк в логе доставки
    
    2023-09-24 09:59:18.54614692 Key:205f03b5-9c64-4752-9fb4-0b8695789648, Status:Persisted, Details:Success, Size:28, Topic:testTopic, Offset:20553, Partition:0, BrokerID:0
	2023-09-24 10:01:57.958599455 Hash:ed59c8ed6c3209e9e6e434c2d9fbe52a, Status:Persisted, Details:Success, Size:28, Topic:testTopic, Offset:20554, Partition:0, BrokerID:0
	2023-09-24 10:18:32.495522885 Error: Configuration property "socket.blocking.max.ms" value 0 is outside allowed range 1..60000
	2023-09-24 12:56:10.311876162 Hash:dbee3cf28689a286066db820f6a8583e, Status:NotPersisted, Details:Local Message timed out, Size:9546, Topic:testTopic, Offset:-1001, Partition:-1, BrokerID:-1

### Синхронная отправка

Для **синхронной** отправки необходимо использовать метод **ОтправитьСообщениеСОжиданиемРезультата**, состав параметров аналогичен методу асинхронной отправки. Метод возвращает значение типа булево - "Истина", если при доставке все брокеры вернули подтверждение о приемке сообщения, "Ложь", если хотя бы один брокер не подтвердил получение или не было получено подтверждение от брокеров в течении 20 секунд. Крайне рекомендуется не изменять значение параметра **acks**, который по умолчанию имеет значение "all" или "-1".

Дополнительно, в рамках синхронной отправки рекомендуется устанавливать параметры **queue.buffering.max.ms** и **socket.blocking.max.ms** в самые минимальные значения, чтобы во внутреннем буфере сообщений не происходило накопление данных и сообщения могли отправляться сразу, минуя буфер.

Так же следует обратить внимание на параметр **message.timeout.ms**. Данным параметром мы указываем, сколько времени в мс. мы должны ждать подтверждения от брокеров. Брокеры могут и не отправить подтверждения, если, к примеру, они не доступны. В этом случае, рекомендуется устанавливать данный параметр в диапозоне от 1000 до 20000 мс. В противном случае - недоступности брокеров и включеном логировании в компоненте - в логе продюсера не будет никакой информации об проблеме.

```1c
УстановитьПараметр("queue.buffering.max.ms", "1");
УстановитьПараметр("socket.blocking.max.ms", "1");
УстановитьПараметр("message.timeout.ms", "5000");

ОтправитьСообщениеСОжиданиемРезультата(Сообщение, Топик, Партиция, Ключ, Заголовки)
```

Список параметров метода идентичен методу *ОтправитьСообщение*


## Остановка продюсера

После того как была произведена отправка всех необходимых сообщений и больше нет необходимости в использовании инстанса продюсера - необходимо обяхательно производить остановку продюсера.

```1c
ОстановитьПродюсера()
```

## Пример асинхронной отправки

```1c

	Если ТипЗнч(Данные) <> Тип("Массив") Тогда
		Сообщения = ОбщегоНазначенияКлиентСервер.ЗначениеВМассиве(Данные);
	Иначе
		Сообщения = Данные;
	КонецЕсли;
	
	Если ТипЗнч(Параметры) = Тип("Соответствие") ИЛИ (ТипЗнч(Параметры) = Тип("Структура")) Тогда
		Для каждого Параметр Из Параметры Цикл
			Компонента.УстановитьПараметр(Строка(Параметр.Ключ), Строка(Параметр.Значение));		
		КонецЦикла;
	КонецЕсли; 	
	
    // инициализируем подключение к брокеру
    // достаточно указать одного брокера из кластера, либо есть возможность перечислить брокеров через ,
	РезультатИнициализации = Компонента.ИнициализироватьПродюсера(Брокеры);
	
	Если РезультатИнициализации Тогда
		
		Для каждого СообщениеВКафку Из Сообщения Цикл			        			
			// Parametr1 - Тело сообщения (строка)
            // Parametr2 - Топик (строка)
			// Parametr3 - Номер партиции, по умолчанию = -1 (число)
			// Parametr4 - Произвольный ключ, идентифицирующий сообщение, например GUID (строка)
			// Parametr5 - Заголовки (строка), "ключ1,значение1;ключ2,значение2"
			Компонента.ОтправитьСообщение(СообщениеВКафку, Топик,, Ключ, Заголовки);  
		КонецЦикла;
		
		Компонента.ОстановитьПродюсера();
		Компонента = Неопределено;

	КонецЕсли;
		
```