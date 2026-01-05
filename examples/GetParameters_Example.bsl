// Пример использования метода ПолучитьПараметры / GetParameters

Процедура ПримерПолученияПараметров()

	// Создаем экземпляр компоненты
	Компонента = Новый("AddIn.SimpleKafka1C.SimpleKafka1C");

	// Устанавливаем различные параметры
	Компонента.УстановитьПараметр("bootstrap.servers", "localhost:9092");
	Компонента.УстановитьПараметр("group.id", "test-consumer-group");
	Компонента.УстановитьПараметр("message.max.bytes", "10485760");
	Компонента.УстановитьПараметр("security.protocol", "SASL_SSL");
	Компонента.УстановитьПараметр("sasl.mechanism", "PLAIN");
	Компонента.УстановитьПараметр("compression.codec", "snappy");

	// Получаем все установленные параметры в формате JSON
	ПараметрыJSON = Компонента.ПолучитьПараметры();

	// Выводим результат
	Сообщить("Установленные параметры:");
	Сообщить(ПараметрыJSON);

	// Пример разбора JSON для использования в коде
	ЧтениеJSON = Новый ЧтениеJSON;
	ЧтениеJSON.УстановитьСтроку(ПараметрыJSON);

	Результат = ПрочитатьJSON(ЧтениеJSON);

	Если Результат.Свойство("parameters") Тогда

		Сообщить("Всего параметров: " + Результат.parameters.Количество());

		Для Каждого Параметр Из Результат.parameters Цикл
			Сообщить("  " + Параметр.key + " = " + Параметр.value);
		КонецЦикла;

	КонецЕсли;

	ЧтениеJSON.Закрыть();

КонецПроцедуры

// Пример с использованием английских имен методов
Procedure GetParametersExample()

	// Create component instance
	Component = New("AddIn.SimpleKafka1C.SimpleKafka1C");

	// Set various parameters
	Component.SetParameter("bootstrap.servers", "localhost:9092");
	Component.SetParameter("client.id", "my-app-v1");
	Component.SetParameter("request.timeout.ms", "30000");
	Component.SetParameter("socket.timeout.ms", "60000");

	// Get all parameters as JSON
	ParametersJSON = Component.GetParameters();

	// Output result
	Message("Configured parameters:");
	Message(ParametersJSON);

	// Expected output:
	// {
	//     "parameters": [
	//         { "key": "bootstrap.servers", "value": "localhost:9092" },
	//         { "key": "client.id", "value": "my-app-v1" },
	//         { "key": "request.timeout.ms", "value": "30000" },
	//         { "key": "socket.timeout.ms", "value": "60000" }
	//     ]
	// }

EndProcedure
