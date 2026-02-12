// Avro serialization/deserialization methods for SimpleKafka1C

#include <boost/json.hpp>
#include <boost/json/monotonic_resource.hpp>

#include <avro/Encoder.hh>
#include <avro/Compiler.hh>
#include <avro/Types.hh>
#include <avro/DataFile.hh>
#include <avro/GenericDatum.hh>
#include <avro/Generic.hh>

#include <fstream>
#include <sstream>
#include <iomanip>

#include "SimpleKafka1C.h"

//================================== Avro ==========================================

bool SimpleKafka1C::putAvroSchema(const variant_t& schemaJsonName, const variant_t& schemaJson)
{
	try
	{
		// Проверяем, существует ли схема с таким именем
		auto it = schemesMap.find(std::get<std::string>(schemaJsonName));

		if (it == schemesMap.end())
		{
			// Схема не существует, компилируем и добавляем ее в map
			const avro::ValidSchema compiledScheme = avro::compileJsonSchemaFromString(std::get<std::string>(schemaJson));
			schemesMap[std::get<std::string>(schemaJsonName)] = compiledScheme;
		}
	}
	catch (std::exception const& ex)
	{
		msg_err = "Scheme compileError '";
		msg_err += ex.what();
	}

	return msg_err.empty();
}

// Вспомогательная функция для рекурсивного заполнения GenericDatum из JSON
static bool fillAvroFromJson(avro::GenericDatum& datum, const boost::json::value& jsonValue, std::string& errMsg, const std::string& path = "")
{
	// Обработка union типов
	if (datum.isUnion())
	{
		if (jsonValue.is_null())
		{
			// null - первая ветка union (обычно ["null", "type"])
			datum.selectBranch(0);
			return true;
		}
		// Не null - выбираем вторую ветку
		datum.selectBranch(1);
	}

	switch (datum.type())
	{
	case avro::AVRO_STRING:
		if (!jsonValue.is_string())
		{
			errMsg = "Expected string at " + path;
			return false;
		}
		datum.value<std::string>() = std::string(jsonValue.as_string());
		return true;

	case avro::AVRO_LONG:
		if (!jsonValue.is_int64())
		{
			errMsg = "Expected long at " + path;
			return false;
		}
		datum.value<int64_t>() = jsonValue.as_int64();
		return true;

	case avro::AVRO_INT:
		if (!jsonValue.is_int64())
		{
			errMsg = "Expected int at " + path;
			return false;
		}
		datum.value<int32_t>() = static_cast<int32_t>(jsonValue.as_int64());
		return true;

	case avro::AVRO_FLOAT:
		if (jsonValue.is_double())
			datum.value<float>() = static_cast<float>(jsonValue.as_double());
		else if (jsonValue.is_int64())
			datum.value<float>() = static_cast<float>(jsonValue.as_int64());
		else
		{
			errMsg = "Expected float at " + path;
			return false;
		}
		return true;

	case avro::AVRO_DOUBLE:
		if (jsonValue.is_double())
			datum.value<double>() = jsonValue.as_double();
		else if (jsonValue.is_int64())
			datum.value<double>() = static_cast<double>(jsonValue.as_int64());
		else
		{
			errMsg = "Expected double at " + path;
			return false;
		}
		return true;

	case avro::AVRO_BOOL:
		if (!jsonValue.is_bool())
		{
			errMsg = "Expected bool at " + path;
			return false;
		}
		datum.value<bool>() = jsonValue.as_bool();
		return true;

	case avro::AVRO_NULL:
		datum.value<avro::null>() = avro::null();
		return true;

	case avro::AVRO_BYTES:
	{
		if (!jsonValue.is_string())
		{
			errMsg = "Expected string (bytes) at " + path;
			return false;
		}
		std::string strVal = std::string(jsonValue.as_string());
		std::vector<uint8_t> bytes(strVal.begin(), strVal.end());
		datum.value<std::vector<uint8_t>>() = bytes;
		return true;
	}

	case avro::AVRO_FIXED:
	{
		if (!jsonValue.is_string())
		{
			errMsg = "Expected string (fixed) at " + path;
			return false;
		}
		avro::GenericFixed& fixed = datum.value<avro::GenericFixed>();
		std::string strVal = std::string(jsonValue.as_string());
		// Проверка на UUID формат
		if (strVal.length() == 36 && strVal[8] == '-' && strVal[13] == '-')
		{
			std::string hex;
			for (char c : strVal)
			{
				if (c != '-') hex += c;
			}
			std::vector<uint8_t>& bytes = fixed.value();
			bytes.resize(16);
			for (size_t j = 0; j < 16; j++)
			{
				std::string byteStr = hex.substr(j * 2, 2);
				bytes[j] = static_cast<uint8_t>(std::stoul(byteStr, nullptr, 16));
			}
		}
		else
		{
			std::vector<uint8_t>& bytes = fixed.value();
			bytes.assign(strVal.begin(), strVal.end());
		}
		return true;
	}

	case avro::AVRO_RECORD:
	{
		if (!jsonValue.is_object())
		{
			errMsg = "Expected object at " + path;
			return false;
		}
		const boost::json::object& obj = jsonValue.as_object();
		avro::GenericRecord& record = datum.value<avro::GenericRecord>();
		size_t fieldCount = record.fieldCount();
		const avro::NodePtr& schema = record.schema();

		for (size_t i = 0; i < fieldCount; ++i)
		{
			std::string fieldName = schema->nameAt(i);
			auto it = obj.find(fieldName);
			if (it == obj.end())
			{
				// Поле отсутствует - проверяем, есть ли default или это union с null
				avro::GenericDatum& fieldDatum = record.fieldAt(i);
				if (fieldDatum.isUnion())
				{
					// Union с null - устанавливаем null
					fieldDatum.selectBranch(0);
				}
				// Иначе оставляем значение по умолчанию
				continue;
			}
			avro::GenericDatum& fieldDatum = record.fieldAt(i);
			std::string fieldPath = path.empty() ? fieldName : path + "." + fieldName;
			if (!fillAvroFromJson(fieldDatum, it->value(), errMsg, fieldPath))
			{
				return false;
			}
		}
		return true;
	}

	case avro::AVRO_ENUM:
	{
		if (!jsonValue.is_string())
		{
			errMsg = "Expected string (enum symbol) at " + path;
			return false;
		}
		avro::GenericEnum& enumVal = datum.value<avro::GenericEnum>();
		std::string symbol = std::string(jsonValue.as_string());
		enumVal.set(symbol);
		return true;
	}

	case avro::AVRO_ARRAY:
	{
		if (!jsonValue.is_array())
		{
			errMsg = "Expected array at " + path;
			return false;
		}
		const boost::json::array& arr = jsonValue.as_array();
		avro::GenericArray& avroArray = datum.value<avro::GenericArray>();
		const avro::NodePtr& schema = avroArray.schema();
		avroArray.value().clear();

		for (size_t i = 0; i < arr.size(); ++i)
		{
			avro::GenericDatum elemDatum(schema->leafAt(0));
			std::string elemPath = path + "[" + std::to_string(i) + "]";
			if (!fillAvroFromJson(elemDatum, arr[i], errMsg, elemPath))
			{
				return false;
			}
			avroArray.value().push_back(elemDatum);
		}
		return true;
	}

	case avro::AVRO_MAP:
	{
		if (!jsonValue.is_object())
		{
			errMsg = "Expected object (map) at " + path;
			return false;
		}
		const boost::json::object& obj = jsonValue.as_object();
		avro::GenericMap& avroMap = datum.value<avro::GenericMap>();
		const avro::NodePtr& schema = avroMap.schema();
		avroMap.value().clear();

		for (const auto& kv : obj)
		{
			avro::GenericDatum valueDatum(schema->leafAt(1));
			std::string elemPath = path + "[\"" + std::string(kv.key()) + "\"]";
			if (!fillAvroFromJson(valueDatum, kv.value(), errMsg, elemPath))
			{
				return false;
			}
			avroMap.value().push_back(std::make_pair(std::string(kv.key()), valueDatum));
		}
		return true;
	}

	default:
		errMsg = "Unsupported Avro type at " + path;
		return false;
	}
}

bool SimpleKafka1C::convertToAvroFormat(const variant_t& msgJson, const variant_t& schemaJsonName, const variant_t& format, const variant_t& schemaId)
{
	avroFile.clear();
	auto it = schemesMap.find(std::get<std::string>(schemaJsonName));
	avro::ValidSchema schema;

	if (it != schemesMap.end())
	{
		schema = it->second;
	}
	else
	{
		msg_err = u8"Имя схемы не известно - " + std::get<std::string>(schemaJsonName);
		return false;
	}

	avro::GenericDatum datum(schema);
	if (datum.type() != avro::AVRO_RECORD)
	{
		msg_err = u8"Некорректная схема - корневой тип должен быть record";
		return false;
	}

	// Определяем формат вывода: "" / "ocf" / "raw" / "confluent"
	std::string fmtStr;
	if (std::holds_alternative<std::string>(format))
	{
		fmtStr = std::get<std::string>(format);
	}

	int32_t sid = 0;
	if (std::holds_alternative<int32_t>(schemaId))
	{
		sid = std::get<int32_t>(schemaId);
	}

	if (!fmtStr.empty() && fmtStr != "ocf" && fmtStr != "raw" && fmtStr != "confluent")
	{
		msg_err = u8"Неизвестный формат: " + fmtStr + u8". Допустимые значения: ocf, raw, confluent";
		return false;
	}

	bool useOcf = fmtStr.empty() || fmtStr == "ocf";

	// Разбираем исходный JSON
	boost::json::monotonic_resource mr;
	boost::json::value jsonInput;
	try
	{
		jsonInput = boost::json::parse(std::get<std::string>(msgJson), &mr);
	}
	catch (std::exception const& ex)
	{
		msg_err = "Error parsing JSON - ";
		msg_err += ex.what();
		return false;
	}

	try
	{
		// Собираем все записи из JSON
		std::vector<avro::GenericDatum> records;

		// Определяем формат входных данных:
		// 1. Объект {...} - одна запись (новый стандартный формат)
		// 2. Массив [{...}, {...}] - несколько записей (новый стандартный формат)
		// 3. "Столбцовый" формат {"field1": [v1, v2], "field2": [v1, v2]} - для совместимости

		if (jsonInput.is_object())
		{
			const boost::json::object& obj = jsonInput.as_object();

			// Проверяем формат: если первое значение - массив, это "столбцовый" формат
			bool isColumnarFormat = false;
			if (!obj.empty())
			{
				const auto& firstValue = obj.cbegin()->value();
				if (firstValue.is_array())
				{
					// Дополнительная проверка: в стандартном формате array тоже может быть значением поля
					// Проверяем, совпадают ли размеры всех массивов (признак столбцового формата)
					size_t firstSize = firstValue.as_array().size();
					bool allSameSize = true;
					for (const auto& kv : obj)
					{
						if (!kv.value().is_array() || kv.value().as_array().size() != firstSize)
						{
							allSameSize = false;
							break;
						}
					}
					// Если все поля - массивы одинаковой длины, считаем это столбцовым форматом
					// (кроме случая когда длина = 1, тогда это может быть и стандартный формат)
					isColumnarFormat = allSameSize && firstSize > 1;
				}
			}

			if (isColumnarFormat)
			{
				// Старый "столбцовый" формат для совместимости
				const auto first_array = obj.cbegin();
				const size_t numElements = first_array->value().as_array().size();

				for (size_t i = 0; i < numElements; i++)
				{
					boost::json::object jsonRecord;
					for (const auto& kv : obj)
					{
						jsonRecord[kv.key()] = kv.value().as_array().at(i);
					}

					avro::GenericDatum recordDatum(schema);
					if (!fillAvroFromJson(recordDatum, boost::json::value(jsonRecord), msg_err))
					{
						return false;
					}
					records.push_back(std::move(recordDatum));
				}
			}
			else
			{
				// Стандартный формат: одна запись как объект
				if (!fillAvroFromJson(datum, jsonInput, msg_err))
				{
					return false;
				}
				records.push_back(std::move(datum));
			}
		}
		else if (jsonInput.is_array())
		{
			// Стандартный формат: массив записей
			const boost::json::array& arr = jsonInput.as_array();
			for (size_t i = 0; i < arr.size(); ++i)
			{
				avro::GenericDatum recordDatum(schema);
				std::string recordPath = "[" + std::to_string(i) + "]";
				if (!fillAvroFromJson(recordDatum, arr[i], msg_err, recordPath))
				{
					return false;
				}
				records.push_back(std::move(recordDatum));
			}
		}
		else
		{
			msg_err = u8"JSON должен быть объектом или массивом объектов";
			return false;
		}

		if (useOcf)
		{
			// Avro Object Container Format (OCF) через DataFileWriter
			MemoryOutputStream* memOutStr = new MemoryOutputStream(100000);
			std::unique_ptr<avro::OutputStream> os(memOutStr);
			avro::DataFileWriter<avro::GenericDatum> writer(std::move(os), schema);

			for (const auto& record : records)
			{
				writer.write(record);
			}

			writer.flush();
			memOutStr->snapshot(avroFile);
			writer.close();
		}
		else
		{
			// Raw Avro binary encoding
			std::unique_ptr<avro::OutputStream> out = avro::memoryOutputStream();
			avro::EncoderPtr encoder = avro::binaryEncoder();
			encoder->init(*out);

			for (const auto& record : records)
			{
				avro::GenericWriter::write(*encoder, record, schema);
			}
			encoder->flush();

			// Для Confluent Wire Format: magic byte (0x00) + 4 байта schema ID (big-endian)
			if (fmtStr == "confluent")
			{
				avroFile.resize(5);
				avroFile[0] = 0x00;
				avroFile[1] = static_cast<uint8_t>((sid >> 24) & 0xFF);
				avroFile[2] = static_cast<uint8_t>((sid >> 16) & 0xFF);
				avroFile[3] = static_cast<uint8_t>((sid >> 8) & 0xFF);
				avroFile[4] = static_cast<uint8_t>(sid & 0xFF);
			}

			// Копируем raw Avro данные
			std::unique_ptr<avro::InputStream> inStream = avro::memoryInputStream(*out);
			const uint8_t* data;
			size_t len;
			while (inStream->next(&data, &len))
			{
				avroFile.insert(avroFile.end(), data, data + len);
			}
		}
	}
	catch (std::exception const& ex)
	{
		msg_err += "Error converting to Avro: ";
		msg_err += ex.what();
	}

	return msg_err.empty();
}

bool SimpleKafka1C::saveAvroFile(const variant_t& fileName)
{
	if (avroFile.empty())
	{
		msg_err = u8"AVRO файл пустой";
		return false;
	}

	try
	{
		std::ofstream out(std::get<std::string>(fileName), std::ios::out | std::ios::binary);
		out.write(reinterpret_cast<const char*>(avroFile.data()), avroFile.size());
		out.close();
	}
	catch (std::exception const& ex)
	{
		msg_err = ex.what();
	}

	return msg_err.empty();
}

// Escape JSON string
static std::string escapeJsonString(const std::string& input)
{
	std::ostringstream oss;
	for (char c : input)
	{
		switch (c)
		{
		case '"':  oss << "\\\""; break;
		case '\\': oss << "\\\\"; break;
		case '\b': oss << "\\b"; break;
		case '\f': oss << "\\f"; break;
		case '\n': oss << "\\n"; break;
		case '\r': oss << "\\r"; break;
		case '\t': oss << "\\t"; break;
		default:
			if (static_cast<unsigned char>(c) < 0x20)
			{
				oss << "\\u" << std::hex << std::setfill('0') << std::setw(4) << static_cast<int>(c);
			}
			else
			{
				oss << c;
			}
			break;
		}
	}
	return oss.str();
}

// Convert GenericDatum to JSON string
static std::string convertAvroDatumToJsonString(const avro::GenericDatum& datum)
{
	std::ostringstream oss;

	switch (datum.type())
	{
	case avro::AVRO_NULL:
		oss << "null";
		break;

	case avro::AVRO_BOOL:
		oss << (datum.value<bool>() ? "true" : "false");
		break;

	case avro::AVRO_INT:
		oss << datum.value<int32_t>();
		break;

	case avro::AVRO_LONG:
		oss << datum.value<int64_t>();
		break;

	case avro::AVRO_FLOAT:
		oss << std::setprecision(9) << datum.value<float>();
		break;

	case avro::AVRO_DOUBLE:
		oss << std::setprecision(17) << datum.value<double>();
		break;

	case avro::AVRO_STRING:
		oss << "\"" << escapeJsonString(datum.value<std::string>()) << "\"";
		break;

	case avro::AVRO_BYTES:
	{
		const auto& bytes = datum.value<std::vector<uint8_t>>();
		oss << "\"";
		for (uint8_t byte : bytes)
		{
			oss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(byte);
		}
		oss << "\"";
		break;
	}

	case avro::AVRO_FIXED:
	{
		const auto& fixed = datum.value<avro::GenericFixed>();
		const auto& bytes = fixed.value();
		oss << "\"";
		for (uint8_t byte : bytes)
		{
			oss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(byte);
		}
		oss << "\"";
		break;
	}

	case avro::AVRO_RECORD:
	{
		const auto& record = datum.value<avro::GenericRecord>();
		oss << "{";
		bool first = true;
		for (size_t i = 0; i < record.fieldCount(); ++i)
		{
			if (!first) oss << ",";
			first = false;
			oss << "\"" << escapeJsonString(record.schema()->nameAt(i)) << "\":";
			oss << convertAvroDatumToJsonString(record.fieldAt(i));
		}
		oss << "}";
		break;
	}

	case avro::AVRO_ENUM:
	{
		const auto& enumVal = datum.value<avro::GenericEnum>();
		oss << "\"" << escapeJsonString(enumVal.symbol()) << "\"";
		break;
	}

	case avro::AVRO_ARRAY:
	{
		const auto& array = datum.value<avro::GenericArray>();
		oss << "[";
		bool first = true;
		for (const auto& item : array.value())
		{
			if (!first) oss << ",";
			first = false;
			oss << convertAvroDatumToJsonString(item);
		}
		oss << "]";
		break;
	}

	case avro::AVRO_MAP:
	{
		const auto& map = datum.value<avro::GenericMap>();
		oss << "{";
		bool first = true;
		for (const auto& kv : map.value())
		{
			if (!first) oss << ",";
			first = false;
			oss << "\"" << escapeJsonString(kv.first) << "\":";
			oss << convertAvroDatumToJsonString(kv.second);
		}
		oss << "}";
		break;
	}

	case avro::AVRO_UNION:
	{
		const auto& unionDatum = datum.value<avro::GenericUnion>().datum();
		oss << convertAvroDatumToJsonString(unionDatum);
		break;
	}

	default:
		oss << "null";
		break;
	}

	return oss.str();
}

variant_t SimpleKafka1C::decodeAvroMessage(const variant_t& avroData, const variant_t& schemaJsonName, const variant_t& asJson)
{
	try
	{
		// Получаем бинарные данные
		const std::vector<char>* dataPtr = nullptr;
		size_t dataSize = 0;

		if (std::holds_alternative<std::vector<char>>(avroData))
		{
			dataPtr = &std::get<std::vector<char>>(avroData);
			dataSize = dataPtr->size();
		}
		else if (std::holds_alternative<std::string>(avroData))
		{
			const std::string& str = std::get<std::string>(avroData);
			messageData.assign(str.begin(), str.end());
			dataPtr = &messageData;
			dataSize = messageData.size();
		}
		else
		{
			msg_err = u8"Неверный тип данных для avroData. Ожидается строка или двоичные данные";
			return std::string("");
		}

		if (dataSize == 0)
		{
			msg_err = u8"AVRO данные пусты";
			return std::string("");
		}

		// Безопасное получение параметров
		bool returnAsJson = true;
		if (std::holds_alternative<bool>(asJson))
		{
			returnAsJson = std::get<bool>(asJson);
		}

		std::string schemaName;
		if (std::holds_alternative<std::string>(schemaJsonName))
		{
			schemaName = std::get<std::string>(schemaJsonName);
		}

		// Создаём поток для чтения из памяти
		std::unique_ptr<avro::InputStream> in = avro::memoryInputStream(
			reinterpret_cast<const uint8_t*>(dataPtr->data()), dataSize);

		// Создаём DataFileReader
		avro::DataFileReader<avro::GenericDatum> reader(std::move(in));

		// Получаем схему из файла
		avro::ValidSchema schema = reader.dataSchema();

		// Используем схему из карты, если указана
		if (!schemaName.empty())
		{
			auto it = schemesMap.find(schemaName);
			if (it != schemesMap.end())
			{
				schema = it->second;
			}
		}

		// Читаем все записи
		std::vector<avro::GenericDatum> records;
		avro::GenericDatum datum(schema);
		while (reader.read(datum))
		{
			records.push_back(datum);
			datum = avro::GenericDatum(schema);
		}

		if (records.empty())
		{
			if (returnAsJson)
			{
				return std::string("{}");
			}
			else
			{
				return std::vector<char>();
			}
		}

		if (returnAsJson)
		{
			// Конвертируем в JSON
			if (records.size() == 1)
			{
				return convertAvroDatumToJsonString(records[0]);
			}
			else
			{
				std::ostringstream oss;
				oss << "[";
				bool first = true;
				for (const auto& record : records)
				{
					if (!first) oss << ",";
					first = false;
					oss << convertAvroDatumToJsonString(record);
				}
				oss << "]";
				return oss.str();
			}
		}
		else
		{
			// Возвращаем raw Avro данные (без OCF контейнера)
			std::unique_ptr<avro::OutputStream> out = avro::memoryOutputStream();
			avro::EncoderPtr encoder = avro::binaryEncoder();
			encoder->init(*out);

			for (const auto& record : records)
			{
				avro::GenericWriter::write(*encoder, record, schema);
			}
			encoder->flush();

			// Копируем данные из output stream
			std::unique_ptr<avro::InputStream> inStream = avro::memoryInputStream(*out);
			const uint8_t* data;
			size_t len;
			std::vector<char> result;
			while (inStream->next(&data, &len))
			{
				result.insert(result.end(), data, data + len);
			}
			return result;
		}
	}
	catch (const avro::Exception& ex)
	{
		msg_err = "AVRO error: ";
		msg_err += ex.what();
		return std::string("");
	}
	catch (const std::exception& ex)
	{
		msg_err = "Error decoding AVRO: ";
		msg_err += ex.what();
		return std::string("");
	}
	catch (...)
	{
		msg_err = "Unknown error decoding AVRO";
		return std::string("");
	}
}

variant_t SimpleKafka1C::getAvroSchema(const variant_t& avroData)
{
	try
	{
		// Получаем бинарные данные
		const std::vector<char>* dataPtr = nullptr;
		size_t dataSize = 0;

		if (std::holds_alternative<std::vector<char>>(avroData))
		{
			dataPtr = &std::get<std::vector<char>>(avroData);
			dataSize = dataPtr->size();
		}
		else if (std::holds_alternative<std::string>(avroData))
		{
			const std::string& str = std::get<std::string>(avroData);
			messageData.assign(str.begin(), str.end());
			dataPtr = &messageData;
			dataSize = messageData.size();
		}
		else
		{
			msg_err = u8"Неверный тип данных для avroData. Ожидается строка или двоичные данные";
			return std::string("");
		}

		if (dataSize == 0)
		{
			msg_err = u8"AVRO данные пусты";
			return std::string("");
		}

		// Создаём поток для чтения
		std::unique_ptr<avro::InputStream> in = avro::memoryInputStream(
			reinterpret_cast<const uint8_t*>(dataPtr->data()), dataSize);

		// Создаём DataFileReader
		avro::DataFileReader<avro::GenericDatum> reader(std::move(in));

		// Получаем схему и возвращаем как JSON строку
		avro::ValidSchema schema = reader.dataSchema();
		std::ostringstream oss;
		schema.toJson(oss);
		return oss.str();
	}
	catch (const avro::Exception& ex)
	{
		msg_err = "AVRO error: ";
		msg_err += ex.what();
		return std::string("");
	}
	catch (const std::exception& ex)
	{
		msg_err = "Error getting AVRO schema: ";
		msg_err += ex.what();
		return std::string("");
	}
}
