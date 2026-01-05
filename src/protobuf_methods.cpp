// Protobuf methods implementation - to be appended to SimpleKafka1C.cpp

//================================== Protobuf ==========================================

// Helper class for error collection during proto parsing
class ProtoErrorCollector : public google::protobuf::io::ErrorCollector
{
public:
	std::string errors;

	void RecordError(int line, int column, const std::string& message) override
	{
		errors += "Line " + std::to_string(line) + ", Column " + std::to_string(column) + ": " + message + "\n";
	}

	void RecordWarning(int line, int column, const std::string& message) override
	{
		// Ignore warnings for now
	}
};

bool SimpleKafka1C::putProtoSchema(const variant_t& schemaName, const variant_t& protoSchema)
{
	try
	{
		std::string name = std::get<std::string>(schemaName);
		std::string schema = std::get<std::string>(protoSchema);

		// Check if schema already exists
		auto it = protoDescriptors.find(name);
		if (it != protoDescriptors.end())
		{
			// Schema already exists, skip
			return true;
		}

		// Parse the .proto schema
		google::protobuf::io::ArrayInputStream input(schema.data(), schema.size());
		ProtoErrorCollector errorCollector;
		google::protobuf::io::Tokenizer tokenizer(&input, &errorCollector);

		google::protobuf::compiler::Parser parser;
		google::protobuf::FileDescriptorProto fileDescProto;
		fileDescProto.set_name(name + ".proto");

		if (!parser.Parse(&tokenizer, &fileDescProto))
		{
			msg_err = u8"Ошибка парсинга proto схемы: " + errorCollector.errors;
			return false;
		}

		// Build descriptor from FileDescriptorProto
		const google::protobuf::FileDescriptor* fileDesc = protoPool->BuildFile(fileDescProto);
		if (!fileDesc)
		{
			msg_err = u8"Не удалось построить дескриптор из proto схемы";
			return false;
		}

		// Find the message type (assuming first message in file)
		if (fileDesc->message_type_count() > 0)
		{
			const google::protobuf::Descriptor* descriptor = fileDesc->message_type(0);
			protoDescriptors[name] = std::shared_ptr<google::protobuf::Descriptor>(
				const_cast<google::protobuf::Descriptor*>(descriptor), [](google::protobuf::Descriptor*) {});
		}
		else
		{
			msg_err = u8"Proto схема не содержит определений сообщений";
			return false;
		}
	}
	catch (std::exception const& ex)
	{
		msg_err = "Proto schema error: ";
		msg_err += ex.what();
		return false;
	}

	return msg_err.empty();
}

bool SimpleKafka1C::convertToProtobufFormat(const variant_t& msgJson, const variant_t& schemaName)
{
	protobufData.clear();

	try
	{
		std::string name = std::get<std::string>(schemaName);
		std::string jsonData = std::get<std::string>(msgJson);

		// Get descriptor
		auto it = protoDescriptors.find(name);
		if (it == protoDescriptors.end())
		{
			msg_err = u8"Схема protobuf не найдена: " + name;
			return false;
		}

		const google::protobuf::Descriptor* descriptor = it->second.get();

		// Create dynamic message
		const google::protobuf::Message* prototype = protoFactory->GetPrototype(descriptor);
		if (!prototype)
		{
			msg_err = u8"Не удалось создать прототип сообщения";
			return false;
		}

		std::unique_ptr<google::protobuf::Message> message(prototype->New());

		// Parse JSON to protobuf
		google::protobuf::util::JsonParseOptions options;
		options.ignore_unknown_fields = false;

		auto status = google::protobuf::util::JsonStringToMessage(jsonData, message.get(), options);
		if (!status.ok())
		{
			msg_err = u8"Ошибка преобразования JSON в protobuf: " + std::string(status.message());
			return false;
		}

		// Serialize to binary
		if (!message->SerializeToString(&protobufData))
		{
			msg_err = u8"Ошибка сериализации protobuf сообщения";
			return false;
		}
	}
	catch (std::exception const& ex)
	{
		msg_err = "Protobuf conversion error: ";
		msg_err += ex.what();
		return false;
	}

	return msg_err.empty();
}

bool SimpleKafka1C::saveProtobufFile(const variant_t& fileName)
{
	if (protobufData.empty())
	{
		msg_err = u8"Protobuf данные пусты";
		return false;
	}

	try
	{
		std::ofstream out(std::get<std::string>(fileName), std::ios::out | std::ios::binary);
		out.write(protobufData.data(), protobufData.size());
		out.close();
	}
	catch (std::exception const& ex)
	{
		msg_err = ex.what();
		return false;
	}

	return msg_err.empty();
}

variant_t SimpleKafka1C::decodeProtobufMessage(const variant_t& protobufData, const variant_t& schemaName, const variant_t& asJson)
{
	try
	{
		std::string name = std::get<std::string>(schemaName);

		// Get binary data
		const std::string* dataPtr = nullptr;
		std::string tempData;

		if (std::holds_alternative<std::vector<char>>(protobufData))
		{
			const std::vector<char>& vec = std::get<std::vector<char>>(protobufData);
			tempData = std::string(vec.begin(), vec.end());
			dataPtr = &tempData;
		}
		else if (std::holds_alternative<std::string>(protobufData))
		{
			dataPtr = &std::get<std::string>(protobufData);
		}
		else
		{
			msg_err = u8"Неверный тип данных для protobufData";
			return std::string("");
		}

		if (dataPtr->empty())
		{
			msg_err = u8"Protobuf данные пусты";
			return std::string("");
		}

		// Get descriptor
		auto it = protoDescriptors.find(name);
		if (it == protoDescriptors.end())
		{
			msg_err = u8"Схема protobuf не найдена: " + name;
			return std::string("");
		}

		const google::protobuf::Descriptor* descriptor = it->second.get();

		// Create dynamic message
		const google::protobuf::Message* prototype = protoFactory->GetPrototype(descriptor);
		if (!prototype)
		{
			msg_err = u8"Не удалось создать прототип сообщения";
			return std::string("");
		}

		std::unique_ptr<google::protobuf::Message> message(prototype->New());

		// Parse binary data
		if (!message->ParseFromString(*dataPtr))
		{
			msg_err = u8"Ошибка десериализации protobuf сообщения";
			return std::string("");
		}

		bool returnAsJson = std::get<bool>(asJson);

		if (returnAsJson)
		{
			// Convert to JSON
			std::string jsonOutput;
			google::protobuf::util::JsonPrintOptions options;
			options.add_whitespace = false;
			options.preserve_proto_field_names = true;

			auto status = google::protobuf::util::MessageToJsonString(*message, &jsonOutput, options);
			if (!status.ok())
			{
				msg_err = u8"Ошибка преобразования protobuf в JSON: " + std::string(status.message());
				return std::string("");
			}

			return jsonOutput;
		}
		else
		{
			// Return binary data as is
			return *dataPtr;
		}
	}
	catch (std::exception const& ex)
	{
		msg_err = "Protobuf decode error: ";
		msg_err += ex.what();
		return std::string("");
	}
}

int32_t SimpleKafka1C::produceProtobuf(const variant_t& topicName, const variant_t& partition, const variant_t& key, const variant_t& heads)
{
	cl_dr_cb.delivered = RdKafka::Message::MSG_STATUS_NOT_PERSISTED;
	if (hProducer == nullptr)
	{
		msg_err = u8"Продюсер не инициализирован";
		return -1;
	}
	if (protobufData.empty())
	{
		msg_err = u8"Protobuf данные пусты";
		return -1;
	}

	std::string tTopicName = std::get<std::string>(topicName);
	auto currentPartition = std::get<int>(partition);
	std::ofstream eventFile{};

	openEventFile(producerLogName, eventFile);
	if (eventFile.is_open())
	{
		eventFile << currentDateTime() << " Info: produceProtobuf. TopicName-" << tTopicName << " currentPartition-" << currentPartition << " protobufData.size()- " << protobufData.size() << std::endl;
	}

retry:
	RdKafka::ErrorCode resp = hProducer->produce(
		tTopicName,
		currentPartition == -1 ? RdKafka::Topic::PARTITION_UA : currentPartition,
		RdKafka::Producer::RK_MSG_COPY,
		const_cast<char*>(protobufData.data()),
		protobufData.size(),
		std::get<std::string>(key).c_str(), std::get<std::string>(key).size(),
		0,
		nullptr,
		nullptr);

	if (resp != RdKafka::ERR_NO_ERROR)
	{
		if (resp == RdKafka::ERR__QUEUE_FULL)
		{
			hProducer->poll(1000 /*block for max 1000ms*/);
			if (eventFile.is_open())
			{
				eventFile << currentDateTime() << " Error: " << "Достигнуто максимальное количество ожидающих сообщений: queue.buffering.max.message" << std::endl;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(1000));
			goto retry;
		}
		if (resp == RdKafka::ERR_MSG_SIZE_TOO_LARGE)
		{
			msg_err = u8"Размер сообщения превышает лимит message.max.bytes (по умолчанию 1 MB). Используйте УстановитьПараметр для увеличения лимита.";
			if (eventFile.is_open())
			{
				eventFile << currentDateTime() << " Error: " << msg_err << std::endl;
			}
			return cl_dr_cb.delivered;
		}
		msg_err = RdKafka::err2str(resp);
	}

	hProducer->poll(0 /*non-blocking*/);

	if (eventFile.is_open())
	{
		if (!msg_err.empty())
			eventFile << currentDateTime() << " Error produceProtobuf: " << msg_err << std::endl;
		else if (resp != RdKafka::ERR_NO_ERROR)
			eventFile << " Errorcode produceProtobuf: " << resp << std::endl;
		else
			eventFile << currentDateTime() << " Info produceProtobuf. Success" << std::endl;
	}

	return cl_dr_cb.delivered;
}

int32_t SimpleKafka1C::produceProtobufWithWaitResult(const variant_t& topicName, const variant_t& partition, const variant_t& key, const variant_t& heads)
{
	if (produceProtobuf(topicName, partition, key, heads) != -1)
	{
		hProducer->flush(20 * 1000);		 // wait for max 20 seconds
		if (hProducer->outq_len() > 0)
		{
			msg_err = u8"Не доставлено сообщений - " + hProducer->outq_len();

			std::ofstream eventFile{};
			openEventFile(producerLogName, eventFile);
			if (eventFile.is_open()) eventFile << currentDateTime() << " Info: produceProtobufWithWaitResult: " << msg_err << std::endl;

			return RdKafka::Message::MSG_STATUS_NOT_PERSISTED;
		}
		else if (cl_dr_cb.delivered != RdKafka::Message::MSG_STATUS_PERSISTED)
		{
			msg_err = u8"Не доставлено. Подробности см в логе";
		}
		return cl_dr_cb.delivered;
	}
	return -1;
}
