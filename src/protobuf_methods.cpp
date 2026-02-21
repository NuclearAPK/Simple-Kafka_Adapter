// Protobuf serialization/deserialization methods for SimpleKafka1C

#include <boost/json.hpp>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/io/tokenizer.h>

#include <fstream>
#include <thread>
#include <chrono>

#include "SimpleKafka1C.h"
#include "utils.h"

// Undefine Windows API macros that conflict with protobuf
#ifdef _WIN32
#undef GetMessage
#endif

//================================== Protobuf helpers ===================================

// Helper to convert JSON value to protobuf field
static bool SetProtobufFieldFromJson(google::protobuf::Message* message,
	const google::protobuf::FieldDescriptor* field,
	const boost::json::value& json_value)
{
	const google::protobuf::Reflection* reflection = message->GetReflection();

	try {
		switch (field->type()) {
		case google::protobuf::FieldDescriptor::TYPE_STRING:
			if (json_value.is_string())
				reflection->SetString(message, field, std::string(json_value.as_string()));
			break;
		case google::protobuf::FieldDescriptor::TYPE_INT32:
		case google::protobuf::FieldDescriptor::TYPE_SINT32:
		case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
			if (json_value.is_int64())
				reflection->SetInt32(message, field, static_cast<int32_t>(json_value.as_int64()));
			break;
		case google::protobuf::FieldDescriptor::TYPE_INT64:
		case google::protobuf::FieldDescriptor::TYPE_SINT64:
		case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
			if (json_value.is_int64())
				reflection->SetInt64(message, field, json_value.as_int64());
			break;
		case google::protobuf::FieldDescriptor::TYPE_UINT32:
		case google::protobuf::FieldDescriptor::TYPE_FIXED32:
			if (json_value.is_uint64())
				reflection->SetUInt32(message, field, static_cast<uint32_t>(json_value.as_uint64()));
			break;
		case google::protobuf::FieldDescriptor::TYPE_UINT64:
		case google::protobuf::FieldDescriptor::TYPE_FIXED64:
			if (json_value.is_uint64())
				reflection->SetUInt64(message, field, json_value.as_uint64());
			break;
		case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
			if (json_value.is_double())
				reflection->SetDouble(message, field, json_value.as_double());
			break;
		case google::protobuf::FieldDescriptor::TYPE_FLOAT:
			if (json_value.is_double())
				reflection->SetFloat(message, field, static_cast<float>(json_value.as_double()));
			break;
		case google::protobuf::FieldDescriptor::TYPE_BOOL:
			if (json_value.is_bool())
				reflection->SetBool(message, field, json_value.as_bool());
			break;
		case google::protobuf::FieldDescriptor::TYPE_MESSAGE:
		{
			if (json_value.is_object()) {
				google::protobuf::Message* sub_message = reflection->MutableMessage(message, field);
				const boost::json::object& obj = json_value.as_object();
				const google::protobuf::Descriptor* sub_descriptor = sub_message->GetDescriptor();
				for (auto& kv : obj) {
					const google::protobuf::FieldDescriptor* sub_field =
						sub_descriptor->FindFieldByName(std::string(kv.key()));
					if (sub_field) {
						SetProtobufFieldFromJson(sub_message, sub_field, kv.value());
					}
				}
			}
			break;
		}
		default:
			return false;
		}
		return true;
	}
	catch (...) {
		return false;
	}
}

// Helper to convert protobuf field to JSON value
static boost::json::value GetJsonFromProtobufField(const google::protobuf::Message& message,
	const google::protobuf::FieldDescriptor* field)
{
	const google::protobuf::Reflection* reflection = message.GetReflection();

	switch (field->type()) {
	case google::protobuf::FieldDescriptor::TYPE_STRING:
		return boost::json::string(reflection->GetString(message, field));
	case google::protobuf::FieldDescriptor::TYPE_INT32:
	case google::protobuf::FieldDescriptor::TYPE_SINT32:
	case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
		return reflection->GetInt32(message, field);
	case google::protobuf::FieldDescriptor::TYPE_INT64:
	case google::protobuf::FieldDescriptor::TYPE_SINT64:
	case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
		return reflection->GetInt64(message, field);
	case google::protobuf::FieldDescriptor::TYPE_UINT32:
	case google::protobuf::FieldDescriptor::TYPE_FIXED32:
		return reflection->GetUInt32(message, field);
	case google::protobuf::FieldDescriptor::TYPE_UINT64:
	case google::protobuf::FieldDescriptor::TYPE_FIXED64:
		return reflection->GetUInt64(message, field);
	case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
		return reflection->GetDouble(message, field);
	case google::protobuf::FieldDescriptor::TYPE_FLOAT:
		return reflection->GetFloat(message, field);
	case google::protobuf::FieldDescriptor::TYPE_BOOL:
		return reflection->GetBool(message, field);
	case google::protobuf::FieldDescriptor::TYPE_MESSAGE:
	{
		const google::protobuf::Message& sub_message = reflection->GetMessage(message, field);
		const google::protobuf::Descriptor* sub_descriptor = sub_message.GetDescriptor();
		boost::json::object obj;

		const google::protobuf::Reflection* sub_reflection = sub_message.GetReflection();
		for (int i = 0; i < sub_descriptor->field_count(); i++) {
			const google::protobuf::FieldDescriptor* sub_field = sub_descriptor->field(i);
			if (sub_reflection->HasField(sub_message, sub_field)) {
				obj[sub_field->name()] = GetJsonFromProtobufField(sub_message, sub_field);
			}
		}
		return obj;
	}
	default:
		return boost::json::value();
	}
}

//================================== Protobuf ==========================================

// ProtobufContext class - encapsulates all protobuf-related objects
class SimpleKafka1C::ProtobufContext
{
public:
	std::map<std::string, const google::protobuf::Descriptor*> descriptors;
	google::protobuf::DescriptorPool pool;
	google::protobuf::DynamicMessageFactory factory;

	ProtobufContext() : factory(&pool) {}
};

bool SimpleKafka1C::putProtoSchema(const variant_t& schemaName, const variant_t& protoSchema)
{
	try
	{
		if (!protoContext)
		{
			protoContext = std::make_shared<ProtobufContext>();
		}

		std::string name = std::get<std::string>(schemaName);
		std::string schema = std::get<std::string>(protoSchema);

		// Check if schema already exists
		auto it = protoContext->descriptors.find(name);
		if (it != protoContext->descriptors.end())
		{
			// Schema already exists, skip
			return true;
		}

		// Parse the .proto schema
		google::protobuf::io::ArrayInputStream input(schema.data(), static_cast<int>(schema.size()));
		google::protobuf::io::Tokenizer tokenizer(&input, nullptr);

		google::protobuf::compiler::Parser parser;
		google::protobuf::FileDescriptorProto fileDescProto;
		fileDescProto.set_name(name + ".proto");

		if (!parser.Parse(&tokenizer, &fileDescProto))
		{
			msg_err = "Proto schema parsing error";
			return false;
		}

		// Build descriptor from FileDescriptorProto
		const google::protobuf::FileDescriptor* fileDesc = protoContext->pool.BuildFile(fileDescProto);
		if (!fileDesc)
		{
			msg_err = "Failed to build descriptor from proto schema";
			return false;
		}

		// Find the message type (assuming first message in file)
		if (fileDesc->message_type_count() > 0)
		{
			const google::protobuf::Descriptor* descriptor = fileDesc->message_type(0);
			protoContext->descriptors[name] = descriptor;
		}
		else
		{
			msg_err = "Proto schema contains no message definitions";
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
		if (!protoContext)
		{
			msg_err = "Protobuf context not initialized";
			return false;
		}

		std::string name = std::get<std::string>(schemaName);
		std::string jsonData = std::get<std::string>(msgJson);

		// Get descriptor
		auto it = protoContext->descriptors.find(name);
		if (it == protoContext->descriptors.end())
		{
			msg_err = "Protobuf schema not found: " + name;
			return false;
		}

		const google::protobuf::Descriptor* descriptor = it->second;

		// Create dynamic message
		const google::protobuf::Message* prototype = protoContext->factory.GetPrototype(descriptor);
		if (!prototype)
		{
			msg_err = "Failed to create message prototype";
			return false;
		}

		std::unique_ptr<google::protobuf::Message> message(prototype->New());

		// Parse JSON to protobuf using boost::json
		try {
			boost::json::value json_val = boost::json::parse(jsonData);
			if (!json_val.is_object()) {
				msg_err = "JSON must be an object";
				return false;
			}

			const boost::json::object& json_obj = json_val.as_object();
			for (auto& kv : json_obj) {
				const google::protobuf::FieldDescriptor* field =
					descriptor->FindFieldByName(std::string(kv.key()));
				if (field) {
					if (!SetProtobufFieldFromJson(message.get(), field, kv.value())) {
						msg_err = "Field assignment error: " + std::string(kv.key());
						return false;
					}
				}
			}
		}
		catch (const std::exception& e) {
			msg_err = "JSON parsing error: ";
			msg_err += e.what();
			return false;
		}

		// Serialize to binary
		if (!message->SerializeToString(&protobufData))
		{
			msg_err = "Protobuf message serialization error";
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
		msg_err = "Protobuf data is empty";
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
		if (!protoContext)
		{
			msg_err = "Protobuf context not initialized";
			return std::string("");
		}

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
			msg_err = "Invalid data type for protobufData";
			return std::string("");
		}

		if (dataPtr->empty())
		{
			msg_err = "Protobuf data is empty";
			return std::string("");
		}

		// Get descriptor
		auto it = protoContext->descriptors.find(name);
		if (it == protoContext->descriptors.end())
		{
			msg_err = "Protobuf schema not found: " + name;
			return std::string("");
		}

		const google::protobuf::Descriptor* descriptor = it->second;

		// Create dynamic message
		const google::protobuf::Message* prototype = protoContext->factory.GetPrototype(descriptor);
		if (!prototype)
		{
			msg_err = "Failed to create message prototype";
			return std::string("");
		}

		std::unique_ptr<google::protobuf::Message> message(prototype->New());

		// Parse binary data
		if (!message->ParseFromString(*dataPtr))
		{
			msg_err = "Protobuf message deserialization error";
			return std::string("");
		}

		bool returnAsJson = std::get<bool>(asJson);

		if (returnAsJson)
		{
			// Convert to JSON using boost::json
			try {
				boost::json::object json_obj;

				for (int i = 0; i < descriptor->field_count(); i++)
				{
					const google::protobuf::FieldDescriptor* field = descriptor->field(i);
					const google::protobuf::Reflection* reflection = message->GetReflection();

					if (reflection->HasField(*message, field))
					{
						json_obj[field->name()] = GetJsonFromProtobufField(*message, field);
					}
				}

				std::string jsonOutput = boost::json::serialize(json_obj);
				return jsonOutput;
			}
			catch (const std::exception& e) {
				msg_err = "Protobuf to JSON conversion error: ";
				msg_err += e.what();
				return std::string("");
			}
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
		msg_err = "Producer not initialized";
		return -1;
	}
	if (protobufData.empty())
	{
		msg_err = "Protobuf data is empty";
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

	RdKafka::Headers* hdrs = parseKafkaHeaders(std::get<std::string>(heads));

	RdKafka::ErrorCode resp = produceWithRetry([&]() -> RdKafka::ErrorCode {
		return hProducer->produce(
			tTopicName,
			currentPartition == -1 ? RdKafka::Topic::PARTITION_UA : currentPartition,
			RdKafka::Producer::RK_MSG_COPY,
			const_cast<char*>(protobufData.data()),
			protobufData.size(),
			std::get<std::string>(key).c_str(), std::get<std::string>(key).size(),
			0,
			hdrs,
			nullptr);
	}, eventFile);

	if (resp != RdKafka::ERR_NO_ERROR)
	{
		if (resp == RdKafka::ERR_MSG_SIZE_TOO_LARGE)
		{
			msg_err = "Message size exceeds message.max.bytes limit (default 1 MB). Use SetParameter to increase the limit.";
			if (hdrs != nullptr)
			{
				delete hdrs;
			}
			if (eventFile.is_open())
			{
				eventFile << currentDateTime() << " Error: " << msg_err << std::endl;
			}
			return cl_dr_cb.delivered;
		}
		msg_err = RdKafka::err2str(resp);
		if (hdrs != nullptr)
		{
			delete hdrs;
		}
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
	return produceAndWaitResult([&]() { return produceProtobuf(topicName, partition, key, heads); }, "produceProtobufWithWaitResult");
}
