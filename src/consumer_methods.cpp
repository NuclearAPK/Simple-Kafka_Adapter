/*
 *  Simple Kafka 1C - Consumer Methods
 *  Consumer implementation
 */

#include "SimpleKafka1C.h"
#include "utils.h"

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/json.hpp>

#include <fstream>
#include <sstream>

namespace {
std::string base64Encode(const uint8_t* data, size_t len)
{
	static constexpr char table[] =
		"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

	std::string out;
	out.reserve(((len + 2) / 3) * 4);

	for (size_t i = 0; i < len; i += 3)
	{
		const uint32_t octetA = data[i];
		const uint32_t octetB = (i + 1 < len) ? data[i + 1] : 0;
		const uint32_t octetC = (i + 2 < len) ? data[i + 2] : 0;
		const uint32_t triple = (octetA << 16) | (octetB << 8) | octetC;

		out.push_back(table[(triple >> 18) & 0x3F]);
		out.push_back(table[(triple >> 12) & 0x3F]);
		out.push_back((i + 1 < len) ? table[(triple >> 6) & 0x3F] : '=');
		out.push_back((i + 2 < len) ? table[triple & 0x3F] : '=');
	}

	return out;
}
}

//================================== Consumer ==========================================

bool SimpleKafka1C::initConsumer(const variant_t& brokers)
{
	std::string tBrokers = std::get<std::string>(brokers);

	// Валидация адреса брокеров
	if (!isValidBrokerList(tBrokers, msg_err))
	{
		return false;
	}

	std::ofstream eventFile{};
	RdKafkaConfPtr conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

	cl_event_cb.logDir = std::get<std::string>(*logDirectory);
	cl_event_cb.formatLogFiles = std::get<std::string>(*formatLogFiles);
	cl_event_cb.consumerLogName = consumerLogName;
	cl_event_cb.statLogName = statLogName;
	cl_event_cb.pid = pid;
	cl_event_cb.clientid = clientID();

	openEventFile(consumerLogName, eventFile);
	if (eventFile.is_open()) eventFile << currentDateTime() << " Simple Kafka version: " << Version << " (librdkafka version: " << RdKafka::version_str() << ")" << std::endl;

	// дополнительные параметры
	cl_event_cb.statisticsOn = false;
	if (!applyKafkaSettings(conf.get(), msg_err, &cl_event_cb.statisticsOn))
	{
		if (eventFile.is_open()) eventFile << currentDateTime() << msg_err << std::endl;
		return false;
	}
	// обязательный параметр
	if (conf->set("metadata.broker.list", tBrokers, msg_err) != RdKafka::Conf::CONF_OK)
	{
		if (eventFile.is_open()) eventFile << currentDateTime() << msg_err << std::endl;
		return false;
	}

	// обратные вызовы для получения статистики и получения ошибок для дальнейшей обработки
	conf->set("event_cb", &cl_event_cb, msg_err);

	if (cl_rebalance_cb.offsets.size() > 0)
	{
		conf->set("rebalance_cb", &cl_rebalance_cb, msg_err);
	}

	if (eventFile.is_open()) eventFile << currentDateTime() << " Info: initConsumer. brokers-" << tBrokers << std::endl;

	hConsumer = RdKafka::KafkaConsumer::create(conf.get(), msg_err);
	if (!hConsumer)
	{
		msg_err = enrichSslError(msg_err);
		if (eventFile.is_open()) eventFile << currentDateTime() << msg_err << std::endl;
		return false;
	}

	if (eventFile.is_open())
		eventFile << currentDateTime() << " Created consumer: " << hConsumer->name() << std::endl;

	return true;
}

bool SimpleKafka1C::subscribe(const variant_t& topic)
{
	std::ofstream eventFile{};
	openEventFile(consumerLogName, eventFile);
	if (hConsumer == nullptr)
	{
		msg_err = "Consumer not initialized";
		return false;
	}

	std::vector<std::string> topics;
	boost::algorithm::split(topics, std::get<std::string>(topic), boost::is_any_of(","));

	RdKafka::ErrorCode resp = hConsumer->subscribe(topics);

	if (resp != RdKafka::ERR_NO_ERROR)
	{
		msg_err = RdKafka::err2str(resp);
		if (eventFile.is_open()) eventFile << currentDateTime() << " Failed to start subscribe consumer: " << msg_err << std::endl;
		return false;
	}

	return true;
}

bool SimpleKafka1C::setWaitingTimeout(const variant_t& timeout)
{
	waitMessageTimeout = std::get<int32_t>(timeout);
	return true;
}

bool SimpleKafka1C::setProducerFlushTimeout(const variant_t& timeout)
{
	producerFlushTimeout = std::get<int32_t>(timeout);
	return true;
}

bool SimpleKafka1C::setConsumerCloseTimeout(const variant_t& timeout)
{
	consumerCloseTimeout = std::get<int32_t>(timeout);
	return true;
}

bool SimpleKafka1C::setAdminOperationTimeout(const variant_t& timeout)
{
	adminOperationTimeout = std::get<int32_t>(timeout);
	return true;
}

bool SimpleKafka1C::setReadingPosition(const variant_t& topicName, const variant_t& offset, const variant_t& partition)
{
	auto assignOffset = std::get<int32_t>(offset);
	auto assignTopic = std::get<std::string>(topicName);
	auto assignPartition = std::get<int32_t>(partition);

	RdKafka::TopicPartition* ptr = RdKafka::TopicPartition::create(assignTopic, assignPartition, assignOffset);
	cl_rebalance_cb.offsets.push_back(ptr);
	return true;
}

bool SimpleKafka1C::setReadingPositions(const variant_t& jsonTopicPartitions)
{
	using namespace boost::json;
	std::string jsonString = std::get<std::string>(jsonTopicPartitions);

	auto parsed_data = parse(jsonString);
	auto meta = parsed_data.at("metadata");

	if (meta.is_array())
	{
		for (size_t i = 0; i < meta.as_array().size(); i++)
		{
			std::string topic_ = value_to<std::string>(meta.at(i).at("topic"));
			int partition_ = value_to<int>(meta.at(i).at("partition"));
			long long offset_ = value_to<long long>(meta.at(i).at("offset"));

			RdKafka::TopicPartition* ptr = RdKafka::TopicPartition::create(topic_, partition_, offset_);
			cl_rebalance_cb.offsets.push_back(ptr);
		}
	}
	return true;
}

std::string SimpleKafka1C::consume()
{
	if (hConsumer == nullptr)
	{
		msg_err = "Consumer not initialized";
		return EMPTYSTR;
	}

	std::stringstream s{};
	msg_err = EMPTYSTR;
	std::ofstream eventFile{};
	boost::property_tree::ptree jsonObj;
	RdKafka::Headers* headers;
	RdKafka::Message* msg = hConsumer->consume(waitMessageTimeout);
	RdKafka::ErrorCode resultConsume = msg->err();

	openEventFile(consumerLogName, eventFile);
	if (resultConsume == RdKafka::ERR_NO_ERROR)
	{
		boost::property_tree::ptree headersChildren;
		auto payload = static_cast<char*>(msg->payload());

		if (msg->key() && (*msg->key()).length() > 0)
		{
			jsonObj.put("key", *msg->key());
		}

		headers = msg->headers();
		if (headers)
		{
			std::vector<RdKafka::Headers::Header> hdrs = headers->get_all();
			for (size_t i = 0; i < hdrs.size(); i++)
			{
				RdKafka::Headers::Header hdr = hdrs[i];

				boost::property_tree::ptree node;
				node.put("key", hdr.key().c_str());
				node.put("value", (const char*)hdr.value());

				headersChildren.push_back(boost::property_tree::ptree::value_type("", node));
			}
		}

		RdKafka::MessageTimestamp ts = msg->timestamp();

		jsonObj.put("partition", msg->partition());
		jsonObj.put("offset", (long)msg->offset());
		jsonObj.put("message", std::string(slice(payload, 0, msg->len())));
		jsonObj.put("topic", msg->topic_name());
		jsonObj.put("broker_id", msg->broker_id());
		jsonObj.put("timestamp", ts.timestamp);

		if (headersChildren.size())
		{
			jsonObj.put_child("headers", headersChildren);
		}

		delete msg;
	}
	else
	{
		if (resultConsume != RdKafka::ERR__TIMED_OUT) {
			msg_err = msg->errstr();
			if (eventFile.is_open())
			{
				eventFile << currentDateTime() << " Error: " << msg_err << std::endl;
			}
		}
		delete msg;
		return EMPTYSTR;
	}
	boost::property_tree::write_json(s, jsonObj, true);

	return s.str();
}

bool SimpleKafka1C::getMessage()
{
	if (hConsumer == nullptr)
	{
		msg_err = "Consumer not initialized";
		return false;
	}

	RdKafka::Message* msg = hConsumer->consume(waitMessageTimeout);
	RdKafka::ErrorCode resultConsume = msg->err();

	std::ofstream eventFile;

	openEventFile(consumerLogName, eventFile);
	clearMessageMetadata();
	if (resultConsume == RdKafka::ERR_NO_ERROR)
	{
		this->messageLen = msg->len();

		u_char* charBuf = (u_char*)msg->payload();
		std::vector<char> binaryData(charBuf, charBuf + this->messageLen);
		messageData = binaryData;

		if (msg->key() && (*msg->key()).length() > 0)
		{
			this->key = *msg->key();
		}

		RdKafka::Headers* headers;
		headers = msg->headers();
		if (headers)
		{
			std::vector<RdKafka::Headers::Header> hdrs = headers->get_all();
			for (size_t i = 0; i < hdrs.size(); i++) {
				RdKafka::Headers::Header hdr = hdrs[i];

				HeadersMessage mh{ hdr.key().c_str(), (const char*)hdr.value() };
				messageHeaders.push_back(mh);
			}
		}

		RdKafka::MessageTimestamp ts = msg->timestamp();

		this->timestamp = ts.timestamp;
		this->partition = msg->partition();
		this->offset = msg->offset();
		this->topic = msg->topic_name();
		this->broker_id = msg->broker_id();

		// Обновляем метрики консьюмера
		consumerMetrics.messagesConsumed++;
		consumerMetrics.bytesConsumed += this->messageLen;

		delete msg;
	}
	else
	{
		if (resultConsume == RdKafka::ERR__TIMED_OUT) {
			consumerMetrics.pollTimeouts++;
		}
		else {
			msg_err = msg->errstr();
			consumerMetrics.errorsCount++;
			if (eventFile.is_open()) eventFile << currentDateTime() << " Error: " << msg_err << std::endl;
		}
		delete msg;
		return false;
	}

	return true;
}

std::string SimpleKafka1C::consumeBatch(const variant_t& maxMessages, const variant_t& maxWaitMs, const variant_t& asBase64)
{
	if (hConsumer == nullptr)
	{
		msg_err = "Consumer not initialized";
		return "";
	}

	int32_t maxMsgCount = std::get<int32_t>(maxMessages);
	int32_t maxWait = std::get<int32_t>(maxWaitMs);

	if (maxMsgCount <= 0) maxMsgCount = 100;
	if (maxWait <= 0) maxWait = 1000;
	bool encodeAsBase64 = std::holds_alternative<bool>(asBase64) && std::get<bool>(asBase64);

	boost::json::array messagesArray;
	auto startTime = std::chrono::steady_clock::now();
	int32_t messagesRead = 0;

	while (messagesRead < maxMsgCount)
	{
		// Проверяем, не истекло ли время ожидания
		auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
			std::chrono::steady_clock::now() - startTime).count();

		if (elapsed >= maxWait)
			break;

		int32_t remainingWait = static_cast<int32_t>(maxWait - elapsed);
		if (remainingWait <= 0) remainingWait = 1;

		RdKafka::Message* msg = hConsumer->consume(remainingWait);
		RdKafka::ErrorCode resultConsume = msg->err();

		if (resultConsume == RdKafka::ERR_NO_ERROR)
		{
			boost::json::object msgObj;

			// Данные сообщения
			const char* payload = static_cast<const char*>(msg->payload());
			if (encodeAsBase64)
			{
				msgObj["message"] = base64Encode(reinterpret_cast<const uint8_t*>(payload), msg->len());
			}
			else
			{
				msgObj["message"] = std::string(payload, msg->len());
			}

			// Ключ
			if (msg->key() && !msg->key()->empty())
			{
				msgObj["key"] = *msg->key();
			}

			// Метаданные
			msgObj["topic"] = msg->topic_name();
			msgObj["partition"] = msg->partition();
			msgObj["offset"] = msg->offset();
			msgObj["broker_id"] = msg->broker_id();

			RdKafka::MessageTimestamp ts = msg->timestamp();
			msgObj["timestamp"] = ts.timestamp;

			// Заголовки
			RdKafka::Headers* headers = msg->headers();
			if (headers)
			{
				boost::json::array headersArray;
				std::vector<RdKafka::Headers::Header> hdrs = headers->get_all();
				for (const auto& hdr : hdrs)
				{
					boost::json::object headerObj;
					headerObj["key"] = hdr.key();
					headerObj["value"] = std::string(static_cast<const char*>(hdr.value()), hdr.value_size());
					headersArray.push_back(headerObj);
				}
				msgObj["headers"] = headersArray;
			}

			messagesArray.push_back(msgObj);
			messagesRead++;

			// Обновляем метрики
			consumerMetrics.messagesConsumed++;
			consumerMetrics.bytesConsumed += msg->len();
		}
		else if (resultConsume == RdKafka::ERR__TIMED_OUT)
		{
			consumerMetrics.pollTimeouts++;
			// При таймауте выходим из цикла, если уже есть сообщения
			if (messagesRead > 0)
			{
				delete msg;
				break;
			}
		}
		else
		{
			consumerMetrics.errorsCount++;
		}

		delete msg;
	}

	boost::json::object result;
	result["messages"] = messagesArray;
	result["count"] = messagesRead;

	return boost::json::serialize(result);
}

variant_t SimpleKafka1C::getMessageData(const variant_t& binaryResult)
{
	bool res = std::get<bool>(binaryResult);
	if (res)
	{
		return this->messageData;
	}
	else
	{
		return std::string(messageData.begin(), messageData.end());
	}
}

std::string SimpleKafka1C::getMessageKey()
{
	return this->key;
}

std::string SimpleKafka1C::getMessageHeaders()
{
	std::stringstream s{};
	boost::property_tree::ptree jsonObj;
	boost::property_tree::ptree headersChildren;

	for (size_t i = 0; i < messageHeaders.size(); i++)
	{
		boost::property_tree::ptree node;
		node.put("key", messageHeaders[i].Key);
		node.put("value", messageHeaders[i].Value);

		headersChildren.push_back(boost::property_tree::ptree::value_type("", node));
	}

	if (headersChildren.size())
	{
		jsonObj.put_child("headers", headersChildren);
	}

	boost::property_tree::write_json(s, jsonObj, true);
	return s.str();
}

int32_t SimpleKafka1C::getMessageOffset()
{
	return (int32_t) this->offset;
}

std::string SimpleKafka1C::getMessageTopicName()
{
	return this->topic;
}

int32_t SimpleKafka1C::getMessageBrokerID()
{
	return this->broker_id;
}

double SimpleKafka1C::getMessageTimestamp()
{
	return static_cast<double>(this->timestamp) / 1000.0;
}

std::string SimpleKafka1C::getMessageTimestampISO()
{
	auto seconds = std::chrono::seconds(this->timestamp / 1000);
	auto millis = std::chrono::milliseconds(this->timestamp % 1000);
	std::chrono::system_clock::time_point tp(seconds);

	std::time_t tt = std::chrono::system_clock::to_time_t(tp);
	std::tm utc_tm;
#ifdef _WIN32
	gmtime_s(&utc_tm, &tt);
#else
	gmtime_r(&tt, &utc_tm);
#endif

	char buf[32];
	std::snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
		utc_tm.tm_year + 1900, utc_tm.tm_mon + 1, utc_tm.tm_mday,
		utc_tm.tm_hour, utc_tm.tm_min, utc_tm.tm_sec,
		static_cast<int>(millis.count()));

	return std::string(buf);
}

int32_t SimpleKafka1C::getMessagePartition()
{
	return this->partition;
}

bool SimpleKafka1C::commitOffset(const variant_t& topicName, const variant_t& offset, const variant_t& partition)
{
	std::vector<RdKafka::TopicPartition*> offsets;
	std::int32_t tOffset = std::get<std::int32_t>(offset);
	std::int32_t tPartition = std::get<std::int32_t>(partition);
	std::string tTopicName = std::get<std::string>(topicName);

	RdKafka::TopicPartition* ptr = RdKafka::TopicPartition::create(tTopicName, tPartition, tOffset);
	offsets.push_back(ptr);

	RdKafka::ErrorCode err = hConsumer->commitSync(offsets);

	// Очистка TopicPartition* для предотвращения утечки памяти
	for (auto tp : offsets) {
		delete tp;
	}

	if (err != RdKafka::ERR_NO_ERROR)
	{
		return false;
	}
	return true;
}

bool SimpleKafka1C::stopConsumer()
{
	if (hConsumer != nullptr)
	{
		RdKafka::ErrorCode closeErr = hConsumer->close();

		if (closeErr != RdKafka::ERR_NO_ERROR)
		{
			msg_err = "Consumer close error: " + RdKafka::err2str(closeErr);
			delete hConsumer;
			hConsumer = nullptr;
			RdKafka::wait_destroyed(consumerCloseTimeout);
			return false;
		}

		delete hConsumer;
		hConsumer = nullptr;

		// Ожидаем завершения всех фоновых операций
		int waitResult = RdKafka::wait_destroyed(consumerCloseTimeout);
		if (waitResult != 0)
		{
			msg_err = "Consumer wait_destroyed timeout: " + std::to_string(waitResult) + " object(s) still exist";
			return false;
		}
	}
	return true;
}

void SimpleKafka1C::clearMessageMetadata()
{
	this->messageData.clear();
	this->messageHeaders.clear();

	this->messageLen = 0;
	this->timestamp = 0;
	this->key = EMPTYSTR;
	this->topic = EMPTYSTR;
	this->broker_id = 0;
	this->partition = 0;
	this->offset = 0;
}
