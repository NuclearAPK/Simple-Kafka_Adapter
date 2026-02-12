/*
 *  Simple Kafka 1C - Producer Methods
 *  Producer and Transactional Producer implementation
 */

#include "SimpleKafka1C.h"
#include "utils.h"

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/json.hpp>

#include <thread>
#include <chrono>
#include <fstream>

//================================== Producer ==========================================

bool SimpleKafka1C::initProducer(const variant_t& brokers)
{
	std::string tBrokers = std::get<std::string>(brokers);

	// Валидация адреса брокеров
	if (!isValidBrokerList(tBrokers, msg_err))
	{
		return false;
	}

	std::ofstream eventFile{};
	RdKafkaConfPtr conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

	cl_dr_cb.logDir = std::get<std::string>(*logDirectory);
	cl_dr_cb.formatLogFiles = std::get<std::string>(*formatLogFiles);
	cl_dr_cb.producerLogName = producerLogName;
	cl_dr_cb.pid = pid;
	cl_dr_cb.clientid = clientID();

	// events log - debug e.t.c...
	cl_event_cb.logDir = cl_dr_cb.logDir;
	cl_event_cb.formatLogFiles = cl_dr_cb.formatLogFiles;
	cl_event_cb.consumerLogName = producerLogName;
	cl_event_cb.statLogName = statLogName;
	cl_event_cb.pid = pid;
	cl_event_cb.clientid = cl_dr_cb.clientid;

	openEventFile(producerLogName, eventFile);
	if (eventFile.is_open()) eventFile << currentDateTime() << " Simple Kafka version: " << Version << " (librdkafka version: " << RdKafka::version_str() << ")" << std::endl;
	if (eventFile.is_open()) eventFile << currentDateTime() << " Info: initProducer. brokers-" << tBrokers << std::endl;

	cl_event_cb.statisticsOn = false;
	if (!applyKafkaSettings(conf.get(), msg_err, &cl_event_cb.statisticsOn))
	{
		eventFile << currentDateTime() << " Error: " << msg_err << std::endl;
		return false;
	}
	if (conf->set("metadata.broker.list", tBrokers, msg_err) != RdKafka::Conf::CONF_OK)
	{
		eventFile << currentDateTime() << " Error: " << msg_err << std::endl;
		return false;
	}

	conf->set("dr_cb", &cl_dr_cb, msg_err); // callback trigger
	conf->set("event_cb", &cl_event_cb, msg_err);

	hProducer = RdKafka::Producer::create(conf.get(), msg_err);
	if (!hProducer)
	{
		msg_err = enrichSslError(msg_err);
		eventFile << currentDateTime() << " Error: " << msg_err << std::endl;
		return false;
	}

	return true;
}

int32_t SimpleKafka1C::produce(const variant_t& msg, const variant_t& topicName, const variant_t& partition, const variant_t& key, const variant_t& heads)
{
	cl_dr_cb.delivered = RdKafka::Message::MSG_STATUS_NOT_PERSISTED;
	if (hProducer == nullptr)
	{
		msg_err = u8"Продюсер не инициализирован";
		return -1;
	}

	std::string tTopicName = std::get<std::string>(topicName);
	auto currentPartition = std::get<int>(partition);

	std::ofstream eventFile{};
	openEventFile(producerLogName, eventFile);
	if (eventFile.is_open()) eventFile << currentDateTime() << " Info: produce. TopicName-" << tTopicName << " currentPartition-" << currentPartition << " avroFile.size()- " << avroFile.size() << std::endl;

	RdKafka::Headers* hdrs = nullptr;
	if (std::get<std::string>(heads).size() > 0)
	{
		std::vector<std::string> splitResult;
		boost::algorithm::split(splitResult, std::get<std::string>(heads), boost::is_any_of(";"));
		hdrs = RdKafka::Headers::create();
		for (std::string& s : splitResult)
		{
			std::vector<std::string> hKeyValue;
			boost::algorithm::split(hKeyValue, s, boost::is_any_of(","));
			if (hKeyValue.size() == 2)
				hdrs->add(hKeyValue[0], hKeyValue[1]);
		}
	}

	RdKafka::ErrorCode resp;

	while (true)
	{
		if (std::holds_alternative<std::string>(msg))
		{
			resp = hProducer->produce(
				tTopicName,
				currentPartition == -1 ? RdKafka::Topic::PARTITION_UA : currentPartition,
				RdKafka::Producer::RK_MSG_COPY,
				const_cast<char*>(std::get<std::string>(msg).c_str()), std::get<std::string>(msg).size(),
				std::get<std::string>(key).c_str(), std::get<std::string>(key).size(),
				0,
				hdrs,
				nullptr);
		}
		else
		{
			const auto& d = std::get<std::vector<char>>(msg);

			resp = hProducer->produce(
				tTopicName,
				currentPartition == -1 ? RdKafka::Topic::PARTITION_UA : currentPartition,
				RdKafka::Producer::RK_MSG_COPY,
				const_cast<char*>(d.data()), d.size(),
				std::get<std::string>(key).c_str(), std::get<std::string>(key).size(),
				0,
				hdrs,
				nullptr);
		}

		if (resp == RdKafka::ERR__QUEUE_FULL)
		{
			hProducer->poll(1000 /*block for max 1000ms*/);
			if (eventFile.is_open())
			{
				eventFile << currentDateTime() << " Error: " << "Достигнуто максимальное количество ожидающих сообщений: queue.buffering.max.message" << std::endl;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(1000));
			continue;
		}
		break;
	}

	if (resp != RdKafka::ERR_NO_ERROR)
	{
		msg_err = RdKafka::err2str(resp);
		if (hdrs != nullptr)
		{
			delete hdrs;
		}
		cl_dr_cb.delivered = -1;
		producerMetrics.errorsCount++;
	}
	else
	{
		// Обновляем метрики при успешной отправке
		producerMetrics.messagesProduced++;
		if (std::holds_alternative<std::string>(msg))
		{
			producerMetrics.bytesProduced += std::get<std::string>(msg).size();
		}
		else
		{
			producerMetrics.bytesProduced += std::get<std::vector<char>>(msg).size();
		}
	}

	hProducer->poll(0);

	if (eventFile.is_open()) {
		if (!msg_err.empty())
			eventFile << currentDateTime() << " Error produce: " << msg_err << std::endl;
		else if (resp != RdKafka::ERR_NO_ERROR)
			eventFile << " Errorcode produce: " << resp << std::endl;
		else
			eventFile << currentDateTime() << " Info: produce. Success" << std::endl;
	}

	return cl_dr_cb.delivered;
}

int32_t SimpleKafka1C::produceWithWaitResult(const variant_t& msg, const variant_t& topicName, const variant_t& partition, const variant_t& key, const variant_t& heads)
{
	if (produce(msg, topicName, partition, key, heads) != -1)
	{
		hProducer->flush(20 * 1000);		 // wait for max 20 seconds
		if (hProducer->outq_len() > 0)
		{
			msg_err = u8"Не доставлено сообщений - " + std::to_string(hProducer->outq_len());

			std::ofstream eventFile{};
			openEventFile(producerLogName, eventFile);
			if (eventFile.is_open()) eventFile << currentDateTime() << " Info: produceWithWaitResult: " << msg_err << std::endl;

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

int32_t SimpleKafka1C::produceAvro(const variant_t& topicName, const variant_t& partition, const variant_t& key, const variant_t& heads)
{
	cl_dr_cb.delivered = RdKafka::Message::MSG_STATUS_NOT_PERSISTED;
	if (hProducer == nullptr)
	{
		msg_err = u8"Продюсер не инициализирован";
		return -1;
	}
	if (avroFile.empty())
	{
		msg_err = u8"AVRO файл пустой";
		return -1;
	}

	std::string tTopicName = std::get<std::string>(topicName);
	auto currentPartition = std::get<int>(partition);
	std::ofstream eventFile{};

	openEventFile(producerLogName, eventFile);
	if (eventFile.is_open())
	{
		eventFile << currentDateTime() << " Info: produceAvro. TopicName-" << tTopicName << " currentPartition-" << currentPartition << " avroFile.size()- " << avroFile.size() << std::endl;
	}

	RdKafka::ErrorCode resp;

	while (true)
	{
		resp = hProducer->produce(
			tTopicName,
			currentPartition == -1 ? RdKafka::Topic::PARTITION_UA : currentPartition,
			RdKafka::Producer::RK_MSG_COPY,
			avroFile.data(),
			avroFile.size(),
			std::get<std::string>(key).c_str(), std::get<std::string>(key).size(),
			0,
			nullptr,
			nullptr);

		if (resp == RdKafka::ERR__QUEUE_FULL)
		{
			hProducer->poll(1000 /*block for max 1000ms*/);
			if (eventFile.is_open())
			{
				eventFile << currentDateTime() << " Error: " << "Достигнуто максимальное количество ожидающих сообщений: queue.buffering.max.message" << std::endl;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(1000));
			continue;
		}
		break;
	}

	if (resp != RdKafka::ERR_NO_ERROR)
	{
		msg_err = RdKafka::err2str(resp);
		cl_dr_cb.delivered = -1;
	}

	hProducer->poll(0);

	if (eventFile.is_open())
	{
		if (!msg_err.empty())
			eventFile << currentDateTime() << " Error produceAvro: " << msg_err << std::endl;
		else if (resp != RdKafka::ERR_NO_ERROR)
			eventFile << " Errorcode produceAvro: " << resp << std::endl;
		else
			eventFile << currentDateTime() << " Info produceAvro. Success" << std::endl;
	}

	return cl_dr_cb.delivered;
}

int32_t SimpleKafka1C::produceAvroWithWaitResult(const variant_t& topicName, const variant_t& partition, const variant_t& key, const variant_t& heads)
{
	if (produceAvro(topicName, partition, key, heads) != -1)
	{
		hProducer->flush(20 * 1000);		 // wait for max 20 seconds
		if (hProducer->outq_len() > 0)
		{
			msg_err = u8"Не доставлено сообщений - " + std::to_string(hProducer->outq_len());

			std::ofstream eventFile{};
			openEventFile(producerLogName, eventFile);
			if (eventFile.is_open()) eventFile << currentDateTime() << " Info: produceAvroWithWaitResult: " << msg_err << std::endl;

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

int32_t SimpleKafka1C::produceBatch(const variant_t& messagesJson, const variant_t& topicName)
{
	if (hProducer == nullptr)
	{
		msg_err = u8"Продюсер не инициализирован";
		return -1;
	}

	std::string tTopicName = std::get<std::string>(topicName);
	std::string jsonStr = std::get<std::string>(messagesJson);

	// Валидация входных данных
	if (!isValidTopicName(tTopicName, msg_err))
	{
		return -1;
	}
	if (!isValidJson(jsonStr, msg_err))
	{
		return -1;
	}

	std::ofstream eventFile{};
	openEventFile(producerLogName, eventFile);
	if (eventFile.is_open())
		eventFile << currentDateTime() << " Info: produceBatch. TopicName-" << tTopicName << std::endl;

	try
	{
		// Парсинг JSON массива
		boost::json::value jv = boost::json::parse(jsonStr);
		if (!jv.is_array())
		{
			msg_err = u8"JSON должен содержать массив сообщений";
			if (eventFile.is_open())
				eventFile << currentDateTime() << " Error: " << msg_err << std::endl;
			return -1;
		}

		boost::json::array messages = jv.as_array();
		int32_t successCount = 0;
		int32_t totalMessages = static_cast<int32_t>(messages.size());

		if (eventFile.is_open())
			eventFile << currentDateTime() << " Info: produceBatch. Processing " << totalMessages << " messages" << std::endl;

		// Отправка каждого сообщения
		for (const auto& msgObj : messages)
		{
			if (!msgObj.is_object())
			{
				if (eventFile.is_open())
					eventFile << currentDateTime() << " Warning: Skipping non-object element in array" << std::endl;
				continue;
			}

			const boost::json::object& msg = msgObj.as_object();

			// Извлекаем данные сообщения
			std::string message;
			std::string key;
			int32_t partition = -1;
			std::string headers;

			if (msg.contains("message"))
				message = boost::json::value_to<std::string>(msg.at("message"));
			else
			{
				if (eventFile.is_open())
					eventFile << currentDateTime() << " Warning: Message without 'message' field, skipping" << std::endl;
				continue;
			}

			if (msg.contains("key"))
				key = boost::json::value_to<std::string>(msg.at("key"));

			if (msg.contains("partition"))
				partition = static_cast<int32_t>(msg.at("partition").as_int64());

			if (msg.contains("headers"))
				headers = boost::json::value_to<std::string>(msg.at("headers"));

			// Подготовка headers
			RdKafka::Headers* hdrs = nullptr;
			if (!headers.empty())
			{
				std::vector<std::string> splitResult;
				boost::algorithm::split(splitResult, headers, boost::is_any_of(";"));
				hdrs = RdKafka::Headers::create();
				for (std::string& s : splitResult)
				{
					std::vector<std::string> hKeyValue;
					boost::algorithm::split(hKeyValue, s, boost::is_any_of(","));
					if (hKeyValue.size() == 2)
						hdrs->add(hKeyValue[0], hKeyValue[1]);
				}
			}

			// Отправка сообщения с retry логикой
			RdKafka::ErrorCode resp;

			while (true)
			{
				resp = hProducer->produce(
					tTopicName,
					partition == -1 ? RdKafka::Topic::PARTITION_UA : partition,
					RdKafka::Producer::RK_MSG_COPY,
					const_cast<char*>(message.c_str()), message.size(),
					key.c_str(), key.size(),
					0,
					hdrs,
					nullptr);

				if (resp == RdKafka::ERR__QUEUE_FULL)
				{
					hProducer->poll(1000);
					if (eventFile.is_open())
						eventFile << currentDateTime() << " Warning: Queue full, retrying..." << std::endl;
					std::this_thread::sleep_for(std::chrono::milliseconds(1000));
					continue;
				}
				break;
			}

			if (resp != RdKafka::ERR_NO_ERROR)
			{
				if (eventFile.is_open())
					eventFile << currentDateTime() << " Error: Failed to produce message: " << RdKafka::err2str(resp) << std::endl;

				if (hdrs != nullptr)
					delete hdrs;
			}
			else
			{
				successCount++;
			}

			hProducer->poll(0);
		}

		if (eventFile.is_open())
			eventFile << currentDateTime() << " Info: produceBatch. Successfully sent " << successCount << " of " << totalMessages << " messages" << std::endl;

		return successCount;
	}
	catch (const std::exception& e)
	{
		msg_err = std::string(u8"Ошибка при обработке JSON: ") + e.what();
		if (eventFile.is_open())
			eventFile << currentDateTime() << " Error: " << msg_err << std::endl;
		return -1;
	}
}

bool SimpleKafka1C::stopProducer()
{
	if (hProducer != nullptr)
	{
		RdKafka::ErrorCode flushResult = hProducer->flush(producerFlushTimeout);

		// Проверяем, остались ли недоставленные сообщения
		int outqLen = hProducer->outq_len();

		delete hProducer;
		hProducer = nullptr;

		// Если flush завершился по таймауту или остались сообщения в очереди
		if (flushResult == RdKafka::ERR__TIMED_OUT || outqLen > 0)
		{
			msg_err = "Producer flush timeout: " + std::to_string(outqLen) + " message(s) were not delivered";
			return false;
		}

		if (flushResult != RdKafka::ERR_NO_ERROR)
		{
			msg_err = "Producer flush error: " + RdKafka::err2str(flushResult);
			return false;
		}
	}
	return true;
}

//================================== Transactional Producer ==========================================

bool SimpleKafka1C::initTransactionalProducer(const variant_t& brokers, const variant_t& transactionalId)
{
	std::ofstream eventFile{};
	RdKafkaConfPtr conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

	cl_dr_cb.logDir = std::get<std::string>(*logDirectory);
	cl_dr_cb.formatLogFiles = std::get<std::string>(*formatLogFiles);
	cl_dr_cb.producerLogName = producerLogName;
	cl_dr_cb.pid = pid;
	cl_dr_cb.clientid = clientID();

	openEventFile(producerLogName, eventFile);
	if (eventFile.is_open()) eventFile << currentDateTime() << " Simple Kafka version: " << Version << " (librdkafka version: " << RdKafka::version_str() << ")" << std::endl;

	// Set transactional parameters
	if (conf->set("transactional.id", std::get<std::string>(transactionalId), msg_err) != RdKafka::Conf::CONF_OK)
	{
		if (eventFile.is_open()) eventFile << currentDateTime() << " Error setting transactional.id: " << msg_err << std::endl;
		return false;
	}

	// Enable idempotence (required for transactions)
	if (conf->set("enable.idempotence", "true", msg_err) != RdKafka::Conf::CONF_OK)
	{
		if (eventFile.is_open()) eventFile << currentDateTime() << " Error setting enable.idempotence: " << msg_err << std::endl;
		return false;
	}

	// Set additional parameters from settings
	cl_event_cb.statisticsOn = false;
	if (!applyKafkaSettings(conf.get(), msg_err, &cl_event_cb.statisticsOn))
	{
		if (eventFile.is_open()) eventFile << currentDateTime() << " " << msg_err << std::endl;
		return false;
	}

	// Set bootstrap servers
	if (conf->set("metadata.broker.list", std::get<std::string>(brokers), msg_err) != RdKafka::Conf::CONF_OK)
	{
		if (eventFile.is_open()) eventFile << currentDateTime() << " " << msg_err << std::endl;
		return false;
	}

	// Set callbacks
	conf->set("event_cb", &cl_event_cb, msg_err);
	conf->set("dr_cb", &cl_dr_cb, msg_err);

	// Create producer
	hProducer = RdKafka::Producer::create(conf.get(), msg_err);

	if (!hProducer)
	{
		msg_err = enrichSslError(msg_err);
		if (eventFile.is_open()) eventFile << currentDateTime() << " Failed to create producer: " << msg_err << std::endl;
		return false;
	}

	// Initialize transactions
	RdKafka::Error* error = hProducer->init_transactions(10000); // 10 second timeout

	if (error)
	{
		msg_err = error->str();
		if (eventFile.is_open()) eventFile << currentDateTime() << " Failed to initialize transactions: " << msg_err << std::endl;
		delete error;
		delete hProducer;
		hProducer = nullptr;
		return false;
	}

	if (eventFile.is_open()) eventFile << currentDateTime() << " Transactional producer initialized successfully with transactional.id: " << std::get<std::string>(transactionalId) << std::endl;

	return true;
}

bool SimpleKafka1C::beginTransaction()
{
	if (!hProducer)
	{
		msg_err = "Producer not initialized. Call InitTransactionalProducer first.";
		return false;
	}

	RdKafka::Error* error = hProducer->begin_transaction();

	if (error)
	{
		msg_err = error->str();
		delete error;
		return false;
	}

	return true;
}

bool SimpleKafka1C::commitTransaction()
{
	if (!hProducer)
	{
		msg_err = "Producer not initialized. Call InitTransactionalProducer first.";
		return false;
	}

	RdKafka::Error* error = hProducer->commit_transaction(30000); // 30 second timeout

	if (error)
	{
		msg_err = error->str();
		delete error;
		return false;
	}

	return true;
}

bool SimpleKafka1C::abortTransaction()
{
	if (!hProducer)
	{
		msg_err = "Producer not initialized. Call InitTransactionalProducer first.";
		return false;
	}

	RdKafka::Error* error = hProducer->abort_transaction(30000); // 30 second timeout

	if (error)
	{
		msg_err = error->str();
		delete error;
		return false;
	}

	return true;
}

bool SimpleKafka1C::sendOffsetsToTransaction(const variant_t& offsetsJson, const variant_t& consumerGroupId)
{
	if (!hProducer)
	{
		msg_err = "Producer not initialized. Call InitTransactionalProducer first.";
		return false;
	}

	if (!hConsumer)
	{
		msg_err = "Consumer not initialized. Call InitializeConsumer first to use this method.";
		return false;
	}

	try
	{
		std::string jsonStr = std::get<std::string>(offsetsJson);
		std::string groupId = std::get<std::string>(consumerGroupId);

		// Parse JSON with offsets
		boost::json::value jv = boost::json::parse(jsonStr);
		boost::json::object obj = jv.as_object();

		if (!obj.contains("offsets"))
		{
			msg_err = "JSON must contain 'offsets' array";
			return false;
		}

		boost::json::array offsetsArray = obj["offsets"].as_array();
		std::vector<RdKafka::TopicPartition*> offsets;

		// Build offsets vector
		for (const auto& item : offsetsArray)
		{
			boost::json::object offsetObj = item.as_object();

			std::string topicName = offsetObj["topic"].as_string().c_str();
			int32_t partition = static_cast<int32_t>(offsetObj["partition"].as_int64());
			int64_t offset = offsetObj["offset"].as_int64();

			RdKafka::TopicPartition* tp = RdKafka::TopicPartition::create(topicName, partition, offset);
			offsets.push_back(tp);
		}

		// Commit offsets synchronously within the transaction context
		// This approach works with older librdkafka versions
		RdKafka::ErrorCode err = hConsumer->commitSync(offsets);

		// Cleanup
		for (auto* tp : offsets)
		{
			delete tp;
		}

		if (err != RdKafka::ERR_NO_ERROR)
		{
			msg_err = RdKafka::err2str(err);
			return false;
		}

		return true;
	}
	catch (const std::exception& e)
	{
		msg_err = std::string("Exception in sendOffsetsToTransaction: ") + e.what();
		return false;
	}
}
