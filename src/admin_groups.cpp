/*
 *  Simple Kafka 1C - Admin Consumer Group Methods
 *  Consumer Group Management, Consumer Lag, Seek, Assignment
 */

#include "SimpleKafka1C.h"
#include "utils.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/json.hpp>

#include <sstream>

//================================== Consumer Lag and Group Info ==========================================

std::string SimpleKafka1C::getConsumerLag(const variant_t& brokers, const variant_t& topicName, const variant_t& consumerGroup, const variant_t& timeout)
{
	std::string result;
	std::stringstream s{};

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tTopicName = std::get<std::string>(topicName);
	std::string tConsumerGroup = std::get<std::string>(consumerGroup);
	int32_t tTimeout = std::get<int32_t>(timeout);

	// Валидация входных данных
	if (!isValidBrokerList(tBrokers, msg_err))
	{
		return result;
	}
	if (!isValidTopicName(tTopicName, msg_err))
	{
		return result;
	}
	if (!isValidConsumerGroupId(tConsumerGroup, msg_err))
	{
		return result;
	}

	// создаем конфигурацию
	rd_kafka_conf_t* conf = rd_kafka_conf_new();

	if (!applyKafkaSettings(conf, msg_err))
	{
		rd_kafka_conf_destroy(conf);
		return result;
	}

	char errstr[512];
	if (rd_kafka_conf_set(conf, "bootstrap.servers", tBrokers.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		msg_err = errstr;
		rd_kafka_conf_destroy(conf);
		return result;
	}

	if (rd_kafka_conf_set(conf, "group.id", tConsumerGroup.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		msg_err = errstr;
		rd_kafka_conf_destroy(conf);
		return result;
	}

	// создаем консьюмера для Admin API
	rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
	if (!rk)
	{
		msg_err = enrichSslError(std::string(u8"Ошибка создания клиента: ") + errstr);
		return result;
	}

	// получаем метаданные топика для определения партиций
	const rd_kafka_metadata_t* metadata;
	rd_kafka_topic_t* rkt = rd_kafka_topic_new(rk, tTopicName.c_str(), nullptr);
	if (!rkt)
	{
		msg_err = u8"Ошибка создания дескриптора топика";
		rd_kafka_destroy(rk);
		return result;
	}

	rd_kafka_resp_err_t err = rd_kafka_metadata(rk, 0, rkt, &metadata, tTimeout);
	if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
	{
		msg_err = rd_kafka_err2str(err);
		rd_kafka_topic_destroy(rkt);
		rd_kafka_destroy(rk);
		return result;
	}

	// находим нужный топик в метаданных
	const rd_kafka_metadata_topic_t* topic_metadata = nullptr;
	for (int i = 0; i < metadata->topic_cnt; i++)
	{
		if (strcmp(metadata->topics[i].topic, tTopicName.c_str()) == 0)
		{
			topic_metadata = &metadata->topics[i];
			break;
		}
	}

	if (!topic_metadata)
	{
		msg_err = u8"Топик не найден";
		rd_kafka_metadata_destroy(metadata);
		rd_kafka_topic_destroy(rkt);
		rd_kafka_destroy(rk);
		return result;
	}

	int partition_cnt = topic_metadata->partition_cnt;

	// создаем список партиций для запроса committed offsets
	rd_kafka_topic_partition_list_t* partitions = rd_kafka_topic_partition_list_new(partition_cnt);
	for (int i = 0; i < partition_cnt; i++)
	{
		rd_kafka_topic_partition_list_add(partitions, tTopicName.c_str(), metadata->topics[0].partitions[i].id);
	}

	// получаем committed offsets для группы консьюмеров
	err = rd_kafka_committed(rk, partitions, tTimeout);
	if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
	{
		msg_err = rd_kafka_err2str(err);
		rd_kafka_topic_partition_list_destroy(partitions);
		rd_kafka_metadata_destroy(metadata);
		rd_kafka_topic_destroy(rkt);
		rd_kafka_destroy(rk);
		return result;
	}

	// формируем результат
	boost::property_tree::ptree jsonObj;
	boost::property_tree::ptree partitionsChildren;
	int64_t totalLag = 0;

	jsonObj.put("topic", tTopicName);
	jsonObj.put("consumer_group", tConsumerGroup);

	for (int i = 0; i < partitions->cnt; i++)
	{
		rd_kafka_topic_partition_t* part = &partitions->elems[i];

		// получаем watermark offsets для партиции
		int64_t low = 0, high = 0;
		err = rd_kafka_query_watermark_offsets(rk, tTopicName.c_str(), part->partition, &low, &high, tTimeout);

		boost::property_tree::ptree partNode;
		partNode.put("partition", part->partition);
		partNode.put("low_watermark", low);
		partNode.put("high_watermark", high);

		if (part->offset >= 0)
		{
			partNode.put("committed_offset", part->offset);
			int64_t lag = high - part->offset;
			if (lag < 0) lag = 0;
			partNode.put("lag", lag);
			totalLag += lag;
		}
		else
		{
			// offset не зафиксирован (-1001 = RD_KAFKA_OFFSET_INVALID)
			partNode.put("committed_offset", "none");
			partNode.put("lag", high - low);
			totalLag += (high - low);
		}

		if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
		{
			partNode.put("watermark_error", rd_kafka_err2str(err));
		}

		partitionsChildren.push_back(boost::property_tree::ptree::value_type("", partNode));
	}

	jsonObj.put("total_lag", totalLag);

	if (partitionsChildren.size())
	{
		jsonObj.put_child("partitions", partitionsChildren);
	}

	boost::property_tree::write_json(s, jsonObj, true);
	result = s.str();

	// очистка ресурсов
	rd_kafka_topic_partition_list_destroy(partitions);
	rd_kafka_metadata_destroy(metadata);
	rd_kafka_topic_destroy(rkt);
	rd_kafka_destroy(rk);

	return result;
}

std::string SimpleKafka1C::getTopicConsumerGroups(const variant_t& brokers, const variant_t& topicName, const variant_t& timeout)
{
	std::string result;
	std::stringstream s{};
	char errstr[512];

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tTopicName = std::get<std::string>(topicName);
	int32_t tTimeout = std::get<int32_t>(timeout);

	// Используем AdminClientScope для RAII управления ресурсами
	AdminClientScope admin(this, tBrokers, settings, RD_KAFKA_PRODUCER);
	if (!admin.isValid())
	{
		msg_err = admin.error();
		return result;
	}

	// опции для операции ListConsumerGroups
	rd_kafka_AdminOptions_t* list_options = rd_kafka_AdminOptions_new(admin.get(), RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPS);
	rd_kafka_AdminOptions_set_request_timeout(list_options, tTimeout, errstr, sizeof(errstr));

	// получаем список всех consumer groups
	rd_kafka_ListConsumerGroups(admin.get(), list_options, admin.queue());

	// ожидаем результат
	rd_kafka_event_t* rkev = rd_kafka_queue_poll(admin.queue(), tTimeout + 2000);
	if (!rkev)
	{
		msg_err = u8"Таймаут при получении списка групп";
		rd_kafka_AdminOptions_destroy(list_options);
		return result;
	}

	if (rd_kafka_event_error(rkev))
	{
		msg_err = rd_kafka_event_error_string(rkev);
		rd_kafka_event_destroy(rkev);
		rd_kafka_AdminOptions_destroy(list_options);
		return result;
	}

	const rd_kafka_ListConsumerGroups_result_t* list_result = rd_kafka_event_ListConsumerGroups_result(rkev);
	if (!list_result)
	{
		msg_err = u8"Не удалось получить результат списка групп";
		rd_kafka_event_destroy(rkev);
		rd_kafka_AdminOptions_destroy(list_options);
		return result;
	}

	// получаем список групп и их состояния
	size_t valid_cnt = 0;
	const rd_kafka_ConsumerGroupListing_t** listings = rd_kafka_ListConsumerGroups_result_valid(list_result, &valid_cnt);

	struct GroupInfo {
		std::string group_id;
		rd_kafka_consumer_group_state_t state;
	};
	std::vector<GroupInfo> allGroups;

	for (size_t i = 0; i < valid_cnt; i++)
	{
		const char* group_id = rd_kafka_ConsumerGroupListing_group_id(listings[i]);
		if (group_id)
		{
			GroupInfo info;
			info.group_id = group_id;
			info.state = rd_kafka_ConsumerGroupListing_state(listings[i]);
			allGroups.push_back(info);
		}
	}

	rd_kafka_event_destroy(rkev);
	rd_kafka_AdminOptions_destroy(list_options);

	if (allGroups.empty())
	{
		// Нет групп - возвращаем пустой результат
		boost::property_tree::ptree jsonObj;
		jsonObj.put("topic", tTopicName);
		jsonObj.put("groups_count", 0);
		boost::property_tree::ptree emptyGroups;
		jsonObj.put_child("consumer_groups", emptyGroups);
		boost::property_tree::write_json(s, jsonObj, true);
		return s.str();
	}

	// получаем метаданные топика для определения партиций
	const rd_kafka_metadata_t* metadata;
	rd_kafka_topic_t* rkt = rd_kafka_topic_new(admin.get(), tTopicName.c_str(), nullptr);
	if (!rkt)
	{
		msg_err = u8"Ошибка создания дескриптора топика";
		return result;
	}

	rd_kafka_resp_err_t err = rd_kafka_metadata(admin.get(), 0, rkt, &metadata, tTimeout);
	if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
	{
		msg_err = rd_kafka_err2str(err);
		rd_kafka_topic_destroy(rkt);
		return result;
	}

	// находим количество партиций топика
	int partition_cnt = 0;
	for (int i = 0; i < metadata->topic_cnt; i++)
	{
		if (strcmp(metadata->topics[i].topic, tTopicName.c_str()) == 0)
		{
			partition_cnt = metadata->topics[i].partition_cnt;
			break;
		}
	}

	rd_kafka_metadata_destroy(metadata);
	rd_kafka_topic_destroy(rkt);

	if (partition_cnt == 0)
	{
		msg_err = u8"Топик не найден или не имеет партиций";
		return result;
	}

	// формируем результат
	boost::property_tree::ptree jsonObj;
	boost::property_tree::ptree groupsChildren;

	jsonObj.put("topic", tTopicName);

	// для каждой группы проверяем наличие committed offsets для топика
	for (size_t i = 0; i < allGroups.size(); i++)
	{
		const std::string& group_id = allGroups[i].group_id;

		// создаем список партиций для запроса
		rd_kafka_topic_partition_list_t* partitions = rd_kafka_topic_partition_list_new(partition_cnt);
		for (int p = 0; p < partition_cnt; p++)
		{
			rd_kafka_topic_partition_list_add(partitions, tTopicName.c_str(), p);
		}

		// создаем запрос ListConsumerGroupOffsets
		rd_kafka_ListConsumerGroupOffsets_t* grp_offsets = rd_kafka_ListConsumerGroupOffsets_new(group_id.c_str(), partitions);
		rd_kafka_ListConsumerGroupOffsets_t* grp_offsets_arr[1] = { grp_offsets };

		rd_kafka_AdminOptions_t* offsets_options = rd_kafka_AdminOptions_new(admin.get(), RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS);
		rd_kafka_AdminOptions_set_request_timeout(offsets_options, tTimeout, errstr, sizeof(errstr));

		rd_kafka_ListConsumerGroupOffsets(admin.get(), grp_offsets_arr, 1, offsets_options, admin.queue());

		rd_kafka_event_t* offset_ev = rd_kafka_queue_poll(admin.queue(), tTimeout + 1000);

		bool hasOffsets = false;
		boost::property_tree::ptree offsetsChildren;

		if (offset_ev)
		{
			if (!rd_kafka_event_error(offset_ev))
			{
				const rd_kafka_ListConsumerGroupOffsets_result_t* offset_result =
					rd_kafka_event_ListConsumerGroupOffsets_result(offset_ev);

				if (offset_result)
				{
					size_t res_cnt = 0;
					const rd_kafka_group_result_t** group_results =
						rd_kafka_ListConsumerGroupOffsets_result_groups(offset_result, &res_cnt);

					if (res_cnt > 0 && group_results[0])
					{
						const rd_kafka_topic_partition_list_t* result_partitions =
							rd_kafka_group_result_partitions(group_results[0]);

						if (result_partitions)
						{
							for (int p = 0; p < result_partitions->cnt; p++)
							{
								// offset >= 0 означает что группа имеет committed offset для этой партиции
								if (result_partitions->elems[p].offset >= 0)
								{
									hasOffsets = true;

									boost::property_tree::ptree offsetNode;
									offsetNode.put("partition", result_partitions->elems[p].partition);
									offsetNode.put("offset", result_partitions->elems[p].offset);
									offsetsChildren.push_back(boost::property_tree::ptree::value_type("", offsetNode));
								}
							}
						}
					}
				}
			}
			rd_kafka_event_destroy(offset_ev);
		}

		rd_kafka_AdminOptions_destroy(offsets_options);
		rd_kafka_ListConsumerGroupOffsets_destroy(grp_offsets);
		rd_kafka_topic_partition_list_destroy(partitions);

		// если группа имеет offsets для этого топика - добавляем её в результат
		if (hasOffsets)
		{
			boost::property_tree::ptree groupNode;
			groupNode.put("group_id", group_id);
			groupNode.put("state", rd_kafka_consumer_group_state_name(allGroups[i].state));

			if (offsetsChildren.size())
			{
				groupNode.put_child("offsets", offsetsChildren);
			}

			groupsChildren.push_back(boost::property_tree::ptree::value_type("", groupNode));
		}
	}

	jsonObj.put("groups_count", groupsChildren.size());
	jsonObj.put_child("consumer_groups", groupsChildren);

	boost::property_tree::write_json(s, jsonObj, true);
	result = s.str();

	return result;
}

std::string SimpleKafka1C::getConsumerCurrentGroupOffset(const variant_t& times, const variant_t& timeout)
{
	if (hConsumer == nullptr)
	{
		msg_err = u8"Консьюмер не инициализирован";
		return EMPTYSTR;
	}

	std::stringstream s{};
	long long timeline = 0;
	std::string str_timeline = std::get<std::string>(times);

	if (!str_timeline.empty())
	{
		timeline = std::stoll(str_timeline);
	}

	auto tm = std::get<int32_t>(timeout);

	boost::property_tree::ptree jsonObj;
	boost::property_tree::ptree topicsChildren;

	class RdKafka::Metadata* metadata;
	hConsumer->metadata(true, nullptr, &metadata, 1000);

	std::vector<RdKafka::TopicPartition*> partitions;
	RdKafka::Metadata::TopicMetadataIterator it;
	typedef RdKafka::TopicMetadata::PartitionMetadataIterator PartitionIterator;

	for (it = metadata->topics()->begin(); it != metadata->topics()->end(); ++it)
	{
		for (PartitionIterator partIt = (*it)->partitions()->begin(); partIt != (*it)->partitions()->end(); ++partIt)
		{
			if (timeline > 0) {
				RdKafka::TopicPartition* tp = RdKafka::TopicPartition::create((*it)->topic().c_str(), (*partIt)->id(), timeline);
				partitions.push_back(tp);
			}
			else
			{
				RdKafka::TopicPartition* tp = RdKafka::TopicPartition::create((*it)->topic().c_str(), (*partIt)->id());
				partitions.push_back(tp);
			}
		}
	}

	RdKafka::ErrorCode err;

	if (timeline == 0) {
		do {
			err = hConsumer->committed(partitions, tm);
			tm = tm + 1000;
		} while (err);
	}
	else
	{
		do {
			err = hConsumer->offsetsForTimes(partitions, tm);
			tm = tm + 1000;
		} while (err);
	}

	if (err != RdKafka::ERR_NO_ERROR)
	{
		msg_err = RdKafka::err2str(err);
		return EMPTYSTR;
	}

	delete metadata;

	for (auto tp : partitions)
	{
		boost::property_tree::ptree node;
		node.put("topic", tp->topic());
		node.put("partition", tp->partition());
		node.put("offset", tp->offset());

		topicsChildren.push_back(boost::property_tree::ptree::value_type("", node));
		delete tp;
	}

	if (topicsChildren.size())
	{
		jsonObj.put_child("metadata", topicsChildren);
	}
	boost::property_tree::write_json(s, jsonObj, true);

	return s.str();
}

std::string SimpleKafka1C::getConsumerGroupOffsets(const variant_t& brokers, const variant_t& times, const variant_t& timeout)
{
	initConsumer(brokers);
	std::string result = getConsumerCurrentGroupOffset(times, timeout);
	stopConsumer();

	return result;
}

//================================== Consumer Group Management ==========================================

bool SimpleKafka1C::deleteConsumerGroup(const variant_t& brokers, const variant_t& groupId)
{
	char errstr[512];
	std::string tBrokers = std::get<std::string>(brokers);
	std::string tGroupId = std::get<std::string>(groupId);

	// Валидация входных данных
	if (!isValidBrokerList(tBrokers, msg_err))
	{
		return false;
	}
	if (!isValidConsumerGroupId(tGroupId, msg_err))
	{
		return false;
	}

	// Используем AdminClientScope для RAII управления ресурсами
	AdminClientScope admin(this, tBrokers, settings, RD_KAFKA_PRODUCER);
	if (!admin.isValid())
	{
		msg_err = admin.error();
		return false;
	}

	// Используем DeleteConsumerGroupOffsets для удаления офсетов группы
	// Если нужно удалить саму группу, она должна быть неактивной (без потребителей)
	rd_kafka_DeleteConsumerGroupOffsets_t* del_groups[1];
	del_groups[0] = rd_kafka_DeleteConsumerGroupOffsets_new(tGroupId.c_str(), NULL);

	// опции для операции
	rd_kafka_AdminOptions_t* options = rd_kafka_AdminOptions_new(admin.get(), RD_KAFKA_ADMIN_OP_DELETECONSUMERGROUPOFFSETS);
	rd_kafka_AdminOptions_set_request_timeout(options, 10000, errstr, sizeof(errstr));

	// выполняем операцию удаления
	rd_kafka_DeleteConsumerGroupOffsets(admin.get(), del_groups, 1, options, admin.queue());

	// ожидаем результат
	rd_kafka_event_t* rkev = rd_kafka_queue_poll(admin.queue(), 12000);

	// освобождаем ресурсы
	rd_kafka_DeleteConsumerGroupOffsets_destroy(del_groups[0]);
	rd_kafka_AdminOptions_destroy(options);

	if (!rkev)
	{
		msg_err = u8"Таймаут при удалении группы консьюмеров";
		return false;
	}

	if (rd_kafka_event_error(rkev))
	{
		msg_err = rd_kafka_event_error_string(rkev);
		rd_kafka_event_destroy(rkev);
		return false;
	}

	rd_kafka_event_destroy(rkev);

	return true;
}

bool SimpleKafka1C::resetConsumerGroupOffsets(const variant_t& brokers, const variant_t& groupId,
                                               const variant_t& topicName, const variant_t& resetTo)
{
	if (!hConsumer)
	{
		msg_err = "Consumer not initialized. Call InitializeConsumer first.";
		return false;
	}

	try
	{
		std::string tTopicName = std::get<std::string>(topicName);
		std::string tResetTo = std::get<std::string>(resetTo);

		// Получаем метаданные топика для определения количества партиций
		RdKafka::Metadata* metadata = nullptr;
		RdKafka::ErrorCode err = hConsumer->metadata(false, nullptr, &metadata, 5000);

		if (err != RdKafka::ERR_NO_ERROR)
		{
			msg_err = RdKafka::err2str(err);
			return false;
		}

		// Находим топик в метаданных
		const RdKafka::Metadata::TopicMetadataVector* topics = metadata->topics();
		int partition_cnt = 0;

		for (auto it = topics->begin(); it != topics->end(); ++it)
		{
			if ((*it)->topic() == tTopicName)
			{
				partition_cnt = static_cast<int>((*it)->partitions()->size());
				break;
			}
		}

		delete metadata;

		if (partition_cnt == 0)
		{
			msg_err = "Topic not found or has no partitions";
			return false;
		}

		// Создаем список партиций для сброса офсетов
		std::vector<RdKafka::TopicPartition*> partitions;

		for (int i = 0; i < partition_cnt; i++)
		{
			int64_t offset;

			if (tResetTo == "earliest")
			{
				offset = RdKafka::Topic::OFFSET_BEGINNING;
			}
			else if (tResetTo == "latest")
			{
				offset = RdKafka::Topic::OFFSET_END;
			}
			else
			{
				// Пытаемся преобразовать в timestamp
				try
				{
					offset = std::stoll(tResetTo);
				}
				catch (...)
				{
					msg_err = "Invalid resetTo value. Use 'earliest', 'latest', or timestamp";
					return false;
				}
			}

			RdKafka::TopicPartition* tp = RdKafka::TopicPartition::create(tTopicName, i, offset);
			partitions.push_back(tp);
		}

		// Фиксируем офсеты
		err = hConsumer->commitSync(partitions);

		// Освобождаем память
		for (auto* tp : partitions)
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
		msg_err = std::string("Exception in resetConsumerGroupOffsets: ") + e.what();
		return false;
	}
}

//================================== Advanced Consumer Position Management ==========================================

bool SimpleKafka1C::seekToBeginning(const variant_t& topicName, const variant_t& partition)
{
	if (!hConsumer)
	{
		msg_err = "Consumer not initialized. Call InitializeConsumer first.";
		return false;
	}

	try
	{
		std::string tTopicName = std::get<std::string>(topicName);
		int32_t tPartition = std::get<int32_t>(partition);

		// Создаем TopicPartition с offset BEGINNING
		std::unique_ptr<RdKafka::TopicPartition> tp(
			RdKafka::TopicPartition::create(tTopicName, tPartition, RdKafka::Topic::OFFSET_BEGINNING));

		// Выполняем seek
		RdKafka::ErrorCode err = hConsumer->seek(*tp, 5000);

		if (err != RdKafka::ERR_NO_ERROR)
		{
			msg_err = RdKafka::err2str(err);
			return false;
		}

		return true;
	}
	catch (const std::exception& e)
	{
		msg_err = std::string("Exception in seekToBeginning: ") + e.what();
		return false;
	}
}

bool SimpleKafka1C::seekToEnd(const variant_t& topicName, const variant_t& partition)
{
	if (!hConsumer)
	{
		msg_err = "Consumer not initialized. Call InitializeConsumer first.";
		return false;
	}

	try
	{
		std::string tTopicName = std::get<std::string>(topicName);
		int32_t tPartition = std::get<int32_t>(partition);

		// Создаем TopicPartition с offset END
		std::unique_ptr<RdKafka::TopicPartition> tp(
			RdKafka::TopicPartition::create(tTopicName, tPartition, RdKafka::Topic::OFFSET_END));

		// Выполняем seek
		RdKafka::ErrorCode err = hConsumer->seek(*tp, 5000);

		if (err != RdKafka::ERR_NO_ERROR)
		{
			msg_err = RdKafka::err2str(err);
			return false;
		}

		return true;
	}
	catch (const std::exception& e)
	{
		msg_err = std::string("Exception in seekToEnd: ") + e.what();
		return false;
	}
}

bool SimpleKafka1C::seekToTimestamp(const variant_t& topicName, const variant_t& partition, const variant_t& timestamp)
{
	if (!hConsumer)
	{
		msg_err = "Consumer not initialized. Call InitializeConsumer first.";
		return false;
	}

	try
	{
		std::string tTopicName = std::get<std::string>(topicName);
		int32_t tPartition = std::get<int32_t>(partition);
		int64_t tTimestamp = std::get<int32_t>(timestamp); // timestamp в миллисекундах

		// Создаем TopicPartition для поиска по timestamp
		std::unique_ptr<RdKafka::TopicPartition> tp(RdKafka::TopicPartition::create(tTopicName, tPartition));
		std::vector<RdKafka::TopicPartition*> partitions;
		partitions.push_back(tp.get());

		// Устанавливаем timestamp для поиска
		tp->set_offset(tTimestamp);

		// Получаем офсет для указанного timestamp
		RdKafka::ErrorCode err = hConsumer->offsetsForTimes(partitions, 5000);

		if (err != RdKafka::ERR_NO_ERROR)
		{
			msg_err = RdKafka::err2str(err);
			return false;
		}

		// Получаем найденный офсет
		int64_t foundOffset = tp->offset();

		if (foundOffset < 0)
		{
			msg_err = "No offset found for the specified timestamp";
			return false;
		}

		// Выполняем seek к найденному офсету
		err = hConsumer->seek(*tp, 5000);

		if (err != RdKafka::ERR_NO_ERROR)
		{
			msg_err = RdKafka::err2str(err);
			return false;
		}

		return true;
	}
	catch (const std::exception& e)
	{
		msg_err = std::string("Exception in seekToTimestamp: ") + e.what();
		return false;
	}
}

//================================== Consumer Assignment (Manual Partition Assignment) ==========================================

bool SimpleKafka1C::assign(const variant_t& jsonTopicPartitions)
{
	if (!hConsumer)
	{
		msg_err = "Consumer not initialized. Call InitializeConsumer first.";
		return false;
	}

	try
	{
		std::string jsonStr = std::get<std::string>(jsonTopicPartitions);

		// Валидация JSON
		if (!isValidJson(jsonStr, msg_err))
		{
			return false;
		}

		boost::json::value jv = boost::json::parse(jsonStr);

		if (!jv.is_array())
		{
			msg_err = u8"JSON должен содержать массив топиков и партиций";
			return false;
		}

		boost::json::array topicPartitions = jv.as_array();
		std::vector<RdKafka::TopicPartition*> partitions;

		for (const auto& item : topicPartitions)
		{
			if (!item.is_object())
				continue;

			const boost::json::object& obj = item.as_object();

			if (!obj.contains("topic") || !obj.contains("partition"))
			{
				msg_err = u8"Каждый элемент должен содержать 'topic' и 'partition'";
				for (auto* tp : partitions)
					delete tp;
				return false;
			}

			std::string topic = boost::json::value_to<std::string>(obj.at("topic"));
			int32_t partition = static_cast<int32_t>(obj.at("partition").as_int64());

			// Опционально: можно задать начальный offset
			int64_t offset = RdKafka::Topic::OFFSET_INVALID;
			if (obj.contains("offset"))
			{
				offset = obj.at("offset").as_int64();
			}

			RdKafka::TopicPartition* tp = RdKafka::TopicPartition::create(topic, partition, offset);
			partitions.push_back(tp);
		}

		if (partitions.empty())
		{
			msg_err = u8"Не найдено валидных топиков и партиций для назначения";
			return false;
		}

		// Выполняем assign
		RdKafka::ErrorCode err = hConsumer->assign(partitions);

		// Освобождаем память
		for (auto* tp : partitions)
			delete tp;

		if (err != RdKafka::ERR_NO_ERROR)
		{
			msg_err = RdKafka::err2str(err);
			return false;
		}

		return true;
	}
	catch (const std::exception& e)
	{
		msg_err = std::string("Exception in assign: ") + e.what();
		return false;
	}
}

std::string SimpleKafka1C::getAssignment()
{
	if (!hConsumer)
	{
		msg_err = "Consumer not initialized. Call InitializeConsumer first.";
		return "";
	}

	try
	{
		std::vector<RdKafka::TopicPartition*> partitions;
		RdKafka::ErrorCode err = hConsumer->assignment(partitions);

		if (err != RdKafka::ERR_NO_ERROR)
		{
			msg_err = RdKafka::err2str(err);
			return "";
		}

		// Формируем JSON с назначенными партициями
		std::stringstream s{};
		boost::property_tree::ptree jsonObj;
		boost::property_tree::ptree partitionsArray;

		jsonObj.put("count", partitions.size());

		for (const auto* tp : partitions)
		{
			boost::property_tree::ptree partitionObj;
			partitionObj.put("topic", tp->topic());
			partitionObj.put("partition", tp->partition());
			partitionObj.put("offset", tp->offset());
			partitionsArray.push_back(std::make_pair("", partitionObj));
		}

		jsonObj.put_child("partitions", partitionsArray);

		// Освобождаем память
		for (auto* tp : partitions)
			delete tp;

		boost::property_tree::write_json(s, jsonObj, true);
		return s.str();
	}
	catch (const std::exception& e)
	{
		msg_err = std::string("Exception in getAssignment: ") + e.what();
		return "";
	}
}

bool SimpleKafka1C::unassign()
{
	if (!hConsumer)
	{
		msg_err = "Consumer not initialized. Call InitializeConsumer first.";
		return false;
	}

	try
	{
		// Передаем пустой список партиций для отмены назначения
		std::vector<RdKafka::TopicPartition*> empty;
		RdKafka::ErrorCode err = hConsumer->assign(empty);

		if (err != RdKafka::ERR_NO_ERROR)
		{
			msg_err = RdKafka::err2str(err);
			return false;
		}

		return true;
	}
	catch (const std::exception& e)
	{
		msg_err = std::string("Exception in unassign: ") + e.what();
		return false;
	}
}
