/*
 *  Simple Kafka 1C - Admin Topic Methods
 *  Topic management: create, delete, config, list
 */

#include "SimpleKafka1C.h"
#include "utils.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/json.hpp>

#include <fstream>
#include <sstream>

//================================== AdminClientScope =========================================

SimpleKafka1C::AdminClientScope::AdminClientScope(const std::string& brokers,
                                                   const std::vector<KafkaSettings>& settings,
                                                   rd_kafka_type_t type)
{
	char errstr[512];

	rd_kafka_conf_t* conf = rd_kafka_conf_new();

	// Применяем дополнительные настройки
	for (const auto& setting : settings)
	{
		if (rd_kafka_conf_set(conf, setting.Key.c_str(), setting.Value.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		{
			errstr_msg = errstr;
			rd_kafka_conf_destroy(conf);
			return;
		}
	}

	// Устанавливаем bootstrap.servers
	if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		errstr_msg = errstr;
		rd_kafka_conf_destroy(conf);
		return;
	}

	// Создаем клиента
	rk = rd_kafka_new(type, conf, errstr, sizeof(errstr));
	if (!rk)
	{
		errstr_msg = std::string(u8"Ошибка создания клиента: ") + errstr;
		return;
	}

	// Создаем очередь для получения результатов
	rkqu = rd_kafka_queue_new(rk);
}

SimpleKafka1C::AdminClientScope::~AdminClientScope()
{
	if (rkqu)
	{
		rd_kafka_queue_destroy(rkqu);
	}
	if (rk)
	{
		rd_kafka_destroy(rk);
	}
}

//================================== Topic List =========================================

std::string SimpleKafka1C::getListOfTopics(const variant_t& brokers)
{
	std::string result;
	std::stringstream s{};
	std::ofstream eventFile{};

	boost::property_tree::ptree jsonObj;
	RdKafkaConfPtr conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

	std::string tBrokers = std::get<std::string>(brokers);

	// дополнительные параметры
	for (size_t i = 0; i < settings.size(); i++)
	{
		if (conf->set(settings[i].Key, settings[i].Value, msg_err) != RdKafka::Conf::CONF_OK)
		{
			if (eventFile.is_open()) eventFile << currentDateTime() << msg_err << std::endl;
			return result;
		}
	}

	if (conf->set("metadata.broker.list", tBrokers, msg_err) != RdKafka::Conf::CONF_OK)
	{
		return result;
	}

	// создаем фейкового продюсера
	std::unique_ptr<RdKafka::Producer> producer(RdKafka::Producer::create(conf.get(), msg_err));
	if (!producer)
	{
		msg_err = u8"Ошибка создания фейкового продюсера";
		return result;
	}

	RdKafka::Topic* topicKafka = nullptr;
	class RdKafka::Metadata* metadata;
	RdKafka::ErrorCode err = producer->metadata(true, topicKafka, &metadata, 5000);
	if (err != RdKafka::ERR_NO_ERROR)
	{
		msg_err = RdKafka::err2str(err);
		return result;
	}

	RdKafka::Metadata::TopicMetadataIterator it;
	boost::property_tree::ptree topicsChildren;

	for (it = metadata->topics()->begin(); it != metadata->topics()->end(); ++it)
	{
		boost::property_tree::ptree node;
		node.put("topic", (*it)->topic().c_str());
		node.put("partitions", (*it)->partitions()->size());

		topicsChildren.push_back(boost::property_tree::ptree::value_type("", node));
	}

	delete metadata;

	if (topicsChildren.size())
	{
		jsonObj.put_child("topics", topicsChildren);
	}
	boost::property_tree::write_json(s, jsonObj, true);

	return s.str();
}

//================================== Topic Create/Delete =========================================

bool SimpleKafka1C::createTopic(const variant_t& brokers, const variant_t& topicName, const variant_t& partition, const variant_t& replication_factor)
{
	char errstr[512];
	int timeout_ms = adminOperationTimeout;

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tTopicName = std::get<std::string>(topicName);
	int partition_cnt = std::get<int32_t>(partition);
	int rep_factor = std::get<int32_t>(replication_factor);

	// Валидация входных данных
	if (!isValidBrokerList(tBrokers, msg_err))
	{
		return false;
	}
	if (!isValidTopicName(tTopicName, msg_err))
	{
		return false;
	}
	if (!isValidPartition(partition_cnt, msg_err))
	{
		return false;
	}
	if (!isValidReplicationFactor(rep_factor, msg_err))
	{
		return false;
	}

	// Используем AdminClientScope для RAII управления ресурсами
	AdminClientScope admin(tBrokers, settings, RD_KAFKA_PRODUCER);
	if (!admin.isValid())
	{
		msg_err = admin.error();
		return false;
	}

	// создаем описание нового топика
	rd_kafka_NewTopic_t* newt = rd_kafka_NewTopic_new(tTopicName.c_str(), partition_cnt, rep_factor, errstr, sizeof(errstr));
	if (!newt)
	{
		msg_err = u8"Ошибка создания описания топика: " + std::string(errstr);
		return false;
	}

	// опции операции
	rd_kafka_AdminOptions_t* options = rd_kafka_AdminOptions_new(admin.get(), RD_KAFKA_ADMIN_OP_CREATETOPICS);
	rd_kafka_AdminOptions_set_operation_timeout(options, timeout_ms, errstr, sizeof(errstr));

	// выполняем создание топика
	rd_kafka_NewTopic_t* newt_arr[1] = { newt };
	rd_kafka_CreateTopics(admin.get(), newt_arr, 1, options, admin.queue());

	// ожидаем результат
	rd_kafka_event_t* rkev = rd_kafka_queue_poll(admin.queue(), timeout_ms + 2000);

	bool success = false;
	if (rkev)
	{
		if (rd_kafka_event_error(rkev))
		{
			msg_err = rd_kafka_event_error_string(rkev);
		}
		else
		{
			const rd_kafka_CreateTopics_result_t* res = rd_kafka_event_CreateTopics_result(rkev);
			if (res)
			{
				size_t res_cnt;
				const rd_kafka_topic_result_t** terr = rd_kafka_CreateTopics_result_topics(res, &res_cnt);

				if (res_cnt > 0 && terr[0])
				{
					rd_kafka_resp_err_t err = rd_kafka_topic_result_error(terr[0]);
					if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
					{
						msg_err = rd_kafka_topic_result_error_string(terr[0]);
					}
					else
					{
						success = true;
					}
				}
			}
		}
		rd_kafka_event_destroy(rkev);
	}
	else
	{
		msg_err = u8"Таймаут ожидания ответа";
	}

	// очистка ресурсов
	rd_kafka_AdminOptions_destroy(options);
	rd_kafka_NewTopic_destroy(newt);

	return success;
}

bool SimpleKafka1C::deleteTopic(const variant_t& brokers, const variant_t& topicName)
{
	char errstr[512];
	int timeout_ms = adminOperationTimeout;

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tTopicName = std::get<std::string>(topicName);

	// Валидация входных данных
	if (!isValidBrokerList(tBrokers, msg_err))
	{
		return false;
	}
	if (!isValidTopicName(tTopicName, msg_err))
	{
		return false;
	}

	// Используем AdminClientScope для RAII управления ресурсами
	AdminClientScope admin(tBrokers, settings, RD_KAFKA_PRODUCER);
	if (!admin.isValid())
	{
		msg_err = admin.error();
		return false;
	}

	// создаем описание топика для удаления
	rd_kafka_DeleteTopic_t* delt = rd_kafka_DeleteTopic_new(tTopicName.c_str());

	// опции операции
	rd_kafka_AdminOptions_t* options = rd_kafka_AdminOptions_new(admin.get(), RD_KAFKA_ADMIN_OP_DELETETOPICS);
	rd_kafka_AdminOptions_set_operation_timeout(options, timeout_ms, errstr, sizeof(errstr));

	// выполняем удаление топика
	rd_kafka_DeleteTopic_t* delt_arr[1] = { delt };
	rd_kafka_DeleteTopics(admin.get(), delt_arr, 1, options, admin.queue());

	// ожидаем результат
	rd_kafka_event_t* rkev = rd_kafka_queue_poll(admin.queue(), timeout_ms + 2000);

	bool success = false;
	if (rkev)
	{
		if (rd_kafka_event_error(rkev))
		{
			msg_err = rd_kafka_event_error_string(rkev);
		}
		else
		{
			const rd_kafka_DeleteTopics_result_t* res = rd_kafka_event_DeleteTopics_result(rkev);
			if (res)
			{
				size_t res_cnt;
				const rd_kafka_topic_result_t** terr = rd_kafka_DeleteTopics_result_topics(res, &res_cnt);

				if (res_cnt > 0 && terr[0])
				{
					rd_kafka_resp_err_t err = rd_kafka_topic_result_error(terr[0]);
					if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
					{
						msg_err = rd_kafka_topic_result_error_string(terr[0]);
					}
					else
					{
						success = true;
					}
				}
			}
		}
		rd_kafka_event_destroy(rkev);
	}
	else
	{
		msg_err = u8"Таймаут ожидания ответа";
	}

	// очистка ресурсов
	rd_kafka_AdminOptions_destroy(options);
	rd_kafka_DeleteTopic_destroy(delt);

	return success;
}

bool SimpleKafka1C::deleteRecords(const variant_t& brokers, const variant_t& topicName, const variant_t& partitionsJson, const variant_t& timeout)
{
	char errstr[512];

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tTopicName = std::get<std::string>(topicName);
	std::string tPartitionsJson = std::get<std::string>(partitionsJson);
	int32_t tTimeout = std::get<int32_t>(timeout);

	// Валидация входных данных
	if (!isValidBrokerList(tBrokers, msg_err))
	{
		return false;
	}
	if (!isValidTopicName(tTopicName, msg_err))
	{
		return false;
	}
	if (!isValidJson(tPartitionsJson, msg_err))
	{
		return false;
	}

	// парсим JSON с партициями и офсетами
	boost::property_tree::ptree pt;
	std::stringstream ss(tPartitionsJson);

	try {
		boost::property_tree::read_json(ss, pt);
	}
	catch (const std::exception& e) {
		msg_err = u8"Ошибка парсинга JSON: " + std::string(e.what());
		return false;
	}

	// Используем AdminClientScope для RAII управления ресурсами
	AdminClientScope admin(tBrokers, settings, RD_KAFKA_PRODUCER);
	if (!admin.isValid())
	{
		msg_err = admin.error();
		return false;
	}

	// создаем список партиций и офсетов для удаления
	rd_kafka_topic_partition_list_t* partitions = rd_kafka_topic_partition_list_new(1);

	try {
		for (const auto& partition_node : pt.get_child("partitions"))
		{
			int32_t partition = partition_node.second.get<int32_t>("partition");
			int64_t offset = partition_node.second.get<int64_t>("offset");

			rd_kafka_topic_partition_list_add(partitions, tTopicName.c_str(), partition)->offset = offset;
		}
	}
	catch (const std::exception& e) {
		msg_err = u8"Ошибка чтения данных партиций: " + std::string(e.what());
		rd_kafka_topic_partition_list_destroy(partitions);
		return false;
	}

	if (partitions->cnt == 0)
	{
		msg_err = u8"Не указаны партиции для удаления записей";
		rd_kafka_topic_partition_list_destroy(partitions);
		return false;
	}

	// создаем описание для удаления записей
	rd_kafka_DeleteRecords_t* delr = rd_kafka_DeleteRecords_new(partitions);

	// опции операции
	rd_kafka_AdminOptions_t* options = rd_kafka_AdminOptions_new(admin.get(), RD_KAFKA_ADMIN_OP_DELETERECORDS);
	rd_kafka_AdminOptions_set_operation_timeout(options, tTimeout, errstr, sizeof(errstr));

	// выполняем удаление записей
	rd_kafka_DeleteRecords_t* delr_arr[1] = { delr };
	rd_kafka_DeleteRecords(admin.get(), delr_arr, 1, options, admin.queue());

	// ожидаем результат
	rd_kafka_event_t* rkev = rd_kafka_queue_poll(admin.queue(), tTimeout + 2000);

	bool success = false;
	if (rkev)
	{
		if (rd_kafka_event_error(rkev))
		{
			msg_err = rd_kafka_event_error_string(rkev);
		}
		else
		{
			const rd_kafka_DeleteRecords_result_t* res = rd_kafka_event_DeleteRecords_result(rkev);
			if (res)
			{
				const rd_kafka_topic_partition_list_t* offsets = rd_kafka_DeleteRecords_result_offsets(res);

				if (offsets && offsets->cnt > 0)
				{
					// проверяем результаты для каждой партиции
					bool all_ok = true;
					std::stringstream error_details;

					for (int i = 0; i < offsets->cnt; i++)
					{
						const rd_kafka_topic_partition_t* part = &offsets->elems[i];
						if (part->err != RD_KAFKA_RESP_ERR_NO_ERROR)
						{
							all_ok = false;
							error_details << u8"Партиция " << part->partition
								<< u8": " << rd_kafka_err2str(part->err) << "; ";
						}
					}

					if (all_ok)
					{
						success = true;
					}
					else
					{
						msg_err = error_details.str();
					}
				}
				else
				{
					msg_err = u8"Пустой результат удаления записей";
				}
			}
		}
		rd_kafka_event_destroy(rkev);
	}
	else
	{
		msg_err = u8"Таймаут ожидания ответа";
	}

	// очистка ресурсов
	rd_kafka_AdminOptions_destroy(options);
	rd_kafka_DeleteRecords_destroy(delr);
	rd_kafka_topic_partition_list_destroy(partitions);

	return success;
}

//================================== Topic Config =========================================

std::string SimpleKafka1C::getTopicConfig(const variant_t& brokers, const variant_t& topicName, const variant_t& timeout)
{
	std::string result;
	std::stringstream s{};
	char errstr[512];

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tTopicName = std::get<std::string>(topicName);
	int32_t tTimeout = std::get<int32_t>(timeout);

	// Используем AdminClientScope для RAII управления ресурсами
	AdminClientScope admin(tBrokers, settings, RD_KAFKA_PRODUCER);
	if (!admin.isValid())
	{
		msg_err = admin.error();
		return result;
	}

	// создаем ресурс конфигурации для топика
	rd_kafka_ConfigResource_t* config_resource = rd_kafka_ConfigResource_new(
		RD_KAFKA_RESOURCE_TOPIC, tTopicName.c_str());

	// опции операции
	rd_kafka_AdminOptions_t* options = rd_kafka_AdminOptions_new(admin.get(), RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS);
	rd_kafka_AdminOptions_set_operation_timeout(options, tTimeout, errstr, sizeof(errstr));

	// выполняем запрос конфигурации
	rd_kafka_ConfigResource_t* config_arr[1] = { config_resource };
	rd_kafka_DescribeConfigs(admin.get(), config_arr, 1, options, admin.queue());

	// ожидаем результат
	rd_kafka_event_t* rkev = rd_kafka_queue_poll(admin.queue(), tTimeout + 2000);

	if (rkev)
	{
		if (rd_kafka_event_error(rkev))
		{
			msg_err = rd_kafka_event_error_string(rkev);
		}
		else
		{
			const rd_kafka_DescribeConfigs_result_t* res = rd_kafka_event_DescribeConfigs_result(rkev);
			if (res)
			{
				size_t res_cnt;
				const rd_kafka_ConfigResource_t** resources = rd_kafka_DescribeConfigs_result_resources(res, &res_cnt);

				if (res_cnt > 0)
				{
					boost::property_tree::ptree jsonObj;
					boost::property_tree::ptree configChildren;

					rd_kafka_resp_err_t err = rd_kafka_ConfigResource_error(resources[0]);
					if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
					{
						msg_err = rd_kafka_ConfigResource_error_string(resources[0]);
					}
					else
					{
						jsonObj.put("topic", tTopicName);

						size_t config_cnt;
						const rd_kafka_ConfigEntry_t** entries = rd_kafka_ConfigResource_configs(resources[0], &config_cnt);

						for (size_t i = 0; i < config_cnt; i++)
						{
							const char* name = rd_kafka_ConfigEntry_name(entries[i]);
							const char* value = rd_kafka_ConfigEntry_value(entries[i]);
							rd_kafka_ConfigSource_t source = rd_kafka_ConfigEntry_source(entries[i]);
							int is_read_only = rd_kafka_ConfigEntry_is_read_only(entries[i]);
							int is_default = rd_kafka_ConfigEntry_is_default(entries[i]);
							int is_sensitive = rd_kafka_ConfigEntry_is_sensitive(entries[i]);

							boost::property_tree::ptree node;
							node.put("name", name ? name : "");
							node.put("value", (value && !is_sensitive) ? value : "");
							node.put("is_read_only", is_read_only ? true : false);
							node.put("is_default", is_default ? true : false);
							node.put("is_sensitive", is_sensitive ? true : false);

							std::string source_str;
							switch (source)
							{
							case RD_KAFKA_CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG:
								source_str = "DYNAMIC_TOPIC_CONFIG";
								break;
							case RD_KAFKA_CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG:
								source_str = "DYNAMIC_BROKER_CONFIG";
								break;
							case RD_KAFKA_CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER_CONFIG:
								source_str = "DYNAMIC_DEFAULT_BROKER_CONFIG";
								break;
							case RD_KAFKA_CONFIG_SOURCE_STATIC_BROKER_CONFIG:
								source_str = "STATIC_BROKER_CONFIG";
								break;
							case RD_KAFKA_CONFIG_SOURCE_DEFAULT_CONFIG:
								source_str = "DEFAULT_CONFIG";
								break;
							default:
								source_str = "UNKNOWN";
								break;
							}
							node.put("source", source_str);

							configChildren.push_back(boost::property_tree::ptree::value_type("", node));
						}

						if (configChildren.size())
						{
							jsonObj.put_child("configs", configChildren);
						}

						boost::property_tree::write_json(s, jsonObj, true);
						result = s.str();
					}
				}
			}
		}
		rd_kafka_event_destroy(rkev);
	}
	else
	{
		msg_err = u8"Таймаут ожидания ответа";
	}

	// очистка ресурсов
	rd_kafka_AdminOptions_destroy(options);
	rd_kafka_ConfigResource_destroy(config_resource);

	return result;
}

bool SimpleKafka1C::setTopicConfig(const variant_t& brokers, const variant_t& topicName, const variant_t& configJson, const variant_t& timeout)
{
	char errstr[512];

	std::string tBrokers = std::get<std::string>(brokers);
	std::string tTopicName = std::get<std::string>(topicName);
	std::string tConfigJson = std::get<std::string>(configJson);
	int32_t tTimeout = std::get<int32_t>(timeout);

	// Валидация входных данных
	if (!isValidBrokerList(tBrokers, msg_err))
	{
		return false;
	}
	if (!isValidTopicName(tTopicName, msg_err))
	{
		return false;
	}
	if (!isValidJson(tConfigJson, msg_err))
	{
		return false;
	}

	// Используем AdminClientScope для RAII управления ресурсами
	AdminClientScope admin(tBrokers, settings, RD_KAFKA_PRODUCER);
	if (!admin.isValid())
	{
		msg_err = admin.error();
		return false;
	}

	// создаем ресурс конфигурации для топика
	rd_kafka_ConfigResource_t* config_resource = rd_kafka_ConfigResource_new(
		RD_KAFKA_RESOURCE_TOPIC, tTopicName.c_str());

	// парсим JSON с настройками
	// Формат: {"retention.ms": "86400000", "cleanup.policy": "delete"}
	try
	{
		boost::json::value parsed = boost::json::parse(tConfigJson);
		boost::json::object obj = parsed.as_object();

		for (auto& kv : obj)
		{
			std::string key = kv.key();
			std::string value;

			if (kv.value().is_string())
			{
				value = kv.value().as_string();
			}
			else if (kv.value().is_int64())
			{
				value = std::to_string(kv.value().as_int64());
			}
			else if (kv.value().is_bool())
			{
				value = kv.value().as_bool() ? "true" : "false";
			}
			else if (kv.value().is_double())
			{
				value = std::to_string(kv.value().as_double());
			}
			else
			{
				continue;
			}

			rd_kafka_ConfigResource_set_config(config_resource, key.c_str(), value.c_str());
		}
	}
	catch (std::exception const& ex)
	{
		msg_err = u8"Ошибка парсинга JSON: " + std::string(ex.what());
		rd_kafka_ConfigResource_destroy(config_resource);
		return false;
	}

	// опции операции
	rd_kafka_AdminOptions_t* options = rd_kafka_AdminOptions_new(admin.get(), RD_KAFKA_ADMIN_OP_ALTERCONFIGS);
	rd_kafka_AdminOptions_set_operation_timeout(options, tTimeout, errstr, sizeof(errstr));

	// выполняем изменение конфигурации
	rd_kafka_ConfigResource_t* config_arr[1] = { config_resource };
	rd_kafka_AlterConfigs(admin.get(), config_arr, 1, options, admin.queue());

	// ожидаем результат
	rd_kafka_event_t* rkev = rd_kafka_queue_poll(admin.queue(), tTimeout + 2000);

	bool success = false;
	if (rkev)
	{
		if (rd_kafka_event_error(rkev))
		{
			msg_err = rd_kafka_event_error_string(rkev);
		}
		else
		{
			const rd_kafka_AlterConfigs_result_t* res = rd_kafka_event_AlterConfigs_result(rkev);
			if (res)
			{
				size_t res_cnt;
				const rd_kafka_ConfigResource_t** resources = rd_kafka_AlterConfigs_result_resources(res, &res_cnt);

				if (res_cnt > 0)
				{
					rd_kafka_resp_err_t err = rd_kafka_ConfigResource_error(resources[0]);
					if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
					{
						msg_err = rd_kafka_ConfigResource_error_string(resources[0]);
					}
					else
					{
						success = true;
					}
				}
			}
		}
		rd_kafka_event_destroy(rkev);
	}
	else
	{
		msg_err = u8"Таймаут ожидания ответа";
	}

	// очистка ресурсов
	rd_kafka_AdminOptions_destroy(options);
	rd_kafka_ConfigResource_destroy(config_resource);

	return success;
}
