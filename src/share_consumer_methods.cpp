/*
 *  Simple Kafka 1C - Share Consumer (KIP-932, Queues for Kafka)
 *  Queue-style reading through a share group. Preview API of librdkafka (C API only).
 *  Specification: specs/share-consumer.md
 */

#include "SimpleKafka1C.h"
#include "utils.h"

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/json.hpp>

#include <algorithm>
#include <cctype>
#include <fstream>
#include <memory>

namespace
{
	constexpr char SHARE_LOG_PREFIX[] = " [share] ";

	// Regular-consumer properties rd_kafka_share_consumer_new() rejects (librdkafka 2.16.0,
	// rdkafka_conf.c). They are skipped so the same settings serve both consumer kinds.
	const std::set<std::string> SHARE_INAPPLICABLE_SETTINGS = {
		"enable.auto.commit",
		"auto.offset.reset",
		"isolation.level",
		"enable.partition.eof",
		"partition.assignment.strategy",
		"group.protocol",
		"group.protocol.type",
		"group.remote.assignor",
		"group.instance.id",
		"session.timeout.ms",
		"heartbeat.interval.ms",
		"socket.max.fails",
		"queued.min.messages",
		"queued.max.messages.kbytes",
		"fetch.queue.backoff.ms",
		"fetch.error.backoff.ms",
		"message.copy.max.bytes",
		"topic.blacklist",
		"allow.auto.create.topics",
		"consume.callback.max.messages",
	};

	// Takes ownership of error; returns its text (empty for NULL).
	std::string consumeError(rd_kafka_error_t* error)
	{
		if (error == nullptr)
		{
			return {};
		}
		std::string text = rd_kafka_error_string(error);
		rd_kafka_error_destroy(error);
		return text;
	}

	// Accepts a String (the main way: 1C narrows Numbers above int32, #91) or a Number
	bool offsetFromVariant(const variant_t& value, int64_t& out, std::string& error)
	{
		if (std::holds_alternative<std::string>(value))
		{
			const std::string& raw = std::get<std::string>(value);
			try
			{
				size_t consumed = 0;
				out = std::stoll(raw, &consumed);
				if (consumed != raw.size())
				{
					throw std::invalid_argument("trailing characters");
				}
			}
			catch (...)
			{
				error = "offset must be a whole number, got \"" + raw + "\"";
				return false;
			}
		}
		else if (!variantToInt64(value, out, error))
		{
			error = "offset - " + error;
			return false;
		}
		if (out < 0)
		{
			error = "offset must be >= 0";
			return false;
		}
		return true;
	}

	bool parseAckType(std::string text, rd_kafka_share_AcknowledgeType_t& type, std::string& error)
	{
		std::transform(text.begin(), text.end(), text.begin(),
			[](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
		if (text == "accept")
		{
			type = RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT;
		}
		else if (text == "release")
		{
			type = RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE;
		}
		else if (text == "reject")
		{
			type = RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT;
		}
		else
		{
			error = "Invalid acknowledgement type \"" + text + "\", expected accept, release or reject";
			return false;
		}
		return true;
	}
}

void SimpleKafka1C::ShareLogContext::openLog(std::ofstream& file) const
{
	if (logDir.empty())
	{
		return;
	}
	std::string name = consumerLogName;
	if (!clientId.empty())
	{
		name += clientId + "_";
	}
	file.open(logDir + name + std::to_string(pid) + "_" + currentDateTime(logFormat.c_str()) + ".log", std::ios_base::app);
}

void SimpleKafka1C::ShareLogContext::logCallback(const rd_kafka_t* rk, int level, const char* fac, const char* buf)
{
	const auto* ctx = static_cast<const ShareLogContext*>(rd_kafka_opaque(rk));
	if (ctx == nullptr)
	{
		return;
	}
	std::ofstream file{};
	ctx->openLog(file);
	if (file.is_open())
	{
		file << currentDateTime() << SHARE_LOG_PREFIX << "LOG-" << level << "-" << fac << ": " << buf << std::endl;
	}
}

void SimpleKafka1C::ShareLogContext::errorCallback(rd_kafka_t* rk, int err, const char* reason, void* opaque)
{
	(void)rk;
	const auto* ctx = static_cast<const ShareLogContext*>(opaque);
	if (ctx == nullptr)
	{
		return;
	}
	const bool fatal = (err == RD_KAFKA_RESP_ERR__FATAL);
	const std::string text = std::string(fatal ? "FATAL " : "") + "ERROR ("
		+ rd_kafka_err2str(static_cast<rd_kafka_resp_err_t>(err)) + "): " + (reason ? reason : "");
	if (ctx->asyncError)
	{
		std::lock_guard<std::mutex> lk(ctx->asyncError->mtx);
		ctx->asyncError->lastError = text;
		ctx->asyncError->fatal = fatal;
	}
	std::ofstream file{};
	ctx->openLog(file);
	if (file.is_open())
	{
		file << currentDateTime() << SHARE_LOG_PREFIX << text << std::endl;
	}
}

bool SimpleKafka1C::initShareConsumer(const variant_t& brokers, const variant_t& groupId)
{
	std::lock_guard<std::mutex> lk(shareState.mtx);

	// A stale asynchronous error must not mask the result of a new initialization
	if (asyncErrorState)
	{
		std::lock_guard<std::mutex> errLk(asyncErrorState->mtx);
		asyncErrorState->lastError.clear();
		asyncErrorState->fatal = false;
	}

	if (shareState.handle != nullptr)
	{
		closeShareConsumerLocked();
	}

	const std::string tBrokers = std::get<std::string>(brokers);
	const std::string tGroupId = std::get<std::string>(groupId);

	if (!isValidBrokerList(tBrokers, msg_err))
	{
		return false;
	}
	if (!isValidConsumerGroupId(tGroupId, msg_err))
	{
		return false;
	}

	auto log = std::make_shared<ShareLogContext>();
	log->logDir = std::get<std::string>(*logDirectory);
	log->logFormat = std::get<std::string>(*formatLogFiles);
	log->clientId = clientID();
	log->pid = pid;
	log->asyncError = asyncErrorState;

	std::ofstream eventFile{};
	log->openLog(eventFile);
	if (eventFile.is_open())
	{
		eventFile << currentDateTime() << SHARE_LOG_PREFIX << "Simple Kafka version: " << Version
			<< " (librdkafka version: " << rd_kafka_version_str() << ")" << std::endl;
	}

	rd_kafka_conf_t* conf = rd_kafka_conf_new();

	std::vector<std::string> skipped;
	if (!applyKafkaSettings(conf, msg_err, &SHARE_INAPPLICABLE_SETTINGS, &skipped))
	{
		if (eventFile.is_open()) eventFile << currentDateTime() << SHARE_LOG_PREFIX << msg_err << std::endl;
		rd_kafka_conf_destroy(conf);
		return false;
	}
	for (const auto& key : skipped)
	{
		if (eventFile.is_open())
			eventFile << currentDateTime() << SHARE_LOG_PREFIX << "ignored property " << key
				<< " (not applicable for share consumer)" << std::endl;
	}

	// Set after the user settings so they cannot be overridden: the component works in
	// explicit acknowledgement mode only (specs/share-consumer.md, 4.1)
	char errstr[512] = {0};
	const std::pair<const char*, const char*> mandatory[] = {
		{"bootstrap.servers", tBrokers.c_str()},
		{"group.id", tGroupId.c_str()},
		{"share.acknowledgement.mode", "explicit"},
	};
	for (const auto& kv : mandatory)
	{
		if (rd_kafka_conf_set(conf, kv.first, kv.second, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
		{
			msg_err = errstr;
			if (eventFile.is_open()) eventFile << currentDateTime() << SHARE_LOG_PREFIX << msg_err << std::endl;
			rd_kafka_conf_destroy(conf);
			return false;
		}
	}

	rd_kafka_conf_set_opaque(conf, log.get());
	rd_kafka_conf_set_log_cb(conf, ShareLogContext::logCallback);
	rd_kafka_conf_set_error_cb(conf, ShareLogContext::errorCallback);

	// On success the handle owns conf; on failure conf stays with the caller
	errstr[0] = '\0';
	rd_kafka_share_t* handle = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
	if (handle == nullptr)
	{
		msg_err = enrichSslError(std::string("Share consumer creation error: ") + errstr);
		if (eventFile.is_open()) eventFile << currentDateTime() << SHARE_LOG_PREFIX << msg_err << std::endl;
		rd_kafka_conf_destroy(conf);
		return false;
	}

	shareState.handle = handle;
	shareState.log = std::move(log);

	if (eventFile.is_open())
		eventFile << currentDateTime() << SHARE_LOG_PREFIX << "Created share consumer. brokers-" << tBrokers
			<< " group-" << tGroupId << std::endl;

	return true;
}

bool SimpleKafka1C::shareSubscribe(const variant_t& topics)
{
	std::lock_guard<std::mutex> lk(shareState.mtx);

	if (shareState.handle == nullptr)
	{
		msg_err = "Share consumer not initialized";
		return false;
	}

	std::vector<std::string> names;
	boost::algorithm::split(names, std::get<std::string>(topics), boost::is_any_of(","));

	rd_kafka_topic_partition_list_t* list = rd_kafka_topic_partition_list_new(static_cast<int>(names.size()));
	for (auto& name : names)
	{
		boost::algorithm::trim(name);
		if (name.empty())
		{
			continue;
		}
		if (!isValidTopicName(name, msg_err))
		{
			rd_kafka_topic_partition_list_destroy(list);
			return false;
		}
		rd_kafka_topic_partition_list_add(list, name.c_str(), RD_KAFKA_PARTITION_UA);
	}

	if (list->cnt == 0)
	{
		rd_kafka_topic_partition_list_destroy(list);
		msg_err = "Topic list is empty";
		return false;
	}

	const rd_kafka_resp_err_t err = rd_kafka_share_subscribe(shareState.handle, list);
	rd_kafka_topic_partition_list_destroy(list);

	std::ofstream eventFile{};
	shareState.log->openLog(eventFile);
	if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
	{
		msg_err = rd_kafka_err2str(err);
		if (eventFile.is_open()) eventFile << currentDateTime() << SHARE_LOG_PREFIX << "Failed to subscribe: " << msg_err << std::endl;
		return false;
	}

	if (eventFile.is_open()) eventFile << currentDateTime() << SHARE_LOG_PREFIX << "Subscribed: " << std::get<std::string>(topics) << std::endl;
	return true;
}

bool SimpleKafka1C::shareUnsubscribe()
{
	std::lock_guard<std::mutex> lk(shareState.mtx);

	if (shareState.handle == nullptr)
	{
		msg_err = "Share consumer not initialized";
		return false;
	}

	const rd_kafka_resp_err_t err = rd_kafka_share_unsubscribe(shareState.handle);
	if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
	{
		msg_err = rd_kafka_err2str(err);
		return false;
	}
	return true;
}

std::string SimpleKafka1C::shareReadBatch(const variant_t& maxWaitMs, const variant_t& asBase64, const variant_t& releaseUnacked)
{
	std::lock_guard<std::mutex> lk(shareState.mtx);

	if (shareState.handle == nullptr)
	{
		msg_err = "Share consumer not initialized";
		return "";
	}
	if (!std::holds_alternative<int32_t>(maxWaitMs) || !std::holds_alternative<bool>(asBase64)
		|| !std::holds_alternative<bool>(releaseUnacked))
	{
		msg_err = "shareReadBatch: maxWaitMs must be a number, asBase64 and releaseUnacked must be booleans";
		return "";
	}
	const int32_t timeoutMs = std::max<int32_t>(0, std::get<int32_t>(maxWaitMs));

	std::ofstream eventFile{};
	shareState.log->openLog(eventFile);

	// Explicit mode: librdkafka refuses to poll while a record of the previous batch is
	// unacknowledged. Check it here to return a clear error without a broker round trip.
	size_t pending = 0;
	for (const auto& rec : shareState.records)
	{
		if (rec.ack == 0)
		{
			++pending;
		}
	}
	if (pending > 0)
	{
		if (!std::get<bool>(releaseUnacked))
		{
			msg_err = "Previous batch has unacknowledged records";
			return "";
		}
		for (auto& rec : shareState.records)
		{
			if (rec.ack != 0)
			{
				continue;
			}
			const rd_kafka_resp_err_t err = rd_kafka_share_acknowledge_type(
				shareState.handle, rec.msg, RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE);
			if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
			{
				msg_err = std::string("Failed to release unacknowledged record: ") + rd_kafka_err2str(err);
				if (eventFile.is_open()) eventFile << currentDateTime() << SHARE_LOG_PREFIX << msg_err << std::endl;
				return "";
			}
			rec.ack = RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE;
		}
		if (eventFile.is_open())
			eventFile << currentDateTime() << SHARE_LOG_PREFIX << "Released " << pending
				<< " unacknowledged record(s) of the previous batch" << std::endl;
	}

	dropShareBatchLocked();

	rd_kafka_messages_t* batch = nullptr;
	rd_kafka_error_t* pollError = rd_kafka_share_poll(shareState.handle, timeoutMs, &batch);
	if (pollError != nullptr)
	{
		const bool fatal = rd_kafka_error_is_fatal(pollError);
		msg_err = std::string(fatal ? "Fatal error: " : "") + consumeError(pollError);
		if (eventFile.is_open()) eventFile << currentDateTime() << SHARE_LOG_PREFIX << "Poll error: " << msg_err << std::endl;
		rd_kafka_messages_destroy(batch);
		return "";
	}

	shareState.batch = batch;
	const size_t count = batch ? rd_kafka_messages_count(batch) : 0;
	shareState.records.reserve(count);

	const bool encodeAsBase64 = std::get<bool>(asBase64);
	boost::json::array messagesArray;
	for (size_t i = 0; i < count; ++i)
	{
		const rd_kafka_message_t* msg = rd_kafka_messages_get(batch, i);
		ShareHeldRecord rec;
		rec.msg = msg;

		boost::json::object msgObj;
		if (msg->err != RD_KAFKA_RESP_ERR_NO_ERROR)
		{
			// Record-level error: librdkafka has already acknowledged it (REJECT for corrupt or
			// unsupported batches, RELEASE for decompression failures), there is no data
			rec.ack = SHARE_ACK_AUTO;
			msgObj["topic"] = msg->rkt ? rd_kafka_topic_name(msg->rkt) : "";
			msgObj["partition"] = msg->partition;
			msgObj["offset"] = msg->offset;
			msgObj["error"] = rd_kafka_message_errstr(msg);
			if (eventFile.is_open())
				eventFile << currentDateTime() << SHARE_LOG_PREFIX << "Record error " << msgObj["topic"].as_string().c_str()
					<< "[" << msg->partition << "]@" << msg->offset << ": " << rd_kafka_message_errstr(msg) << std::endl;
		}
		else
		{
			msgObj = messageToJson(msg, encodeAsBase64);
		}
		msgObj["delivery_count"] = rd_kafka_message_delivery_count(msg);

		shareState.records.push_back(rec);
		messagesArray.push_back(std::move(msgObj));
	}

	boost::json::object result;
	result["messages"] = std::move(messagesArray);
	result["count"] = count;
	return boost::json::serialize(result);
}

bool SimpleKafka1C::acknowledgeShareRecordLocked(const std::string& topic, int32_t partition, int64_t offset,
	const std::string& ackType, std::string& error)
{
	rd_kafka_share_AcknowledgeType_t type{};
	if (!parseAckType(ackType, type, error))
	{
		return false;
	}

	ShareHeldRecord* found = nullptr;
	for (auto& rec : shareState.records)
	{
		if (rec.msg->partition == partition && rec.msg->offset == offset
			&& rec.msg->rkt != nullptr && topic == rd_kafka_topic_name(rec.msg->rkt))
		{
			found = &rec;
			break;
		}
	}
	if (found == nullptr)
	{
		error = "Record not found in current batch";
		return false;
	}
	if (found->ack == SHARE_ACK_AUTO)
	{
		error = "Record has error and is acknowledged automatically";
		return false;
	}
	// librdkafka accepts a second acknowledgement and keeps the last one; a repeated
	// decision in 1C code is more likely a bug, so it is refused
	if (found->ack != 0)
	{
		error = "Record is already acknowledged";
		return false;
	}

	const rd_kafka_resp_err_t err = rd_kafka_share_acknowledge_type(shareState.handle, found->msg, type);
	if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
	{
		error = rd_kafka_err2str(err);
		return false;
	}
	found->ack = type;
	return true;
}

bool SimpleKafka1C::shareAcknowledge(const variant_t& topic, const variant_t& partition, const variant_t& offset, const variant_t& ackType)
{
	std::lock_guard<std::mutex> lk(shareState.mtx);

	if (shareState.handle == nullptr)
	{
		msg_err = "Share consumer not initialized";
		return false;
	}
	if (!std::holds_alternative<std::string>(topic) || !std::holds_alternative<int32_t>(partition)
		|| !std::holds_alternative<std::string>(ackType))
	{
		msg_err = "shareAcknowledge: topic and ackType must be strings, partition must be a number";
		return false;
	}
	int64_t tOffset = 0;
	if (!offsetFromVariant(offset, tOffset, msg_err))
	{
		return false;
	}

	return acknowledgeShareRecordLocked(std::get<std::string>(topic), std::get<int32_t>(partition), tOffset,
		std::get<std::string>(ackType), msg_err);
}

bool SimpleKafka1C::shareAcknowledgeBatch(const variant_t& acksJson)
{
	std::lock_guard<std::mutex> lk(shareState.mtx);

	if (shareState.handle == nullptr)
	{
		msg_err = "Share consumer not initialized";
		return false;
	}
	if (!std::holds_alternative<std::string>(acksJson))
	{
		msg_err = "shareAcknowledgeBatch: expected a JSON string";
		return false;
	}

	boost::json::value root;
	try
	{
		root = boost::json::parse(std::get<std::string>(acksJson));
	}
	catch (const std::exception& e)
	{
		msg_err = std::string("Invalid JSON: ") + e.what();
		return false;
	}
	if (!root.is_array())
	{
		msg_err = "Invalid JSON: expected an array of {\"topic\",\"partition\",\"offset\",\"ack\"}";
		return false;
	}

	// Every element is processed; successful ones stay acknowledged even if others fail
	std::string errors;
	size_t failed = 0;
	const boost::json::array& items = root.as_array();
	for (size_t i = 0; i < items.size(); ++i)
	{
		std::string error;
		const boost::json::object* item = items[i].if_object();
		const boost::json::value* jTopic = item ? item->if_contains("topic") : nullptr;
		const boost::json::value* jPartition = item ? item->if_contains("partition") : nullptr;
		const boost::json::value* jOffset = item ? item->if_contains("offset") : nullptr;
		const boost::json::value* jAck = item ? item->if_contains("ack") : nullptr;

		if (!jTopic || !jTopic->is_string() || !jPartition || !jPartition->is_int64()
			|| !jOffset || !(jOffset->is_string() || jOffset->is_int64() || jOffset->is_uint64())
			|| !jAck || !jAck->is_string())
		{
			error = "expected {\"topic\": string, \"partition\": number, \"offset\": string or number, \"ack\": string}";
		}
		else
		{
			const variant_t offsetValue = jOffset->is_string()
				? variant_t(std::string(jOffset->as_string()))
				: variant_t(std::to_string(jOffset->to_number<int64_t>()));
			int64_t tOffset = 0;
			if (offsetFromVariant(offsetValue, tOffset, error))
			{
				acknowledgeShareRecordLocked(std::string(jTopic->as_string()),
					static_cast<int32_t>(jPartition->as_int64()), tOffset, std::string(jAck->as_string()), error);
			}
		}

		if (!error.empty())
		{
			++failed;
			errors += (errors.empty() ? "" : "; ") + std::string("item ") + std::to_string(i) + ": " + error;
		}
	}

	if (failed > 0)
	{
		msg_err = "Failed to acknowledge " + std::to_string(failed) + " of " + std::to_string(items.size())
			+ " record(s): " + errors;
		return false;
	}
	return true;
}

bool SimpleKafka1C::shareCommit(const variant_t& timeoutMs)
{
	std::lock_guard<std::mutex> lk(shareState.mtx);

	if (shareState.handle == nullptr)
	{
		msg_err = "Share consumer not initialized";
		return false;
	}
	if (!std::holds_alternative<int32_t>(timeoutMs))
	{
		msg_err = "shareCommit: timeoutMs must be a number";
		return false;
	}

	rd_kafka_topic_partition_list_t* partitions = nullptr;
	rd_kafka_error_t* commitError = rd_kafka_share_commit_sync(shareState.handle,
		std::max<int32_t>(0, std::get<int32_t>(timeoutMs)), &partitions);

	std::ofstream eventFile{};
	shareState.log->openLog(eventFile);

	if (commitError != nullptr)
	{
		msg_err = "Share commit error: " + consumeError(commitError);
		if (eventFile.is_open()) eventFile << currentDateTime() << SHARE_LOG_PREFIX << msg_err << std::endl;
		rd_kafka_topic_partition_list_destroy(partitions);
		return false;
	}

	// Per-partition failures are not retried by librdkafka
	std::string errors;
	if (partitions != nullptr)
	{
		for (int i = 0; i < partitions->cnt; ++i)
		{
			const rd_kafka_topic_partition_t& tp = partitions->elems[i];
			if (tp.err != RD_KAFKA_RESP_ERR_NO_ERROR)
			{
				errors += (errors.empty() ? "" : "; ") + std::string(tp.topic) + "[" + std::to_string(tp.partition)
					+ "]: " + rd_kafka_err2str(tp.err);
			}
		}
		rd_kafka_topic_partition_list_destroy(partitions);
	}

	if (!errors.empty())
	{
		msg_err = "Share commit failed for " + errors;
		if (eventFile.is_open()) eventFile << currentDateTime() << SHARE_LOG_PREFIX << msg_err << std::endl;
		return false;
	}
	return true;
}

void SimpleKafka1C::dropShareBatchLocked()
{
	shareState.records.clear();
	rd_kafka_messages_destroy(shareState.batch);
	shareState.batch = nullptr;
}

bool SimpleKafka1C::stopShareConsumer()
{
	std::lock_guard<std::mutex> lk(shareState.mtx);
	return closeShareConsumerLocked();
}

bool SimpleKafka1C::closeShareConsumerLocked()
{
	if (shareState.handle == nullptr)
	{
		return true;
	}

	// Acknowledgements are already recorded by librdkafka; the messages are not needed
	// for close() and must be freed before destroy()
	dropShareBatchLocked();

	// close() commits pending acknowledgements and leaves the group (bounded by
	// socket.timeout.ms); destroy() releases the handle even if close failed.
	const std::string closeError = consumeError(rd_kafka_share_consumer_close(shareState.handle));
	const std::string destroyError = consumeError(rd_kafka_share_destroy(shareState.handle));
	shareState.handle = nullptr;

	std::ofstream eventFile{};
	if (shareState.log)
	{
		shareState.log->openLog(eventFile);
	}
	// the opaque must stay alive until destroy() returns
	shareState.log.reset();

	if (!closeError.empty() || !destroyError.empty())
	{
		msg_err = "Share consumer close error: " + (closeError.empty() ? destroyError : closeError);
		if (eventFile.is_open()) eventFile << currentDateTime() << SHARE_LOG_PREFIX << msg_err << std::endl;
		return false;
	}

	if (eventFile.is_open()) eventFile << currentDateTime() << SHARE_LOG_PREFIX << "Share consumer closed" << std::endl;
	return true;
}
