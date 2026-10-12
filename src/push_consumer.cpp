/*
 *  Simple Kafka 1C - Push Consumer
 *  Background consumption with delivery to 1C via ExternalEvent
 */

#include "SimpleKafka1C.h"
#include "utils.h"

#include <boost/json.hpp>

#include <algorithm>
#include <fstream>
#include <limits>
#include <memory>

namespace
{
	constexpr char PUSH_EVENT_SOURCE[] = "SimpleKafka";
	constexpr char PUSH_EVENT_MESSAGES[] = "Messages";
	constexpr char PUSH_EVENT_ERROR[] = "Error";
	constexpr char PUSH_EVENT_STOPPED[] = "Stopped";

	// Upper bound for a single consume() call, so a stop request is noticed quickly.
	constexpr int32_t PUSH_POLL_SLICE_MS = 500;
	// Pause between attempts to queue an event while the 1C event buffer is full.
	constexpr int32_t PUSH_RETRY_DELAY_MS = 100;

	bool readPositiveInt32(const variant_t& value, const char* name, int32_t& out, std::string& errorMsg)
	{
		int64_t tmp = 0;
		if (!variantToInt64(value, tmp, errorMsg))
		{
			errorMsg = std::string(name) + ": " + errorMsg;
			return false;
		}
		if (tmp <= 0 || tmp > (std::numeric_limits<int32_t>::max)())
		{
			errorMsg = std::string(name) + " must be a positive number";
			return false;
		}
		out = static_cast<int32_t>(tmp);
		return true;
	}
}

//================================== Push Consumer ==========================================

bool SimpleKafka1C::pushConsumerActive()
{
	if (pushState.running.load())
	{
		msg_err = "Push consumer is running";
		return true;
	}
	return false;
}

bool SimpleKafka1C::startPushConsumer(const variant_t& maxMessages, const variant_t& maxWaitMs, const variant_t& asBase64, const variant_t& bufferDepth)
{
	if (hConsumer == nullptr)
	{
		msg_err = "Consumer not initialized";
		return false;
	}

	if (pushState.running.load())
	{
		msg_err = "Push consumer is already running";
		return false;
	}

	int32_t tMaxMessages = 0;
	int32_t tMaxWaitMs = 0;
	int32_t tBufferDepth = 0;
	if (!readPositiveInt32(maxMessages, "MaxMessages", tMaxMessages, msg_err)
		|| !readPositiveInt32(maxWaitMs, "MaxWaitMs", tMaxWaitMs, msg_err)
		|| !readPositiveInt32(bufferDepth, "EventBufferDepth", tBufferDepth, msg_err))
	{
		return false;
	}

	if (!std::holds_alternative<bool>(asBase64))
	{
		msg_err = "AsBase64 must be Boolean";
		return false;
	}

	// The worker may have finished on its own (fatal error); reap it before restarting.
	if (pushState.worker.joinable())
	{
		pushState.worker.join();
	}

	if (!SetEventBufferDepth(tBufferDepth))
	{
		msg_err = "Failed to set event buffer depth";
		return false;
	}

	pushState.maxMessages = tMaxMessages;
	pushState.maxWaitMs = tMaxWaitMs;
	pushState.asBase64 = std::get<bool>(asBase64);
	pushState.logDir = cl_event_cb.logDir;
	pushState.logFormat = cl_event_cb.formatLogFiles;
	pushState.clientId = cl_event_cb.clientid;
	pushState.stopRequested = false;
	pushState.running = true;

	try
	{
		pushState.worker = std::thread(&SimpleKafka1C::pushConsumerLoop, this);
	}
	catch (const std::exception& e)
	{
		pushState.running = false;
		msg_err = std::string("Failed to start push consumer thread: ") + e.what();
		return false;
	}

	return true;
}

bool SimpleKafka1C::stopPushConsumer()
{
	{
		std::lock_guard<std::mutex> lk(pushState.mtx);
		pushState.stopRequested = true;
	}
	pushState.cv.notify_all();

	if (pushState.worker.joinable())
	{
		pushState.worker.join();
	}

	pushState.running = false;
	return true;
}

bool SimpleKafka1C::isPushConsumerRunning()
{
	return pushState.running.load();
}

void SimpleKafka1C::pushConsumerLoop()
{
	RdKafka::KafkaConsumer* consumer = hConsumer;

	// Same naming scheme as openEventFile, but built from the snapshot taken at start.
	auto writeLog = [this](const std::string& text)
	{
		if (pushState.logDir.empty())
			return;

		std::string bufname = consumerLogName;
		if (!pushState.clientId.empty())
			bufname += pushState.clientId + "_";

		std::ofstream eventFile(pushState.logDir + bufname + std::to_string(pid) + "_"
			+ currentDateTime(pushState.logFormat.c_str()) + ".log", std::ios_base::app);
		if (eventFile.is_open())
			eventFile << currentDateTime() << " " << text << std::endl;
	};

	// Waits for PUSH_RETRY_DELAY_MS or a stop request. Returns true if stop was requested.
	auto waitRetry = [this]()
	{
		std::unique_lock<std::mutex> lk(pushState.mtx);
		return pushState.cv.wait_for(lk, std::chrono::milliseconds(PUSH_RETRY_DELAY_MS),
			[this] { return pushState.stopRequested.load(); });
	};

	writeLog("Info: push consumer started");

	std::string stopReason;

	while (!pushState.stopRequested.load())
	{
		boost::json::array messagesArray;
		int32_t messagesRead = 0;
		auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(pushState.maxWaitMs);

		while (messagesRead < pushState.maxMessages && !pushState.stopRequested.load())
		{
			auto now = std::chrono::steady_clock::now();
			if (now >= deadline)
			{
				if (messagesRead > 0)
					break;
				// Empty batches are never sent: start a new wait window instead.
				deadline = now + std::chrono::milliseconds(pushState.maxWaitMs);
			}

			const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
			const int32_t pollMs = static_cast<int32_t>(
				(std::max<long long>)(1, (std::min<long long>)(remaining, PUSH_POLL_SLICE_MS)));

			std::unique_ptr<RdKafka::Message> msg(consumer->consume(pollMs));
			if (!msg)
				continue;

			const RdKafka::ErrorCode err = msg->err();
			if (err == RdKafka::ERR_NO_ERROR)
			{
				try
				{
					messagesArray.push_back(messageToJson(*msg, pushState.asBase64));
					messagesRead++;
					consumerMetrics.messagesConsumed++;
					consumerMetrics.bytesConsumed += msg->len();
				}
				catch (const std::exception& e)
				{
					consumerMetrics.errorsCount++;
					const std::string text = std::string("Failed to process consumed message: ") + e.what();
					writeLog("Error: " + text);
					ExternalEventFromThread(PUSH_EVENT_SOURCE, PUSH_EVENT_ERROR, text);
				}
			}
			else if (err == RdKafka::ERR__TIMED_OUT)
			{
				consumerMetrics.pollTimeouts++;
			}
			else if (err == RdKafka::ERR__PARTITION_EOF)
			{
				// End of partition is not an error.
			}
			else if (err == RdKafka::ERR__FATAL)
			{
				consumerMetrics.errorsCount++;
				stopReason = msg->errstr();
				break;
			}
			else
			{
				consumerMetrics.errorsCount++;
				const std::string text = msg->errstr();
				writeLog("Error: " + text);
				ExternalEventFromThread(PUSH_EVENT_SOURCE, PUSH_EVENT_ERROR, text);
			}
		}

		// Messages read before a fatal error are still delivered.
		if (messagesRead > 0)
		{
			boost::json::object result;
			result["messages"] = std::move(messagesArray);
			result["count"] = messagesRead;
			const std::string payload = boost::json::serialize(result);

			// The 1C event buffer is full: keep the batch and retry, no new messages are read meanwhile.
			while (!ExternalEventFromThread(PUSH_EVENT_SOURCE, PUSH_EVENT_MESSAGES, payload))
			{
				if (waitRetry())
				{
					writeLog("Warning: push consumer stopped with an undelivered batch of "
						+ std::to_string(messagesRead) + " messages");
					break;
				}
			}
		}

		if (!stopReason.empty())
			break;
	}

	if (!stopReason.empty())
	{
		writeLog("Error: push consumer stopped: " + stopReason);
		ExternalEventFromThread(PUSH_EVENT_SOURCE, PUSH_EVENT_STOPPED, stopReason);
	}
	else
	{
		writeLog("Info: push consumer stopped");
	}

	pushState.running = false;
}
