/*
 *  Native harness: runs acceptance scenarios of specs/share-consumer.md against
 *  SimpleKafka1C.dll and the Docker test broker (docker/kafka-share).
 *
 *  Usage: native_harness <path to SimpleKafka1C.dll> [brokers] [--only AC-1,AC-18]
 *         [--expect-rdkafka 2.16.0] [--no-share-brokers host:port]
 */

#include "addin_host.h"

#include <boost/json.hpp>
#include <librdkafka/rdkafka.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <set>
#include <sstream>
#include <thread>

namespace fs = std::filesystem;

namespace
{
	struct Context
	{
		std::string dllPath;
		std::string brokers = "127.0.0.1:9092";
		std::string expectRdkafka = "2.16.0";
		// Broker without share groups (Kafka < 4.2 or share.version=0), for AC-15
		std::string noShareBrokers;
		std::string runId;
		fs::path logRoot;
		std::unique_ptr<Library> library;
	};

	// Thrown by REQUIRE to abort the current scenario
	struct ScenarioFailed
	{
	};

	struct ScenarioSkipped
	{
		std::string reason;
	};

	int g_checks = 0;
	int g_failures = 0;

	void check(bool ok, const std::string& what)
	{
		++g_checks;
		if (!ok) ++g_failures;
		std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what.c_str());
	}

	void require(bool ok, const std::string& what)
	{
		check(ok, what);
		if (!ok) throw ScenarioFailed{};
	}

	void skip(const std::string& reason)
	{
		throw ScenarioSkipped{reason};
	}

	bool contains(const std::string& text, const std::string& part)
	{
		return text.find(part) != std::string::npos;
	}

	// The call result is evaluated before the body, so lastError() belongs to that call
	void expect(AddIn& addin, bool ok, const std::string& what)
	{
		check(ok, ok ? what : what + ": " + addin.lastError());
	}

	void ensure(AddIn& addin, bool ok, const std::string& what)
	{
		expect(addin, ok, what);
		if (!ok) throw ScenarioFailed{};
	}

	std::string uniqueName(const Context& ctx, const std::string& prefix)
	{
		static int counter = 0;
		return prefix + "-" + ctx.runId + "-" + std::to_string(++counter);
	}

	// Per-scenario log directory, with a trailing separator as the component expects
	std::string logDirFor(const Context& ctx, const std::string& scenario)
	{
		const fs::path dir = ctx.logRoot / scenario;
		fs::create_directories(dir);
		return (dir / "").string();
	}

	std::string readLogs(const std::string& dir)
	{
		std::string all;
		for (const auto& entry : fs::directory_iterator(dir))
		{
			std::ifstream in(entry.path(), std::ios::binary);
			std::stringstream ss;
			ss << in.rdbuf();
			all += ss.str();
		}
		return all;
	}

	//-------------------------------------------------------------------------------------
	// Broker-side preparation through librdkafka directly: the component has no API for
	// group configs, and a new share group starts from latest (records produced before
	// the group exists would be invisible).

	bool setShareGroupEarliest(const Context& ctx, const std::string& group, std::string& error)
	{
		char errstr[512];
		rd_kafka_conf_t* conf = rd_kafka_conf_new();
		rd_kafka_conf_set(conf, "bootstrap.servers", ctx.brokers.c_str(), errstr, sizeof(errstr));
		rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
		if (rk == nullptr)
		{
			error = errstr;
			rd_kafka_conf_destroy(conf);
			return false;
		}
		rd_kafka_queue_t* q = rd_kafka_queue_new(rk);
		rd_kafka_ConfigResource_t* res = rd_kafka_ConfigResource_new(RD_KAFKA_RESOURCE_GROUP, group.c_str());
		rd_kafka_error_t* addErr = rd_kafka_ConfigResource_add_incremental_config(
			res, "share.auto.offset.reset", RD_KAFKA_ALTER_CONFIG_OP_TYPE_SET, "earliest");
		bool ok = false;
		if (addErr != nullptr)
		{
			error = rd_kafka_error_string(addErr);
			rd_kafka_error_destroy(addErr);
		}
		else
		{
			rd_kafka_IncrementalAlterConfigs(rk, &res, 1, nullptr, q);
			rd_kafka_event_t* ev = rd_kafka_queue_poll(q, 15000);
			if (ev == nullptr)
			{
				error = "IncrementalAlterConfigs timeout";
			}
			else
			{
				rd_kafka_resp_err_t err = rd_kafka_event_error(ev);
				const char* msg = rd_kafka_event_error_string(ev);
				const auto* result = rd_kafka_event_IncrementalAlterConfigs_result(ev);
				if (err == RD_KAFKA_RESP_ERR_NO_ERROR && result != nullptr)
				{
					size_t cnt = 0;
					const rd_kafka_ConfigResource_t** rres = rd_kafka_IncrementalAlterConfigs_result_resources(result, &cnt);
					if (cnt > 0)
					{
						err = rd_kafka_ConfigResource_error(rres[0]);
						msg = rd_kafka_ConfigResource_error_string(rres[0]);
					}
				}
				ok = (err == RD_KAFKA_RESP_ERR_NO_ERROR);
				if (!ok) error = msg ? msg : rd_kafka_err2str(err);
				rd_kafka_event_destroy(ev);
			}
		}
		rd_kafka_ConfigResource_destroy(res);
		rd_kafka_queue_destroy(q);
		rd_kafka_destroy(rk);
		return ok;
	}

	//-------------------------------------------------------------------------------------
	// Component-level helpers

	void createTopic(const Context& ctx, const std::string& topic, int32_t partitions)
	{
		AddIn admin(*ctx.library);
		const bool ok = admin.callBool("СоздатьТопик", {ctx.brokers, topic, partitions, int32_t{1}});
		require(ok, "СоздатьТопик " + topic + (ok ? "" : ": " + admin.lastError()));
	}

	// Produces "<prefix>-<i>" messages, i = 0..count-1, and flushes
	// separateBatches: wait for each delivery, so every message is its own record batch
	// (the broker hands share consumers whole record batches)
	void produceMessages(const Context& ctx, const std::string& topic, int count, const std::string& prefix,
		bool separateBatches = false)
	{
		AddIn producer(*ctx.library);
		ensure(producer, producer.callBool("ИнициализироватьПродюсера", {ctx.brokers}), "ИнициализироватьПродюсера");
		const char* method = separateBatches ? "ОтправитьСообщениеСОжиданиемРезультата" : "ОтправитьСообщение";
		for (int i = 0; i < count; ++i)
		{
			const Value r = producer.call(method,
				{prefix + "-" + std::to_string(i), topic, int32_t{-1}, "key-" + std::to_string(i)});
			if (std::get<int32_t>(r) == -1 || (separateBatches && std::get<int32_t>(r) != 2))
			{
				require(false, std::string(method) + ": " + producer.lastError());
			}
		}
		ensure(producer, producer.callBool("ОстановитьПродюсера"), "ОстановитьПродюсера");
	}

	// Reads with ЧитатьПакетСообщений until `expected` messages arrive or the deadline passes
	std::vector<boost::json::object> consumeRegular(AddIn& consumer, size_t expected, int deadlineMs)
	{
		std::vector<boost::json::object> got;
		const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(deadlineMs);
		while (got.size() < expected && std::chrono::steady_clock::now() < until)
		{
			const std::string json = consumer.callString("ЧитатьПакетСообщений", {int32_t{100}, int32_t{1000}});
			if (json.empty())
			{
				require(false, "ЧитатьПакетСообщений: " + consumer.lastError());
			}
			const boost::json::object root = boost::json::parse(json).as_object();
			for (const auto& m : root.at("messages").as_array())
			{
				got.push_back(m.as_object());
			}
		}
		return got;
	}

	// Topic with `count` messages "<prefix>-<i>" and a share consumer subscribed to it;
	// the group reads from the beginning of the topic
	void prepareShareConsumer(Context& ctx, AddIn& c, const std::string& topic, const std::string& group,
		int count, const std::string& prefix)
	{
		createTopic(ctx, topic, 1);
		if (count > 0)
		{
			produceMessages(ctx, topic, count, prefix);
		}
		std::string error;
		require(setShareGroupEarliest(ctx, group, error), "share.auto.offset.reset=earliest для " + group + (error.empty() ? "" : ": " + error));
		ensure(c, c.callBool("ИнициализироватьКонсьюмераОчереди", {ctx.brokers, group}), "ИнициализироватьКонсьюмераОчереди");
		ensure(c, c.callBool("ПодписатьсяНаОчередь", {topic}), "ПодписатьсяНаОчередь");
	}

	// One ПрочитатьПакетИзОчереди call; an empty result fails the scenario
	boost::json::array shareRead(AddIn& c, int32_t waitMs, bool releaseUnacked = false)
	{
		const std::string json = c.callString("ПрочитатьПакетИзОчереди", {waitMs, false, releaseUnacked});
		if (json.empty())
		{
			require(false, "ПрочитатьПакетИзОчереди: " + c.lastError());
		}
		const boost::json::object root = boost::json::parse(json).as_object();
		const boost::json::array& messages = root.at("messages").as_array();
		if (root.at("count").to_number<int64_t>() != static_cast<int64_t>(messages.size()))
		{
			require(false, "count не совпадает с размером messages");
		}
		return messages;
	}

	// Reads until a non-empty batch arrives (the first polls go to joining the group)
	boost::json::array shareReadFirst(AddIn& c, int deadlineMs, bool releaseUnacked = false)
	{
		const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(deadlineMs);
		while (std::chrono::steady_clock::now() < until)
		{
			boost::json::array batch = shareRead(c, 1000, releaseUnacked);
			if (!batch.empty())
			{
				return batch;
			}
		}
		return {};
	}

	int64_t offsetOf(const boost::json::value& m)
	{
		return m.at("offset").to_number<int64_t>();
	}

	void acknowledge(AddIn& c, const boost::json::value& m, const std::string& ack)
	{
		const bool ok = c.callBool("ПодтвердитьСообщениеОчереди",
			{std::string(m.at("topic").as_string()), static_cast<int32_t>(m.at("partition").to_number<int64_t>()),
			 std::to_string(offsetOf(m)), ack});
		if (!ok)
		{
			require(false, "ПодтвердитьСообщениеОчереди " + std::to_string(offsetOf(m)) + " " + ack + ": " + c.lastError());
		}
	}

	using Decide = std::function<std::string(const boost::json::value&)>;
	using StopWhen = std::function<bool(const std::vector<boost::json::value>&)>;

	// Reads for up to durationMs, acknowledging every record with decide(); stops early
	// when stopWhen() says so. Returns every record seen, in order.
	std::vector<boost::json::value> drain(AddIn& c, int durationMs, const Decide& decide, const StopWhen& stopWhen = nullptr)
	{
		std::vector<boost::json::value> seen;
		const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(durationMs);
		while (std::chrono::steady_clock::now() < until)
		{
			for (const auto& m : shareRead(c, 1000))
			{
				acknowledge(c, m, decide(m));
				seen.push_back(m);
			}
			if (stopWhen && stopWhen(seen))
			{
				break;
			}
		}
		return seen;
	}

	std::string acceptAll(const boost::json::value&)
	{
		return "accept";
	}

	StopWhen seenCount(size_t n)
	{
		return [n](const std::vector<boost::json::value>& seen) { return seen.size() >= n; };
	}

	// Joins the group with a new consumer and reads for a while: records acknowledged
	// before must not come back
	std::vector<boost::json::value> redeliveredAfterReconnect(Context& ctx, const std::string& topic, const std::string& group)
	{
		AddIn c(*ctx.library);
		ensure(c, c.callBool("ИнициализироватьКонсьюмераОчереди", {ctx.brokers, group}), "переподключение к группе");
		ensure(c, c.callBool("ПодписатьсяНаОчередь", {topic}), "подписка после переподключения");
		return drain(c, 12000, acceptAll);
	}

	std::string offsetsText(const std::vector<boost::json::value>& records)
	{
		std::string text;
		for (const auto& m : records)
		{
			text += " " + std::to_string(offsetOf(m)) + "(dc=" + std::to_string(m.at("delivery_count").to_number<int64_t>()) + ")";
		}
		return text.empty() ? " нет" : text;
	}

	std::set<std::string> keysOf(const boost::json::object& obj)
	{
		std::set<std::string> keys;
		for (const auto& kv : obj)
		{
			keys.insert(std::string(kv.key()));
		}
		return keys;
	}

	//-------------------------------------------------------------------------------------
	// Scenarios

	void ac1(Context& ctx)
	{
		const std::string topic = uniqueName(ctx, "q-h");
		const std::string group = uniqueName(ctx, "sg-h");
		createTopic(ctx, topic, 1);

		AddIn c(*ctx.library);
		expect(c, c.callBool("ИнициализироватьКонсьюмераОчереди", {ctx.brokers, group}), "ИнициализироватьКонсьюмераОчереди → Истина");
		expect(c, c.callBool("ПодписатьсяНаОчередь", {topic}), "ПодписатьсяНаОчередь → Истина");
		check(c.callBool("ОстановитьКонсьюмераОчереди"), "ОстановитьКонсьюмераОчереди → Истина");
	}

	void ac2(Context& ctx)
	{
		AddIn c(*ctx.library);
		check(!c.callBool("ИнициализироватьКонсьюмераОчереди", {ctx.brokers, std::string()}),
			"пустая группа → Ложь");
		const std::string err = c.lastError();
		check(contains(err, "group ID cannot be empty"), "текст ошибки: " + err);

		check(!c.callBool("ПодписатьсяНаОчередь", {std::string("q-test")}), "подписка без инициализации → Ложь");
		check(contains(c.lastError(), "Share consumer not initialized"), "текст ошибки: Share consumer not initialized");
	}

	void ac4(Context& ctx)
	{
		const std::string topic = uniqueName(ctx, "q-h");
		const std::string group = uniqueName(ctx, "sg-h");
		AddIn c(*ctx.library);
		prepareShareConsumer(ctx, c, topic, group, 5, "a");

		const auto seen = drain(c, 30000, acceptAll, seenCount(5));
		require(seen.size() == 5, "прочитаны и подтверждены accept 5 записей");
		expect(c, c.callBool("ЗафиксироватьПодтвержденияОчереди"), "ЗафиксироватьПодтвержденияОчереди → Истина");
		expect(c, c.callBool("ЗафиксироватьПодтвержденияОчереди"), "фиксация без ожидающих подтверждений → Истина");
		c.callBool("ОстановитьКонсьюмераОчереди");

		const auto again = redeliveredAfterReconnect(ctx, topic, group);
		check(again.empty(), "после переподключения записи не доставляются (повторно:" + offsetsText(again) + ")");
	}

	void ac5(Context& ctx)
	{
		AddIn c(*ctx.library);
		prepareShareConsumer(ctx, c, uniqueName(ctx, "q-h"), uniqueName(ctx, "sg-h"), 1, "b");
		const boost::json::array batch = shareReadFirst(c, 30000);
		require(batch.size() == 1, "прочитана 1 запись");
		const int64_t offset = offsetOf(batch[0]);
		acknowledge(c, batch[0], "release");

		const auto seen = drain(c, 20000, acceptAll, seenCount(1));
		check(seen.size() == 1 && offsetOf(seen[0]) == offset && seen[0].at("delivery_count").to_number<int64_t>() == 2,
			"запись " + std::to_string(offset) + " пришла снова с delivery_count = 2 (получено:" + offsetsText(seen) + ")");
	}

	void ac6(Context& ctx)
	{
		AddIn c(*ctx.library);
		prepareShareConsumer(ctx, c, uniqueName(ctx, "q-h"), uniqueName(ctx, "sg-h"), 2, "c");
		// offset 0 is rejected, the rest accepted
		const auto first = drain(c, 30000,
			[](const boost::json::value& m) { return offsetOf(m) == 0 ? std::string("reject") : std::string("accept"); },
			seenCount(2));
		require(first.size() == 2, "прочитаны 2 записи, запись 0 отклонена");
		const auto again = drain(c, 10000, acceptAll);
		check(again.empty(), "отклонённая запись больше не доставляется (повторно:" + offsetsText(again) + ")");
	}

	void ac7(Context& ctx)
	{
		const std::string topic = uniqueName(ctx, "q-h");
		const std::string group = uniqueName(ctx, "sg-h");
		AddIn a(*ctx.library);
		AddIn b(*ctx.library);
		// Small fetches so that one reader does not acquire the whole partition at once
		for (AddIn* x : {&a, &b})
		{
			x->call("УстановитьПараметр", {std::string("max.poll.records"), std::string("5")});
			x->call("УстановитьПараметр", {std::string("fetch.max.bytes"), std::string("200")});
		}
		prepareShareConsumer(ctx, a, topic, group, 0, "p");
		ensure(b, b.callBool("ИнициализироватьКонсьюмераОчереди", {ctx.brokers, group}), "второй экземпляр: инициализация");
		ensure(b, b.callBool("ПодписатьсяНаОчередь", {topic}), "второй экземпляр: подписка");
		// Both members join the group (several seconds) before the records appear,
		// otherwise the first one acquires everything alone
		const auto joined = std::chrono::steady_clock::now() + std::chrono::seconds(10);
		while (std::chrono::steady_clock::now() < joined)
		{
			require(shareRead(a, 500).empty() && shareRead(b, 500).empty(), "пустой топик до записи");
		}
		produceMessages(ctx, topic, 100, "p", true);

		std::set<int64_t> offsetsA;
		std::set<int64_t> offsetsB;
		const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(60);
		while (offsetsA.size() + offsetsB.size() < 100 && std::chrono::steady_clock::now() < until)
		{
			for (const auto& m : shareRead(a, 200))
			{
				acknowledge(a, m, "accept");
				offsetsA.insert(offsetOf(m));
			}
			for (const auto& m : shareRead(b, 200))
			{
				acknowledge(b, m, "accept");
				offsetsB.insert(offsetOf(m));
			}
		}
		std::set<int64_t> all(offsetsA);
		all.insert(offsetsB.begin(), offsetsB.end());
		check(!offsetsA.empty() && !offsetsB.empty(),
			"оба экземпляра получили записи (A " + std::to_string(offsetsA.size()) + ", B " + std::to_string(offsetsB.size()) + ")");
		check(all.size() == 100, "уникальных смещений 100 (получено " + std::to_string(all.size()) + ")");
		check(offsetsA.size() + offsetsB.size() == all.size(), "смещения не пересекаются");
	}

	void ac9(Context& ctx)
	{
		const std::string topic = uniqueName(ctx, "q-h");
		const std::string group = uniqueName(ctx, "sg-h");
		AddIn a(*ctx.library);
		prepareShareConsumer(ctx, a, topic, group, 1, "l");
		const boost::json::array batch = shareReadFirst(a, 30000);
		require(batch.size() == 1, "A прочитал запись и не подтверждает её");
		const int64_t offset = offsetOf(batch[0]);
		const auto readAt = std::chrono::steady_clock::now();

		AddIn b(*ctx.library);
		ensure(b, b.callBool("ИнициализироватьКонсьюмераОчереди", {ctx.brokers, group}), "B: инициализация");
		ensure(b, b.callBool("ПодписатьсяНаОчередь", {topic}), "B: подписка");
		const auto seen = drain(b, 40000, acceptAll, seenCount(1));
		const auto afterMs = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - readAt).count();
		check(seen.size() == 1 && offsetOf(seen[0]) == offset && seen[0].at("delivery_count").to_number<int64_t>() == 2,
			"B получил запись " + std::to_string(offset) + " с delivery_count = 2 через " + std::to_string(afterMs) + " мс (получено:" + offsetsText(seen) + ")");
		check(afterMs >= 14000, "не раньше блокировки 15 с");
	}

	void ac10to12(Context& ctx)
	{
		AddIn c(*ctx.library);
		const std::string topic = uniqueName(ctx, "q-h");
		prepareShareConsumer(ctx, c, topic, uniqueName(ctx, "sg-h"), 1, "e");
		const boost::json::array batch = shareReadFirst(c, 30000);
		require(batch.size() == 1, "прочитана 1 запись");
		const int64_t offset = offsetOf(batch[0]);

		// AC-10
		check(!c.callBool("ПодтвердитьСообщениеОчереди", {topic, int32_t{0}, std::string("999"), std::string("accept")}),
			"AC-10: чужое смещение → Ложь");
		check(c.lastError() == "Record not found in current batch", "AC-10: ошибка " + c.lastError());
		check(!c.callBool("ПодтвердитьСообщениеОчереди", {std::string("other-topic"), int32_t{0}, std::to_string(offset), std::string("accept")}),
			"AC-10: чужой топик → Ложь");

		// AC-11
		check(!c.callBool("ПодтвердитьСообщениеОчереди", {topic, int32_t{0}, std::to_string(offset), std::string("commit")}),
			"AC-11: решение commit → Ложь");
		check(contains(c.lastError(), "expected accept, release or reject"), "AC-11: ошибка " + c.lastError());

		// AC-12: the record cannot be placed above int32 on the test broker, so only parsing
		// of such offsets is checked: they reach the batch lookup instead of failing earlier
		check(!c.callBool("ПодтвердитьСообщениеОчереди", {topic, int32_t{0}, std::string("3000000000"), std::string("accept")}),
			"AC-12: Строка 3000000000 → Ложь");
		check(c.lastError() == "Record not found in current batch", "AC-12: Строка разобрана, ошибка " + c.lastError());
		check(!c.callBool("ПодтвердитьСообщениеОчереди", {topic, int32_t{0}, int64_t{3000000000}, std::string("accept")}),
			"AC-12: Число 3000000000 (VTYPE_I8) → Ложь");
		check(c.lastError() == "Record not found in current batch", "AC-12: Число разобрано, ошибка " + c.lastError());
		check(!c.callBool("ПодтвердитьСообщениеОчереди", {topic, int32_t{0}, std::string("12abc"), std::string("accept")}),
			"AC-12: Строка 12abc → Ложь");

		// Number offset, upper-case decision, repeated acknowledgement
		expect(c, c.callBool("ПодтвердитьСообщениеОчереди", {topic, int32_t{0}, static_cast<int32_t>(offset), std::string("ACCEPT")}),
			"смещение Числом и ACCEPT в верхнем регистре → Истина");
		check(!c.callBool("ПодтвердитьСообщениеОчереди", {topic, int32_t{0}, std::to_string(offset), std::string("release")}),
			"повторное подтверждение → Ложь");
		check(c.lastError() == "Record is already acknowledged", "ошибка " + c.lastError());
		expect(c, c.callBool("ЗафиксироватьПодтвержденияОчереди"), "фиксация → Истина");
	}

	void ac13(Context& ctx)
	{
		const std::string topic = uniqueName(ctx, "q-h");
		const std::string group = uniqueName(ctx, "sg-h");
		AddIn c(*ctx.library);
		prepareShareConsumer(ctx, c, topic, group, 3, "k");
		boost::json::array batch = shareReadFirst(c, 30000);
		require(batch.size() == 3, "прочитан пакет из 3 записей (" + std::to_string(batch.size()) + ")");

		boost::json::array acks;
		acks.push_back({{"topic", topic}, {"partition", 0}, {"offset", std::to_string(offsetOf(batch[0]))}, {"ack", "accept"}});
		acks.push_back({{"topic", topic}, {"partition", 0}, {"offset", 999}, {"ack", "accept"}});
		acks.push_back({{"topic", topic}, {"partition", 0}, {"offset", offsetOf(batch[2])}, {"ack", "accept"}});
		check(!c.callBool("ПодтвердитьСообщенияОчереди", {boost::json::serialize(acks)}), "пакет с одним чужим смещением → Ложь");
		const std::string err = c.lastError();
		check(contains(err, "1 of 3") && contains(err, "item 1: Record not found in current batch") && !contains(err, "item 0")
			&& !contains(err, "item 2"), "в ошибке один ошибочный элемент: " + err);
		check(!c.callBool("ПодтвердитьСообщенияОчереди", {std::string("{")}), "неверный JSON → Ложь");

		// Record 1 stays unacknowledged; it comes back after the stop, records 0 and 2 do not
		const int64_t middle = offsetOf(batch[1]);
		expect(c, c.callBool("ЗафиксироватьПодтвержденияОчереди"), "фиксация → Истина");
		c.callBool("ОстановитьКонсьюмераОчереди");
		AddIn r(*ctx.library);
		ensure(r, r.callBool("ИнициализироватьКонсьюмераОчереди", {ctx.brokers, group}), "переподключение");
		ensure(r, r.callBool("ПодписатьсяНаОчередь", {topic}), "подписка");
		const auto again = drain(r, 40000, acceptAll, seenCount(1));
		check(again.size() == 1 && offsetOf(again[0]) == middle,
			"повторно пришла только неподтверждённая запись " + std::to_string(middle) + " (получено:" + offsetsText(again) + ")");
	}

	void ac14(Context& ctx)
	{
		const std::string topic = uniqueName(ctx, "q-h");
		const std::string group = uniqueName(ctx, "sg-h");

		AddIn c(*ctx.library);
		prepareShareConsumer(ctx, c, topic, group, 3, "z");
		// Acknowledged but not committed: close() must deliver the acknowledgements
		const auto seen = drain(c, 30000, acceptAll, seenCount(3));
		require(seen.size() == 3, "прочитаны и подтверждены 3 записи без фиксации");
		expect(c, c.callBool("ОстановитьКонсьюмераОчереди"), "ОстановитьКонсьюмераОчереди → Истина");
		const auto again = redeliveredAfterReconnect(ctx, topic, group);
		check(again.empty(), "после остановки записи не доставляются повторно (повторно:" + offsetsText(again) + ")");

		expect(c, c.callBool("ОстановитьКонсьюмераОчереди"), "ОстановитьКонсьюмераОчереди без консьюмера → Истина");
		check(c.callBool("ОстановитьКонсьюмераОчереди"), "повторная остановка → Истина");
		check(!c.callBool("ПодписатьсяНаОчередь", {topic}), "после остановки подписка → Ложь");

		// Re-initialization on a live handle closes the previous one
		ensure(c, c.callBool("ИнициализироватьКонсьюмераОчереди", {ctx.brokers, group}), "повторная инициализация");
		check(c.callBool("ИнициализироватьКонсьюмераОчереди", {ctx.brokers, group}), "инициализация поверх живого → Истина");
		check(c.callBool("ПодписатьсяНаОчередь", {topic}), "подписка после переинициализации → Истина");
		check(c.callBool("ОтписатьсяОтОчереди"), "ОтписатьсяОтОчереди → Истина");
		// Destructor of the object stops the consumer (AddIn goes out of scope)
	}

	void ac3(Context& ctx)
	{
		const std::string topic = uniqueName(ctx, "q-h");
		AddIn c(*ctx.library);
		prepareShareConsumer(ctx, c, topic, uniqueName(ctx, "sg-h"), 10, "s");

		const boost::json::array batch = shareReadFirst(c, 30000);
		require(!batch.empty(), "ПрочитатьПакетИзОчереди вернул записи (" + std::to_string(batch.size()) + ")");

		bool dcOk = true;
		bool bodyOk = true;
		for (const auto& v : batch)
		{
			const auto& m = v.as_object();
			dcOk = dcOk && m.contains("delivery_count") && m.at("delivery_count").to_number<int64_t>() == 1;
			bodyOk = bodyOk && contains(std::string(m.at("message").as_string()), "s-") && !m.contains("error");
		}
		check(dcOk, "delivery_count = 1 у всех записей");
		check(bodyOk, "тела s-N, поля error нет");

		// Same fields as ЧитатьПакетСообщений plus delivery_count (AC-18 for the format)
		AddIn r(*ctx.library);
		r.call("УстановитьПараметр", {std::string("group.id"), uniqueName(ctx, "cg-h")});
		r.call("УстановитьПараметр", {std::string("auto.offset.reset"), std::string("earliest")});
		ensure(r, r.callBool("ИнициализироватьКонсьюмера", {ctx.brokers}), "ИнициализироватьКонсьюмера");
		ensure(r, r.callBool("Подписаться", {topic}), "Подписаться");
		const auto regular = consumeRegular(r, 10, 20000);
		require(regular.size() == 10, "обычный consumer прочитал 10");
		const auto& first = batch[0].as_object();
		const int64_t offset = first.at("offset").to_number<int64_t>();
		const auto same = std::find_if(regular.begin(), regular.end(),
			[&](const boost::json::object& m) { return m.at("offset").to_number<int64_t>() == offset; });
		require(same != regular.end(), "запись с тем же смещением есть в ЧитатьПакетСообщений");
		std::set<std::string> expectedKeys = keysOf(*same);
		expectedKeys.insert("delivery_count");
		check(keysOf(first) == expectedKeys, "набор полей = ЧитатьПакетСообщений + delivery_count");
		check(first.at("message") == same->at("message") && first.at("key") == same->at("key")
			&& first.at("timestamp") == same->at("timestamp"), "message, key, timestamp совпадают");
		r.callBool("ОстановитьКонсьюмера");
	}

	void ac8(Context& ctx)
	{
		AddIn c(*ctx.library);
		prepareShareConsumer(ctx, c, uniqueName(ctx, "q-h"), uniqueName(ctx, "sg-h"), 3, "u");
		const boost::json::array batch = shareReadFirst(c, 30000);
		require(!batch.empty(), "пакет прочитан");

		const std::string again = c.callString("ПрочитатьПакетИзОчереди", {int32_t{1000}});
		check(again.empty(), "повторное чтение без подтверждения → пустая строка");
		const std::string err = c.lastError();
		check(err == "Previous batch has unacknowledged records", "ошибка: " + err);
	}

	void ac8a(Context& ctx)
	{
		AddIn c(*ctx.library);
		prepareShareConsumer(ctx, c, uniqueName(ctx, "q-h"), uniqueName(ctx, "sg-h"), 1, "n");
		const boost::json::array batch = shareReadFirst(c, 30000);
		require(batch.size() == 1, "прочитана 1 запись");
		const int64_t offset = batch[0].at("offset").to_number<int64_t>();

		// Released record comes back; it may take more than one poll
		int64_t dc = 0;
		const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(20);
		while (dc == 0 && std::chrono::steady_clock::now() < until)
		{
			for (const auto& v : shareRead(c, 1000, true))
			{
				if (v.at("offset").to_number<int64_t>() == offset)
				{
					dc = v.at("delivery_count").to_number<int64_t>();
				}
			}
		}
		check(dc == 2, "запись " + std::to_string(offset) + " пришла снова с delivery_count = 2 (получено " + std::to_string(dc) + ")");
	}

	void ac15(Context& ctx)
	{
		if (ctx.noShareBrokers.empty())
		{
			skip("нужен брокер без share groups: --no-share-brokers host:port");
		}
		const std::string topic = uniqueName(ctx, "q-h");
		{
			AddIn admin(*ctx.library);
			const bool created = admin.callBool("СоздатьТопик", {ctx.noShareBrokers, topic, int32_t{1}, int32_t{1}});
			expect(admin, created, "СоздатьТопик " + topic);
		}
		AddIn c(*ctx.library);
		const bool init = c.callBool("ИнициализироватьКонсьюмераОчереди", {ctx.noShareBrokers, uniqueName(ctx, "sg-h")});
		const bool sub = init && c.callBool("ПодписатьсяНаОчередь", {topic});
		std::printf("    init=%d subscribe=%d %s\n", init, sub, (init && sub) ? "" : c.lastError().c_str());
		std::string json;
		std::string err;
		const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(15);
		while (init && sub && std::chrono::steady_clock::now() < until)
		{
			json = c.callString("ПрочитатьПакетИзОчереди", {int32_t{1000}});
			err = c.lastError();
			std::printf("    read: '%s' error: '%s'\n", json.c_str(), err.c_str());
			if (json.empty())
			{
				break;
			}
		}
		check(!init || !sub || (json.empty() && !err.empty()), "ошибка вместо пакета, компонента не упала");
	}

	void ac16(Context& ctx)
	{
		const std::string topic = uniqueName(ctx, "t-h");
		const std::string regularGroup = uniqueName(ctx, "cg-h");
		const std::string shareGroup = uniqueName(ctx, "sg-h");
		createTopic(ctx, topic, 1);
		produceMessages(ctx, topic, 5, "m");

		AddIn c(*ctx.library);
		c.call("УстановитьКаталогЛогов", {logDirFor(ctx, "AC-16")});
		// Shared settings: both are regular-consumer-only and must be skipped for the share consumer
		c.call("УстановитьПараметр", {std::string("group.id"), regularGroup});
		c.call("УстановитьПараметр", {std::string("auto.offset.reset"), std::string("earliest")});
		c.call("УстановитьПараметр", {std::string("enable.auto.commit"), std::string("false")});

		ensure(c, c.callBool("ИнициализироватьКонсьюмера", {ctx.brokers}), "ИнициализироватьКонсьюмера");
		ensure(c, c.callBool("Подписаться", {topic}), "Подписаться");
		ensure(c, c.callBool("ИнициализироватьКонсьюмераОчереди", {ctx.brokers, shareGroup}), "share consumer с общими настройками обычного → Истина");
		ensure(c, c.callBool("ПодписатьсяНаОчередь", {topic}), "ПодписатьсяНаОчередь");

		check(c.callBool("ОстановитьКонсьюмераОчереди"), "остановка share consumer → Истина");
		const auto got = consumeRegular(c, 5, 20000);
		check(got.size() == 5, "обычный consumer после остановки share прочитал 5 (получено " + std::to_string(got.size()) + ")");

		ensure(c, c.callBool("ИнициализироватьКонсьюмераОчереди", {ctx.brokers, shareGroup}), "share снова");
		ensure(c, c.callBool("ПодписатьсяНаОчередь", {topic}), "подписка share снова");
		check(c.callBool("ОстановитьКонсьюмера"), "остановка обычного consumer → Истина");
		check(c.callBool("ОтписатьсяОтОчереди"), "share consumer жив после остановки обычного");
		check(c.callBool("ОстановитьКонсьюмераОчереди"), "остановка share consumer → Истина");

		const std::string logs = readLogs(logDirFor(ctx, "AC-16"));
		check(contains(logs, "[share] ignored property auto.offset.reset"), "в логе: ignored property auto.offset.reset");
		check(contains(logs, "[share] ignored property enable.auto.commit"), "в логе: ignored property enable.auto.commit");
	}

	void ac17(Context& ctx)
	{
		const std::string dir = logDirFor(ctx, "AC-17");
		AddIn c(*ctx.library);
		c.call("УстановитьКаталогЛогов", {dir});
		ensure(c, c.callBool("ИнициализироватьКонсьюмераОчереди", {ctx.brokers, uniqueName(ctx, "sg-h")}), "инициализация");
		c.callBool("ОстановитьКонсьюмераОчереди");

		const std::string logs = readLogs(dir);
		const std::string expected = "librdkafka version: " + ctx.expectRdkafka + ")";
		check(contains(logs, expected), "в логе консьюмера: " + expected);
	}

	void ac18(Context& ctx)
	{
		const std::string topic = uniqueName(ctx, "t-h");
		const std::string group = uniqueName(ctx, "cg-h");
		createTopic(ctx, topic, 1);
		produceMessages(ctx, topic, 10, "r");

		AddIn c(*ctx.library);
		c.call("УстановитьПараметр", {std::string("group.id"), group});
		c.call("УстановитьПараметр", {std::string("auto.offset.reset"), std::string("earliest")});
		c.call("УстановитьПараметр", {std::string("enable.auto.commit"), std::string("false")});
		ensure(c, c.callBool("ИнициализироватьКонсьюмера", {ctx.brokers}), "ИнициализироватьКонсьюмера");
		ensure(c, c.callBool("Подписаться", {topic}), "Подписаться");

		const auto got = consumeRegular(c, 10, 20000);
		require(got.size() == 10, "ЧитатьПакетСообщений вернул 10 (получено " + std::to_string(got.size()) + ")");

		std::set<std::string> payloads;
		bool fieldsOk = true;
		for (const auto& m : got)
		{
			for (const char* field : {"message", "key", "topic", "partition", "offset", "broker_id", "timestamp"})
			{
				fieldsOk = fieldsOk && m.contains(field);
			}
			payloads.insert(std::string(m.at("message").as_string()));
		}
		check(fieldsOk, "поля message, key, topic, partition, offset, broker_id, timestamp на месте");
		check(payloads.size() == 10 && payloads.count("r-0") && payloads.count("r-9"), "тела r-0…r-9");

		const int64_t last = got.back().at("offset").to_number<int64_t>();
		expect(c, c.callBool("ЗафиксироватьСмещение", {topic, static_cast<int32_t>(last + 1), int32_t{0}}), "ЗафиксироватьСмещение → Истина");
		check(c.callBool("ОстановитьКонсьюмера"), "ОстановитьКонсьюмера → Истина");

		// The same group resumes after the committed offset
		produceMessages(ctx, topic, 1, "after");
		AddIn c2(*ctx.library);
		c2.call("УстановитьПараметр", {std::string("group.id"), group});
		c2.call("УстановитьПараметр", {std::string("auto.offset.reset"), std::string("earliest")});
		c2.call("УстановитьПараметр", {std::string("enable.auto.commit"), std::string("false")});
		ensure(c2, c2.callBool("ИнициализироватьКонсьюмера", {ctx.brokers}), "повторная ИнициализироватьКонсьюмера");
		ensure(c2, c2.callBool("Подписаться", {topic}), "повторная Подписаться");
		const auto rest = consumeRegular(c2, 1, 20000);
		std::string restText;
		for (const auto& m : rest)
		{
			restText += " " + std::string(m.at("message").as_string()) + "@" + std::to_string(m.at("offset").to_number<int64_t>());
		}
		check(rest.size() == 1 && rest[0].at("message").as_string() == "after-0",
			"после фиксации группа читает только новое сообщение (прочитано:" + restText + ")");
		c2.callBool("ОстановитьКонсьюмера");
	}

	struct Scenario
	{
		const char* id;
		std::function<void(Context&)> run;
	};
}

int main(int argc, char** argv)
{
	if (argc < 2)
	{
		std::printf("usage: native_harness <SimpleKafka1C.dll> [brokers] [--only AC-1,AC-2] [--expect-rdkafka 2.16.0]\n");
		return 2;
	}

	Context ctx;
	ctx.dllPath = argv[1];
	std::set<std::string> only;
	for (int i = 2; i < argc; ++i)
	{
		const std::string arg = argv[i];
		if (arg == "--only" && i + 1 < argc)
		{
			std::stringstream ss(argv[++i]);
			std::string id;
			while (std::getline(ss, id, ','))
			{
				only.insert(id);
			}
		}
		else if (arg == "--no-share-brokers" && i + 1 < argc)
		{
			ctx.noShareBrokers = argv[++i];
		}
		else if (arg == "--expect-rdkafka" && i + 1 < argc)
		{
			ctx.expectRdkafka = argv[++i];
		}
		else
		{
			ctx.brokers = arg;
		}
	}

	ctx.runId = std::to_string(std::chrono::duration_cast<std::chrono::seconds>(
		std::chrono::system_clock::now().time_since_epoch()).count());
	ctx.logRoot = fs::temp_directory_path() / ("native-harness-" + ctx.runId);

	try
	{
		ctx.library = std::make_unique<Library>(ctx.dllPath);
	}
	catch (const std::exception& e)
	{
		std::printf("%s\n", e.what());
		return 2;
	}

	std::printf("dll: %s\nbrokers: %s\nlogs: %s\n\n", ctx.dllPath.c_str(), ctx.brokers.c_str(), ctx.logRoot.string().c_str());

	const std::vector<Scenario> scenarios = {
		{"AC-1", ac1},
		{"AC-2", ac2},
		{"AC-3", ac3},
		{"AC-8", ac8},
		{"AC-8a", ac8a},
		{"AC-4", ac4},
		{"AC-5", ac5},
		{"AC-6", ac6},
		{"AC-7", ac7},
		{"AC-9", ac9},
		{"AC-10-12", ac10to12},
		{"AC-13", ac13},
		{"AC-14", ac14},
		{"AC-15", ac15},
		{"AC-16", ac16},
		{"AC-17", ac17},
		{"AC-18", ac18},
	};

	int failedScenarios = 0;
	int skipped = 0;
	for (const auto& s : scenarios)
	{
		if (!only.empty() && only.count(s.id) == 0)
		{
			continue;
		}
		std::printf("%s\n", s.id);
		const int before = g_failures;
		try
		{
			s.run(ctx);
		}
		catch (const ScenarioFailed&)
		{
		}
		catch (const ScenarioSkipped& sk)
		{
			std::printf("  SKIP %s\n", sk.reason.c_str());
			++skipped;
			continue;
		}
		catch (const std::exception& e)
		{
			check(false, std::string("exception: ") + e.what());
		}
		if (g_failures != before)
		{
			++failedScenarios;
		}
	}

	std::printf("\n%d checks, %d failed, %d scenario(s) failed, %d skipped\n", g_checks, g_failures, failedScenarios, skipped);
	return g_failures == 0 ? 0 : 1;
}
