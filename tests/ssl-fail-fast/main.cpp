/*
 *  Acceptance test of specs/ssl-fail-fast.md against a broker whose certificate does not
 *  match its host name (e.g. CN=kafka served as kafka.internal, no SAN).
 *
 *  Usage: ssl_fail_fast <SimpleKafka1C.dll> <brokers host:port> <cert dir> [topic] [wrong CA]
 *                       [--sasl host:port] [--closed host:port] [--share]
 *    cert dir holds ca-cert.txt, client.crt, client.key
 *    --sasl    SASL_PLAINTEXT listener with PLAIN user test (AC-8, a wrong password is sent)
 *    --closed  address nobody listens on (AC-12, transient errors keep waiting)
 *    --share   share consumer scenario (AC-7, needs a broker with share groups)
 *
 *  Every scenario is run twice: with default settings (must fail fast, <= 1000 ms, with the
 *  CN/SAN hint) and with ssl.endpoint.identification.algorithm=none (must succeed).
 */

#include "addin_host.h"

#include <chrono>
#include <cstdio>
#include <functional>
#include <string>
#include <thread>
#include <vector>

namespace
{
	int g_checks = 0;
	int g_failures = 0;
	constexpr long long kFailFastMs = 1000;
	constexpr int32_t kPersisted = 2;  // RdKafka::Message::MSG_STATUS_PERSISTED

	std::string g_brokers;
	std::string g_dir;
	std::string g_topic = "Test";

	void check(bool ok, const std::string& what, const std::string& detail = "")
	{
		++g_checks;
		if (!ok) ++g_failures;
		std::printf("  %s %s%s%s\n", ok ? "ok  " : "FAIL", what.c_str(), detail.empty() ? "" : " | ", detail.substr(0, 220).c_str());
		std::fflush(stdout);
	}

	bool has(const std::string& text, const std::string& part) { return text.find(part) != std::string::npos; }

	void configure(AddIn& c, bool noHostCheck, const std::string& caFile = "ca-cert.txt")
	{
		auto set = [&](const char* k, const std::string& v) { c.call("УстановитьПараметр", {std::string(k), v}); };
		set("security.protocol", "SSL");
		set("ssl.ca.location", caFile.find(':') != std::string::npos ? caFile : g_dir + "\\" + caFile);
		set("ssl.certificate.location", g_dir + "\\client.crt");
		set("ssl.key.location", g_dir + "\\client.key");
		if (noHostCheck)
			set("ssl.endpoint.identification.algorithm", "none");
	}

	long long timed(const std::function<void()>& fn)
	{
		const auto t0 = std::chrono::steady_clock::now();
		fn();
		return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
	}

	// Default settings: the call must fail fast with the host name hint
	void expectFailFast(const std::string& name, AddIn& c, long long ms, bool failed)
	{
		const std::string err = c.lastError();
		check(failed, name + ": error value returned");
		check(ms <= kFailFastMs, name + ": " + std::to_string(ms) + " ms <= 1000");
		check(has(err, "SSL handshake failed") && has(err, "CN/SAN"), name + ": text with SSL reason and CN/SAN hint", err);
	}

	void expectSuccess(const std::string& name, AddIn& c, long long ms, bool ok)
	{
		check(ok, name + ": succeeds with endpoint identification off (" + std::to_string(ms) + " ms)", c.lastError());
	}

	// AC-1, AC-2, AC-11: service clients
	void serviceClients(Library& lib)
	{
		std::printf("AC-1/AC-2 service clients, default settings\n");
		struct Case { const char* name; std::function<bool(AddIn&)> call; };
		const Case cases[] = {
			{"GetTopics", [](AddIn& c) { return !c.callString("ПолучитьСписокТопиков", {g_brokers, int32_t{10000}}).empty(); }},
			{"PingBroker", [](AddIn& c) { return c.callBool("ПроверитьДоступностьБрокера", {g_brokers, int32_t{10000}}); }},
			{"GetClusterInfo", [](AddIn& c) { return !c.callString("ПолучитьИнформациюОКластере", {g_brokers}).empty(); }},
			{"GetTopicMetadata", [](AddIn& c) { return !c.callString("ПолучитьМетаданныеТопика", {g_brokers, g_topic, int32_t{10000}}).empty(); }},
			{"GetConsumerGroupList", [](AddIn& c) { return !c.callString("ПолучитьСписокГруппКонсьюмеров", {g_brokers, int32_t{10000}}).empty(); }},
			{"ReadMessageByOffset", [](AddIn& c) { return !c.callString("ПрочитатьСообщениеПоСмещению", {g_brokers, g_topic, int32_t{0}, int32_t{0}, int32_t{10000}}).empty(); }},
		};
		for (const auto& tc : cases)
		{
			AddIn c(lib);
			configure(c, false);
			bool ok = true;
			const long long ms = timed([&] { ok = tc.call(c); });
			expectFailFast(tc.name, c, ms, !ok);
		}

		std::printf("AC-11 service clients, ssl.endpoint.identification.algorithm=none\n");
		for (const auto& tc : cases)
		{
			if (std::string(tc.name) == "ReadMessageByOffset")
				continue;  // the topic may be empty: not an SSL property
			AddIn c(lib);
			configure(c, true);
			bool ok = false;
			const long long ms = timed([&] { ok = tc.call(c); });
			expectSuccess(tc.name, c, ms, ok);
		}
	}

	// AC-3: producer, sticky error
	void producer(Library& lib)
	{
		std::printf("AC-3 producer\n");
		{
			AddIn c(lib);
			configure(c, false);
			check(c.callBool("ИнициализироватьПродюсера", {g_brokers}), "producer created");
			int32_t r = 0;
			long long ms = timed([&] {
				r = std::get<int32_t>(c.call("ОтправитьСообщениеСОжиданиемРезультата",
					{std::string("ssl-fail-fast"), g_topic, int32_t{-1}, std::string(""), std::string("")}));
			});
			expectFailFast("ProduceWithWaitResult", c, ms, r != kPersisted);
			ms = timed([&] {
				r = std::get<int32_t>(c.call("ОтправитьСообщение",
					{std::string("ssl-fail-fast"), g_topic, int32_t{-1}, std::string(""), std::string("")}));
			});
			check(r == -1 && ms < 100, "R8 sticky: next Produce fails at once (" + std::to_string(ms) + " ms)", c.lastError());
			ms = timed([&] { c.callBool("ОстановитьПродюсера"); });
			check(ms <= kFailFastMs, "StopProducer: " + std::to_string(ms) + " ms <= 1000");
		}
		{
			AddIn c(lib);
			configure(c, true);
			check(c.callBool("ИнициализироватьПродюсера", {g_brokers}), "producer created (no host check)");
			int32_t r = 0;
			const long long ms = timed([&] {
				r = std::get<int32_t>(c.call("ОтправитьСообщениеСОжиданиемРезультата",
					{std::string("ssl-fail-fast"), g_topic, int32_t{-1}, std::string(""), std::string("")}));
			});
			expectSuccess("ProduceWithWaitResult", c, ms, r == kPersisted);
			c.callBool("ОстановитьПродюсера");
		}
	}

	// AC-4: transactional producer
	void transactions(Library& lib)
	{
		std::printf("AC-4 transactional producer\n");
		AddIn c(lib);
		configure(c, false);
		bool ok = true;
		const long long ms = timed([&] {
			ok = c.callBool("ИнициализироватьТранзакционногоПродюсера", {g_brokers, std::string("ssl-fail-fast-tx")});
		});
		expectFailFast("InitTransactionalProducer", c, ms, !ok);
	}

	// AC-5: consumer
	void consumer(Library& lib)
	{
		std::printf("AC-5 consumer\n");
		AddIn c(lib);
		configure(c, false);
		c.call("УстановитьПараметр", {std::string("group.id"), std::string("ssl-fail-fast")});
		check(c.callBool("ИнициализироватьКонсьюмера", {g_brokers}), "consumer created");
		check(c.callBool("Подписаться", {g_topic}), "subscribed");
		c.call("УстановитьТаймаутОжидания", {int32_t{10000}});
		std::string r;
		long long ms = timed([&] { r = c.callString("Слушать"); });
		expectFailFast("Consume", c, ms, r.empty());
		ms = timed([&] { r = c.callString("ЧитатьПакетСообщений", {int32_t{10}, int32_t{10000}, false}); });
		check(r.empty() && ms < 100, "R8 sticky: ConsumeBatch fails at once (" + std::to_string(ms) + " ms)", c.lastError());
		c.callBool("ОстановитьКонсьюмера");
	}

	// AC-6: push consumer sends exactly one Stopped event
	void pushConsumer(Library& lib)
	{
		std::printf("AC-6 push consumer\n");
		AddIn c(lib);
		configure(c, false);
		c.call("УстановитьПараметр", {std::string("group.id"), std::string("ssl-fail-fast-push")});
		check(c.callBool("ИнициализироватьКонсьюмера", {g_brokers}), "consumer created");
		check(c.callBool("Подписаться", {g_topic}), "subscribed");
		check(c.callBool("ЗапуститьФоновоеЧтение", {int32_t{10}, int32_t{1000}, false, int32_t{100}}), "push consumer started");
		std::this_thread::sleep_for(std::chrono::milliseconds(2500));
		auto events = c.platform.takeEvents();
		int stopped = 0;
		std::string data;
		for (const auto& e : events)
			if (e.message == "Stopped") { ++stopped; data = e.data; }
		check(stopped == 1, "exactly one Stopped event (" + std::to_string(events.size()) + " event(s) total)", data);
		check(has(data, "SSL handshake failed") && has(data, "CN/SAN"), "Stopped event carries the reason");
		c.callBool("ОстановитьКонсьюмера");
	}

	// AC-7: share consumer
	void shareConsumer(Library& lib)
	{
		std::printf("AC-7 share consumer\n");
		AddIn c(lib);
		configure(c, false);
		check(c.callBool("ИнициализироватьКонсьюмераОчереди", {g_brokers, std::string("ssl-fail-fast-share")}), "share consumer created");
		check(c.callBool("ПодписатьсяНаОчередь", {g_topic}), "subscribed");
		std::string r;
		long long ms = timed([&] { r = c.callString("ПрочитатьПакетИзОчереди", {int32_t{10000}, false, false}); });
		expectFailFast("ShareReadBatch", c, ms, r.empty());
		bool ok = true;
		ms = timed([&] { ok = c.callBool("ЗафиксироватьПодтвержденияОчереди", {int32_t{10000}}); });
		check(!ok && ms < 100, "R8 sticky: ShareCommit fails at once (" + std::to_string(ms) + " ms)", c.lastError());
	}

	// AC-8: wrong SASL password
	void saslWrongPassword(Library& lib, const std::string& brokers)
	{
		std::printf("AC-8 SASL PLAIN, wrong password\n");
		AddIn c(lib);
		auto set = [&](const char* k, const std::string& v) { c.call("УстановитьПараметр", {std::string(k), v}); };
		set("security.protocol", "SASL_PLAINTEXT");
		set("sasl.mechanism", "PLAIN");
		set("sasl.username", "test");
		set("sasl.password", "wrong");
		bool ok = true;
		const long long ms = timed([&] { ok = !c.callString("ПолучитьСписокТопиков", {brokers, int32_t{10000}}).empty(); });
		const std::string err = c.lastError();
		check(!ok && ms <= kFailFastMs, "GetTopics fails fast (" + std::to_string(ms) + " ms)");
		check(has(err, "sasl.password"), "SASL hint", err);
	}

	// AC-12: nobody listens: transient errors, the full timeout is kept
	void closedPort(Library& lib, const std::string& brokers)
	{
		std::printf("AC-12 PLAINTEXT, closed port\n");
		AddIn c(lib);
		bool ok = true;
		const long long ms = timed([&] { ok = !c.callString("ПолучитьСписокТопиков", {brokers, int32_t{3000}}).empty(); });
		const std::string err = c.lastError();
		check(!ok, "GetTopics fails");
		check(ms >= 2900 && ms <= 3600, "waits the whole timeout (" + std::to_string(ms) + " ms)");
		check(has(err, "Broker transport failure") && has(err, " | "), "timeout text has the last async error", err);
	}

	// AC-9: wrong CA with host check off -> CA hint, no CN/SAN hint
	void wrongCa(Library& lib, const std::string& wrongCaFile)
	{
		std::printf("AC-9 wrong CA, ssl.endpoint.identification.algorithm=none\n");
		AddIn c(lib);
		configure(c, true, wrongCaFile);
		bool ok = true;
		const long long ms = timed([&] { ok = !c.callString("ПолучитьСписокТопиков", {g_brokers, int32_t{10000}}).empty(); });
		const std::string err = c.lastError();
		check(!ok && ms <= kFailFastMs, "GetTopics fails fast (" + std::to_string(ms) + " ms)");
		check(has(err, "ssl.ca.location") && !has(err, "CN/SAN"), "CA hint only", err);
	}
}

int main(int argc, char** argv)
{
	if (argc < 4)
	{
		std::printf("Usage: ssl_fail_fast <SimpleKafka1C.dll> <brokers> <cert dir> [topic] [wrong CA]"
			" [--sasl host:port] [--closed host:port] [--share]\n");
		return 2;
	}
	Library lib(argv[1]);
	g_brokers = argv[2];
	g_dir = argv[3];

	std::vector<std::string> positional;
	std::string saslBrokers, closedBrokers;
	bool share = false;
	for (int i = 4; i < argc; ++i)
	{
		const std::string a = argv[i];
		if (a == "--sasl" && i + 1 < argc) saslBrokers = argv[++i];
		else if (a == "--closed" && i + 1 < argc) closedBrokers = argv[++i];
		else if (a == "--share") share = true;
		else positional.push_back(a);
	}
	if (!positional.empty()) g_topic = positional[0];

	serviceClients(lib);
	producer(lib);
	transactions(lib);
	consumer(lib);
	pushConsumer(lib);
	if (positional.size() > 1) wrongCa(lib, positional[1]);
	if (share) shareConsumer(lib);
	if (!saslBrokers.empty()) saslWrongPassword(lib, saslBrokers);
	if (!closedBrokers.empty()) closedPort(lib, closedBrokers);

	std::printf("\n%d checks, %d failures\n", g_checks, g_failures);
	return g_failures == 0 ? 0 : 1;
}
