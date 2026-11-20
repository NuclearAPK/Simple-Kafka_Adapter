/*
 *  Unit test of src/connect_failure.* (specs/ssl-fail-fast.md, R1-R6). No broker needed.
 *  Usage: connect_failure_test
 */

#include "connect_failure.h"

#include <librdkafka/rdkafka.h>

#include <chrono>
#include <cstdio>
#include <string>
#include <thread>

using namespace connect_failure;

namespace
{
	int g_checks = 0;
	int g_failures = 0;

	void check(bool ok, const std::string& what)
	{
		++g_checks;
		if (!ok) ++g_failures;
		std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what.c_str());
	}

	bool has(const std::string& text, const std::string& part) { return text.find(part) != std::string::npos; }

	// Text librdkafka 2.16.0 produced on the kafka.internal stand (broker cert without SAN)
	const std::string kSslText =
		"ssl://kafka.internal:9093/bootstrap: SSL handshake failed: error:0A000086:SSL routines::"
		"certificate verify failed: broker certificate could not be verified, verify that "
		"ssl.ca.location is correctly configured or root CA certificates are installed";

	void classification()
	{
		std::printf("R1 classification\n");
		check(classifyKafkaError(RD_KAFKA_RESP_ERR__SSL, false) == FailureClass::Fatal, "__SSL is fatal");
		check(classifyKafkaError(RD_KAFKA_RESP_ERR__AUTHENTICATION, false) == FailureClass::Fatal, "__AUTHENTICATION is fatal");
		check(classifyKafkaError(RD_KAFKA_RESP_ERR_SASL_AUTHENTICATION_FAILED, false) == FailureClass::Fatal, "SASL_AUTHENTICATION_FAILED is fatal");
		check(classifyKafkaError(RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED, false) == FailureClass::Fatal, "TOPIC_AUTHORIZATION_FAILED is fatal");
		check(classifyKafkaError(RD_KAFKA_RESP_ERR__TRANSPORT, false) == FailureClass::Transient, "__TRANSPORT is transient");
		check(classifyKafkaError(RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN, false) == FailureClass::Transient, "__ALL_BROKERS_DOWN is transient");
		check(classifyKafkaError(RD_KAFKA_RESP_ERR__TRANSPORT, true) == FailureClass::Fatal, "fatal flag wins");
		check(classifyKafkaError(RD_KAFKA_RESP_ERR_NO_ERROR, false) == FailureClass::None, "NO_ERROR is none");
	}

	void hints()
	{
		std::printf("R4 hints\n");
		HintContext ctx{"kafka.internal:9093", "", "SSL"};
		const std::string h = connectHint(kSslText, ctx);
		check(has(h, "kafka.internal") && has(h, "CN/SAN") && has(h, "ssl.endpoint.identification.algorithm=none"),
			"verify failed + default -> host name hint");

		ctx.endpointIdentification = " NONE ";
		const std::string h2 = connectHint(kSslText, ctx);
		check(has(h2, "ssl.ca.location") && !has(h2, "CN/SAN"), "verify failed + none -> CA hint only");

		check(has(connectHint("SASL authentication error: Authentication failed", ctx), "sasl.password"), "SASL hint");
		check(has(connectHint("ssl.key.location failed: private key", ctx), "ssl.key.password"), "client key hint");
		check(connectHint("Connection refused", ctx).empty(), "no hint for plain transport error");
		check(has(connectHint("SSL: no alternative certificate subject name matches target host name 'x'", HintContext{"x:443", "", ""}), "CN/SAN"),
			"curl host name text -> host name hint");

		check(firstBrokerHost("ssl://a.b:9093,c:9093") == "a.b", "firstBrokerHost strips scheme and port");
		check(firstBrokerHost("[::1]:9092") == "::1", "firstBrokerHost IPv6");
		check(firstBrokerHost("https://sr.local/api") == "sr.local", "firstBrokerHost URL without port");

		HintContext http{"https://sr.local:8081", "", "", true};
		const std::string hh = connectHint("SSL peer certificate or SSH remote key was not OK", http);
		check(has(hh, "sr.local") && has(hh, "CN/SAN") && !has(hh, "ssl.endpoint"), "R10 curl hint without librdkafka settings");

		HintContext ip{"https://127.0.0.1:8443", "", "", true};
		const std::string hip = connectHint("CURL error: SSL peer certificate or SSH remote key was not OK "
			"(schannel: SEC_E_UNTRUSTED_ROOT (0x80090325) - The certificate chain was issued by an authority that is not trusted.)", ip);
		check(has(hip, "127.0.0.1"), "R10 curl schannel text, IP URL");

		// "Цепь" in CP1251: system code page text must become valid UTF-8
		const std::string cp1251 = "schannel: \xD6\xE5\xEF\xFC";
		const std::string utf8 = ensureUtf8(cp1251);
		check(utf8 != cp1251 && utf8.find('\xD6') == std::string::npos, "ensureUtf8 converts CP1251 text");
		check(ensureUtf8(u8"уже UTF-8") == u8"уже UTF-8", "ensureUtf8 keeps valid UTF-8");
		ConnectFailureState cs;
		cs.record(RD_KAFKA_RESP_ERR__TRANSPORT, cp1251, false);
		check(cs.lastText() == utf8, "record stores UTF-8");
	}

	void state()
	{
		std::printf("R2/R3/R5 state and text\n");
		HintContext ctx{"kafka.internal:9093", "", "SSL"};
		ConnectFailureState s;
		s.record(RD_KAFKA_RESP_ERR__TRANSPORT, "Connection refused", false);
		check(!s.hasFatal(), "transient error is not fatal");
		const std::string t = formatConnectError("GetTopics", "Local: Broker transport failure", s, ctx);
		check(t == "GetTopics: Local: Broker transport failure | Connection refused", "R5 timeout text: " + t);

		s.record(RD_KAFKA_RESP_ERR__SSL, kSslText, false);
		s.record(RD_KAFKA_RESP_ERR__SSL, "second", false);
		check(s.hasFatal() && s.fatalText() == kSslText, "first fatal text is kept");
		const std::string f = formatConnectError("GetTopics", "ignored", s, ctx);
		check(has(f, "GetTopics: ssl://kafka.internal") && has(f, " | ") && has(f, "CN/SAN") && !has(f, "ignored"),
			"R3 fatal text with hint");

		s.reset();
		check(!s.hasFatal() && s.lastText().empty(), "reset clears state");
	}

	void sliced()
	{
		std::printf("R6 sliced wait\n");
		using clock = std::chrono::steady_clock;
		ConnectFailureState s;

		int pumps = 0;
		auto t0 = clock::now();
		WaitResult r = waitSliced(10000, s, [](int32_t) { return false; },
			[&] { if (++pumps == 2) s.record(RD_KAFKA_RESP_ERR__SSL, "x", false); });
		auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - t0).count();
		check(r == WaitResult::Fatal && pumps == 2, "stops on fatal after pump");
		check(ms < 100, "no sleeping by itself (" + std::to_string(ms) + " ms)");

		s.reset();
		int32_t total = 0;
		r = waitSliced(1200, s, [&](int32_t slice) {
			total += slice;
			std::this_thread::sleep_for(std::chrono::milliseconds(slice));
			return false; }, [] {});
		check(r == WaitResult::Timeout, "timeout without errors");
		check(total >= 1100 && total <= 1300, "slices sum to timeout (" + std::to_string(total) + ")");

		r = waitSliced(1000, s, [](int32_t) { return true; }, [] {});
		check(r == WaitResult::Done, "done on success");

		s.record(RD_KAFKA_RESP_ERR__SSL, "x", false);
		bool called = false;
		r = waitSliced(1000, s, [&](int32_t) { called = true; return true; }, [] {});
		check(r == WaitResult::Fatal && !called, "R8 sticky fatal: step not called");
	}
}

int main()
{
	classification();
	hints();
	state();
	sliced();
	std::printf("\n%d checks, %d failures\n", g_checks, g_failures);
	return g_failures == 0 ? 0 : 1;
}
