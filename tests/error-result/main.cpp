/*
 *  Error result test: runs acceptance scenarios of specs/error-result-values.md against
 *  SimpleKafka1C.dll. A method gets an argument of a wrong type, std::get throws before
 *  the method's own try, and Component::CallAsFunc must return a value of the method's
 *  declared type. No broker is needed.
 *
 *  Usage: error_result <path to SimpleKafka1C.dll>
 */

#include "addin_host.h"

#include <cstdio>
#include <string>

namespace
{
	int g_checks = 0;
	int g_failures = 0;

	void check(bool ok, const std::string& what, const std::string& detail)
	{
		++g_checks;
		if (!ok) ++g_failures;
		std::printf("  %s %-44s (%s)\n", ok ? "ok  " : "FAIL", what.c_str(), detail.c_str());
	}

	const char* typeName(const Value& v)
	{
		switch (v.index())
		{
		case 0: return "Undefined";
		case 1: return "int32";
		case 2: return "int64";
		case 3: return "double";
		case 4: return "Boolean";
		case 5: return "String";
		}
		return "?";
	}

	std::string describe(const Value& v)
	{
		return std::string(typeName(v)) + " " + valueToString(v);
	}

	void errorText(AddIn& addin)
	{
		const std::string text = addin.lastError();
		check(!text.empty() && text.front() != ' ', "AC-6 error text has no leading space", "\"" + text + "\"");
	}

	void ac1(AddIn& addin)
	{
		std::printf("AC-1 boolean method\n");
		const Value v = addin.call("УстановитьТаймаутОжидания", {std::string("не число")});
		const auto* b = std::get_if<bool>(&v);
		check(b != nullptr && !*b, "returns False", describe(v));
		errorText(addin);
	}

	void ac2(AddIn& addin)
	{
		std::printf("AC-2 integer method\n");
		// Every int32_t method checks the producer first and returns -1 by itself without it.
		// librdkafka creates a producer without connecting, so an unreachable address is enough.
		addin.call("УстановитьТаймаутОчисткиПродюсера", {int32_t(100)});
		if (!addin.callBool("ИнициализироватьПродюсера", {std::string("127.0.0.1:1")}))
		{
			check(false, "producer initialized", addin.lastError());
			return;
		}
		const Value v = addin.call("ОтправитьПакетСообщений", {std::string("[]"), int32_t(123)});
		const auto* i = std::get_if<int32_t>(&v);
		check(i != nullptr && *i == -1, "returns -1", describe(v));
		errorText(addin);
		addin.call("ОстановитьПродюсера");
	}

	void ac3(AddIn& addin)
	{
		std::printf("AC-3 string method\n");
		const Value v = addin.call("ПолучитьСписокТопиков", {int32_t(123)});
		const auto* s = std::get_if<std::string>(&v);
		check(s != nullptr && s->empty(), "returns an empty string", describe(v));
		errorText(addin);
	}

	void ac4(AddIn& addin)
	{
		std::printf("AC-4 number method\n");
		const Value v = addin.call("ПолучитьКоличествоСообщенийВПартиции",
			{int32_t(123), std::string("topic"), int32_t(0)});
		const auto* d = std::get_if<double>(&v);
		check(d != nullptr && *d == -1.0, "returns -1", describe(v));
		errorText(addin);
	}

	void ac5(AddIn& addin)
	{
		std::printf("AC-5 method of any result type\n");
		const Value v = addin.call("ПолучитьДанныеСообщения", {std::string("не булево")});
		check(std::holds_alternative<std::monostate>(v), "returns Undefined", describe(v));
		errorText(addin);
	}

	void ac7(AddIn& addin)
	{
		std::printf("AC-7 successful call is unchanged\n");
		const Value v = addin.call("УстановитьТаймаутОжидания", {int32_t(1000)});
		const auto* b = std::get_if<bool>(&v);
		check(b != nullptr && *b, "returns True", describe(v));
		const std::string text = addin.lastError();
		check(text.empty(), "error text is empty", "\"" + text + "\"");
	}
}

int main(int argc, char** argv)
{
	std::setvbuf(stdout, nullptr, _IONBF, 0);

	if (argc < 2)
	{
		std::printf("usage: error_result <SimpleKafka1C.dll>\n");
		return 2;
	}

	try
	{
		Library library(argv[1]);
		AddIn addin(library);
		std::printf("dll: %s\n\n", argv[1]);

		ac1(addin);
		ac2(addin);
		ac3(addin);
		ac4(addin);
		ac5(addin);
		ac7(addin);
	}
	catch (const std::exception& e)
	{
		std::printf("aborted: %s\n", e.what());
		return 2;
	}

	std::printf("\n%d checks, %d failed\n", g_checks, g_failures);
	return g_failures == 0 ? 0 : 1;
}
