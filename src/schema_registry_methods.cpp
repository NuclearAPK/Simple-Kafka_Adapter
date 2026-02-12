// Schema Registry API methods for SimpleKafka1C

#include <boost/json.hpp>
#include <curl/curl.h>
#include <string>

#include "SimpleKafka1C.h"
#include "utils.h"

//================================== Schema Registry API ==========================================

// Callback для записи данных от CURL
static size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* userp)
{
	size_t totalSize = size * nmemb;
	userp->append(static_cast<char*>(contents), totalSize);
	return totalSize;
}

std::string SimpleKafka1C::httpRequest(const std::string& url, const std::string& method,
                                        const std::string& body, const std::string& contentType)
{
	std::string response;

	if (!curlHandle)
	{
		curlHandle = curl_easy_init();
	}

	if (!curlHandle)
	{
		msg_err = u8"Ошибка инициализации CURL";
		return "";
	}

	curl_easy_reset(curlHandle);

	curl_easy_setopt(curlHandle, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curlHandle, CURLOPT_WRITEFUNCTION, WriteCallback);
	curl_easy_setopt(curlHandle, CURLOPT_WRITEDATA, &response);
	curl_easy_setopt(curlHandle, CURLOPT_TIMEOUT, 30L);

	struct curl_slist* headers = nullptr;
	headers = curl_slist_append(headers, ("Content-Type: " + contentType).c_str());
	headers = curl_slist_append(headers, "Accept: application/vnd.schemaregistry.v1+json");
	curl_easy_setopt(curlHandle, CURLOPT_HTTPHEADER, headers);

	if (method == "POST")
	{
		curl_easy_setopt(curlHandle, CURLOPT_POST, 1L);
		curl_easy_setopt(curlHandle, CURLOPT_POSTFIELDS, body.c_str());
		curl_easy_setopt(curlHandle, CURLOPT_POSTFIELDSIZE, body.size());
	}
	else if (method == "DELETE")
	{
		curl_easy_setopt(curlHandle, CURLOPT_CUSTOMREQUEST, "DELETE");
	}
	else if (method == "PUT")
	{
		curl_easy_setopt(curlHandle, CURLOPT_CUSTOMREQUEST, "PUT");
		curl_easy_setopt(curlHandle, CURLOPT_POSTFIELDS, body.c_str());
		curl_easy_setopt(curlHandle, CURLOPT_POSTFIELDSIZE, body.size());
	}

	CURLcode res = curl_easy_perform(curlHandle);

	curl_slist_free_all(headers);

	if (res != CURLE_OK)
	{
		msg_err = std::string(u8"CURL ошибка: ") + curl_easy_strerror(res);
		return "";
	}

	long httpCode = 0;
	curl_easy_getinfo(curlHandle, CURLINFO_RESPONSE_CODE, &httpCode);

	if (httpCode >= 400)
	{
		msg_err = u8"HTTP ошибка " + std::to_string(httpCode) + ": " + response;
		return "";
	}

	return response;
}

std::string SimpleKafka1C::registerSchema(const variant_t& registryUrl, const variant_t& subject, const variant_t& schema)
{
	std::string url = std::get<std::string>(registryUrl);
	std::string subj = std::get<std::string>(subject);
	std::string schemaStr = std::get<std::string>(schema);

	// Валидация URL
	//if (!isValidUrl(url))
	//{
	//	msg_err = u8"Некорректный URL Schema Registry: " + url;
	//	return "";
	//}

	// Валидация JSON схемы
	//if (!isValidJson(schemaStr, msg_err))
	//{
	//	return "";
	//}

	// Убираем trailing slash
	if (!url.empty() && url.back() == '/')
		url.pop_back();

	std::string fullUrl = url + "/subjects/" + subj + "/versions";

	// Формируем JSON тело запроса
	boost::json::object requestBody;
	requestBody["schema"] = schemaStr;

	std::string body = boost::json::serialize(requestBody);

	std::string response = httpRequest(fullUrl, "POST", body, "application/vnd.schemaregistry.v1+json");

	return response;
}

std::string SimpleKafka1C::getSchemaById(const variant_t& registryUrl, const variant_t& schemaId)
{
	std::string url = std::get<std::string>(registryUrl);
	int32_t id = std::get<int32_t>(schemaId);

	// Валидация URL
	//if (!isValidUrl(url))
	//{
	//	msg_err = u8"Некорректный URL Schema Registry: " + url;
	//	return "";
	//}

	// Убираем trailing slash
	if (!url.empty() && url.back() == '/')
		url.pop_back();

	std::string fullUrl = url + "/schemas/ids/" + std::to_string(id);

	return httpRequest(fullUrl, "GET");
}

std::string SimpleKafka1C::getLatestSchema(const variant_t& registryUrl, const variant_t& subject)
{
	std::string url = std::get<std::string>(registryUrl);
	std::string subj = std::get<std::string>(subject);

	// Валидация URL
	//if (!isValidUrl(url))
	//{
	//	msg_err = u8"Некорректный URL Schema Registry: " + url;
	//	return "";
	//}

	// Убираем trailing slash
	if (!url.empty() && url.back() == '/')
		url.pop_back();

	std::string fullUrl = url + "/subjects/" + subj + "/versions/latest";

	return httpRequest(fullUrl, "GET");
}

std::string SimpleKafka1C::getSchemaVersions(const variant_t& registryUrl, const variant_t& subject)
{
	std::string url = std::get<std::string>(registryUrl);
	std::string subj = std::get<std::string>(subject);

	// Валидация URL
	//if (!isValidUrl(url))
	//{
	//	msg_err = u8"Некорректный URL Schema Registry: " + url;
	//	return "";
	//}

	// Убираем trailing slash
	if (!url.empty() && url.back() == '/')
		url.pop_back();

	std::string fullUrl = url + "/subjects/" + subj + "/versions";

	return httpRequest(fullUrl, "GET");
}

bool SimpleKafka1C::deleteSchema(const variant_t& registryUrl, const variant_t& subject, const variant_t& version)
{
	std::string url = std::get<std::string>(registryUrl);
	std::string subj = std::get<std::string>(subject);
	std::string ver = std::get<std::string>(version);

	// Валидация URL
	//if (!isValidUrl(url))
	//{
	//	msg_err = u8"Некорректный URL Schema Registry: " + url;
	//	return false;
	//}

	// Убираем trailing slash
	if (!url.empty() && url.back() == '/')
		url.pop_back();

	std::string fullUrl = url + "/subjects/" + subj + "/versions/" + ver;

	std::string response = httpRequest(fullUrl, "DELETE");

	return !response.empty();
}
