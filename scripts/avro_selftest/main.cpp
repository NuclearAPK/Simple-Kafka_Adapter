// Standalone reproducer / regression test for the avro-cpp recursive-schema
// decode bug (see CHANGELOG: avro-cpp < 1.12.1 crashes on deeply nested
// recursive schemas with `vector::_M_range_check ... >= size`).
//
// It mirrors EXACTLY the decode path of SimpleKafka1C::decodeAvroMessage:
//   compileJsonSchema -> strip Confluent wire header -> binaryDecoder ->
//   GenericReader -> read(GenericDatum).
//
// Usage: avro_selftest <schema.json> <message1.bin> [message2.bin ...]
// Exit code: 0 if every message decodes, 1 if any decode fails.

#include <avro/Compiler.hh>
#include <avro/Decoder.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>
#include <avro/Stream.hh>
#include <avro/ValidSchema.hh>

#include <cstdint>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

// Faithful copy of the traversal in SimpleKafka1C::convertAvroDatumToJsonString.
// The live component calls decode AND this convert inside the same try{}, so a
// crash here surfaces as the same "Error decoding AVRO" message. The only size-2
// vector access is GenericEnum::symbol() (enum stateName has 2 symbols).
static std::string convertAvroDatumToJsonString(const avro::GenericDatum &datum)
{
    std::ostringstream oss;
    switch (datum.type())
    {
    case avro::AVRO_NULL: oss << "null"; break;
    case avro::AVRO_BOOL: oss << (datum.value<bool>() ? "true" : "false"); break;
    case avro::AVRO_INT: oss << datum.value<int32_t>(); break;
    case avro::AVRO_LONG: oss << datum.value<int64_t>(); break;
    case avro::AVRO_FLOAT: oss << datum.value<float>(); break;
    case avro::AVRO_DOUBLE: oss << datum.value<double>(); break;
    case avro::AVRO_STRING: oss << "\"" << datum.value<std::string>() << "\""; break;
    case avro::AVRO_BYTES: oss << "\"bytes:" << datum.value<std::vector<uint8_t>>().size() << "\""; break;
    case avro::AVRO_FIXED: oss << "\"fixed:" << datum.value<avro::GenericFixed>().value().size() << "\""; break;
    case avro::AVRO_RECORD:
    {
        const auto &r = datum.value<avro::GenericRecord>();
        oss << "{";
        for (size_t i = 0; i < r.fieldCount(); ++i)
        {
            if (i) oss << ",";
            oss << "\"" << r.schema()->nameAt(i) << "\":" << convertAvroDatumToJsonString(r.fieldAt(i));
        }
        oss << "}";
        break;
    }
    case avro::AVRO_ENUM:
        oss << "\"" << datum.value<avro::GenericEnum>().symbol() << "\"";  // <-- size-2 suspect
        break;
    case avro::AVRO_ARRAY:
    {
        const auto &a = datum.value<avro::GenericArray>();
        oss << "[";
        bool first = true;
        for (const auto &it : a.value()) { if (!first) oss << ","; first = false; oss << convertAvroDatumToJsonString(it); }
        oss << "]";
        break;
    }
    case avro::AVRO_MAP:
    {
        const auto &m = datum.value<avro::GenericMap>();
        oss << "{";
        bool first = true;
        for (const auto &kv : m.value()) { if (!first) oss << ","; first = false; oss << "\"" << kv.first << "\":" << convertAvroDatumToJsonString(kv.second); }
        oss << "}";
        break;
    }
    case avro::AVRO_UNION:
        oss << convertAvroDatumToJsonString(datum.value<avro::GenericUnion>().datum());
        break;
    default: oss << "null"; break;
    }
    return oss.str();
}

static std::vector<char> readFile(const char *path)
{
    std::ifstream f(path, std::ios::binary);
    if (!f)
    {
        throw std::runtime_error(std::string("cannot open file: ") + path);
    }
    return std::vector<char>((std::istreambuf_iterator<char>(f)),
                             std::istreambuf_iterator<char>());
}

static bool decodeOne(const avro::ValidSchema &schema, const char *msgPath)
{
    try
    {
        // The file is fed to the decoder as is. Base64 input is deliberately NOT
        // detected here: the component requires an explicit IsBase64 flag because
        // a raw Avro payload can consist entirely of base64-alphabet bytes, so
        // guessing would silently decode a valid message into garbage — and this
        // tool exists to localise decode bugs, not to introduce its own. Decode
        // base64 outside the tool if the message arrives in that form.
        std::vector<char> data = readFile(msgPath);

        size_t off = 0;
        if (!data.empty() && static_cast<uint8_t>(data[0]) == 0x00)
        {
            if (data.size() < 5)
            {
                std::cerr << "FAILED " << msgPath
                          << " : Confluent header shorter than 5 bytes\n";
                return false;
            }
            off = 5; // magic byte + 4-byte schema id
        }

        std::unique_ptr<avro::InputStream> in = avro::memoryInputStream(
            reinterpret_cast<const uint8_t *>(data.data() + off),
            data.size() - off);

        avro::DecoderPtr decoder = avro::binaryDecoder();
        decoder->init(*in);

        avro::GenericReader reader(schema, decoder);
        avro::GenericDatum datum(schema);
        reader.read(datum);

        // The live component also converts the datum to JSON in the same try{}.
        std::string json = convertAvroDatumToJsonString(datum);

        std::cout << "OK     " << msgPath << " (payload "
                  << (data.size() - off) << " bytes, json " << json.size() << " chars)\n";
        return true;
    }
    catch (const std::exception &e)
    {
        std::cerr << "FAILED " << msgPath << " : " << e.what() << "\n";
        return false;
    }
}

int main(int argc, char **argv)
{
    if (argc < 3)
    {
        std::cerr << "usage: avro_selftest <schema.json> <message.bin> "
                     "[message.bin ...]\n";
        return 2;
    }

    avro::ValidSchema schema;
    try
    {
        std::vector<char> sb = readFile(argv[1]);
        std::string schemaStr(sb.begin(), sb.end());
        // Use the SAME avro entry point as the component's putAvroSchema().
        schema = avro::compileJsonSchemaFromString(schemaStr);
    }
    catch (const std::exception &e)
    {
        std::cerr << "schema compile failed: " << e.what() << "\n";
        return 2;
    }

    int failures = 0;
    for (int i = 2; i < argc; ++i)
    {
        if (!decodeOne(schema, argv[i]))
        {
            ++failures;
        }
    }

    std::cout << "\n" << (argc - 2 - failures) << "/" << (argc - 2)
              << " messages decoded successfully\n";
    return failures == 0 ? 0 : 1;
}
