# Changelog

Все значимые изменения в этом проекте будут задокументированы в этом файле.

Формат основан на [Keep a Changelog](https://keepachangelog.com/ru/1.0.0/),
и этот проект придерживается [Semantic Versioning](https://semver.org/lang/ru/).

## [1.6.1] - 2026-01-05

### Добавлено
- Полная поддержка Protocol Buffers (Protobuf):
  - `СохранитьСхемуProtobuf` / `PutProtoSchema` - сохранение .proto схемы для последующего использования
  - `ПреобразоватьВФорматProtobuf` / `ConvertToProtobufFormat` - конвертация JSON данных в Protobuf формат
  - `СохранитьФайлProtobuf` / `SaveProtobufFile` - сохранение Protobuf данных в файл
  - `ДекодироватьСообщениеProtobuf` / `DecodeProtobufMessage` - декодирование Protobuf сообщений в JSON или бинарные данные
  - `ОтправитьСообщениеProtobuf` / `ProduceProtobuf` - асинхронная отправка Protobuf сообщений в Kafka
  - `ОтправитьСообщениеProtobufСОжиданиемРезультата` / `ProduceProtobufWithWaitResult` - синхронная отправка Protobuf сообщений
- Метод декодирования AVRO сообщений:
  - `ДекодироватьСообщениеAVRO` / `DecodeAvroMessage` - декодирование AVRO сообщений в JSON или бинарные данные
- Реализован метод получения параметров:
  - `ПолучитьПараметры` / `GetParameters` - получение всех установленных через `УстановитьПараметр` параметров в формате JSON
- Документация по статической линковке Protobuf в Visual Studio ([docs/visual_studio_build.md](./docs/visual_studio_build.md))
- Вспомогательные функции для работы с Protobuf:
  - `SetProtobufFieldFromJson` - конвертация JSON значений в поля Protobuf
  - `GetJsonFromProtobufField` - конвертация полей Protobuf в JSON значения
- Функция `convertAvroDatumToJson` для конвертации AVRO данных в JSON

### Исправлено
- **Критическое**: Исправлена кодировка русских имен методов - все методы теперь корректно отображаются в 1С (вместо `�������������������������` теперь `ПолучитьСообщениеОбОшибке`)
- Исправлен конфликт между Windows API макросом `GetMessage` и методом `google::protobuf::Reflection::GetMessage()` путем добавления `#undef GetMessage`
- Исправлены ошибки компиляции под Linux GCC:
  - Явное преобразование `std::string` в `boost::json::value` через `boost::json::string()`
  - Удалены неиспользуемые переменные (warning об unused variable `branch`)
- Улучшена совместимость сборки между Windows MSVC и Linux GCC компиляторами

### Изменено
- Обновлены заголовочные файлы:
  - Добавлены заголовки Protobuf: `google/protobuf/descriptor.h`, `google/protobuf/message.h`, `google/protobuf/dynamic_message.h`, `google/protobuf/compiler/parser.h`, `google/protobuf/io/tokenizer.h`, `google/protobuf/io/zero_copy_stream_impl.h`
- Обновлен `CMakeLists.txt` с настройками для статической линковки Protobuf в Visual Studio
- Улучшена документация по сборке проекта через Visual Studio IDE

### Техническая информация
- Добавлено 636+ строк кода для поддержки Protobuf
- Класс `ProtobufContext` для управления схемами и сообщениями Protobuf
- Поддержка динамического создания Protobuf сообщений на основе .proto схем
- Файл должен быть сохранён в кодировке UTF-8 with BOM для корректного отображения русских символов

## [1.6.0]
### Добавлено
- Admin API функции для управления топиками

## [1.5.3]
### Исправлено
- Различные исправления ошибок

---

[1.6.1]: https://github.com/NuclearAPK/Simple-Kafka_Adapter/compare/v1.6.0...v1.6.1
[1.6.0]: https://github.com/NuclearAPK/Simple-Kafka_Adapter/compare/v1.5.3...v1.6.0
[1.5.3]: https://github.com/NuclearAPK/Simple-Kafka_Adapter/releases/tag/v1.5.3
