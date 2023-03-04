/*
 *  Modern Native AddIn
 *  Copyright (C) 2018  Infactum
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

#ifndef COMPONENT_H
#define COMPONENT_H

#include <ctime>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include <AddInDefBase.h>
#include <ComponentBase.h>
#include <IMemoryManager.h>
#include <types.h>

template<class... Ts>
struct overloaded : Ts ... {
    using Ts::operator()...;
};
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

#define UNDEFINED std::monostate()

typedef std::variant<
        std::monostate,
        int32_t,
        double,
        bool,
        std::string,
        std::tm,
        std::vector<char>
> variant_t;

class Component : public IComponentBase {
public:

    bool ADDIN_API Init(void *connection_) final;

    bool ADDIN_API setMemManager(void *memory_manager_) final;

    long ADDIN_API GetInfo() final { return 2100; };

    void ADDIN_API Done() final {};

    void ADDIN_API SetLocale(const WCHAR_T *locale) final;

    bool ADDIN_API RegisterExtensionAs(WCHAR_T **ext_name) final;

    long ADDIN_API GetNProps() final;

    long ADDIN_API FindProp(const WCHAR_T *prop_name) final;

    const WCHAR_T *ADDIN_API GetPropName(long num, long lang_alias) final;

    bool ADDIN_API GetPropVal(const long num, tVariant *value) final;

    bool ADDIN_API SetPropVal(const long num, tVariant *value) final;

    bool ADDIN_API IsPropReadable(const long lPropNum) final;

    bool ADDIN_API IsPropWritable(const long lPropNum) final;

    long ADDIN_API GetNMethods() final;

    long ADDIN_API FindMethod(const WCHAR_T *method_name) final;

    const WCHAR_T *ADDIN_API GetMethodName(const long num, const long lang_alias) final;

    long ADDIN_API GetNParams(const long method_num) final;

    bool ADDIN_API GetParamDefValue(const long method_num, const long param_num, tVariant *def_value) final;

    bool ADDIN_API HasRetVal(const long method_num) final;

    bool ADDIN_API CallAsProc(const long method_num, tVariant *params, const long array_size) final;

    bool ADDIN_API CallAsFunc(const long method_num, tVariant *ret_value, tVariant *params,
                              const long array_size) final;

protected:
    virtual std::string extensionName() = 0;

    void AddError(unsigned short code, const std::string &src, const std::string &msg, bool throw_excp);

    bool ExternalEvent(const std::string &src, const std::string &msg, const std::string &data);

    bool SetEventBufferDepth(long depth);

    long GetEventBufferDepth();

    void AddProperty(const std::wstring &alias, const std::wstring &alias_ru,
                     std::function<std::shared_ptr<variant_t>(void)> getter = nullptr,
                     std::function<void(variant_t &&)> setter = nullptr);

    void AddProperty(const std::wstring &alias, const std::wstring &alias_ru,
                     std::shared_ptr<variant_t> storage);

    template<typename T, typename C, typename ... Ts>
    void AddMethod(const std::wstring &alias, const std::wstring &alias_ru, C *c, T(C::*f)(Ts ...),
                   std::map<long, variant_t> &&def_args = {});

private:
    class PropertyMeta;

    class MethodMeta;

    template<size_t... Indices>
    static auto refTupleGen(std::vector<variant_t> &v, std::index_sequence<Indices...>);

    static variant_t toStlVariant(tVariant src);

    static std::string toUTF8String(std::basic_string_view<WCHAR_T> src);

    static std::wstring toWstring(std::basic_string_view<WCHAR_T> src);

    static std::u16string toUTF16String(std::string_view src);

    void storeVariable(const std::string &src, tVariant &dst);

    void storeVariable(const std::string &src, WCHAR_T **dst);

    void storeVariable(const std::u16string &src, WCHAR_T **dst);

    void storeVariable(const std::vector<char> &src, tVariant &dst);

    void storeVariable(const variant_t &src, tVariant &dst);

    static std::vector<variant_t> parseParams(tVariant *params, long array_size);

    void storeParams(const std::vector<variant_t> &src, tVariant *dest);

    static std::wstring toUpper(std::wstring str);

    IAddInDefBase *connection;
    IMemoryManager *memory_manager;
    std::vector<PropertyMeta> properties_meta;
    std::vector<MethodMeta> methods_meta;
    static constexpr char UNKNOWN_EXCP[] = u8"Unknown unhandled exception";

};

class Component::PropertyMeta {
public:
    PropertyMeta &operator=(const PropertyMeta &) = delete;

    PropertyMeta(const PropertyMeta &) = delete;

    PropertyMeta(PropertyMeta &&) = default;

    PropertyMeta &operator=(PropertyMeta &&) = default;

    std::wstring alias;
    std::wstring alias_ru;
    std::function<std::shared_ptr<variant_t>(void)> getter;
    std::function<void(variant_t &&)> setter;
};

class Component::MethodMeta {
public:
    MethodMeta &operator=(const MethodMeta &) = delete;

    MethodMeta(const MethodMeta &) = delete;

    MethodMeta(MethodMeta &&) = default;

    MethodMeta &operator=(MethodMeta &&) = default;

    std::wstring alias;
    std::wstring alias_ru;
    long params_count;
    bool returns_value;
    std::map<long, variant_t> default_args;
    std::function<variant_t(std::vector<variant_t> &params)> call;
};

template<size_t... Indices>
auto Component::refTupleGen(std::vector<variant_t> &v, std::index_sequence<Indices...>) {
    return std::forward_as_tuple(v[Indices]...);
}

template<typename T, typename C, typename ... Ts>
void Component::AddMethod(const std::wstring &alias, const std::wstring &alias_ru, C *c, T(C::*f)(Ts ...),
                          std::map<long, variant_t> &&def_args) {

    MethodMeta meta{alias, alias_ru, sizeof...(Ts), !std::is_same<T, void>::value, std::move(def_args),
                    [f, c](std::vector<variant_t> &params) -> variant_t {
                        auto args = refTupleGen(params, std::make_index_sequence<sizeof...(Ts)>());
                        if constexpr (std::is_same<T, void>::value) {
                            std::apply(f, std::tuple_cat(std::make_tuple(c), args));
                            return UNDEFINED;
                        } else {
                            return std::apply(f, std::tuple_cat(std::make_tuple(c), args));
                        }
                    }
    };

    methods_meta.push_back(std::move(meta));
};

#endif //COMPONENT_H
