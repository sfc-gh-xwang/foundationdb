#include <boost/spirit/home/x3/support/ast/variant.hpp>
#include <boost/fusion/adapted/std_pair.hpp>
#include <boost/spirit/home/x3.hpp>
#include <boost/variant.hpp>
#include <boost/optional.hpp>
#include <iostream>
#include <map>
#include <variant>
#include <string>
#include <vector>
#include <type_traits>

using namespace std;

namespace x3 = boost::spirit::x3;

template <typename T>
struct field {
	std::string name;
	boost::optional<T> default_;
};

BOOST_FUSION_ADAPT_TPL_STRUCT(
		(T), (field) (T),
		(std::string, name)
		(boost::optional<T>, default_))

int main() {
    auto stringRule = x3::rule<class StringClass, string>() = x3::lexeme[x3::alpha >> *x3::alnum];

	auto stringConstantRule = x3::rule<class StringClass, string>() = x3::lexeme['"' > *(
             "\\n" >> x3::attr('\n')
           | "\\t" >> x3::attr('\t')
           | "\\r" >> x3::attr('\r')
           | "\\b" >> x3::attr('\b')
           | "\\f" >> x3::attr('\f')
           | "\\"  >> x3::char_("\"\\/") // escaped double-quote, backslash, and slash
           | "\\x" >> x3::int_parser<char, 16, 2, 2>() // bytes in hex: e.g. "\xff"; always two digits
           | ~x3::char_('"')
       ) > '"'];

    //auto stringConstantRule = x3::rule<class StringClass, string>() = x3::lexeme[x3::lit('"') >> *(x3::ascii::char_ - '"') >> '"'];

    auto variantRule = x3::rule<class VariantClass, x3::variant<std::string, float>>() = (stringRule | x3::float_);

    auto pairRule = x3::rule<class PairClass, pair<int, x3::variant<std::string, float>>>() = x3::int_ >> ':' >> variantRule;

    auto mapRule = x3::rule<class MapClass, map<int, x3::variant<std::string, float>>>() = pairRule % ",";
	using FieldType = x3::variant<field<std::string>, field<int>, field<double>>;

	auto stringFieldRule = x3::rule<class StringFieldClass, field<std::string>>() = x3::lit("string") >> stringRule >> -(x3::lit('=') >> stringConstantRule);
	auto intFieldRule = x3::rule<class StringFieldClass, field<int>>() = x3::lit("int") >> stringRule >> -(x3::lit('=') >> x3::int_);
	auto fieldRule = x3::rule<class FieldClass, x3::variant<field<std::string>, field<int>>>() = (stringFieldRule | intFieldRule);

	//auto fieldRule = x3::rule<class FieldClass, x3::variant<field<std::string>, field<int>, field<double>>>() = (x3::lit("string") >> stringRule) | (x3::lit("int") >> x3::int_) | (x3::double_);

	//auto fieldsRule = x3::rule<class FieldsClass, std::vector<FieldType>>() = fieldRule % ",";

	//string input = "string a = \"ab\\\"\"";
	//string input = "string a";
	string inputs[] = { "string a = \"bcd\" ", "string b", " int x", "int x = 10  " };
	for (const auto& input : inputs) {
		x3::variant<field<std::string>, field<int>> f;
		//std::vector<FieldType> v;
		auto bg = input.begin();
		auto ed = input.end();
		auto success = x3::phrase_parse(bg, ed, fieldRule, x3::space, f) && bg == ed;
		if (!success) cout << "Parsing not successful";
		else {
			boost::apply_visitor([](auto&& f_real) {
				using ftype = std::decay_t<decltype(f_real)>;
				cout << f_real.name << " ";
				if constexpr (std::is_same_v<ftype, field<string>>) {
					cout << "(string) ";
				} else {
					cout << "(int) ";
				}
				if (f_real.default_)
					cout << "= " << *f_real.default_ << endl;
				else
					cout << "[no_default]" << endl;
			}, f);
			//cout << f.name << endl << (f.default_ ? *f.default_ : "[none]") << endl;
			/*
			for (auto const& e : v) {
				boost::apply_visitor([](auto&& e_real) {
					cout << typeid(std::decay_t<decltype(e_real)>).name() << ": " << e_real << endl;
				}, e);
			}
			*/
		}
	}
	/*
    string input = "1 : 1.0, 2 : hello, 3 : world";

    map<int, x3::variant<std::string, float>> variantMap;

    auto bg = input.begin(), ed = input.end();

    auto success = x3::phrase_parse(bg, ed, mapRule, x3::space, variantMap) && bg == ed;

    if (!success) cout<<"Parsing not succesfull";

	for (const auto& [k, v] : variantMap) {
		cout << '"' << k << '"' << ", \"";
		boost::apply_visitor([](auto&& v_real) {
			cout << v_real << '"' << endl;
		}, v);
	}
	*/

    return 0;
}
