#include "eenddb_extension.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "matcher.hpp"
#include "tokenizer.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

namespace {

class ParserTokenizer : public BaseTokenizer {
public:
	ParserTokenizer(const string &sql, vector<MatcherToken> &tokens) : BaseTokenizer(sql, tokens) {
	}
	void OnStatementEnd(idx_t pos) override {
		statements.push_back(std::move(tokens));
		tokens.clear();
	}
	void OnLastToken(TokenizeState state, string last_word, idx_t last_pos) override {
		if (last_word.empty()) {
			return;
		}
		tokens.emplace_back(std::move(last_word), last_pos);
	}

	vector<vector<MatcherToken>> statements;
};

} // anonymous namespace

class DutchParserExtension : public ParserExtension {
public:
	DutchParserExtension() {
		parser_override = DutchParser;
	}

	static ParserOverrideResult DutchParser(ParserExtensionInfo *info, const string &query, ParserOptions &options) {
		vector<MatcherToken> root_tokens;

		ParserTokenizer tokenizer(query, root_tokens);
		tokenizer.TokenizeInput();
		tokenizer.statements.push_back(std::move(root_tokens));

		try {
			vector<unique_ptr<SQLStatement>> result;
			for (auto &tokenized_statement : tokenizer.statements) {
				if (tokenized_statement.empty()) {
					continue;
				}
				auto &transformer = PEGTransformerFactory::GetInstance();
				auto statement = transformer.Transform(tokenized_statement, options);
				if (statement) {
					statement->stmt_location = NumericCast<idx_t>(tokenized_statement[0].offset);
					auto last_pos = tokenized_statement[tokenized_statement.size() - 1].offset +
					                tokenized_statement[tokenized_statement.size() - 1].length;
					statement->stmt_length = last_pos - tokenized_statement[0].offset;
				}
				statement->query = query;
				result.push_back(std::move(statement));
			}
			if (!result.empty()) {
				auto &last_statement = result.back();
				last_statement->stmt_length = query.size() - last_statement->stmt_location;
				for (auto &statement : result) {
					statement->query = query.substr(statement->stmt_location, statement->stmt_length);
					statement->stmt_location = 0;
					statement->stmt_length = statement->query.size();
					if (statement->type == StatementType::CREATE_STATEMENT) {
						auto &create = statement->Cast<CreateStatement>();
						create.info->sql = statement->query;
					}
				}
			}
			return ParserOverrideResult(std::move(result));
		} catch (std::exception &e) {
			return ParserOverrideResult(e);
		}
	}
};

static void EnableDutchParserFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &db_config = DBConfig::GetConfig(context);
	db_config.SetOptionByName("allow_parser_override_extension", Value("fallback"));
}

static void DisableDutchParserFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &db_config = DBConfig::GetConfig(context);
	db_config.SetOptionByName("allow_parser_override_extension", Value("default"));
}

static duckdb::unique_ptr<FunctionData> EnableDutchParserBind(ClientContext &context, TableFunctionBindInput &input,
                                                              vector<LogicalType> &return_types,
                                                              vector<string> &names) {
	names.emplace_back("success");
	return_types.emplace_back(LogicalType::BOOLEAN);
	return nullptr;
}

inline void EenddbScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "eenddb " + name.GetString() + " 🐥");
	});
}


static void LoadInternal(ExtensionLoader &loader) {
	auto eenddb_scalar_function = ScalarFunction("eenddb", {LogicalType::VARCHAR}, LogicalType::VARCHAR, EenddbScalarFun);
	loader.RegisterFunction(eenddb_scalar_function);

	TableFunction enable_dutch_parser("enable_dutch_parser", {}, EnableDutchParserFunction, EnableDutchParserBind,
	                                  nullptr);
	loader.RegisterFunction(enable_dutch_parser);

	TableFunction disable_dutch_parser("disable_dutch_parser", {}, DisableDutchParserFunction, EnableDutchParserBind,
	                                   nullptr);
	loader.RegisterFunction(disable_dutch_parser);

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	ParserExtension::Register(config, DutchParserExtension());
}

void EenddbExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string EenddbExtension::Name() {
	return "eenddb";
}

std::string EenddbExtension::Version() const {
	return DefaultVersion();
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(eenddb, loader) {
	LoadInternal(loader);
}
}
