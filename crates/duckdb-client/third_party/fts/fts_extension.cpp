#include "fts_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "fts_indexing.hpp"
#include "libstemmer.h"

namespace duckdb {

static void StemFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input_vector = args.data[0];
	auto &stemmer_vector = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    input_vector, stemmer_vector, result, args.size(), [&](string_t input, string_t stemmer) {
		    auto input_data = input.GetData();
		    auto input_size = input.GetSize();

		    if (stemmer.GetString() == "none") {
			    auto output = StringVector::AddString(result, input_data, input_size);
			    return output;
		    }

		    struct sb_stemmer *s = sb_stemmer_new(stemmer.GetString().c_str(), "UTF_8");
		    if (s == 0) {
			    const char **stemmers = sb_stemmer_list();
			    size_t n_stemmers = 27;
			    throw InvalidInputException(
			        "Unrecognized stemmer '%s'. Supported stemmers are: ['%s'], or use 'none' for no stemming",
			        stemmer.GetString(),
			        StringUtil::Join(stemmers, n_stemmers, "', '", [](const char *st) { return st; }));
		    }

		    auto output_data =
		        const_char_ptr_cast(sb_stemmer_stem(s, reinterpret_cast<const sb_symbol *>(input_data), input_size));
		    auto output_size = sb_stemmer_length(s);
		    auto output = StringVector::AddString(result, output_data, output_size);

		    sb_stemmer_delete(s);
		    return output;
	    });
}

static void LoadInternal(ExtensionLoader &loader) {

	ScalarFunction stem_func("stem", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, StemFunction);

	auto create_fts_index_func =
	    PragmaFunction::PragmaCall("create_fts_index", FTSIndexing::CreateFTSIndexQuery,
	                               {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR);
	create_fts_index_func.named_parameters["stemmer"] = LogicalType::VARCHAR;
	create_fts_index_func.named_parameters["stopwords"] = LogicalType::VARCHAR;
	create_fts_index_func.named_parameters["ignore"] = LogicalType::VARCHAR;
	create_fts_index_func.named_parameters["strip_accents"] = LogicalType::BOOLEAN;
	create_fts_index_func.named_parameters["lower"] = LogicalType::BOOLEAN;
	create_fts_index_func.named_parameters["overwrite"] = LogicalType::BOOLEAN;

	auto drop_fts_index_func =
	    PragmaFunction::PragmaCall("drop_fts_index", FTSIndexing::DropFTSIndexQuery, {LogicalType::VARCHAR});

	loader.RegisterFunction(stem_func);
	loader.RegisterFunction(create_fts_index_func);
	loader.RegisterFunction(drop_fts_index_func);
}

void FtsExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string FtsExtension::Name() {
	return "fts";
}

std::string FtsExtension::Version() const {
#ifdef EXT_VERSION_FTS
	return EXT_VERSION_FTS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(fts, loader) {
	duckdb::LoadInternal(loader);
}

}
