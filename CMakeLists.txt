cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME dutchdb)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/dutchdb_extension.cpp
    src/keyword_map.cpp
    src/keyword_helper.cpp
    src/matcher.cpp
    src/tokenizer.cpp
    src/parser/peg_parser.cpp
    src/transformer/peg_transformer.cpp
    src/transformer/peg_transformer_factory.cpp
    src/transformer/transform_alter.cpp
    src/transformer/transform_analyze.cpp
    src/transformer/transform_attach.cpp
    src/transformer/transform_call.cpp
    src/transformer/transform_checkpoint.cpp
    src/transformer/transform_comment.cpp
    src/transformer/transform_common.cpp
    src/transformer/transform_copy.cpp
    src/transformer/transform_create_index.cpp
    src/transformer/transform_create_macro.cpp
    src/transformer/transform_create_schema.cpp
    src/transformer/transform_create_secret.cpp
    src/transformer/transform_create_sequence.cpp
    src/transformer/transform_create_table.cpp
    src/transformer/transform_create_type.cpp
    src/transformer/transform_create_view.cpp
    src/transformer/transform_deallocate.cpp
    src/transformer/transform_delete.cpp
    src/transformer/transform_describe.cpp
    src/transformer/transform_detach.cpp
    src/transformer/transform_drop.cpp
    src/transformer/transform_execute.cpp
    src/transformer/transform_explain.cpp
    src/transformer/transform_export.cpp
    src/transformer/transform_expression.cpp
    src/transformer/transform_import.cpp
    src/transformer/transform_insert.cpp
    src/transformer/transform_load.cpp
    src/transformer/transform_merge_into.cpp
    src/transformer/transform_pivot.cpp
    src/transformer/transform_pragma.cpp
    src/transformer/transform_prepare.cpp
    src/transformer/transform_select.cpp
    src/transformer/transform_set.cpp
    src/transformer/transform_transaction.cpp
    src/transformer/transform_update.cpp
    src/transformer/transform_use.cpp
    src/transformer/transform_vacuum.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
