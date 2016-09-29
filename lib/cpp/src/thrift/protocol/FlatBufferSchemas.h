#ifndef _THRIFT_PROTOCOL_TFLATBUFFERSCHEMAS_H_
#define _THRIFT_PROTOCOL_TFLATBUFFERSCHEMAS_H_ 1

#include <string>

#include "flatbuffers/reflection.h"
#include "flatbuffers/util.h"

class FlatBufferSchemas {
public:
  struct SchemaError: std::exception {
    SchemaError(const std::string& msg) : msg_(msg) { }
    const char* what() const noexcept {
      return msg_.c_str();
    }
    std::string msg_;
  };

  FlatBufferSchemas(const char* bfbs_filename) : bfbs_filename_(bfbs_filename) {
    if (!flatbuffers::LoadFile(bfbs_filename, true, &bfbs_file_content_)) {
      throw SchemaError("Error loading flatbuffer schema file: " + std::string(bfbs_filename));
    }

    flatbuffers::Verifier verifier(
        reinterpret_cast<const uint8_t *>(bfbs_file_content_.c_str()),
        bfbs_file_content_.length());
    if (!reflection::VerifySchemaBuffer(verifier)) {
      throw SchemaError("Error verifying flatbuffer schema in file " + std::string(bfbs_filename));
    }

    schema_ = reflection::GetSchema(bfbs_file_content_.c_str());
  }

  ~FlatBufferSchemas() { }

  const reflection::Object* getTableSchema(const char* table_name) const {
    auto objects = schema_->objects();
    return objects->LookupByKey(table_name);
  }

  const reflection::Field* getFieldSchema(
      const reflection::Object* table, const char* field_name) const {
    return table->fields()->LookupByKey(field_name);
  }

  const reflection::Field* getFieldSchemaByThriftId(
      const reflection::Object* table, const int16_t fieldId) const {
    for (auto field : *table->fields()) {
      if (fieldId == getFieldThriftId(field)) {
        return field;
      }
    }
    return nullptr;
  }

  int getFieldThriftId(const reflection::Field* field) const {
    return getFieldAttrIntValue(field, "thrift_id");
  }

  int getFieldThriftType(const reflection::Field* field) const {
    return getFieldAttrIntValue(field, "thrift_type");
  }

  // returns 0 on failure -- strtoll.
  int getFieldAttrIntValue(
     const reflection::Field* field, const char* attr_name) const {
    auto attr = field->attributes()->LookupByKey(attr_name);
    int thrift_id = 0;
    if (attr != nullptr) {
      thrift_id = flatbuffers::StringToInt(attr->value()->c_str());
    }
    return thrift_id;
  }

  bool isObjectType(const reflection::Field* field) const {
    return field->type()->base_type() == reflection::BaseType::Obj;
  }

  bool isVectorType(const reflection::Field* field) const {
    return field->type()->base_type() == reflection::BaseType::Vector;
  }

  bool isAuxVectorType(const reflection::Field* field) const {
    if (!isVectorType(field)) {
      return false;
    }
    if (field->type()->element() != reflection::BaseType::Obj) {
      return false;
    }
    auto table = schema_->objects()->Get(field->type()->index());
    auto attrs = table->attributes();
    if (attrs == nullptr) {
      return false;
    }
    auto aux_attr = attrs->LookupByKey("aux_type");
    return aux_attr != nullptr;
  }

  // precondition: the type of field is a table
  const reflection::Object* getFieldTableSchema(
          const reflection::Field* field) const {
    assert(isObjectType(field));
    return schema_->objects()->Get(field->type()->index());
  }

  // precondition: the type of field is an array of table
  const reflection::Object* getFieldElemTableSchema(
          const reflection::Field* field) const {
    assert(isVectorType(field));
    assert(field->type()->element() == reflection::BaseType::Obj);
    return schema_->objects()->Get(field->type()->index());
  }

private:
  const std::string bfbs_filename_;
  std::string bfbs_file_content_;
  const reflection::Schema* schema_;
};

#endif
