#ifndef _THRIFT_PROTOCOL_TFLATBPROTOCOL_TCC_
#define _THRIFT_PROTOCOL_TFLATBPROTOCOL_TCC_ 1

#ifdef DEBUGFLATB
#define Debug(x) x
#else
#define Debug(x) 
#endif

namespace apache {
namespace thrift {
namespace protocol {

/**
 * Helper structs for write.
 */

enum FieldTag : int {
  REGULAR,   // regular field in a user defined table.
  MAP_KEY,   // key field "k" in an aux table for map
  MAP_VAL,   // value field "v" in an aux table for map.
  LIST_VAL,  // value field "v" in an aux table for nested list
};

enum VectorTag {
  LIST,      // it's a usual thrift list
  AUX_MAP,   // it's a thrift map
  AUX_LIST,  // it's a nested thrift list
};

union FieldVal {
  FieldVal(int8_t val) : byte_val(val) { }
  FieldVal(int16_t val) : short_val(val) { }
  FieldVal(int32_t val) : int_val(val) { }
  FieldVal(int64_t val) : long_val(val) { }
  FieldVal(bool val) : bool_val(val) { }
  FieldVal(double val) : double_val(val) { }
  FieldVal(flatbuffers::uoffset_t val) : offset_val(val) { }

  int8_t byte_val;
  int16_t short_val;
  int32_t int_val;
  int64_t long_val;
  bool bool_val;
  double double_val;
  flatbuffers::uoffset_t offset_val;
};

// Store data about a field being written.
//
// A field can be of the usual scalar type, or a table (struct), or a list
// of scalar or table objects (list & map).
struct WriteContext {
  WriteContext(const reflection::Field* field)
      : field_schema(field), tag(REGULAR), vec_size(0), vec_tag(LIST), val(0) { }

  // The flatbuffer schema of the field being written.
  const reflection::Field* const field_schema;

  // indicates whether this is a regular field (as those in thrift object)
  // or an artificially created field for map or list. Flatbuffer doesn't support
  // map or nested lists. We create aux tables to work around such cases.
  // For example, a thrift map is translated in the following way:
  //   map<string, string>
  // =>
  //   [_string_string]
  //   table _string_string {
  //     k:string;
  //     v:string;
  //   }
  // Since the table _string_string doesn't really exist in thrift object we
  // have to manually simulate its events (e.g. writeFieldBegin, writeFieldEnd,
  // writeStructEnd) so we can create the flatbuffer table.
  FieldTag tag;

  // expected size for vector field.
  // (thrift map, list and set are all represented as flatbuffer vector)
  int vec_size;

  // whether the flatbuffer vector is regular thrift list (LIST)
  // or thrift map (AUX_MAP) or list of lists (AUX_LIST).
  VectorTag vec_tag;

  // the final value of the field.
  FieldVal val;

  // An intermediate storage to hold all fields in a table. Applicable
  // when the field is a table or a list of table.
  //
  // The fields get rolled into either val (for field of table) or
  // elems (for field of list of table) when all fields of the table
  // is collected.
  std::vector<std::unique_ptr<WriteContext>> table_fields;

  // An intermediate storage to hold all elements in a vector.
  // They'll get rolled into val when all elements are collected.
  std::vector<FieldVal> elems;
};


/**
 * Helper structs for read.
 */

enum TableTag : int {
  TABLE,  // usual table corresponding to a thrift struct
  MAP_TABLE,  // aux table used to work with thrift map
  LIST_TABLE,  // aux table used to work with thrift nested lists.
};

struct ReadContext {
  ReadContext(const flatbuffers::Table* tbl, const reflection::Object* tbl_schema)
      : table(tbl), table_schema(tbl_schema), table_tag(TABLE),
        field_index(-1), elem_index(-1), vec_size(-1), vec_tag(LIST) { }

  // The current table being read.
  const flatbuffers::Table* const table;

  // The schema of the table.
  const reflection::Object* const table_schema;

  // Whether the table is a regular table or an aux table for a map
  // or an aux table for list.
  TableTag table_tag;

  // Below are vars/func that track the current field of the table.

  void resetFieldIndex(int idx) {
    field_index = idx;
    elem_index = -1;
    vec_size = -1;
    vec_tag = LIST;
  }

  // The index of the current field being read.
  int field_index;

  // Vector field.
  // The index of the current element being read for vector@field_index.
  // -1: not a vector.
  int elem_index;

  // the expected number of elements for vector@field_index.
  int vec_size;

  // Whether field_index is a regular list or aux map or aux list.
  VectorTag vec_tag;
};

/**
 * Write
 */

uint32_t TFlatbProtocol::writeStructBegin(const char* name) {
  if (write_stack_.size() == 0) {
    // Construct a special root field.
    const reflection::Object* table_schema =
        flatb_schema_->getTableSchema(flatb_table_.c_str());
    assert(table_schema != nullptr);
    root_schema_ = table_schema;
    write_stack_.push(new WriteContext(nullptr));
    Debug(std::cout << "start -ROOT-" << std::endl);
  } else {
    Debug(std::cout << "writeStructBegin for field "
         << write_stack_.top()->field_schema->name()->c_str()
         << std::endl);
  }
  return 0;
}

uint32_t TFlatbProtocol::writeStructEnd() {
  // This is the only place we can reliably know a struct
  // is done. We take the opportunity to process the table.
  WriteContext* finfo = write_stack_.top();
  rollUpTable(finfo, getCurrentWrittenTableSchema());

  if (write_stack_.size() == 1) {
    // We are done now.
    fbb_.Finish(flatbuffers::Offset<void>(finfo->val.offset_val));
    flatb_bytes_ = reinterpret_cast<const char*>(fbb_.GetBufferPointer());
    write_flatb_size_ = fbb_.GetSize();
    // clean up
    write_stack_.pop();
    delete finfo;
    Debug(std::cout << "Done -ROOT-" << std::endl);
    return write_flatb_size_;
  } else {
    Debug(std::cout << "writeStructEnd for field "
        << write_stack_.top()->field_schema->name()->c_str()
        << std::endl);
    recordFieldValue(finfo, finfo->val);
  }
  return 0;
}

uint32_t TFlatbProtocol::writeFieldBegin(
    const char* name, const TType fieldType, const int16_t fieldId) {
  auto table_schema = getCurrentWrittenTableSchema();
  auto field_schema = flatb_schema_->getFieldSchema(table_schema, name);
  if (field_schema == nullptr) {
    // search by fieldId. This is less efficient than search by name.
    field_schema = flatb_schema_->getFieldSchemaByThriftId(
        table_schema, fieldId);
  }
  assert(field_schema != nullptr);
  write_stack_.push(new WriteContext(field_schema));
  Debug(std::cout << "writeFieldBegin for field "
      << write_stack_.top()->field_schema->name()->c_str()
      << std::endl;)
  return 0;
}


uint32_t TFlatbProtocol::writeFieldEnd() {
  WriteContext* finfo = write_stack_.top();
  write_stack_.pop();

  Debug(std::cout << "writeFieldEnd for field "
      << finfo->field_schema->name()->c_str() << std::endl);

  // Save the field to the previous level (a field of either table
  // or list of tables), which takes ownership of finfo.
  write_stack_.top()->table_fields.push_back(std::unique_ptr<WriteContext>(finfo));

  // Special handling for aux fields.
  switch (finfo->tag) {
    case FieldTag::MAP_KEY : 
      // Need to explicitly trigger value field "v" start.
      writeAuxFieldBegin("v", FieldTag::MAP_VAL);
      break;
    case FieldTag::MAP_VAL:
    case FieldTag::LIST_VAL:
      // value field is done.
      // we need to explicitly trigger map/list aux struct end.
      writeStructEnd();
      break;
    default:
      break;
  }

  return 0;
}

uint32_t TFlatbProtocol::writeListBegin(
    const TType elemType, const uint32_t size) {
  // update the expected size for the current vector field.
  WriteContext* finfo = write_stack_.top();
  Debug(std::cout << "writeListBegin for field "
      << finfo->field_schema->name()->c_str()
      << " size=" << size << std::endl);
  finfo->vec_size = size;
  if (size == 0) {
    // rollup the empty vector.
    postWriteValue(finfo);
  } else if (flatb_schema_->isAuxVectorType(finfo->field_schema)) {
    finfo->vec_tag = AUX_LIST;
    writeAuxFieldBegin("v", FieldTag::LIST_VAL);
  }
  return 0;
}

uint32_t TFlatbProtocol::writeListEnd() {
  const WriteContext* finfo = write_stack_.top();
  Debug(std::cout << "writeListEnd for field "
      << finfo->field_schema->name()->c_str()
      << std::endl);
  return 0;
}

uint32_t TFlatbProtocol::writeSetBegin(
    const TType elemType, const uint32_t size) {
  // this is the same as writeListBegin().
  Debug(std::cout << "writeSetBegin => ");
  writeListBegin(elemType, size);
  return 0;
}


uint32_t TFlatbProtocol::writeSetEnd() {
  const WriteContext* finfo = write_stack_.top();
  Debug(std::cout << "writeSetEnd for field "
      << finfo->field_schema->name()->c_str()
      << std::endl);
  return 0;
}

uint32_t TFlatbProtocol::writeMapBegin(
    const TType keyType, const TType valType, const uint32_t size) {
  WriteContext* finfo = write_stack_.top();
  finfo->vec_size = size;
  finfo->vec_tag = AUX_MAP;
  Debug(std::cout << "writeMapBegin for field "
      << finfo->field_schema->name()->c_str()
      << " size=" << size << std::endl);
  if (size == 0) {
    postWriteValue(finfo);
  } else {
    writeAuxFieldBegin("k", FieldTag::MAP_KEY);
  }
  return 0;
}

uint32_t TFlatbProtocol::writeMapEnd() {
  Debug(std::cout << "writeMapEnd for field "
      << write_stack_.top()->field_schema->name()->c_str()
      << std::endl);
  return 0;
}

uint32_t TFlatbProtocol::writeBool(const bool value) {
  recordFieldValue(value);
  write_data_size_ += sizeof(bool);
  return 0;
}

uint32_t TFlatbProtocol::writeByte(const int8_t byte) {
  recordFieldValue(byte);
  write_data_size_ += sizeof(int8_t);
  return 0;
}

uint32_t TFlatbProtocol::writeI16(const int16_t i16) {
  recordFieldValue(i16);
  write_data_size_ += sizeof(int16_t);
  return 0;
}

uint32_t TFlatbProtocol::writeI32(const int32_t i32) {
  recordFieldValue(i32);
  write_data_size_ += sizeof(int32_t);
  return 0;
}

uint32_t TFlatbProtocol::writeI64(const int64_t i64) {
  recordFieldValue(i64);
  write_data_size_ += sizeof(int64_t);
  return 0;
}

uint32_t TFlatbProtocol::writeDouble(const double dub) {
  recordFieldValue(dub);
  write_data_size_ += sizeof(double);
  return 0;
}

uint32_t TFlatbProtocol::writeString(const std::string& str) {
  recordFieldValue(fbb_.CreateString(str).o);
  write_data_size_ += str.size();
  return 0;
}

uint32_t TFlatbProtocol::writeBinary(const std::string& str) {
  auto offset = fbb_.CreateVector<int8_t>(
      std::vector<int8_t>(str.c_str(), str.c_str() + str.size()));
  recordFieldValue(offset.o);
  write_data_size_ += str.size();
  return 0;
}

/**
 * Read
 */

uint32_t TFlatbProtocol::readStructBegin(std::string& name) {
  if (read_stack_.size() == 0) {
    Debug(std::cout << "readStructBegin -ROOT-");
    // Start of root
    const flatbuffers::Table* table = flatbuffers::GetAnyRoot(
        reinterpret_cast<const uint8_t*>(flatb_bytes_));
    const reflection::Object* table_schema =
        flatb_schema_->getTableSchema(flatb_table_.c_str());
    assert(table_schema != nullptr);
    read_stack_.push(new ReadContext(table, table_schema));
  } else {
    Debug(std::cout << "readStructBegin");
    // The struct should have the table schema of the current field.
    auto ctx = read_stack_.top();
    const reflection::Field* field = ctx->table_schema->fields()->Get(ctx->field_index);
    const flatbuffers::Table* table;
    const reflection::Object* table_schema;
    if (ctx->elem_index >= 0) {
      // the field is a vector of tables
      table_schema = flatb_schema_->getFieldElemTableSchema(field);
      auto vec = flatbuffers::GetFieldAnyV(*ctx->table, *field);
      table = flatbuffers::GetAnyVectorElemPointer<flatbuffers::Table>(vec, ctx->elem_index);
      // Set the next element index. We could do this in readStructEnd() too.
      ++ctx->elem_index;
    } else {
      // the field is a table.
      table_schema = flatb_schema_->getFieldTableSchema(field);
      table = flatbuffers::GetFieldT(*read_stack_.top()->table, *field);
    }
    read_stack_.push(new ReadContext(table, table_schema));
  }

  Debug(std::cout << " table-schema "
      << read_stack_.top()->table_schema->name()->c_str() << std::endl);
  return 0;
}

uint32_t TFlatbProtocol::readStructEnd() {
  Debug(std::cout << "readStructEnd table-schema "
      << read_stack_.top()->table_schema->name()->c_str()
      << " table_tag? " << read_stack_.top()->table_tag
      << std::endl);
  std::unique_ptr<ReadContext> to_be_del(read_stack_.top());
  read_stack_.pop();
  if (read_stack_.size() == 0) {
    Debug(std::cout << "Done -ROOT-" << std::endl);
  } else {
    postReadValue();
  }
  return 0;
}

uint32_t TFlatbProtocol::readFieldBegin(
    std::string& name, TType& fieldType, int16_t& fieldId) {
  auto ctx = read_stack_.top();
  // Find the index of the next populated field.
  int i = ctx->field_index + 1;
  Debug(std::cout << "readFieldBegin table-schema "
      << ctx->table_schema->name()->c_str() << " start-index " << i); 
  const reflection::Field* field;
  if (ctx->table_tag == TABLE) {
    // skip fields with default value.
    // do this only for non-aux table.
    for (; i < ctx->table_schema->fields()->size(); ++i) {
      field = ctx->table_schema->fields()->Get(i);
      if (ctx->table->CheckField(field->offset())) {
        break;
      }
    }
  }
  if (i >= ctx->table_schema->fields()->size()) {
    // aux table will never get there.
    assert(ctx->table_tag == TABLE);
    // no more field to read. stop.
    fieldType = T_STOP;
    fieldId = 0;
    Debug(std::cout << " T_STOP" << std::endl);
    return 0;
  }
  field = ctx->table_schema->fields()->Get(i);

  Debug(std::cout << " field-index " << i
       << " field-name " << field->name()->c_str()); 

  if (ctx->table_tag == TABLE) {
    fieldType = static_cast<TType>(flatb_schema_->getFieldThriftType(field));
    fieldId = flatb_schema_->getFieldThriftId(field);
    Debug(std::cout << " fieldType " << fieldType << " thriftId " << fieldId);
  } else {
    Debug(std::cout << " (aux)");
  }
  Debug(std::cout << std::endl);
  ctx->resetFieldIndex(i);
  return 0;
}

uint32_t TFlatbProtocol::readMapBegin(
    TType& keyType, TType& valType, uint32_t& size) {
  Debug(std::cout << "readMapBegin");
  auto ctx = read_stack_.top();
  const reflection::Field* field = ctx->table_schema->fields()->Get(ctx->field_index);
  auto vec = flatbuffers::GetFieldAnyV(*ctx->table, *field);
  size = vec->size();
  ctx->vec_size = size;
  ctx->elem_index = 0;
  ctx->vec_tag = AUX_MAP;
  Debug(std::cout << " field-name " << field->name()->c_str()
      << " size " << size << std::endl);

  if (size == 0) {
    postReadValue();
  } else {
    // manually simulate the table start for map kv.
    readAuxStructBegin(MAP_TABLE);
    // manually simulate field start for key.
    readAuxFieldBegin();
  }
  return 0;
}

uint32_t TFlatbProtocol::readListBegin(TType& elemType, uint32_t& size) {
  auto ctx = read_stack_.top();
  const reflection::Field* field = ctx->table_schema->fields()->Get(ctx->field_index);
  Debug(std::cout << "readListBegin field-name " << field->name()->c_str()
      << " size " << size << std::endl);
  assert(flatb_schema_->isVectorType(field));
  auto vec = flatbuffers::GetFieldAnyV(*ctx->table, *field);
  // elemType doesn't matter.
  size = vec->size();
  ctx->elem_index = 0;
  ctx->vec_size = size;

  if (size == 0) {
    postReadValue();
  } else if (flatb_schema_->isAuxVectorType(field)) {
    ctx->vec_tag = AUX_LIST;
    // Manually trigger the table/field start for list aux table.
    readAuxStructBegin(LIST_TABLE);
    readAuxFieldBegin();
  }
  return 0;
}

uint32_t TFlatbProtocol::readSetBegin(TType& elemType, uint32_t& size) {
  Debug(std::cout << "readSetBegin => ");
  return readListBegin(elemType, size);
}

uint32_t TFlatbProtocol::readBool(bool& value) {
  Debug(std::cout << "readBool" << std::endl);
  return readIntValue<bool, reflection::BaseType::Bool>(value);
}

uint32_t TFlatbProtocol::readByte(int8_t& value) {
  Debug(std::cout << "readByte" << std::endl);
  return readIntValue<int8_t, reflection::BaseType::Byte>(value);
}

uint32_t TFlatbProtocol::readI16(int16_t& value) {
  Debug(std::cout << "readI16" << std::endl);
  return readIntValue<int16_t, reflection::BaseType::Short>(value);
}

uint32_t TFlatbProtocol::readI32(int32_t& value) {
  Debug(std::cout << "readI32" << std::endl);
  return readIntValue<int32_t, reflection::BaseType::Int>(value);
}

uint32_t TFlatbProtocol::readI64(int64_t& value) {
  Debug(std::cout << "readI64" << std::endl);
  return readIntValue<int64_t, reflection::BaseType::Long>(value);
}

uint32_t TFlatbProtocol::readDouble(double& value) {
  Debug(std::cout << "readDouble" << std::endl);
  auto ctx = read_stack_.top();
  const reflection::Field* field = ctx->table_schema->fields()->Get(ctx->field_index);
  if (ctx->elem_index >= 0) {
     // array
     auto vec = flatbuffers::GetFieldV<double>(*ctx->table, *field);
     value = vec->Get(ctx->elem_index);
     ++ctx->elem_index;
  } else {
    value = flatbuffers::GetFieldF<double>(*ctx->table, *field);
  }
  Debug(std::cout << "readDouble value " << value);
  Debug(if (ctx->elem_index >= 0) {
      std::cout << " elem-index " << (ctx->elem_index - 1)
      << " exp-size " << ctx->vec_size; });
  Debug(std::cout << std::endl);
  postReadValue();
  return 0;
}

uint32_t TFlatbProtocol::readString(std::string& value) {
  Debug(std::cout << "readString" << std::endl);
  auto ctx = read_stack_.top();
  const reflection::Field* field = ctx->table_schema->fields()->Get(ctx->field_index);
  if (ctx->elem_index >= 0) {
    // array
    auto vec = flatbuffers::GetFieldV<flatbuffers::Offset<flatbuffers::String>>(
        *ctx->table, *field);
    value = vec->Get(ctx->elem_index)->str();
    ++ctx->elem_index;
  } else {
    value = flatbuffers::GetFieldS(*ctx->table, *field)->str();
  }
  Debug(std::cout << "readString value " << value);
  Debug(if (ctx->elem_index >= 0) {
      std::cout << " elem-index " << (ctx->elem_index - 1)
      << " exp-size " << ctx->vec_size; });
  Debug(std::cout << std::endl);
  postReadValue();
  return 0;
}

uint32_t TFlatbProtocol::readBinary(std::string& value) {
  Debug(std::cout << "readBinary" << std::endl);
  auto ctx = read_stack_.top();
  const reflection::Field* field = ctx->table_schema->fields()->Get(ctx->field_index);
  auto vec = flatbuffers::GetFieldV<int8_t>(*ctx->table, *field);
  assert(ctx->elem_index < 0);
  value = std::string(reinterpret_cast<char *>(vec->data()), vec->size());
  Debug(std::cout << "readBinary value " << value << std::endl);
  postReadValue();
  return 0;
}

/**
 * Write helpers
 */

// Create a flatbuffer vector from finfo->elems and
// save its offset to finfo->val.
//
// NOTE: we didn't call fbb_.CreateVectorOfSortedTables<T>()
//       to create the vector for map because the vector is
//       already sorted thanks to std::map<> is used to store
//       and iterate thrift map during serialization.
void TFlatbProtocol::rollUpVector(WriteContext* finfo) {
  auto field_schema = finfo->field_schema;
  assert(flatb_schema_->isVectorType(field_schema));
  switch (field_schema->type()->element()) {
    case reflection::BaseType::Bool :
      {
        std::vector<bool> vec;
        vec.reserve(finfo->elems.size());
        for (auto& e : finfo->elems) {
          vec.push_back(e.bool_val);
        }
        finfo->val.offset_val = fbb_.CreateVector(vec).o;
        break;
      }
    case reflection::BaseType::Byte :
        // byte array (thrift binary) is already rolled up.
        // nothing to do here.
        break;
    case reflection::BaseType::Short :
      {
        std::vector<int16_t> vec;
        vec.reserve(finfo->elems.size());
        for (auto& e : finfo->elems) {
          vec.push_back(e.short_val);
        }
        finfo->val.offset_val = fbb_.CreateVector<int16_t>(vec).o;
        break;
      }
    case reflection::BaseType::Int :
      {
        std::vector<int32_t> vec;
        vec.reserve(finfo->elems.size());
        for (auto& e : finfo->elems) {
          vec.push_back(e.int_val);
        }
        finfo->val.offset_val = fbb_.CreateVector<int32_t>(vec).o;
        break;
      }
    case reflection::BaseType::Long :
      {
        std::vector<int64_t> vec;
        vec.reserve(finfo->elems.size());
        for (auto& e : finfo->elems) {
          vec.push_back(e.long_val);
        }
        finfo->val.offset_val = fbb_.CreateVector<int64_t>(vec).o;
        break;
      }
    case reflection::BaseType::Double :
      {
        std::vector<double> vec;
        vec.reserve(finfo->elems.size());
        for (auto& e : finfo->elems) {
          vec.push_back(e.double_val);
        }
        finfo->val.offset_val = fbb_.CreateVector<double>(vec).o;
        break;
      }
    case reflection::BaseType::String :
    case reflection::BaseType::Vector :
    case reflection::BaseType::Obj :
      {
        std:: vector<flatbuffers::Offset<void> > vec;
        vec.reserve(finfo->elems.size());
        for (auto& e : finfo->elems) {
          vec.push_back(flatbuffers::Offset<void>(e.offset_val));
        }
        finfo->val.offset_val = fbb_.CreateVector<flatbuffers::Offset<void>>(vec).o;
        break;
      }
    default:
      {
        Debug(std::cout << "ERR: elem type " << field_schema->type()->element()
            << " type " << field_schema->type()->base_type()
            << " name " << field_schema->name()->c_str());
        assert(false);
      }
  }
}

// Create a flatbuffer table using finfo->table_fields and save
// its offset to finfo->val.
void TFlatbProtocol::rollUpTable(WriteContext* finfo,
    const reflection::Object* table_schema) {
  auto start = fbb_.StartTable();
  for (auto& field : finfo->table_fields) {
    addFieldToBuilder(field.get());
  }
  auto offset = fbb_.EndTable(start, table_schema->fields()->size());
  finfo->val.offset_val = offset;
  // Clear table_fields in case the field is a list of tables,
  // in which case the next object in the list will start to
  // use table_fields.
  finfo->table_fields.clear();
}

// Apply finfo->val to flatbuffer.
void TFlatbProtocol::addFieldToBuilder(const WriteContext* finfo) {
  auto field_schema = finfo->field_schema;
  switch (field_schema->type()->base_type()) {
    case reflection::BaseType::Bool :
        fbb_.AddElement<uint8_t>(field_schema->offset(),
                                 static_cast<uint8_t>(finfo->val.bool_val),
                                 static_cast<uint8_t>(field_schema->default_integer()));
        break;
    case reflection::BaseType::Byte :
        fbb_.AddElement<int8_t>(field_schema->offset(),
                                finfo->val.byte_val,
                                static_cast<int8_t>(field_schema->default_integer()));
        break;
    case reflection::BaseType::Short :
        fbb_.AddElement<int16_t>(field_schema->offset(),
                                 finfo->val.short_val,
                                 static_cast<int16_t>(field_schema->default_integer()));
        break;
    case reflection::BaseType::Int :
        fbb_.AddElement<int32_t>(field_schema->offset(),
                                 finfo->val.int_val,
                                 static_cast<int32_t>(field_schema->default_integer()));
        break;
    case reflection::BaseType::Long :
        fbb_.AddElement<int64_t>(field_schema->offset(),
                                 finfo->val.long_val,
                                 field_schema->default_integer());
        break;
    case reflection::BaseType::Double :
        fbb_.AddElement<double>(field_schema->offset(),
                                finfo->val.double_val,
                                field_schema->default_real());
        break;
    case reflection::BaseType::String :
    case reflection::BaseType::Vector :
    case reflection::BaseType::Obj :
        fbb_.AddOffset(field_schema->offset(), flatbuffers::Offset<void>(finfo->val.offset_val));
        break;
    default: assert(false);
  }
}

// Save val to the current field.
void TFlatbProtocol::recordFieldValue(const FieldVal& val) {
  recordFieldValue(write_stack_.top(), val);
}

// Record the specified val in finfo->val if the field is not
// a vector. Otherwise, record val in finfo->elems.
void TFlatbProtocol::recordFieldValue(WriteContext* finfo, const FieldVal& val) {
  auto field_schema = finfo->field_schema;
  switch (field_schema->type()->base_type()) {
    case reflection::BaseType::Bool :
    case reflection::BaseType::Byte :
    case reflection::BaseType::Short :
    case reflection::BaseType::Int :
    case reflection::BaseType::Long :
    case reflection::BaseType::Double :
    case reflection::BaseType::String :
    case reflection::BaseType::Obj :
        finfo->val = val;
        break;
    case reflection::BaseType::Vector :
        if (field_schema->type()->element() == reflection::BaseType::Byte) {
          // byte array (thrift binary) is treated as string.
          finfo->val = val;
        } else {
          finfo->elems.push_back(val);
        }
        break;
    default: assert(false);
  } 

  postWriteValue(finfo);
}

void TFlatbProtocol::writeAuxFieldBegin(const char* name, FieldTag tag) {
  // Manually push in field "k".
  // fieldtype and thrift-id don't matter here.
  writeFieldBegin(name, T_STOP, -1);
  // tag it.
  write_stack_.top()->tag = tag;
}

// After a value is recorded for the field, we check the following:
// 1. if the field is a vector and if we have all the elements, we
// roll up the vector and create its final val.
// 2. handle for thrift map by manually kicking off or ending some
// events.
void TFlatbProtocol::postWriteValue(WriteContext* finfo) {
  assert(finfo == write_stack_.top());
  auto field_schema = finfo->field_schema;
  if (flatb_schema_->isVectorType(field_schema)
    && field_schema->type()->element() != reflection::BaseType::Byte) {
    // vector type

    if (finfo->elems.size() < finfo->vec_size) {
      // The field is not done for we haven't collected all elements.
      if (finfo->vec_tag == AUX_MAP) {
        // it's a map, we need to kick off the next map
        // struct. writeStructBegin() is never interesting
        // so we start with its key field.
        writeAuxFieldBegin("k", FieldTag::MAP_KEY);
      } else if (finfo->vec_tag == AUX_LIST) {
        // it's a aux list. kick off the next element struct.
        writeAuxFieldBegin("v", FieldTag::LIST_VAL);
      }
      return;
    }

    // we have collected all the elements of the vector/map.
    // roll it up.
    rollUpVector(finfo);
  }

  // The field is done. We'll manually trigger writeFieldEnd()
  // if the current field is a map key/val field.
  if (finfo->tag != REGULAR) {
    // explicitly trigger field-end for finfo.
    writeFieldEnd();
  }
}

// Return the table (struct) schema currently being written.
// It's either the type of the current field (if the field
// is a table), or the type of its element (if the field is
// a list of table).
const reflection::Object* TFlatbProtocol::getCurrentWrittenTableSchema() {
  if (write_stack_.size() == 1) {
    // root
    return root_schema_;
  }

  auto field_schema = write_stack_.top()->field_schema;
  if (flatb_schema_->isObjectType(field_schema)) {
    return flatb_schema_->getFieldTableSchema(field_schema);
  }
  // Must be a vector of tables.
  return flatb_schema_->getFieldElemTableSchema(field_schema);
}

/**
 * Read helpers
 */

// manually trigger a readFieldBegin() for map kv table.
void TFlatbProtocol::readAuxFieldBegin() {
  std::string n;
  TType t;
  int16_t i;
  // n, t, i have no significance here.
  readFieldBegin(n, t, i);
}

// manually trigger a readStructBegin() for a map-kv or list-v table.
void TFlatbProtocol::readAuxStructBegin(TableTag tag) {
  std::string n;
  readStructBegin(n); 
  read_stack_.top()->table_tag = tag;
}

// after a value is read (either scalar or a struct), the function
// manually triggers necessary map-kv events.
void TFlatbProtocol::postReadValue() {
  auto ctx = read_stack_.top();
  if (ctx->elem_index >= 0 && ctx->elem_index < ctx->vec_size) {
    // vector or map field and we haven't collected all the entries.

    if (ctx->vec_tag == AUX_MAP) {
      // it's a map field.
      // trigger the events for reading the next map-kv table.
      readAuxStructBegin(MAP_TABLE);
      readAuxFieldBegin();
    } else if (ctx->vec_tag == AUX_LIST) {
      readAuxStructBegin(LIST_TABLE);
      readAuxFieldBegin();
    }
    return;
  }
  
  // The field is done. If it's a map-kv table, we trigger next event.
  if (ctx->table_tag == MAP_TABLE) {
    if (ctx->field_index == 1) {
      // since there are only two fields in the table, it means we have
      // read both.
      // simulate the struct end.
      readStructEnd();
    } else {
      // simulate the next field read in the map-kv table.
      readAuxFieldBegin();
    }
  } else if (ctx->table_tag == LIST_TABLE) {
    readStructEnd();
  }
}

template<typename T, reflection::BaseType type>
uint32_t TFlatbProtocol::readIntValue(T& value) {
  auto ctx = read_stack_.top();
  const reflection::Field* field = ctx->table_schema->fields()->Get(ctx->field_index);
  if (ctx->elem_index >= 0) {
    // array
    auto vec = flatbuffers::GetFieldAnyV(*ctx->table, *field);
    value = flatbuffers::GetAnyVectorElemI(vec, type, ctx->elem_index);
    ++ctx->elem_index;
  } else {
    value = flatbuffers::GetFieldI<T>(*ctx->table, *field);
  }
  Debug(std::cout << "readIntValue value " << value << " type " << type);
  Debug(if (ctx->elem_index >= 0) {
      std::cout << " elem-index " << (ctx->elem_index - 1)
      << " exp-size " << ctx->vec_size; });
  Debug(std::cout << std::endl);

  postReadValue();
  return 0;
}

}
}
}
#endif
