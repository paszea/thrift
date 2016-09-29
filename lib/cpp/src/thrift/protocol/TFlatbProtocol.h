#ifndef _THRIFT_PROTOCOL_TFLATBPROTOCOL_H_
#define _THRIFT_PROTOCOL_TFLATBPROTOCOL_H_ 1
#include <thrift/protocol/TVirtualProtocol.h>
#include <thrift/protocol/FlatBufferSchemas.h>

#include <stack>
#include <iostream>
#include <boost/shared_ptr.hpp>

#include "flatbuffers/reflection.h"

namespace apache {
namespace thrift {
namespace protocol {

enum FieldTag : int;
enum TableTag : int;
union FieldVal;
struct WriteContext;
struct ReadContext;

class TFlatbProtocol : public TVirtualProtocol<TFlatbProtocol> {
public:
  // Writing: Thrift => FlatBuffer
  TFlatbProtocol(const FlatBufferSchemas* flatb_schema, const std::string& flatb_table)
    : TVirtualProtocol<TFlatbProtocol>(boost::shared_ptr<TTransport>()),
      flatb_schema_(flatb_schema),
      flatb_bytes_(0),
      root_schema_(nullptr),
      write_flatb_size_(0),
      write_data_size_(0),
      flatb_table_(flatb_table) {
  }

  // Reading: FlatBuffer => Thrift 
  TFlatbProtocol(const FlatBufferSchemas* flatb_schema, const std::string& flatb_table, const char* flatb_bytes)
    : TVirtualProtocol<TFlatbProtocol>(boost::shared_ptr<TTransport>()),
      flatb_schema_(flatb_schema),
      flatb_bytes_(flatb_bytes),
      root_schema_(nullptr),
      write_flatb_size_(0),
      write_data_size_(0),
      flatb_table_(flatb_table) {
  }

  ~TFlatbProtocol() { }

  /**
   * Writing functions
   */

  uint32_t writeStructBegin(const char* name);

  uint32_t writeStructEnd();

  uint32_t writeFieldBegin(const char* name, const TType fieldType, const int16_t fieldId);

  uint32_t writeFieldEnd();

  uint32_t writeListBegin(const TType elemType, const uint32_t size);

  uint32_t writeListEnd();

  uint32_t writeSetBegin(const TType elemType, const uint32_t size);

  uint32_t writeSetEnd();

  uint32_t writeMapBegin(const TType keyType, const TType valType, const uint32_t size);

  uint32_t writeMapEnd();

  uint32_t writeBool(const bool value);

  uint32_t writeByte(const int8_t byte);

  uint32_t writeI16(const int16_t i16);

  uint32_t writeI32(const int32_t i32);

  uint32_t writeI64(const int64_t i64);

  uint32_t writeDouble(const double dub);

  uint32_t writeString(const std::string& str);

  uint32_t writeBinary(const std::string& str);

  uint32_t writeMessageBegin(const std::string& name,
                             const TMessageType messageType,
                             const int32_t seqid) {
    return 0;
  }
  uint32_t writeMessageEnd() { return 0; }
  uint32_t writeFieldStop() { return 0; }

  /**
   * Reading functions
   */
  uint32_t readStructBegin(std::string& name);

  uint32_t readStructEnd();

  uint32_t readFieldBegin(std::string& name, TType& fieldType, int16_t& fieldId);

  uint32_t readMapBegin(TType& keyType, TType& valType, uint32_t& size);

  uint32_t readListBegin(TType& elemType, uint32_t& size);

  uint32_t readSetBegin(TType& elemType, uint32_t& size);

  uint32_t readBool(bool& value);

  // Provide the default readBool() implementation for std::vector<bool>
  using TVirtualProtocol<TFlatbProtocol>::readBool;

  uint32_t readByte(int8_t& value);

  uint32_t readI16(int16_t& value);

  uint32_t readI32(int32_t& value);

  uint32_t readI64(int64_t& value);

  uint32_t readDouble(double& value); 

  uint32_t readString(std::string& value);

  uint32_t readBinary(std::string& value);

  uint32_t readMessageBegin(std::string& name, TMessageType& messageType, int32_t& seqid) {
    return 0;
  }
  uint32_t readMessageEnd() { return 0; }
  uint32_t readFieldEnd() { return 0; }
  uint32_t readMapEnd() { return 0; }
  uint32_t readListEnd() { return 0; }
  uint32_t readSetEnd() { return 0; }

  /*
   * Accessors
   */
  const char* getFlatbBytes() const {
    return flatb_bytes_;
  }

  size_t getFlatbSize() const {
    return write_flatb_size_;
  }

  size_t getWriteDataSize() const {
    return write_data_size_;
  }

private:
  void rollUpVector(WriteContext* finfo);

  void rollUpTable(WriteContext* finfo,
                   const reflection::Object* table_schema);

  void addFieldToBuilder(const WriteContext* finfo);

  void recordFieldValue(const FieldVal& val);

  void recordFieldValue(WriteContext* finfo, const FieldVal& val);

  void writeAuxFieldBegin(const char* name, FieldTag tag);

  void postWriteValue(WriteContext* finfo);

  const reflection::Object* getCurrentWrittenTableSchema();

  void readAuxFieldBegin();

  void readAuxStructBegin(TableTag tag);

  void postReadValue();

  template<typename T, reflection::BaseType type>
  uint32_t readIntValue(T& value);

  //
  // Member variables
  //

  const FlatBufferSchemas* flatb_schema_;
  const char *flatb_bytes_;

  // write
  const reflection::Object* root_schema_;
  std::stack<WriteContext*> write_stack_;
  flatbuffers::FlatBufferBuilder fbb_;
  size_t write_flatb_size_;
  size_t write_data_size_;

  // read
  const std::string flatb_table_;
  std::stack<ReadContext*> read_stack_;
};

}
}
} // apache::thrift::protocol

#include <thrift/protocol/TFlatbProtocol.tcc>

#endif
