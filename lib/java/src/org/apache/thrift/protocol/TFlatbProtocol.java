package org.apache.thrift.protocol;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Stack;
import java.nio.ByteBuffer;
import org.apache.thrift.TException;
import com.google.flatbuffers.reflection.*;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.Table;

public class TFlatbProtocol extends TProtocol {
  private FlatBufferSchemas flatb_schema_;
  private String flatb_table_;
  private ByteBuffer flatb_bb_;

  // write
  private com.google.flatbuffers.reflection.Object root_schema_;
  private Stack<WriteContext> write_stack_;
  private FlatBufferBuilder fbb_;

  // Writing: Thrift => FlatBuffer
  public TFlatbProtocol(FlatBufferSchemas flatb_schema, String flatb_table) {
    super(null);
    flatb_schema_ = flatb_schema;
    flatb_table_ = flatb_table;
    flatb_bb_ = null;
    root_schema_ = null;
    write_stack_ = new Stack<WriteContext>();
    fbb_ = new FlatBufferBuilder();
  }

  /**
   * Writing functions
   */
  enum FieldTag {
    REGULAR,   // regular field in a user defined table.
    MAP_KEY,   // key field "k" in an aux table for map
    MAP_VAL,   // value field "v" in an aux table for map.
    LIST_VAL,  // value field "v" in an aux table for nested list
  }

  enum VectorTag {
    LIST,      // it's a usual thrift list
    AUX_MAP,   // it's a thrift map
    AUX_LIST,  // it's a nested thrift list
  }

  class FieldVal {
    FieldVal(byte val) { integer_val = val; }
    FieldVal(short val) { integer_val = val; }
    FieldVal(int val) { integer_val = val; }
    FieldVal(long val) { integer_val = val; }
    FieldVal(boolean val) { integer_val = val ? 1 : 0; }
    FieldVal(double val) { real_val = val; }

    long integer_val;
    double real_val;
  }

  class IntFieldValComparator implements Comparator<FieldVal> {
    @Override
    public int compare(FieldVal a, FieldVal b) {
      if (a.integer_val < b.integer_val) return -1;
      if (a.integer_val > b.integer_val) return 1;
      return 0;
    }
  }

  class RealFieldValComparator implements Comparator<FieldVal> {
    @Override
    public int compare(FieldVal a, FieldVal b) {
      if (a.real_val < b.real_val) return -1;
      if (a.real_val > b.real_val) return 1;
      return 0;
    }
  }

  // Store data about a field being written.
  //
  // A field can be of the usual scalar type, or a table (struct), or a list
  // of scalar or table objects (list & map).
  class WriteContext {
    WriteContext(Field field) {
      field_schema = field;
      tag = FieldTag.REGULAR;
      vec_tag = VectorTag.LIST;
    }

    // The flatbuffer schema of the field being written.
    Field field_schema;

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
    ArrayList<WriteContext> table_fields;

    // An intermediate storage to hold all elements in a vector.
    // They'll get rolled into val when all elements are collected.
    ArrayList<FieldVal> elems;
  }

  // Helper class for creating sorted vectors
  static class TableOfLongKey extends Table {
    int keyOffset_;
    TableOfLongKey(int keyOffset) {
      keyOffset_ = keyOffset;
    }
    @Override
    protected int keysCompare(Integer o1, Integer o2, ByteBuffer _bb) {
      long val_1 = _bb.getLong(__offset(keyOffset_, o1, _bb));
      long val_2 = _bb.getLong(__offset(keyOffset_, o2, _bb));
      return val_1 > val_2 ? 1 : val_1 < val_2 ? -1 : 0;
    }
  }
  static class TableOfIntKey extends Table {
    int keyOffset_;
    TableOfIntKey(int keyOffset) {
      keyOffset_ = keyOffset;
    }
    @Override
    protected int keysCompare(Integer o1, Integer o2, ByteBuffer _bb) {
      int val_1 = _bb.getInt(__offset(keyOffset_, o1, _bb));
      int val_2 = _bb.getInt(__offset(keyOffset_, o2, _bb));
      return val_1 > val_2 ? 1 : val_1 < val_2 ? -1 : 0;
    }
  }
  static class TableOfShortKey extends Table {
    int keyOffset_;
    TableOfShortKey(int keyOffset) {
      keyOffset_ = keyOffset;
    }
    @Override
    protected int keysCompare(Integer o1, Integer o2, ByteBuffer _bb) {
      short val_1 = _bb.getShort(__offset(keyOffset_, o1, _bb));
      short val_2 = _bb.getShort(__offset(keyOffset_, o2, _bb));
      return val_1 > val_2 ? 1 : val_1 < val_2 ? -1 : 0;
    }
  }
  static class TableOfByteKey extends Table {
    int keyOffset_;
    TableOfByteKey(int keyOffset) {
      keyOffset_ = keyOffset;
    }
    @Override
    protected int keysCompare(Integer o1, Integer o2, ByteBuffer _bb) {
      byte val_1 = _bb.get(__offset(keyOffset_, o1, _bb));
      byte val_2 = _bb.get(__offset(keyOffset_, o2, _bb));
      return val_1 > val_2 ? 1 : val_1 < val_2 ? -1 : 0;
    }
  }
  static class TableOfDoubleKey extends Table {
    int keyOffset_;
    TableOfDoubleKey(int keyOffset) {
      keyOffset_ = keyOffset;
    }
    @Override
    protected int keysCompare(Integer o1, Integer o2, ByteBuffer _bb) {
      double val_1 = _bb.getDouble(__offset(4, o1, _bb));
      double val_2 = _bb.getDouble(__offset(4, o2, _bb));
      return val_1 > val_2 ? 1 : val_1 < val_2 ? -1 : 0;
    }
  }
  static class TableOfStringKey extends Table {
    int keyOffset_;
    TableOfStringKey(int keyOffset) {
      keyOffset_ = keyOffset;
    }
    @Override
    protected int keysCompare(Integer o1, Integer o2, ByteBuffer _bb) {
      return compareStrings(
          __offset(keyOffset_, o1, _bb), __offset(keyOffset_, o2, _bb), _bb);
    }
  }

  public void writeStructBegin(TStruct struct) throws TException {
    if (write_stack_.size() == 0) {
      // Construct a special root field.
      com.google.flatbuffers.reflection.Object table_schema =
          flatb_schema_.getTableSchema(flatb_table_);
      if (table_schema == null) {
        throw new TException("Can't find table "
            + flatb_table_ + "in schema file");
      }
      root_schema_ = table_schema;
      write_stack_.push(new WriteContext(null));
    }

    write_stack_.peek().table_fields = new ArrayList<WriteContext>();
  }

  public void writeStructEnd() throws TException {
    // This is the only place we can reliably know a struct
    // is done. We take the opportunity to process the table.
    WriteContext finfo = write_stack_.peek();
    rollUpTable(finfo, getCurrentWrittenTableSchema());

    if (write_stack_.size() == 1) {
      // We are done now.
      fbb_.finish((int)finfo.val.integer_val);
      flatb_bb_ = fbb_.dataBuffer();
      // clean up
      write_stack_.pop();
    } else {
      recordFieldValue(finfo, finfo.val);
    }
  }

  public void writeFieldBegin(TField field) throws TException {
    com.google.flatbuffers.reflection.Object table_schema = getCurrentWrittenTableSchema();
    Field field_schema = flatb_schema_.getFieldSchema(
        table_schema, field.name);
    if (field_schema == null) {
      // search by fieldId. This is less efficient than search by name.
      field_schema = flatb_schema_.getFieldSchemaByThriftId(
          table_schema, field.id);
    }
    if (field_schema == null) {
      throw new TException("Can't find field " + field.name
          + " in table " + table_schema.name());
    }
    write_stack_.push(new WriteContext(field_schema));
  }

  public void writeFieldEnd() throws TException {
    WriteContext finfo = write_stack_.pop();

    // Save the field to the previous level (a field of either table
    // or list of tables), which takes ownership of finfo.
    write_stack_.peek().table_fields.add(finfo);

    // Special handling for aux fields.
    switch (finfo.tag) {
      case MAP_KEY : 
        // Need to explicitly trigger value field "v" start.
        writeAuxFieldBegin("v", FieldTag.MAP_VAL);
        break;
      case MAP_VAL:
      case LIST_VAL:
        // value field is done.
        // we need to explicitly trigger map/list aux struct end.
        writeStructEnd();
        break;
      default:
        break;
    }
  }

  public void writeListBegin(TList list) throws TException {
    writeListBegin(list.size);
  }

  private void writeListBegin(int size) throws TException {
    // update the expected size for the current vector field.
    WriteContext finfo = write_stack_.peek();
    finfo.vec_size = size;
    finfo.elems = new ArrayList<FieldVal>();
    if (size == 0) {
      // rollup the empty vector.
      postWriteValue(finfo);
    } else if (flatb_schema_.isAuxVectorType(finfo.field_schema)) {
      finfo.vec_tag = VectorTag.AUX_LIST;
      writeStructBegin(null);
      writeAuxFieldBegin("v", FieldTag.LIST_VAL);
    }
  }

  public void writeListEnd() throws TException {}

  public void writeSetBegin(TSet set) throws TException {
    writeListBegin(set.size);
  }

  public void writeSetEnd() throws TException {}

  public void writeMapBegin(TMap map) throws TException {
    WriteContext finfo = write_stack_.peek();
    finfo.vec_size = map.size;
    finfo.vec_tag = VectorTag.AUX_MAP;
    finfo.elems = new ArrayList<FieldVal>();
    if (map.size == 0) {
      postWriteValue(finfo);
    } else {
      writeStructBegin(null);
      writeAuxFieldBegin("k", FieldTag.MAP_KEY);
    }
  }

  public void writeMapEnd() throws TException {}

  public void writeBool(boolean b) throws TException {
    recordFieldValue(new FieldVal(b));
  }

  public void writeByte(byte b) throws TException {
    recordFieldValue(new FieldVal(b));
  }

  public void writeI16(short i16) throws TException {
    recordFieldValue(new FieldVal(i16));
  }

  public void writeI32(int i32) throws TException {
    recordFieldValue(new FieldVal(i32));
  }

  public void writeI64(long i64) throws TException {
    recordFieldValue(new FieldVal(i64));
  }

  public void writeDouble(double dub) throws TException {
    recordFieldValue(new FieldVal(dub));
  }

  public void writeString(String str) throws TException {
    recordFieldValue(new FieldVal(fbb_.createString(str)));
  }

  public void writeBinary(ByteBuffer bin) throws TException {
    byte[] arr = new byte[bin.remaining()];
    bin.get(arr);
    recordFieldValue(new FieldVal(fbb_.createByteVector(arr)));
  }

  public void writeMessageBegin(TMessage message) throws TException {}
  public void writeMessageEnd() throws TException {}
  public void writeFieldStop() throws TException {}

  /**
   * Reading functions
   */
  public TStruct readStructBegin() throws TException {
    throw new TException("Not implemented"); 
  }

  public void readStructEnd() throws TException {
    throw new TException("Not implemented"); 
  }

  public TField readFieldBegin() throws TException {
    throw new TException("Not implemented"); 
  }

  public TMap readMapBegin() throws TException {
    throw new TException("Not implemented"); 
  }

  public TList readListBegin() throws TException {
    throw new TException("Not implemented"); 
  }

  public TSet readSetBegin() throws TException {
    throw new TException("Not implemented"); 
  }

  public boolean readBool() throws TException {
    throw new TException("Not implemented"); 
  }

  public byte readByte() throws TException {
    throw new TException("Not implemented"); 
  }

  public short readI16() throws TException {
    throw new TException("Not implemented"); 
  }

  public int readI32() throws TException {
    throw new TException("Not implemented"); 
  }

  public long readI64() throws TException {
    throw new TException("Not implemented"); 
  }

  public double readDouble() throws TException {
    throw new TException("Not implemented"); 
  }

  public String readString() throws TException {
    throw new TException("Not implemented"); 
  }

  public ByteBuffer readBinary() throws TException {
    throw new TException("Not implemented"); 
  }

  public TMessage readMessageBegin() throws TException {
    throw new TException("Not implemented"); 
  }

  public void readMessageEnd() throws TException {}
  public void readFieldEnd() throws TException {}
  public void readMapEnd() throws TException {}
  public void readListEnd() throws TException {}
  public void readSetEnd() throws TException {}

  /*
   * Accessors
   */
  public ByteBuffer getFlatbByteBuffer() {
    return flatb_bb_;
  }

  private Table newTableOfKey(byte keyType, int keyOffset) {
    switch (keyType) {
      case BaseType.Long: return new TableOfLongKey(keyOffset);
      case BaseType.Int: return new TableOfIntKey(keyOffset);
      case BaseType.Short: return new TableOfShortKey(keyOffset);
      case BaseType.Bool:
      case BaseType.Byte: return new TableOfByteKey(keyOffset);
      case BaseType.Double: return new TableOfDoubleKey(keyOffset);
      case BaseType.String: return new TableOfStringKey(keyOffset);
      default: return null;
    }
  }

  private void rollUpVector(WriteContext finfo) throws TException {
    Field field_schema = finfo.field_schema;
    if (!flatb_schema_.isVectorType(field_schema)) {
      throw new TException("field " + field_schema.name()
          + " is NOT a vector type as it's supposed to be.");
    }
    boolean isSet =
      flatb_schema_.getFieldThriftType(field_schema) == TType.SET;
    switch (field_schema.type().element()) {
      case BaseType.Bool :
        {
          if (isSet) {
            Collections.sort(finfo.elems, new IntFieldValComparator());
          }
          fbb_.startVector(1, finfo.elems.size(), 1);
          for (int i = finfo.elems.size() - 1; i >= 0; --i) {
            fbb_.addBoolean(finfo.elems.get(i).integer_val > 0);
          }
          finfo.val = new FieldVal(fbb_.endVector());
          break;
        }
      case BaseType.Byte :
        // byte array (thrift binary) is already rolled up.
        // nothing to do here.
        break;
      case BaseType.Short :
        {
          if (isSet) {
            Collections.sort(finfo.elems, new IntFieldValComparator());
          }
          fbb_.startVector(2, finfo.elems.size(), 2);
          for (int i = finfo.elems.size() - 1; i >= 0; --i) {
            fbb_.addShort((short)finfo.elems.get(i).integer_val);
          }
          finfo.val = new FieldVal(fbb_.endVector());
          break;
        }
      case BaseType.Int :
        {
          if (isSet) {
            Collections.sort(finfo.elems, new IntFieldValComparator());
          }
          fbb_.startVector(4, finfo.elems.size(), 4);
          for (int i = finfo.elems.size() - 1; i >= 0; --i) {
            fbb_.addInt((int)finfo.elems.get(i).integer_val);
          }
          finfo.val = new FieldVal(fbb_.endVector());
          break;
        }
      case BaseType.Long :
        {
          if (isSet) {
            Collections.sort(finfo.elems, new IntFieldValComparator());
          }
          fbb_.startVector(8, finfo.elems.size(), 8);
          for (int i = finfo.elems.size() - 1; i >= 0; --i) {
            fbb_.addLong(finfo.elems.get(i).integer_val);
          }
          finfo.val = new FieldVal(fbb_.endVector());
          break;
        }
      case BaseType.Double :
        {
          if (isSet) {
            Collections.sort(finfo.elems, new RealFieldValComparator());
          }
          fbb_.startVector(8, finfo.elems.size(), 8);
          for (int i = finfo.elems.size() - 1; i >= 0; --i) {
            fbb_.addDouble(finfo.elems.get(i).real_val);
          }
          finfo.val = new FieldVal(fbb_.endVector());
          break;
        }
      case BaseType.String :
        // TODO(xun): sort set<string>
      case BaseType.Vector :
      case BaseType.Obj :
        {
          if (flatb_schema_.isAuxMapType(field_schema)) {
            Field keyField = flatb_schema_.getFieldSchema(
                flatb_schema_.getFieldElemTableSchema(field_schema), "k");
            if (keyField != null) {
              Table table = newTableOfKey(
                  keyField.type().baseType(), keyField.offset());
              if (table != null) {
                int[] offsets = new int[finfo.elems.size()];
                for (int i = 0; i < finfo.elems.size(); i++) {
                  offsets[i] = (int)finfo.elems.get(i).integer_val;
                }

                finfo.val = new FieldVal(
                      fbb_.createSortedVectorOfTables(table, offsets));
                break;
              }
            }
          }

          // No sorting.
          fbb_.startVector(4, finfo.elems.size(), 4);
          for (int i = finfo.elems.size() - 1; i >= 0; --i) {
            fbb_.addOffset((int)finfo.elems.get(i).integer_val);
          }
          finfo.val = new FieldVal(fbb_.endVector());
          break;
        }
      default:
        {
          throw new TException("ERR: elem type "
              + field_schema.type().element()
              + " type " + field_schema.type().baseType()
              + " name " + field_schema.name());
        }
    }
  }


  void rollUpTable(WriteContext finfo, com.google.flatbuffers.reflection.Object table_schema)
      throws TException {
    fbb_.startObject(table_schema.fieldsLength());

    for (WriteContext field : finfo.table_fields) {
      addFieldToBuilder(field);
    }
    int offset = fbb_.endObject();

    finfo.val = new FieldVal(offset);
    // Clear table_fields in case the field is a list of tables,
    // in which case the next object in the list will start to
    // use table_fields.
    finfo.table_fields.clear();
  }

  void addFieldToBuilder(WriteContext finfo) throws TException {
    Field field_schema = finfo.field_schema;
    switch (field_schema.type().baseType()) {
      case BaseType.Bool :
          fbb_.addBoolean(field_schema.id(),
                          finfo.val.integer_val > 0,
                          field_schema.defaultInteger() > 0);
          break;
      case BaseType.Byte :
          fbb_.addByte(field_schema.id(),
                       (byte)finfo.val.integer_val,
                       (int)field_schema.defaultInteger());
          break;
      case BaseType.Short :
          fbb_.addShort(field_schema.id(),
                        (short)finfo.val.integer_val,
                        (int)field_schema.defaultInteger());
          break;
      case BaseType.Int :
          fbb_.addInt(field_schema.id(),
                      (int)finfo.val.integer_val,
                      (int)field_schema.defaultInteger());
          break;
      case BaseType.Long :
          fbb_.addLong(field_schema.id(),
                       finfo.val.integer_val,
                       field_schema.defaultInteger());
          break;
      case BaseType.Double :
          fbb_.addDouble(field_schema.id(),
                         finfo.val.real_val,
                         field_schema.defaultReal());
          break;
      case BaseType.String :
      case BaseType.Vector :
      case BaseType.Obj :
          fbb_.addOffset(field_schema.id(),
                         (int)finfo.val.integer_val,
                         0);
          break;
      default: throw new TException("Impossible field type "
                   + field_schema.type().baseType());
    }
  }

  void recordFieldValue(FieldVal val) throws TException {
    recordFieldValue(write_stack_.peek(), val);
  }

  void recordFieldValue(WriteContext finfo, FieldVal val) 
      throws TException {
    Field field_schema = finfo.field_schema;
    switch (field_schema.type().baseType()) {
      case BaseType.Bool :
      case BaseType.Byte :
      case BaseType.Short :
      case BaseType.Int :
      case BaseType.Long :
      case BaseType.Double :
      case BaseType.String :
      case BaseType.Obj :
          finfo.val = val;
          break;
      case BaseType.Vector :
          if (field_schema.type().element() == BaseType.Byte) {
            // byte array (thrift binary) is treated as string.
            finfo.val = val;
          } else {
            finfo.elems.add(val);
          }
          break;
      default: throw new TException("Impossible field type "
                   + field_schema.type().baseType());
    } 

    postWriteValue(finfo);
  }

  void writeAuxFieldBegin(String name, FieldTag tag) throws TException {
    // Manually push in field "k".
    // fieldtype and thrift-id don't matter here.
    TField field = new TField(name, TType.STOP, (short)-1); 
    writeFieldBegin(field);
    // tag it.
    write_stack_.peek().tag = tag;
  }

  void postWriteValue(WriteContext finfo) throws TException {
    Field field_schema = finfo.field_schema;
    if (flatb_schema_.isVectorType(field_schema)
      && field_schema.type().element() != BaseType.Byte) {
      // vector type

      if (finfo.elems.size() < finfo.vec_size) {
        // The field is not done for we haven't collected all elements.
        if (finfo.vec_tag == VectorTag.AUX_MAP) {
          // it's a map, we need to kick off the next map
          // struct. writeStructBegin() is never interesting
          // so we start with its key field.
          writeStructBegin(null);
          writeAuxFieldBegin("k", FieldTag.MAP_KEY);
        } else if (finfo.vec_tag == VectorTag.AUX_LIST) {
          // it's a aux list. kick off the next element struct.
          writeStructBegin(null);
          writeAuxFieldBegin("v", FieldTag.LIST_VAL);
        }
        return;
      }

      // we have collected all the elements of the vector/map.
      // roll it up.
      rollUpVector(finfo);
    }

    // The field is done. We'll manually trigger writeFieldEnd()
    // if the current field is a map key/val field.
    if (finfo.tag != FieldTag.REGULAR) {
      // explicitly trigger field-end for finfo.
      writeFieldEnd();
    }
  }

  com.google.flatbuffers.reflection.Object getCurrentWrittenTableSchema() {
    if (write_stack_.size() == 1) {
      // root
      return root_schema_;
    }

    Field field_schema = write_stack_.peek().field_schema;
    if (flatb_schema_.isObjectType(field_schema)) {
      return flatb_schema_.getFieldTableSchema(field_schema);
    }
    // Must be a vector of tables.
    return flatb_schema_.getFieldElemTableSchema(field_schema);
  }
}
