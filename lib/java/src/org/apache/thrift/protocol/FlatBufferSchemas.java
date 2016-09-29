package org.apache.thrift.protocol;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import com.google.flatbuffers.reflection.*;

public class FlatBufferSchemas {
  public FlatBufferSchemas(String bfbs_filename)
      throws FileNotFoundException, IOException {
    bfbs_filename_ = bfbs_filename;
    File file = new File(bfbs_filename);
    RandomAccessFile f = new RandomAccessFile(file, "r");
    byte[] data = new byte[(int)f.length()];
    f.readFully(data);
    f.close();
    bfbs_file_content_ = ByteBuffer.wrap(data);

    schema_ = Schema.getRootAsSchema(bfbs_file_content_);
  }

  public com.google.flatbuffers.reflection.Object getTableSchema(String table_name) {
    // TODO: use lookupByKey() once it's available.
    for (int i = 0; i < schema_.objectsLength(); ++i) {
      com.google.flatbuffers.reflection.Object obj = schema_.objects(i);
      if (obj.name().equals(table_name)) {
        return obj;
      }
    }
    return null;
  }

  public Field getFieldSchema(com.google.flatbuffers.reflection.Object table, String field_name) {
    // TODO: use lookupByKey() once it's available.
    for (int i = 0; i < table.fieldsLength(); ++i) {
      Field field = table.fields(i);
      if (field.name().equals(field_name)) {
        return field;
      }
    }
    return null;
  }

  public Field getFieldSchemaByThriftId(com.google.flatbuffers.reflection.Object table, int fieldId) {
    for (int i = 0; i < table.fieldsLength(); ++i) {
      Field field = table.fields(i);
      if (fieldId == getFieldThriftId(field)) {
        return field;
      }
    }
    return null;
  }

  public int getFieldThriftId(Field field) {
    return getFieldAttrIntValue(field, "thrift_id");
  }

  public int getFieldThriftType(Field field) {
    return getFieldAttrIntValue(field, "thrift_type");
  }

  public int getFieldAttrIntValue(Field field, String attr_name) {
    for (int i = 0; i < field.attributesLength(); ++i) {
      KeyValue attr = field.attributes(i);
      if (attr.key().equals(attr_name)) {
        return Integer.parseInt(attr.value());
      }
    }
    return 0;
  }

  public boolean isObjectType(Field field) {
    return field.type().baseType() == BaseType.Obj;
  }

  public boolean isVectorType(Field field) {
    return field.type().baseType() == BaseType.Vector;
  }

  public boolean isAuxVectorType(Field field) {
    return isAuxType(field, "list");
  }

  public boolean isAuxMapType(Field field) {
    return isAuxType(field, "map");
  }

  public boolean isAuxType(Field field, String val) {
    if (!isVectorType(field)) {
      return false;
    }
    if (field.type().element() != BaseType.Obj) {
      return false;
    }
    com.google.flatbuffers.reflection.Object table = schema_.objects(field.type().index());
    for (int i = 0; i < table.attributesLength(); ++i) {
      KeyValue attr = table.attributes(i);
      if ("aux_type".equals(attr.key()) && val.equals(attr.value())) {
        return true;
      }
    }
    return false;
  }

  // precondition: the type of field is a table
  public com.google.flatbuffers.reflection.Object getFieldTableSchema(Field field) {
    if (!isObjectType(field)) {
      throw new RuntimeException("Field " + field.name() + " is not an object.");
    }
    return schema_.objects(field.type().index());
  }

  // precondition: the type of field is an array of table
  public com.google.flatbuffers.reflection.Object getFieldElemTableSchema(Field field) {
    if (!isVectorType(field)) {
      throw new RuntimeException("Field " + field.name() + " is not a vector.");
    }
    if (field.type().element() != BaseType.Obj) {
      throw new RuntimeException("Field " + field.name() + " is not a vector of objects.");
    }
    return schema_.objects(field.type().index());
  }

  private String bfbs_filename_;
  private ByteBuffer bfbs_file_content_;
  private Schema schema_;
}
