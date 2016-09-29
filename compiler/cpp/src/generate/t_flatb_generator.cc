#include <fstream>
#include <iostream>
#include <sstream>
#include <limits>
#include <algorithm>
#include <set>

#include <stdlib.h>
#include <sys/stat.h>

#include "t_generator.h"
#include "platform.h"

using std::map;
using std::ofstream;
using std::ostringstream;
using std::string;
using std::vector;
using std::set;


// 1. typedef is ignored. It's just syntax suger. The original type is
// always used in translated flatbuffer.
// 2. enum is ignored. we always translate enum to 'int'. This will
// be correct though not very user friendly.
// TODO: translate it to flatbuffer enum.
// 3. const is ignored too. There is no equivalent support in flatbuffer.
// 4. service is ignored. It's not our intended use case.
// 5. exception is translated to table. There is no special exception
// support in flatbuffer.
// 6. only scalar default value is supported due to limitation of
// flatbuffer. An warning is reported if a field of string, list, etc
// has a default value. Note this won't affect correctness when convert
// a thrift object to flatbuffer object because thrift default value is
// serialized on wire.
// 7. set is tranlsated to list in flatbuffer.
// 8. map is translated to sorted list in the following way:
//  8.1 a internal flatbuffer table is created to store key and value of the map.
//  8.2 the map type is translated to a list of the above internal type.
//  For example: Thrift: 
//       map<i16, i64> field,
//      =>
//       Flatbuffer: 
//       field:[_short_long];
//       table short_long {
//          k:short (key);
//          v:long;
//       }
//  This allows us to use flatbuffers sorted list to implement map.
//  8.3 nested maps are supported, e.g.
//       map<i32, map<i64, i16>> field,
//      =>
//       Flatbuffer: 
//       field:[_int_map_long_short];
//       table _int_map_long_short {
//          k:int (key);
//          v:[_long_short];
//       }
//       table _long_short {
//          k:long (key);
//          v:short;
//       }
// 9. flatbuffer requires field id (if specified) start with 0 and is
// continuous while thrift has no such restriction. So in thrift
// one can assign a new field an id less than the largest existing
// id in the struct without any compatibility issue (as long as the
// id is not already used.) But this is not the case with flatbuffer.
// There is no way we can guarantee backward compatibility of
// translated flatbuffer for such practice.
//
// To solve this problem we developers have to adopt a practice
// that always assign new fields ids that are bigger than any existing
// ids in the struct. Then translated flatbuffer table will have fields
// sorted by thrift field ids such that new fields are always added to
// the end of the flatbuffer table making it backward compatible.
//
// E.g.
//
// struct A {
//   5: optional i32 f3,
//   1: optional i64 f1,
//   3: optional string f2,
// }
// 
// is translated to
//
// table A {
//   f1:long;
//   f2:string;
//   f3:int;
// }
//
// 10. all fields will be made optional in flatbuffer even if they
// are required in Thrift. Flatbuffer supports 'required' fields
// only for non-scalar fields. Due to this limit, we simply make
// all fields optional in the translated flatbuffer.
//
// 11. The id and type of thrift fields are recorded using flatbuffer
// attributes thrift_id & thrift_type. For example:
//
//     5: optional i32 f1
//   =>
//     f1:int (thrift_id: 5, thrift_type: 8)
//    
class t_flatb_generator : public t_generator {
public:
  t_flatb_generator(t_program* program,
                    const std::map<std::string, std::string>& parsed_options,
                    const std::string& option_string)
    : t_generator(program) {
    (void)parsed_options;
    (void)option_string;
    out_dir_base_ = "gen-flatb";
    std::map<std::string, std::string>::const_iterator iter;
    iter = parsed_options.find("namespacescope");
    if (iter != parsed_options.end()) {
      namespacescope_ = iter->second;
    } else {
      namespacescope_ = "flatb";
    }
    iter = parsed_options.find("namespacesuffix");
    if (iter != parsed_options.end()) {
      namespacesuffix_ = iter->second;
    } else {
      namespacesuffix_ = "";
    }
    iter = parsed_options.find("namespaceprefix");
    if (iter != parsed_options.end()) {
      namespaceprefix_ = iter->second;
    } else {
      namespaceprefix_ = "";
    }
  }

  virtual ~t_flatb_generator() {}

  /**
  * Init and close methods
  */

  void init_generator();
  void close_generator();

  void generate_typedef(t_typedef* ttypedef);
  void generate_enum(t_enum* tenum);
  void generate_const(t_const* tconst);
  void generate_struct(t_struct* tstruct);
  void generate_service(t_service* tservice);
  void generate_xception(t_struct* txception);

private:
  std::string namespacescope_;
  std::string namespacesuffix_;
  std::string namespaceprefix_;
  std::ofstream f_fbs_;
  set<string> aux_types_;

  string get_type_str(t_type* ttype,
                      const string& parent_type_name,
                      ostringstream& aux_types,
                      int& type_id,
                      string& warn,
                      string& err);

  string sanitize_name(const string& n) {
    if (n == "table") {
      return "table_";
    }  
    return n;
  }

  string get_namespace(t_program* program) {
    string ns = namespacescope_ == "_FILE_" ?
      program->get_name() : program->get_namespace(namespacescope_);
    if (ns.size() > 0) {
      return namespaceprefix_ + ns + namespacesuffix_;
    }
    return ns;
  }

  bool is_map_key_type(int type_id, const string& key_name);
};

void t_flatb_generator::init_generator() {
  MKDIR(get_out_dir().c_str());

  string f_fbs_name = get_out_dir() + program_->get_name() + ".fbs";
  f_fbs_.open(f_fbs_name.c_str());

  const vector<t_program*>& includes = program_->get_includes();
  for (size_t i = 0; i < includes.size(); ++i) {
    f_fbs_ << "include \"" << includes[i]->get_name() << ".fbs\";" << std::endl;
  }

  string ns = get_namespace(program_);
  if (ns.size() > 0) {
    f_fbs_ << "namespace " << ns << ";" << std::endl;
  }

  f_fbs_ << "attribute \"thrift_id\";" << std::endl;
  f_fbs_ << "attribute \"thrift_type\";" << std::endl;
  f_fbs_ << "attribute \"aux_type\";" << std::endl;

}

void t_flatb_generator::close_generator() {
  f_fbs_.close();
}

void t_flatb_generator::generate_typedef(t_typedef* ttypedef) { (void)ttypedef; }
void t_flatb_generator::generate_enum(t_enum* tenum) { (void)tenum;}
void t_flatb_generator::generate_const(t_const* tconst) { (void)tconst; }
void t_flatb_generator::generate_service(t_service* tservice) { (void)tservice; }
void t_flatb_generator::generate_xception(t_struct* txception) {
  generate_struct(txception);
}

string friendly_name(const string& type_name, int type_id, const string& strip_prefix) {
  string fixed = type_name;
  std::replace(fixed.begin(), fixed.end(), '.', '_');
  if (fixed[0] == '[' && fixed[fixed.size() - 1] == ']') {
      string stripped = fixed.substr(1, fixed.size() - 2);
      if (stripped.substr(0, strip_prefix.size()) == strip_prefix) {
        stripped = stripped.substr(strip_prefix.size());
      }
      string prefix;
      if (type_id == 13) {
        prefix = "map";
      } else if (type_id == 14) {
        prefix = "set";
      } else {
        prefix = "list";
      }
      if (stripped[0] != '_') {
        prefix += "_";
      }
      fixed = prefix + stripped;
  }
  return fixed;
}

string t_flatb_generator::get_type_str(t_type* ttype,
                                       const string& parent_type_name,
                                       ostringstream& aux_types,
                                       int& type_id,
                                       string& warn,
                                       string& err) {
  type_id = -1;
  ttype = ttype->get_true_type();
  t_base_type::t_base base = ((t_base_type*)ttype)->get_base(); 
  if (ttype->is_base_type()) {
    switch (base) {
      case t_base_type::TYPE_STRING: 
          type_id = 11;  // protocol::TType::T_STRING 
          if (((t_base_type*)ttype)->is_binary()) {
              return "[byte]";
          }
          return "string";
      case t_base_type::TYPE_BOOL: type_id = 2; return "bool";  // protocol::TType::T_BOOL
      case t_base_type::TYPE_BYTE: type_id = 3; return "byte";  // protocol::TType::T_I08
      case t_base_type::TYPE_I16:  type_id = 6; return "short"; // protocol::TType::T_I16
      case t_base_type::TYPE_I32:  type_id = 8; return "int";   // protocol::TType::T_I32
      case t_base_type::TYPE_I64:  type_id = 10; return "long";     // protocol::TType::T_I64
      case t_base_type::TYPE_DOUBLE: type_id = 4; return "double";  // protocol::TType::T_DOUBLE
      default: 
           err.append("base type is not supported: ")
              .append(t_base_type::t_base_name(base))
              .append("\n");
           return "Not-supported: " + t_base_type::t_base_name(base);
    }
  }

  if (ttype->is_enum()) {
    type_id = 8;  // protocol::TType::T_I32
    return "int";
  }
  if (ttype->is_struct() || ttype->is_xception()) {
    type_id = 12;  // protocol::TType::T_STRUCT
    t_program* program = ttype->get_program();
    string ns = get_namespace(program);
    if (ns.size() > 0) {
        return ns + "." + ttype->get_name();
    }
    return ttype->get_name();
  }
  if (ttype->is_list() || ttype->is_set()) {
    if (ttype->is_list()) {
        type_id = 15;  // protocol::TType::T_LIST
    } else {
        type_id = 14;  // protocol::TType::T_SET
    }
    int elem_type_id;
    string elem_type_str = get_type_str(
        ((t_list*)ttype)->get_elem_type(), parent_type_name,
        aux_types, elem_type_id, warn, err);
    if (elem_type_id == 13 || elem_type_id == 15 || elem_type_id == 14
        || (elem_type_id == 11 && elem_type_str == "[byte]")) {
      // create an aux type for the list
      string parent_type_prefix = "_" + parent_type_name + "_";
      string elem_type_name = parent_type_prefix
          + friendly_name(elem_type_str, elem_type_id, parent_type_prefix);
      if (aux_types_.insert(elem_type_name).second) {
        aux_types << std::endl << "table " << elem_type_name
            << " (aux_type: \"list\") {" << std::endl;
        aux_types << "  v:" << elem_type_str << " (thrift_type: "
            << elem_type_id << ");" << std::endl;
        aux_types << "}" << std::endl;
      }
      return "[" + elem_type_name + "]";
    }
    return "[" + elem_type_str + "]";
  }
  if (ttype->is_map()) {
    type_id = 13;  // protocol::TType::T_MAP
    t_map* mtype = (t_map*)ttype;
    int key_type_id, val_type_id;
    string key_type_name = get_type_str(mtype->get_key_type(), parent_type_name,
        aux_types, key_type_id, warn, err);
    string val_type_name = get_type_str(mtype->get_val_type(), parent_type_name,
        aux_types, val_type_id, warn, err);

    string parent_type_prefix = "_" + parent_type_name + "_";
    string map_entry_type_name = parent_type_prefix
        + friendly_name(key_type_name, key_type_id, parent_type_prefix)
        + "_"
        + friendly_name(val_type_name, val_type_id, parent_type_prefix);

    bool sorted = true;
    if (!is_map_key_type(key_type_id, key_type_name)) {
      warn.append("map key (type ")
          .append(key_type_name)
          .append(") will not be sorted.\n");
      sorted = false;
    }

    // create an aux type for the map.
    if (aux_types_.insert(map_entry_type_name).second) {
        // output the internal map's definition if it's new.
        aux_types << std::endl << "table " << map_entry_type_name
            << " (aux_type: \"map\") {" << std::endl;
        aux_types << "  k:" << key_type_name << (sorted ? " (key, " : "(")
            << "thrift_type: " << key_type_id << ");" << std::endl; 
        aux_types << "  v:" << val_type_name << " (thrift_type: " 
            << val_type_id << ");" << std::endl; 
        aux_types << "}" << std::endl;
    }
    
    return "[" + map_entry_type_name + "]";
  }

  err.append("type is not supported: ")
     .append(ttype->get_name())
     .append("\n");
  return "Not-supported: " + ttype->get_name();
}

bool get_const_value(t_const_value* value, string& out, string& err) {
  std::ostringstream strs;
  string err_type;

  switch (value->get_type()) {
    case t_const_value::CV_IDENTIFIER:
    case t_const_value::CV_INTEGER:
      strs << value->get_integer();
      break;

    case t_const_value::CV_DOUBLE:
      strs << value->get_double();
      break;

    case t_const_value::CV_STRING:
      err_type = "string";
      break;

    case t_const_value::CV_MAP:
      err_type = "map";
      break;

    case t_const_value::CV_LIST:
      err_type = "list";
      break;

    default:
      err_type = "unknown-type";
  }
  if (err_type.size() > 0) {
    err.append("default value is ignored for \"")
       .append(err_type)
       .append("\" type.\n");
    return false;
  }

  out = strs.str();
  return true;
}

bool t_flatb_generator::is_map_key_type(int type_id, const string& type_name) {
  return (type_id > 1 && type_id < 12 && type_name != "[byte]");
}

void t_flatb_generator::generate_struct(t_struct* tstruct) {
  f_fbs_ << std::endl << "table "
      << sanitize_name(tstruct->get_name()) << " {" << std::endl;

  ostringstream aux_types;
  indent_up();
  vector<t_field*> members = tstruct->get_sorted_members();
  vector<t_field*>::iterator mem_iter;
  for (mem_iter = members.begin(); mem_iter != members.end(); mem_iter++) {
      t_field* field = *mem_iter;
    string err;
    string warn;
    int type_id;
    indent(f_fbs_) << sanitize_name(field->get_name()) << ":"
        << get_type_str(field->get_type(), tstruct->get_name(),
                        aux_types, type_id, warn, err);

    if (field->get_value()) {
        string val;
        if (get_const_value(field->get_value(), val, warn)) {
          f_fbs_ << " = " << val;
        }
    }

    f_fbs_ << " (thrift_id: " << field->get_key()
        << ", thrift_type: " << type_id << ")";
    f_fbs_ << ";" << std::endl;

    if (err.size() > 0) {
      throw "[flatb-error] " + tstruct->get_name()
        + "." + field->get_name() + ": " + err;
    }
    if (warn.size() > 0) {
      std::cerr << "[flatb-warn ] " << tstruct->get_name()
          << "." << field->get_name() << ": " << warn;
    }
  }
  indent_down();
  f_fbs_ << "}" << std::endl;
  f_fbs_ << aux_types.str();
}

THRIFT_REGISTER_GENERATOR(
    flatb,
    "Flatbuffer",
    "    namespacescope=flatb:  Which namespace scope to use for flatbuffer.\n"
    "                           Value _FILE_ indicates to use the file name as the namespace.\n"
    "    namespacesuffix='':  String to append to namespace.\n"
    "    namespaceprefix='':  String to prepend to namespace.\n")
