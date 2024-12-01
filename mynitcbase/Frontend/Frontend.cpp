#include "Frontend.h"

#include <cstring>
#include <iostream>

//Stage 8
int Frontend::create_table(char relname[ATTR_SIZE], int no_attrs, char attributes[][ATTR_SIZE], int type_attrs[]) {
  return Schema::createRel(relname, no_attrs, attributes, type_attrs);
}

int Frontend::drop_table(char relname[ATTR_SIZE]) {
  return Schema::deleteRel(relname);
}

int Frontend::open_table(char relname[ATTR_SIZE]) {
  return Schema::openRel(relname);
  // return SUCCESS;
}

int Frontend::close_table(char relname[ATTR_SIZE]) {
  return Schema::closeRel(relname);
  // return SUCCESS;
}

int Frontend::alter_table_rename(char relname_from[ATTR_SIZE], char relname_to[ATTR_SIZE]) {
  return Schema::renameRel(relname_from,relname_to);
  // return SUCCESS;
}

int Frontend::alter_table_rename_column(char relname[ATTR_SIZE], char attrname_from[ATTR_SIZE],
                                        char attrname_to[ATTR_SIZE]) {
  return Schema::renameAttr(relname,attrname_from,attrname_to);
  // return SUCCESS;
}

int Frontend::create_index(char relname[ATTR_SIZE], char attrname[ATTR_SIZE]) {
  // Schema::createIndex
  return SUCCESS;
}

int Frontend::drop_index(char relname[ATTR_SIZE], char attrname[ATTR_SIZE]) {
  // Schema::dropIndex
  return SUCCESS;
}

int Frontend::insert_into_table_values(char relname[ATTR_SIZE], int attr_count, char attr_values[][ATTR_SIZE]) {
  
  return Algebra::insert(relname,attr_count,attr_values);
  // return SUCCESS;
}

//Stage 9
int Frontend::select_from_table(char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE]) {

    // Call appropriate project() method of the Algebra Layer
  return Algebra::project(relname_source,relname_target);
    // Return Success or Error values appropriately
}

int Frontend::select_attrlist_from_table(char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE], int attr_count, char attr_list[][ATTR_SIZE]) {
  // Algebra::project
  return Algebra::project(relname_source,relname_target,attr_count,attr_list);
  //return SUCCESS;
}

int Frontend::select_from_table_where(char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE],
                                      char attribute[ATTR_SIZE], int op, char value[ATTR_SIZE]) {
  return Algebra::select(relname_source,relname_target,attribute,op,value);
  // return SUCCESS;
}

int Frontend::select_attrlist_from_table_where(char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE],
                                               int attr_count, char attr_list[][ATTR_SIZE],
                                               char attribute[ATTR_SIZE], int op, char value[ATTR_SIZE]) {
  // Algebra::select + Algebra::project??
  int ret=Algebra::select(relname_source,TEMP,attribute,op,value);
  if(ret!=SUCCESS){
    return ret;
  }
  ret=OpenRelTable::openRel(TEMP);
  if(ret<0 || ret>=MAX_OPEN){
    Schema::deleteRel(TEMP);
    return ret;
  }
  ret=Algebra::project(TEMP,relname_target,attr_count,attr_list);
  Schema::closeRel(TEMP);
  Schema::deleteRel(TEMP);
  return ret;
  //return SUCCESS;
}

int Frontend::select_from_join_where(char relname_source_one[ATTR_SIZE], char relname_source_two[ATTR_SIZE],
                                     char relname_target[ATTR_SIZE],
                                     char join_attr_one[ATTR_SIZE], char join_attr_two[ATTR_SIZE]) {
  // Algebra::join
  return SUCCESS;
}

int Frontend::select_attrlist_from_join_where(char relname_source_one[ATTR_SIZE], char relname_source_two[ATTR_SIZE],
                                              char relname_target[ATTR_SIZE],
                                              char join_attr_one[ATTR_SIZE], char join_attr_two[ATTR_SIZE],
                                              int attr_count, char attr_list[][ATTR_SIZE]) {
  // Algebra::join + project
  return SUCCESS;
}

int Frontend::custom_function(int argc, char argv[][ATTR_SIZE]) {
  // argc gives the size of the argv array
  // argv stores every token delimited by space and comma

  // implement whatever you desire

  return SUCCESS;
}