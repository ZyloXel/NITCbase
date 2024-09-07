#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include <iostream>
#include <cstring>

// Stage 1
void print_fn()
{

  // print hello message
  Disk disk_run;                    // initialise a disk object for reading/writing blocks
  unsigned char buffer[BLOCK_SIZE]; // buffer created to hold block data
  Disk::readBlock(buffer, 7000);    // read the block number 7000 into the buffer
  char message[] = "hello";
  memcpy(buffer + 20, message, 6); // copy meassge into the buffer at offset 20
  Disk::writeBlock(buffer, 7000);  // write the modified buffer back to block number 7000

  unsigned char buffer2[BLOCK_SIZE]; // another buffer to read back the modified block
  char message2[6];                  // buffer to hold the read message
  Disk::readBlock(buffer2, 7000);
  memcpy(message2, buffer2 + 20, 6); // copy the message from buffer2 at offset 20 to message2
  std::cout << message2;
}

// Stage 2
// excercise 1. Fn to print relations and their attributes from catalogs
void print_attr()
{
  // create objects for the relation catalog and attribute catalog
  RecBuffer relCatBuffer(RELCAT_BLOCK);   // Initialise RecBuffer for relation catalog
  RecBuffer attrCatBuffer(ATTRCAT_BLOCK); // Initialise AttrBuffer for attribute catalog

  HeadInfo relCatHeader;  // Declare struct HeadInfo for relation catalog header
  HeadInfo attrCatHeader; // Declare struct HeadInfo for attribute catalog header

  // load the headers of both the blocks into relCatHeader and attrCatHeader.
  relCatBuffer.getHeader(&relCatHeader);
  attrCatBuffer.getHeader(&attrCatHeader);

  // Iteration over each entry in relation catalog
  for (int i = 0; i < relCatHeader.numEntries; i++)
  {

    Attribute relCatRecord[RELCAT_NO_ATTRS]; // array declared to store relation catalog record

    relCatBuffer.getRecord(relCatRecord, i); // load the record at index i into relCatRecord(current record)

    printf("Relation: %s\n", relCatRecord[RELCAT_REL_NAME_INDEX].sVal); // prints the name of the relation from relation catalog

    // Process all attribute blocks for the current relation
    int attrCatBlock = ATTRCAT_BLOCK; // Initialise the attribute catalog block number to start reading

    // Reads through the attribute catalog linked by rblock pointers
    while (attrCatBlock != -1)
    {
      attrCatBuffer = RecBuffer(attrCatBlock);
      attrCatBuffer.getHeader(&attrCatHeader); // load the header into attrCatHeader
      for (int j = 0; j < attrCatHeader.numEntries; j++)
      {
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS]; // array declared to store the attribute catalog record
        attrCatBuffer.getRecord(attrCatRecord, j); // load the record at index j into attrCatRecord

        // Checks if attribute belongs to the current relation
        if (strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relCatRecord[RELCAT_REL_NAME_INDEX].sVal) == 0)
        {
          const char *attrType = attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER ? "NUM" : "STR"; // attribute type determination
          printf("  %s: %s\n", attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrType);                  // prints attribute name and type
        }
      }
      attrCatBlock = attrCatHeader.rblock; // updating block number to the next block
    }
  }
}

// excercise 2. Rename attribute class to batch
void rename_attr()
{
  RecBuffer attrCatBuffer(ATTRCAT_BLOCK);  // Initialise AttrBuffer for the attribute catalog
  HeadInfo attrCatHeader;                  // Declare struct HeadInfo for attribute catalog header
  attrCatBuffer.getHeader(&attrCatHeader); // loads the header of the block to attrCatHeader

  // Process all attribute blocks for the current relation
  int attrCatBlock = ATTRCAT_BLOCK; // Initialise the attribute catalog block number to start reading

  while (attrCatBlock != -1)
  {
    attrCatBuffer = RecBuffer(attrCatBlock);
    attrCatBuffer.getHeader(&attrCatHeader);
    for (int i = 0; i < attrCatHeader.numEntries; i++)
    {
      Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
      attrCatBuffer.getRecord(attrCatRecord, i);
      if (strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, "Students") == 0)
      {
        if (strcmp(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, "Class") == 0)
        {
          strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, "Batch");
          attrCatBuffer.setRecord(attrCatRecord, i);
          return;
        }
      }
    }
    attrCatBlock = attrCatHeader.rblock;
  }
}

// Stage 3
// excercise to print the catalog entries for the relation students
void read_cache()
{
  for (int rel_Id = RELCAT_RELID; rel_Id <= 2; ++rel_Id)
  {
    RelCatEntry relcatbuf;
    RelCacheTable::getRelCatEntry(rel_Id, &relcatbuf);
    printf("Relation: %s\n", relcatbuf.relName);

    for (int i = 0; i < relcatbuf.numAttrs; i++)
    {
      AttrCatEntry attrcatbuf;
      AttrCacheTable::getAttrCatEntry(rel_Id, i, &attrcatbuf); // attribute offset i
      printf("  %s: ", attrcatbuf.attrName);
      if(attrcatbuf.attrType==NUMBER){
        printf("NUM \n");
      }
      else{
        printf("STR \n");
      }
    }
  }
}

int main(int argc, char *argv[]) {
  Disk disk_run;
  StaticBuffer buffer;
  OpenRelTable cache;

  return FrontendInterface::handleFrontend(argc, argv);
}
