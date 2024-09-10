#include "BlockBuffer.h"
#include <iostream>
#include <cstdlib>
#include <cstring>
// the declarations for these functions can be found in "BlockBuffer.h"

int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType)
{
  int diff;
  (attrType == NUMBER)
      ? diff = attr1.nVal - attr2.nVal
      : diff = strcmp(attr1.sVal, attr2.sVal);
  if (diff > 0)
    return 1; // attr1 > attr2
  else if (diff < 0)
    return -1; // attr 1 < attr2
  else
    return 0;
}

BlockBuffer::BlockBuffer(int blockNum)
{

  if (blockNum < 0 || blockNum >= DISK_BLOCKS)
    this->blockNum = E_DISKFULL;
  else
    this->blockNum = blockNum;
}

// calls the parent class constructor
RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

// load the block header into the argument pointer
int BlockBuffer::getHeader(struct HeadInfo *head)
{
  unsigned char buffer[BLOCK_SIZE];
  Disk::readBlock(buffer, this->blockNum);
  // read the block at this.blockNum into the buffer

  // populate the numEntries, numAttrs and numSlots fields in *head
  memcpy(&head->numSlots, buffer + 24, 4);
  memcpy(&head->numEntries, buffer + 16, 4);
  memcpy(&head->numAttrs, buffer + 20, 4);
  memcpy(&head->rblock, buffer + 12, 4);
  memcpy(&head->lblock, buffer + 8, 4);

  return SUCCESS;
}

// load the record at slotNum into the argument pointer
int RecBuffer::getRecord(union Attribute *rec, int slotNum)
{
  struct HeadInfo head; // Declare a HeadInfo structure to store the header information

  // Retrieve the header information using the getHeader function
  this->getHeader(&head);

  int attrCount = head.numAttrs; // Number of attributes in the record
  int slotCount = head.numSlots; // Number of slots in the block

  // Create a buffer to read the block data
  unsigned char buffer[BLOCK_SIZE];
  // Read the block at the current block number into the buffer
  Disk::readBlock(buffer, this->blockNum);

  /* Calculate the offset to the desired record in the buffer
     - HEADER_SIZE is the size of the header in the block
     - slotCount is the number of slots, and each slot occupies 1 byte
     - recordSize is the size of one record, calculated as attrCount * ATTR_SIZE
     - The record at slotNum is located at HEADER_SIZE + slotMapSize + (recordSize * slotNum)
  */
  int recordSize = attrCount * ATTR_SIZE; // Calculate the size of one record
  unsigned char *slotPointer = buffer + HEADER_SIZE + slotCount + (recordSize * slotNum);

  // Load the record data into the rec data structure
  memcpy(rec, slotPointer, recordSize);

  return SUCCESS; // Return SUCCESS to indicate the operation was successful
}

int RecBuffer::setRecord(union Attribute *rec, int slotNum)
{
  unsigned char *buffer_ptr;
  int ret = loadBlockAndGetBufferPtr(&buffer_ptr);
  if (ret != SUCCESS)
  {
    return ret; // return any errors that might have occured in the process
  }

  struct HeadInfo head;
  this->getHeader(&head);

  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;

  unsigned char buffer[BLOCK_SIZE];
  Disk::readBlock(buffer, this->blockNum);

  int recordSize = attrCount * ATTR_SIZE;
  unsigned char *slotPointer = buffer + HEADER_SIZE + slotCount + (recordSize * slotNum);

  memcpy(slotPointer, rec, recordSize);
  Disk::writeBlock(buffer, blockNum);

  return SUCCESS;
}

//Stage 4
int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr)
{
  // check whether the block is already present in the buffer using StaticBuffer.getBufferNum()
  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

  if (bufferNum == E_BLOCKNOTINBUFFER)
  {
    bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

    if (bufferNum == E_OUTOFBOUND)
    {
      return E_OUTOFBOUND;
    }

    Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
  }

  // store the pointer to this buffer (blocks[bufferNum]) in *buffPtr
  *buffPtr = StaticBuffer::blocks[bufferNum];

  return SUCCESS;
}

//Stage 4
/* used to get the slotmap from a record block
NOTE: this function expects the caller to allocate memory for `*slotMap`
*/
int RecBuffer::getSlotMap(unsigned char *slotMap) {
  unsigned char *bufferPtr;

  // get the starting address of the buffer containing the block using loadBlockAndGetBufferPtr().
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }

  struct HeadInfo head;
  // get the header of the block using getHeader() function
  ret = getHeader(&head);

  int slotCount = head.numSlots;

  // get a pointer to the beginning of the slotmap in memory by offsetting HEADER_SIZE
  unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;

  // copy the values from `slotMapInBuffer` to `slotMap` (size is `slotCount`)
  memcpy(slotMap, slotMapInBuffer, slotCount);

  return SUCCESS;
}