#include "BlockBuffer.h"
#include <iostream>
#include <cstdlib>
#include <cstring>
// the declarations for these functions can be found in "BlockBuffer.h"

BlockBuffer::BlockBuffer(int blockNum) {

  if (blockNum < 0 || blockNum >= DISK_BLOCKS)
		this->blockNum = E_DISKFULL;
	else
		this->blockNum = blockNum;
}

// calls the parent class constructor
RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

// load the block header into the argument pointer
int BlockBuffer::getHeader(struct HeadInfo *head) {
  unsigned char* buffer_ptr;
  int ret=loadBlockAndGetBufferPtr(&buffer_ptr);
  if(ret!=SUCCESS){
    return ret;
  }
  
  unsigned char buffer[BLOCK_SIZE];
   Disk::readBlock(buffer,this->blockNum);
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
int RecBuffer::getRecord(union Attribute *rec, int slotNum) {
  unsigned char* buffer_ptr;
  int ret=loadBlockAndGetBufferPtr(&buffer_ptr);
  if(ret!=SUCCESS){
    return ret;
  }

  struct HeadInfo head;
  this->getHeader(&head);
  // get the header using this.getHeader() function

  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;
  
   unsigned char buffer[BLOCK_SIZE];
   Disk::readBlock(buffer, this->blockNum);

  // read the block at this.blockNum into a buffer

  /* record at slotNum will be at offset HEADER_SIZE + slotMapSize + (recordSize * slotNum)
     - each record will have size attrCount * ATTR_SIZE
     - slotMap will be of size slotCount
  */
  int recordSize = attrCount * ATTR_SIZE;
  unsigned char *slotPointer = buffer+HEADER_SIZE+slotCount+(recordSize*slotNum);

  // load the record into the rec data structure
  memcpy(rec, slotPointer, recordSize);

  return SUCCESS;
}

int RecBuffer::setRecord(union Attribute *rec, int slotNum) {
    unsigned char *buffer_ptr;
  int ret = loadBlockAndGetBufferPtr(&buffer_ptr);
  if (ret != SUCCESS) {
    return ret;   // return any errors that might have occured in the process
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

int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr) {
  // check whether the block is already present in the buffer using StaticBuffer.getBufferNum()
  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

  if (bufferNum == E_BLOCKNOTINBUFFER) {
    bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

    if (bufferNum == E_OUTOFBOUND) {
      return E_OUTOFBOUND;
    }

    Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
  }

  // store the pointer to this buffer (blocks[bufferNum]) in *buffPtr
  *buffPtr = StaticBuffer::blocks[bufferNum];

  return SUCCESS;
}
