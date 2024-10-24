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

// constructor to initialise blockbuffer with a given number
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
  // memcpy(&head->numSlots, buffer + 24, 4);
  // memcpy(&head->numEntries, buffer + 16, 4);
  // memcpy(&head->numAttrs, buffer + 20, 4);
  // memcpy(&head->rblock, buffer + 12, 4);
  // memcpy(&head->lblock, buffer + 8, 4);

  memcpy(&head->pblock, buffer + 4, 4);
	memcpy(&head->lblock, buffer + 8, 4);
	memcpy(&head->rblock, buffer + 12, 4);
	memcpy(&head->numEntries, buffer + 16, 4);
	memcpy(&head->numAttrs, buffer + 20, 4);
	memcpy(&head->numSlots, buffer + 24, 4);
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

// Stage 6
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

  else
  {
    for (int i = 0; i < BUFFER_CAPACITY; i++)
    {
      if (StaticBuffer::metainfo[i].free == false)
      {
        StaticBuffer::metainfo[i].timeStamp++;
      }
    }
    StaticBuffer::metainfo[blockNum].timeStamp = 0;
  }

  // store the pointer to this buffer (blocks[bufferNum]) in *buffPtr
  *buffPtr = StaticBuffer::blocks[bufferNum];

  return SUCCESS;
}

/* used to get the slotmap from a record block
NOTE: this function expects the caller to allocate memory for `*slotMap`
*/
int RecBuffer::getSlotMap(unsigned char *slotMap)
{
  unsigned char *bufferPtr;

  // get the starting address of the buffer containing the block using loadBlockAndGetBufferPtr().
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS)
  {
    return ret;
  }

  struct HeadInfo head;
  this->getHeader(&head);
  // get the header of the block using getHeader() function

  int slotCount = head.numSlots;

  // get a pointer to the beginning of the slotmap in memory by offsetting HEADER_SIZE
  unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;

  // copy the values from `slotMapInBuffer` to `slotMap` (size is `slotCount`)
  for(int i=0;i<slotCount;i++){
    slotMap[i]=slotMapInBuffer[i];
  }

  return SUCCESS;
}

// Stage 7
// initialise blockbuffer with blocktype and find a free block
BlockBuffer::BlockBuffer(char blockType)
{
  // allocate a block on the disk and a buffer in memory to hold the new block of
  // given type using getFreeBlock function and get the return error codes if any.

  // set the blockNum field of the object to that of the allocated block
  // number if the method returned a valid block number,
  // otherwise set the error code returned as the block number.

  // (The caller must check if the constructor allocatted block successfully
  // by checking the value of block number field.)
  int blockTypeNo;
  switch (blockType)
  {
  case 'R':
    blockTypeNo = REC;
    break;
  case 'I':
    blockTypeNo = IND_INTERNAL;
    break;
  case 'L':
    blockTypeNo = IND_LEAF;
    break;
  default:
    blockTypeNo = UNUSED_BLK;
    break;
  }
  int blockNum = getFreeBlock(blockTypeNo);
  this->blockNum = blockNum;
  if (blockNum < 0 || blockNum >= DISK_BLOCKS)
  {
    return;
  }
}

// call parent non-default constructor with 'R' denoting record block.
RecBuffer::RecBuffer() : BlockBuffer::BlockBuffer('R') {}

// set the header info of the block from headinfo structure
int BlockBuffer::setHeader(struct HeadInfo *head)
{

  unsigned char *bufferPtr;
  // get the starting address of the buffer containing the block using
  // loadBlockAndGetBufferPtr(&bufferPtr).
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);

  // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
  // return the value returned by the call.
  if (ret != SUCCESS)
  {
    return ret;
  }

  // cast bufferPtr to type HeadInfo*
  struct HeadInfo *bufferHeader = (struct HeadInfo *)bufferPtr;
  // copy the fields of the HeadInfo pointed to by head (except reserved) to
  // the header of the block (pointed to by bufferHeader)
  //(hint: bufferHeader->numSlots = head->numSlots )
  bufferHeader->numSlots = head->numSlots;
  bufferHeader->numEntries = head->numEntries;
  bufferHeader->numAttrs = head->numAttrs;
  bufferHeader->lblock = head->lblock;
  bufferHeader->rblock = head->rblock;

  // update dirty bit by calling StaticBuffer::setDirtyBit()
  // if setDirtyBit() failed, return the error code
  return StaticBuffer::setDirtyBit(this->blockNum);

  // return SUCCESS;
}

// set block type
int BlockBuffer::setBlockType(int blockType)
{

  unsigned char *bufferPtr;
  /* get the starting address of the buffer containing the block
     using loadBlockAndGetBufferPtr(&bufferPtr). */
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);

  // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
  // return the value returned by the call.
  if (ret != SUCCESS)
  {
    return ret;
  }

  // store the input block type in the first 4 bytes of the buffer.
  // (hint: cast bufferPtr to int32_t* and then assign it)
  // *((int32_t *)bufferPtr) = blockType;
  *((int32_t *)bufferPtr) = blockType;

  // update the StaticBuffer::blockAllocMap entry corresponding to the
  // object's block number to `blockType`.
  StaticBuffer::blockAllocMap[this->blockNum] = blockType;

  // update dirty bit by calling StaticBuffer::setDirtyBit()
  // if setDirtyBit() failed
  // return the returned value from the call
  return StaticBuffer::setDirtyBit(this->blockNum);

  // return SUCCESS
}

// find free block on disk and initialise it with given block type
int BlockBuffer::getFreeBlock(int blockType)
{

  // iterate through the StaticBuffer::blockAllocMap and find the block number
  // of a free block in the disk.
  int freeBlock = -1;
  for (int i = 0; i < DISK_BLOCKS; i++)
  {
    if (StaticBuffer::blockAllocMap[i] == UNUSED_BLK)
    {
      freeBlock = i;
      break;
    }
  }

  // if no block is free, return E_DISKFULL.
  if (freeBlock == -1)
  {
    return E_DISKFULL;
  }

  // set the object's blockNum to the block number of the free block.
  this->blockNum = freeBlock;

  // find a free buffer using StaticBuffer::getFreeBuffer() .
  int freeBuffer = StaticBuffer::getFreeBuffer(freeBlock);

  // initialize the header of the block passing a struct HeadInfo with values
  // pblock: -1, lblock: -1, rblock: -1, numEntries: 0, numAttrs: 0, numSlots: 0
  // to the setHeader() function.
  HeadInfo header;
  header.pblock = -1;
  header.lblock = -1;
  header.rblock = -1;
  header.numAttrs = 0;
  header.numEntries = 0;
  header.numSlots = 0;

  this->setHeader(&header);

  // update the block type of the block to the input block type using setBlockType().
  this->setBlockType(blockType);

  // return block number of the free block.
  return freeBlock;
}

//set slot map in block buffer
int RecBuffer::setSlotMap(unsigned char *slotMap) {
    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block using
       loadBlockAndGetBufferPtr(&bufferPtr). */
       int ret=loadBlockAndGetBufferPtr(&bufferPtr);

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.
        if(ret!=SUCCESS){
          return ret;
        }

    // get the header of the block using the getHeader() function
    struct HeadInfo head;
    getHeader(&head); //retrieve header info

    int numSlots = head.numSlots; /* the number of slots in the block */;

    // the slotmap starts at bufferPtr + HEADER_SIZE. Copy the contents of the
    // argument `slotMap` to the buffer replacing the existing slotmap.
    // Note that size of slotmap is `numSlots`
    unsigned char* slotMapBuffer=bufferPtr+HEADER_SIZE;
    memcpy(slotMapBuffer,slotMap,numSlots);

    // update dirty bit using StaticBuffer::setDirtyBit
    // if setDirtyBit failed, return the value returned by the call
    return StaticBuffer::setDirtyBit(this->blockNum);

    // return SUCCESS
}

int BlockBuffer::getBlockNum(){
  return this->blockNum;
    //return corresponding block number associated with blockbuffer.
}
