#include "StaticBuffer.h"
#include <iostream>
// the declarations for this class can be found at "StaticBuffer.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
// Stage 6
StaticBuffer::StaticBuffer()
{

  // initialise all blocks as free
  for (int bufferIndex = 0; bufferIndex < 32; bufferIndex++)
  {
    metainfo[bufferIndex].free = true;
    metainfo[bufferIndex].dirty = false;
    metainfo[bufferIndex].timeStamp = -1;
    metainfo[bufferIndex].blockNum = -1;
  }
}

/*
At this stage, we are not writing back from the buffer to the disk since we are
not modifying the buffer. So, we will define an empty destructor for now. In
subsequent stages, we will implement the write-back functionality here.
*/
StaticBuffer::~StaticBuffer()
{
  for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++)
  {
    if (metainfo[bufferIndex].free == false && metainfo[bufferIndex].dirty == true)
    {
      Disk::writeBlock(blocks[bufferIndex], metainfo[bufferIndex].blockNum);
    }
  }
}

int StaticBuffer::getFreeBuffer(int blockNum)
{
  if (blockNum < 0 || blockNum > DISK_BLOCKS)
  {
    return E_OUTOFBOUND;
  }
  int allocatedBuffer;

  // iterate through all the blocks in the StaticBuffer
  // find the first free block in the buffer (check metainfo)
  // assign allocatedBuffer = index of the free block
  for (int bufferIndex = 0; bufferIndex < 32; bufferIndex++)
  {
    if (metainfo[bufferIndex].free == true)
    {
      allocatedBuffer = bufferIndex;
      break;
    }
  }
  metainfo[allocatedBuffer].free = false;
  metainfo[allocatedBuffer].blockNum = blockNum;

  return allocatedBuffer;
}

// Stage 6
/* Get the buffer index where a particular block is stored
   or E_BLOCKNOTINBUFFER otherwise
*/
int StaticBuffer::getBufferNum(int blockNum)
{
  // Check if blockNum is valid (between zero and DISK_BLOCKS)
  // and return E_OUTOFBOUND if not valid.
  if (blockNum < 0 || blockNum > 8091)
  {
    return E_OUTOFBOUND;
  }
  for (int bufferIndex = 0; bufferIndex < 32; bufferIndex++)
  {
    if (metainfo[bufferIndex].blockNum == blockNum)
      return bufferIndex;
  }

  // find and return the bufferIndex which corresponds to blockNum (check metainfo)

  // if block is not in the buffer
  return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::setDirtyBit(int blockNum)
{
  // find the buffer index corresponding to the block using getBufferNum().
  int bufferNum = getBufferNum(blockNum);
  if (bufferNum == E_BLOCKNOTINBUFFER)
    return E_BLOCKNOTINBUFFER;

  if (blockNum == E_OUTOFBOUND)
    return E_OUTOFBOUND;
  else
  {
    metainfo[bufferNum].dirty = true;
  }

  // if block is not present in the buffer (bufferNum = E_BLOCKNOTINBUFFER)
  //     return E_BLOCKNOTINBUFFER

  // if blockNum is out of bound (bufferNum = E_OUTOFBOUND)
  //     return E_OUTOFBOUND

  // else
  //     (the bufferNum is valid)
  //     set the dirty bit of that buffer to true in metainfo

  return SUCCESS;
}
