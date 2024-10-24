#include "StaticBuffer.h"
#include <iostream>
#include <cstring>
// the declarations for this class can be found at "StaticBuffer.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];

//Stage 6 removed
//Stage 7
// declare the blockAllocMap array
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];

StaticBuffer::StaticBuffer() {
  // copy blockAllocMap blocks from disk to buffer (using readblock() of disk)
  // blocks 0 to 3
  for(int i=0;i<BLOCK_ALLOCATION_MAP_SIZE;i++){
    unsigned char buffer[BLOCK_SIZE];
    Disk::readBlock(buffer,i);
    memcpy(blockAllocMap+i*BLOCK_SIZE,buffer,BLOCK_SIZE);
  }
  //Stage 6
  /* initialise metainfo of all the buffer blocks with
     dirty:false, free:true, timestamp:-1 and blockNum:-1
     (you did this already)
  */
 for(int i=0;i<BUFFER_CAPACITY;i++){
  metainfo[i].free=true;
  metainfo[i].dirty=false;
  metainfo[i].timeStamp=-1;
  metainfo[i].blockNum=-1;
 }
}

StaticBuffer::~StaticBuffer() {
  // copy blockAllocMap blocks from buffer to disk(using writeblock() of disk)
  for(int i=0;i<BLOCK_ALLOCATION_MAP_SIZE;i++){
    unsigned char buffer[BLOCK_SIZE];
    memcpy(buffer,blockAllocMap+i*BLOCK_SIZE,BLOCK_SIZE);
    Disk::writeBlock(buffer,i);
  }

  /*iterate through all the buffer blocks,
    write back blocks with metainfo as free:false,dirty:true
    (you did this already)
  */
  //Stage 6
  for(int i=0;i<BUFFER_CAPACITY;i++){
    if(metainfo[i].free == false && metainfo[i].dirty == true){
      Disk::writeBlock(StaticBuffer::blocks[i],metainfo[i].blockNum);
    }
  }
}

int StaticBuffer::getFreeBuffer(int blockNum)
{
  if (blockNum < 0 || blockNum >= DISK_BLOCKS)
  {
    return E_OUTOFBOUND;  //block number out of bouonds
  }

  //timestramp incrementation for all occupied buffers
  for(int i=0;i<BUFFER_CAPACITY;i++){
    if(metainfo[i].free == false){
      metainfo[i].timeStamp++;
    }
  }

  int allocatedBuffer=-1; //initialised to store the index of the allocated variable
  //search for free buffer
  for(int i=0;i<BUFFER_CAPACITY;i++){
    if(metainfo[i].free == true){ //free buffer found
      allocatedBuffer=i;  //set to i(index of free buffer)
      break;
    }
  }

  //no free buffer found
  if(allocatedBuffer==-1){
    int highTS=0;
    for(int i=0;i<BUFFER_CAPACITY;i++) {
      //LRU is used. Buffer having higest timestramp is selected to be replaced
      if(metainfo[i].timeStamp > highTS){
        highTS=metainfo[i].timeStamp;
        allocatedBuffer=i;  
      }
    }
    //selected buffer is dirty, then need to be written back to disk
    if(metainfo[allocatedBuffer].dirty == true){
      Disk::writeBlock(StaticBuffer::blocks[allocatedBuffer],metainfo[allocatedBuffer].blockNum);
    }
  }

  //updating metadata for the allocated buffer
metainfo[allocatedBuffer].free=false;         //marked as buffer occupied
metainfo[allocatedBuffer].dirty=false;        //reset dirty flag
metainfo[allocatedBuffer].blockNum=blockNum;  //set blocknum for the buffer
metainfo[allocatedBuffer].timeStamp=0;        //reset TS for the buffer
return allocatedBuffer;                       //index of the allocated buffer returned

  // iterate through all the blocks in the StaticBuffer
  // find the first free block in the buffer (check metainfo)
  // assign allocatedBuffer = index of the free block  
}

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

  // find and return the bufferIndex which corresponds to blockNum (check metainfo)
  for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++)
  {
    if (metainfo[bufferIndex].blockNum == blockNum)
      return bufferIndex;
  }

  // if block is not in the buffer
  return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::setDirtyBit(int blockNum)
{
  // find the buffer index corresponding to the block using getBufferNum().
  int bufferNum = getBufferNum(blockNum);

  // if block is not present in the buffer (bufferNum = E_BLOCKNOTINBUFFER)
  //     return E_BLOCKNOTINBUFFER
  if (bufferNum == E_BLOCKNOTINBUFFER)
    return E_BLOCKNOTINBUFFER;

  // if blockNum is out of bound (bufferNum = E_OUTOFBOUND)
  //     return E_OUTOFBOUND
  if (blockNum == E_OUTOFBOUND)
    return E_OUTOFBOUND;

  // else
  //     (the bufferNum is valid)
  //     set the dirty bit of that buffer to true in metainfo
  else
  {
    metainfo[bufferNum].dirty = true;
  }
  return SUCCESS;
}