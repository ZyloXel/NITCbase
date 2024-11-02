#include "StaticBuffer.h"
#include <iostream>
#include <cstring>
// the declarations for this class can be found at "StaticBuffer.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];

//Stage 6 removed
//Stage 7
// declare the blockAllocMap array

StaticBuffer::StaticBuffer() {
  // copy blockAllocMap blocks from disk to buffer (using readblock() of disk)
  // blocks 0 to 3
  unsigned char buffer[BLOCK_SIZE];
  int blockAllocMapSlot = 0;
  for(int i=0;i<BLOCK_ALLOCATION_MAP_SIZE;i++){
    Disk::readBlock(buffer,i);
    for(int slot=0;slot<BLOCK_SIZE;slot++){

            blockAllocMap[blockAllocMapSlot++] = buffer[slot];
            
        }
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

// write back all modified blocks on system exit
StaticBuffer::~StaticBuffer(){

    // copy blockAllocMap blocks from buffer to disk(using writeblock() of disk)

    unsigned char buffPtr[BLOCK_SIZE];
    int blockAllocMapSlot = 0;
    for(int blockIdx=0;blockIdx<=3;blockIdx++){
        for(int slot=0;slot<BLOCK_SIZE;slot++){

            buffPtr[slot] = blockAllocMap[blockAllocMapSlot++];
            
        }
        Disk::writeBlock(buffPtr,blockIdx);
    }

    /*iterate through all the buffer blocks,
    write back blocks with metainfo as free=false,dirty=true
    using Disk::writeBlock()
    */
    for(int idx=0;idx<BUFFER_CAPACITY;idx++){
        if(!metainfo[idx].free && metainfo[idx].dirty){
            Disk::writeBlock(blocks[idx],metainfo[idx].blockNum);
        }
    }
}

int StaticBuffer::getFreeBuffer(int blockNum){

    // for checking if the blockNum overflows we return E_OUTOFBOUND
    if(blockNum < 0 || blockNum > DISK_BLOCKS){
        return E_OUTOFBOUND;
    }

    int bufferNum = -1;
    // increase the timeStamp in metaInfo of all occupied buffers.

    // let bufferNum be used to store the buffer number of the free/freed buffer.

    // here we check which bufferBlock is free by iterating through the metinfo of the buffer metinfo and gets the free block index
    for(int bufferBlockIdx = 0;bufferBlockIdx<BUFFER_CAPACITY;bufferBlockIdx++){
        
        if(metainfo[bufferBlockIdx].free && bufferNum == -1){
            bufferNum = bufferBlockIdx;
        }else if(!metainfo[bufferBlockIdx].free){
            metainfo[bufferBlockIdx].timeStamp++;
        }

    }

    if(bufferNum == -1){
        int maxTime = -1,idx;
        for(int bufferBlockIdx = 0;bufferBlockIdx<BUFFER_CAPACITY;bufferBlockIdx++){
            if(!metainfo[bufferNum].free && maxTime < metainfo[bufferBlockIdx].timeStamp){
                maxTime = metainfo[bufferBlockIdx].timeStamp;
                idx = bufferBlockIdx;
            }
        }
        bufferNum = idx;
        if(!metainfo[bufferNum].free && metainfo[bufferNum].dirty){
            Disk::writeBlock(blocks[bufferNum],metainfo[bufferNum].blockNum);
        }
    }

    // we make the block as taken on the metainfo 
    metainfo[bufferNum].free = false;
    metainfo[bufferNum].blockNum = blockNum;
    metainfo[bufferNum].dirty = false;
    metainfo[bufferNum].timeStamp = 0;

    return bufferNum;

}

/* Get the buffer index where a particular block is stored
   or E_BLOCKNOTINBUFFER otherwise
*/
int StaticBuffer::getBufferNum(int blockNum)
{
  // Check if blockNum is valid (between zero and DISK_BLOCKS)
  // and return E_OUTOFBOUND if not valid.
  if (blockNum < 0 || blockNum >= DISK_BLOCKS)
  {
    return E_OUTOFBOUND;
  }

  // find and return the bufferIndex which corresponds to blockNum (check metainfo)
  for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++)
  {
    if (!metainfo[bufferIndex].free &&metainfo[bufferIndex].blockNum == blockNum)
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