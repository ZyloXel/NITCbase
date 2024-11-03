#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include <iostream>
#include <vector>
#include <string.h>
using namespace std;

void printChars(unsigned char* buffer){
  Disk::readBlock(buffer,0);
  for(int i=0;i<20;i++){
    cout<<(int)buffer[i]<<" ";
  }
  cout<<endl;
}

void STAGE2_updateRelationAttribute(const char* relName,const char* attrName,const char* newAttrName){

	/*
		In this we only need the attribute catalogs as tehre is no need to change anything on the relation catalog 
	*/

    vector<RecBuffer> attrCatBufferArray; //since there can be more than one block of attribute we require a vector that stores all attribute blocks so that we can check the entire attribute catalog
    attrCatBufferArray.push_back(RecBuffer(ATTRCAT_BLOCK)); //for the initialization we push an the starting RecBuffer of the ATTRCAT_BLOCK and from there we access all other attr catalog block

    //get the metadata of the block
	  HeadInfo attrCatHeader;

    vector<HeadInfo> attrCatHeaderArray;   //since there can be more than one block of ATTRCAT we require a vector that stores all attribute blocks metadata so that we can access the entire attribute catalogs
    attrCatHeaderArray.push_back(attrCatHeader); // for the initialization we push an the starting HeadInfo of the ATTRCAT_BLOCK and from there we access all other attr catalog block

    attrCatBufferArray[0].getHeader(&(attrCatHeaderArray[0])); //accessing the first attr catalog buffer and getting its metadata
    
    //here using the first ATTRCAT block we access all the other ATTRCAT blocks using the rblock data(rblock points to the next attribute catalog block)
 	for(int i=0;attrCatHeaderArray[i].rblock!=-1;i++){ // here if no rblock is present rblock will be -1
      attrCatBufferArray.push_back(RecBuffer(attrCatHeaderArray[i].rblock)); //we add a new RecBuffer to get the next attribute blocks data
      HeadInfo attrCatHeader;
      attrCatHeaderArray.push_back(attrCatHeader); //pushing a newly initialized HeadInfo structure
      attrCatBufferArray[i+1].getHeader(&(attrCatHeaderArray[i+1])); //the i+1 is for accessing the newly pushed block buffer and get its metadata
    }
	
	
	//here we iterate until we get our rquired data
    for(int attrArrayidx = 0;attrArrayidx<attrCatBufferArray.size();attrArrayidx++){ // this is for accesing all other ATTRCAT blocks since there will be multiple blocks of attribute block type

		for(int j=0;j<attrCatHeaderArray[attrArrayidx].numEntries;j++){ //we iterate through each and every attribute blocks entries

			Attribute attrCatRecord[ATTRCAT_NO_ATTRS]; // here we get the j th attribute from the attribute catalog 
			attrCatBufferArray[attrArrayidx].getRecord(attrCatRecord,j); // (we actually check all the entries in the attribute block)
			
			if(strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal,relName)==0 && strcmp(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,attrName) == 0){ 
				/** 
				 here we check if the attribute catalog relation name and attribute name is same as the given input
				(basically how sql finds using given keys to identify the unique data)
				**/
				memcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,newAttrName,ATTR_SIZE); // here we copy the attrName to our required block
				
				attrCatBufferArray[attrArrayidx].setRecord(attrCatRecord,j); // we set the record in the disk using the set record function (check BockBuffer.cpp for more info)

				return;
			}

		}

	}

}

/*
    note: the reson i removed the vector method implementation was that it didnt suit with the current coding style 
    and thinking in the future the time complexity will be reduced for my new approach
*/

void STAGE2_printingAllRelations(){

	  RecBuffer relCatBuffer(RELCAT_BLOCK);
    RecBuffer attrCatBuffer(ATTRCAT_BLOCK);
    // RecBuffer attrCatBufferArray; //since there can be more than one block of attribute we require a vector that stores all attribute blocks so that we can check the entire attribute catalog
    // attrCatBuffer.push_back(RecBuffer(ATTRCAT_BLOCK)); //for the initialization we push an the starting RecBuffer of the ATTRCAT_BLOCK and from there we access all other attr catalog block

    //get the metadata of the block

    HeadInfo relCatHeader;
	  HeadInfo attrCatHeader;

    // vector<HeadInfo> attrCatHeader;   //since there can be more than one block of ATTRCAT we require a vector that stores all attribute blocks metadata so that we can access the entire attribute catalogs
    // attrCatHeader.push_back(attrCatHeader); // for the initialization we push an the starting HeadInfo of the ATTRCAT_BLOCK and from there we access all other attr catalog block

    relCatBuffer.getHeader(&relCatHeader);
    attrCatBuffer.getHeader(&attrCatHeader); //accessing the first attr catalog buffer and getting its metadata
    
    //i am not using this method(for now this might be a gud approach but it doesnt suit with current coding style)
    //here using the first ATTRCAT block we access all the other ATTRCAT blocks using the rblock data(rblock points to the next attribute catalog block)

    // printing all the relation and its attributes
    for(int i=0;i<relCatHeader.numEntries;i++){

        Attribute relCatRecord[RELCAT_NO_ATTRS]; // getting the i th relation from the relation block
        relCatBuffer.getRecord(relCatRecord,i);

        cout<<"Relation: "<< relCatRecord[RELCAT_REL_NAME_INDEX].sVal << endl; // check the global constant values to know about all the indexes

        // for(int attrArrayidx = 0;attrArrayidx<attrCatBufferArray.size();attrArrayidx++){ // this is for accesing all other ATTRCAT blocks since there will be multiple blocks of attribute block type

        int attrCatBlock = ATTRCAT_BLOCK; 

        while(attrCatBlock!=-1){ // this is to as an index to interate through all ATTRCAT blocks

            attrCatBuffer = RecBuffer(attrCatBlock); // this gets called all the time
            attrCatBuffer.getHeader(&attrCatHeader);

            for(int j=0;j<attrCatHeader.numEntries;j++){ //we iterate through each and every attribute blocks entries

                Attribute attrCatRecord[ATTRCAT_NO_ATTRS]; // here we get the j th attribute from the attribute catalog 
                attrCatBuffer.getRecord(attrCatRecord,j); // (we actually check all the entries in the attribute block)

                if(attrCatRecord[ATTRCAT_REL_NAME_INDEX].nVal == relCatRecord[RELCAT_REL_NAME_INDEX].nVal){ 
                    /** 
                     here we check if the attribute catalog relation name and relation catalog relation name 
                    (basically how sql maps using primary key)
                    **/
                    const char *attrType = attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER ? "NUM" : "STR";
                    cout<<" "<< attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal <<" : "<<attrType<<endl;
                }

            }
            attrCatBlock = attrCatHeader.rblock;

        }
        cout<<endl;
    }
}

void STAGE3_Cache(){
  // recBlockIdx = 0 -> RELCAT_RELID
  // recBlockIdx = 1 -> ATTRCAT_RELID
  for(int relId = 0;relId < 3;relId++){
    RelCatEntry relCatBuf;
    RelCacheTable::getRelCatEntry(relId,&relCatBuf);
    cout<<relCatBuf.relName<<endl;

    for(int attrs = 0;attrs<relCatBuf.numAttrs;attrs++){
      AttrCatEntry attrCatBuf;
      AttrCacheTable::getAttrCatEntry(relId,attrs,&attrCatBuf);
      cout<<" "<<attrCatBuf.attrName<<": "<<attrCatBuf.attrType<<endl;
    }
    cout<<endl;
  }
}

int main(int argc, char *argv[]) {
  /* Initialize the Run Copy of Disk */
  Disk disk_run;
  StaticBuffer buffer;
  OpenRelTable cache;
  /**
    --- stage 1 ---
    unsigned char buffer[BLOCK_SIZE];
    Disk::readBlock(buffer,7000);
    char message[] = "hello";
    memcpy(buffer+20,message,6);
    Disk::writeBlock(buffer,7000);
    unsigned char buffer2[BLOCK_SIZE];
    char message2[6];
    Disk::readBlock(buffer2,7000);
    memcpy(message2,buffer2+20,6);
    cout<< message2 << endl;
    unsigned char buffer3[BLOCK_SIZE];
    printChars(buffer3);

  **/

    // --- stage 2 ---
		// cout<<endl;
		// // cout<<"--old relations--"<<endl;
		// STAGE2_printingAllRelations();
		// STAGE2_updateRelationAttribute("Students","Class","Batch");
		// // cout<<"--new relations--"<<endl;
		// // STAGE2_printingAllRelations();
		// cout<<endl;
    //---stage 3-----
    // first phase is basically chnaging some stage 2 functions
    // STAGE3_Cache();

  // return 0;

  return FrontendInterface::handleFrontend(argc, argv);
}
