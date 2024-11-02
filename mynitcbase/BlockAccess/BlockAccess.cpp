#include "BlockAccess.h"
#include <cstring>

RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op)
{
    // Initialize a RecId object to store the previous record ID
    RecId prevRecId;

    // Retrieve the last search index (block and slot) for the given relation ID
    RelCacheTable::getSearchIndex(relId, &prevRecId);

    // Variables to track the current block and slot in the relation
    int block, slot;

    // Check if the previous record ID is invalid, indicating the search should start from the beginning
    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        // Fetch the relation catalog entry for the given relation ID
        RelCatEntry relCatBuf;
        RelCacheTable::getRelCatEntry(relId, &relCatBuf);

        // Start from the first block and slot of the relation
        block = relCatBuf.firstBlk;
        slot = 0;
    }
    else
    {
        // If there is a valid previous search index, continue from the next slot
        block = prevRecId.block;
        slot = prevRecId.slot + 1;
    }

    RelCatEntry relCatBuf;

    RelCacheTable::getRelCatEntry(relId,&relCatBuf);

    // Loop through each block until the end of the relation is reached
    while (block != -1)
    {
        // Create a record buffer for the current block
        RecBuffer recBuffer(block);

        // Retrieve the header information from the record buffer
        HeadInfo header;
        recBuffer.getHeader(&header);

        // Array to hold attribute values of a record
        Attribute record[header.numAttrs];
        recBuffer.getRecord(record, slot);

        // Slot map to track the occupancy status of each slot in the block.
        unsigned char slotMap[header.numSlots];
        recBuffer.getSlotMap(slotMap);

        // Check if the current slot exceeds the number of slots in the block
        if (slot >= header.numSlots)
        {
            // Move to the next block and reset the slot counter
            block = header.rblock;
            slot = 0;
            continue;
        }

        // If the current slot is unoccupied, move to the next slot
        if (slotMap[slot] == SLOT_UNOCCUPIED)
        {
            slot++;
            continue;
        }

        // Fetch the attribute catalog entry for the specified attribute name
        AttrCatEntry attrCatBuf;
        int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);

        // Compare the attribute value of the record with the given attribute value
        int cmpVal = compareAttrs(record[attrCatBuf.offset], attrVal, attrCatBuf.attrType);

        // Check if the comparison result satisfies the given operation (op)
        if (
            (op == NE && cmpVal != 0) || // If the operation is "not equal to"
            (op == LT && cmpVal < 0) ||  // If the operation is "less than"
            (op == LE && cmpVal <= 0) || // If the operation is "less than or equal to"
            (op == EQ && cmpVal == 0) || // If the operation is "equal to"
            (op == GT && cmpVal > 0) ||  // If the operation is "greater than"
            (op == GE && cmpVal >= 0)    // If the operation is "greater than or equal to"
        )
        {
            // Create a RecId for the matching record and update the search index
            RecId searchIndex = {block, slot};
            RelCacheTable::setSearchIndex(relId, &searchIndex);
            return searchIndex; // Return the matching record ID
        }

        // Move to the next slot in the block
        slot++;
    }
    RelCacheTable::resetSearchIndex(relId);
    // Return an invalid record ID if no matching record is found
    return RecId({-1, -1});
}

int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE])
{
    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    Attribute newRelationName; // set newRelationName with newName
    strcpy(newRelationName.sVal, newName);
    RecId recid;
    recid = linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, newRelationName, EQ);
    if (recid.block != -1 && recid.slot != -1)
    {
        return E_RELEXIST;
    }

    // search the relation catalog for an entry with "RelName" = newRelationName

    // If relation with name newName already exists (result of linearSearch
    //                                               is not {-1, -1})
    //    return E_RELEXIST;

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */

    Attribute oldRelationName; // set oldRelationName with oldName
    strcpy(oldRelationName.sVal, oldName);

    // search the relation catalog for an entry with "RelName" = oldRelationName
    recid = BlockAccess::linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, oldRelationName, EQ);
    if (recid.block == -1 && recid.slot == -1)
    {
        return E_RELNOTEXIST;
    }
    // If relation with name oldName does not exist (result of linearSearch is {-1, -1})
    //    return E_RELNOTEXIST;
    RecBuffer getRelation(recid.block);
    Attribute changeRel[RELCAT_NO_ATTRS];
    getRelation.getRecord(changeRel, recid.slot);
    strcpy(changeRel[RELCAT_REL_NAME_INDEX].sVal, newName);
    getRelation.setRecord(changeRel, recid.slot);
    /* get the relation catalog record of the relation to rename using a RecBuffer
       on the relation catalog [RELCAT_BLOCK] and RecBuffer.getRecord function
    */
    /* update the relation name attribute in the record with newName.
       (use RELCAT_REL_NAME_INDEX) */
    // set back the record value using RecBuffer.setRecord

    /*
    update all the attribute catalog entries in the attribute catalog corresponding
    to the relation with relation name oldName to the relation name newName
    */

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    for (int i = 0; i < changeRel[RELCAT_NO_ATTRIBUTES_INDEX].nVal; i++)
    {
        recid = BlockAccess::linearSearch(ATTRCAT_RELID, ATTRCAT_ATTR_RELNAME, oldRelationName, EQ);
        RecBuffer getRelation1(recid.block);
        Attribute changeAttr[ATTRCAT_NO_ATTRS];
        getRelation1.getRecord(changeAttr, recid.slot);
        strcpy(changeAttr[ATTRCAT_REL_NAME_INDEX].sVal, newName);
        getRelation1.setRecord(changeAttr, recid.slot);
    }

    /* reset the searchIndex of the attribute catalog using
       RelCacheTable::resetSearchIndex() */

    // for i = 0 to numberOfAttributes :
    //     linearSearch on the attribute catalog for relName = oldRelationName
    //     get the record using RecBuffer.getRecord
    //
    //     update the relName field in the record to newName
    //     set back the record using RecBuffer.setRecord

    return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE])
{

    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */

    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    Attribute relNameAttr; // set relNameAttr to relName
    strcpy(relNameAttr.sVal, relName);
    RecId recid; // set relNameAttr to relName
    recid = BlockAccess::linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, relNameAttr, EQ);
    if (recid.block == -1 && recid.slot == -1)
        return E_RELNOTEXIST;

    // Search for the relation with name relName in relation catalog using linearSearch()
    // If relation with name relName does not exist (search returns {-1,-1})
    //    return E_RELNOTEXIST;
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    /* reset the searchIndex of the attribute catalog using
       RelCacheTable::resetSearchIndex() */

    /* declare variable attrToRenameRecId used to store the attr-cat recId
    of the attribute to rename */
    RecId attrToRenameRecId{-1, -1};
    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

    /* iterate over all Attribute Catalog Entry record corresponding to the
       relation to find the required attribute */
    while (true)
    {
        // linear search on the attribute catalog for RelName = relNameAttr
        RecId attrRec = BlockAccess::linearSearch(ATTRCAT_RELID, ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);
        if (attrRec.block == -1 || attrRec.slot == -1)
            break;
        // if there are no more attributes left to check (linearSearch returned {-1,-1})
        //     break;

        RecBuffer changeAttr(attrRec.block);
        changeAttr.getRecord(attrCatEntryRecord, attrRec.slot);
        if (strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName) == 0)
        {
            attrToRenameRecId.block = attrRec.block;
            attrToRenameRecId.slot = attrRec.slot;
        }
        if (strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName) == 0)
            return E_ATTREXIST;

        /* Get the record from the attribute catalog using RecBuffer.getRecord
          into attrCatEntryRecord */

        // if attrCatEntryRecord.attrName = oldName
        //     attrToRenameRecId = block and slot of this record

        // if attrCatEntryRecord.attrName = newName
        //     return E_ATTREXIST;
    }

    // if attrToRenameRecId == {-1, -1}
    //     return E_ATTRNOTEXIST;

    if (attrToRenameRecId.block == -1 && attrToRenameRecId.slot == -1)
        return E_ATTRNOTEXIST;
    RecBuffer changeAttribute(attrToRenameRecId.block);
    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    changeAttribute.getRecord(attrCatRecord, attrToRenameRecId.slot);
    strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName);
    changeAttribute.setRecord(attrCatRecord, attrToRenameRecId.slot);

    // Update the entry corresponding to the attribute in the Attribute Catalog Relation.
    /*   declare a RecBuffer for attrToRenameRecId.block and get the record at
         attrToRenameRecId.slot */
    //   update the AttrName of the record with newName
    //   set back the record with RecBuffer.setRecord

    return SUCCESS;
}

//Stage 7

int BlockAccess::insert(int relId,union Attribute* record){
    
    // get the relation catalog entry from relation cache
    // ( use RelCacheTable::getRelCatEntry() of Cache Layer)
    RelCatEntry relCatEntry;

    // std::cout<<record[0].sVal<<std::endl;

    RelCacheTable::getRelCatEntry(relId,&relCatEntry);
    
    int blockNum =  relCatEntry.firstBlk; /* first record block of the relation (from the rel-cat entry)*/;

    // rec_id will be used to store where the new record will be inserted
    RecId rec_id = {-1, -1};

    int numOfSlots = relCatEntry.numSlotsPerBlk /* number of slots per record block */;
    int numOfAttributes = relCatEntry.numAttrs /* number of attributes of the relation */;

    int prevBlockNum = -1 /* block number of the last element in the linked list = -1 */;

    /*
        Traversing the linked list of existing record blocks of the relation
        until a free slot is found OR
        until the end of the list is reached
    */
    while (blockNum != -1) {
        // create a RecBuffer object for blockNum (using appropriate constructor!)
        RecBuffer blockBuffer(blockNum);
        HeadInfo head;
        
        // get header of block(blockNum) using RecBuffer::getHeader() function
        
        blockBuffer.getHeader(&head);

        // get slot map of block(blockNum) using RecBuffer::getSlotMap() function
        unsigned char slotMap[head.numSlots];

        blockBuffer.getSlotMap(slotMap);

        // search for free slot in the block 'blockNum' and store it's rec-id in rec_id
        // (Free slot can be found by iterating over the slot map of the block)
        /* slot map stores SLOT_UNOCCUPIED if slot is free and
           SLOT_OCCUPIED if slot is occupied) */
        
        for(int i=0;i<head.numSlots;i++){

            if(slotMap[i] == SLOT_UNOCCUPIED){
                rec_id.block = blockNum;
                rec_id.slot = i;
                break;
            }
        }

        /* if a free slot is found, set rec_id and discontinue the traversal
           of the linked list of record blocks (break from the loop) */
        
        if(rec_id.block != -1 && rec_id.slot != -1) break;
        
        /* otherwise, continue to check the next block by updating the
           block numbers as follows:
              update prevBlockNum = blockNum
              update blockNum = header.rblock (next element in the linked
                                               list of record blocks)
        */
       prevBlockNum = blockNum;
       blockNum = head.rblock;

    }

    //  if no free slot is found in existing record blocks (rec_id = {-1, -1})
    if(rec_id.block == -1 && rec_id.slot == -1){
        
        // if relation is RELCAT, do not allocate any more blocks
        //     return E_MAXRELATIONS;
        if(relId == RELCAT_RELID)return E_MAXRELATIONS;

        
        // Otherwise,
        // get a new record block (using the appropriate RecBuffer constructor!)
        // get the block number of the newly allocated block
        // (use BlockBuffer::getBlockNum() function)
        // let ret be the return value of getBlockNum() function call

        RecBuffer blockBuffer;

        int ret = blockBuffer.getBlockNum();

        if(ret == E_DISKFULL) return E_DISKFULL;

        // Assign rec_id.block = new block number(i.e. ret) and rec_id.slot = 0

        rec_id.block = ret,rec_id.slot = 0;

        /*
            set the header of the new record block such that it links with
            existing record blocks of the relation
            set the block's header as follows:
            blockType: REC, pblock: -1
            lblock
                  = -1 (if linked list of existing record blocks was empty
                         i.e this is the first insertion into the relation)
                  = prevBlockNum (otherwise),
            rblock: -1, numEntries: 0,
            numSlots: numOfSlots, numAttrs: numOfAttributes
            (use BlockBuffer::setHeader() function)
        */

        HeadInfo head;

        head.blockType = REC;
        head.pblock = head.rblock = -1;
        head.lblock = prevBlockNum;
        head.numEntries = 0;
        head.numAttrs = numOfAttributes,head.numSlots = numOfSlots;

        blockBuffer.setHeader(&head);

        /*
            set block's slot map with all slots marked as free
            (i.e. store SLOT_UNOCCUPIED for all the entries)
            (use RecBuffer::setSlotMap() function)
        */

        unsigned char slotMap[numOfSlots];

        for(int i=0;i<numOfSlots;i++){
            slotMap[i] = SLOT_UNOCCUPIED;
        }

        blockBuffer.setSlotMap(slotMap);

        if (prevBlockNum != -1){

            // create a RecBuffer object for prevBlockNum
            // get the header of the block prevBlockNum and
            // update the rblock field of the header to the new block
            // number i.e. rec_id.block
            // (use BlockBuffer::setHeader() function)

            RecBuffer prevBlock(prevBlockNum);
            HeadInfo prevHead;

            prevBlock.getHeader(&prevHead);

            prevHead.rblock = rec_id.block;

            prevBlock.setHeader(&prevHead);

        }else{
            // update first block field in the relation catalog entry to the
            // new block (using RelCacheTable::setRelCatEntry() function)

            relCatEntry.firstBlk = rec_id.block;

        }

        relCatEntry.lastBlk = rec_id.block;

        RelCacheTable::setRelCatEntry(relId,&relCatEntry);

    }
    // create a RecBuffer object for rec_id.block
    // insert the record into rec_id'th slot using RecBuffer.setRecord())
    RecBuffer currBlockBuffer(rec_id.block);
    currBlockBuffer.setRecord(record,rec_id.slot);

    /* update the slot map of the block by marking entry of the slot to
       which record was inserted as occupied) */
    
    unsigned char slotMap[numOfSlots];
    // (ie store SLOT_OCCUPIED in free_slot'th entry of slot map)
    // (use RecBuffer::getSlotMap() and RecBuffer::setSlotMap() functions)
    
    currBlockBuffer.getSlotMap(slotMap);
    slotMap[rec_id.slot] = SLOT_OCCUPIED;
    currBlockBuffer.setSlotMap(slotMap);
    

    // increment the numEntries field in the header of the block to
    // which record was inserted
    // (use BlockBuffer::getHeader() and BlockBuffer::setHeader() functions)
    HeadInfo currHeader;
    currBlockBuffer.getHeader(&currHeader);
    currHeader.numEntries++;
    currBlockBuffer.setHeader(&currHeader);
    
    

    
    
    // Increment the number of records field in the relation cache entry for
    // the relation. (use RelCacheTable::setRelCatEntry function)

    RelCacheTable::getRelCatEntry(relId,&relCatEntry);
    relCatEntry.numRecs++;
    RelCacheTable::setRelCatEntry(relId,&relCatEntry);

    return SUCCESS;
}

// int BlockAccess::insert(int relId, Attribute *record){
//     // get the relation catalog entry from the relation cache table
//     RelCatEntry relCatEntry;
//     RelCacheTable::getRelCatEntry(relId, &relCatEntry);

//     int blockNum = relCatEntry.firstBlk; // first record block of the relation.

//     // rec_id will be used to store where the new record will be inserted.
//     RecId rec_id = {-1, -1};

//     int numOfSlots = relCatEntry.numSlotsPerBlk;    // number of slots per record block
//     int numOfAttributes = relCatEntry.numAttrs;     // number of attributes of the relation
//     int prevBlockNum = -1;                          // block number of the last element in the linked list

//     /*
//         Traversing the linked list of existing record block of the relation
//         until a free slot is found or until the end of the list is reached.
//     */
//     while(blockNum != -1){
//         //get the slotmap of the block and search for a free slot.
//         RecBuffer recBuffer(blockNum);
//         struct HeadInfo head;
//         recBuffer.getHeader(&head);
//         unsigned char slotMap[head.numSlots];
//         recBuffer.getSlotMap(slotMap);

//         // iterate over slotmap and store the free in the rec_id
//         for(int i = 0; i < numOfSlots; i++){
//             if(slotMap[i] == SLOT_UNOCCUPIED){
//                 rec_id.block = blockNum;
//                 rec_id.slot = i;
//                 break;
//             }
//         }

//         // if slot is found, then break the traversal.
//         if(rec_id.block != -1 && rec_id.slot != -1){
//             break;
//         }

//         // otherwise, continue to check the next block by updating the block number
//         prevBlockNum = blockNum;
//         blockNum = head.rblock;
//     }

//     // if slot not found
//     if(rec_id.block == -1 && rec_id.slot == -1){
//         // if relation is RELCAT, do not allocate any more blocks
//         if(relId == RELCAT_RELID){
//             return E_MAXRELATIONS; // DOUBTFULL
//         }

//         // else, get a new record block
//         // get the block number of newly allocated block
//         RecBuffer newRecBuffer;
//         int ret = newRecBuffer.getBlockNum();

//         if(ret == E_DISKFULL){
//             return E_DISKFULL;
//         }

//         // Assign rec_id.block = new block number (i.e. ret) and rec_id.slot = 0
//         rec_id.block = ret;
//         rec_id.slot = 0;

//         /*
//             set the header of the new record block such that it links with the existing record
//             blocks of the relation
//          */
//         struct HeadInfo newHead;
//         newHead.blockType = REC;
//         newHead.pblock = -1;
//         newHead.lblock = prevBlockNum;
//         newHead.rblock = -1;
//         newHead.numEntries = 0;
//         newHead.numSlots = numOfSlots;
//         newHead.numAttrs = numOfAttributes;

//         newRecBuffer.setHeader(&newHead);

//         /*
//             set block's slot map with all slots marked as free
//         */
//         unsigned char newSlotMap[numOfSlots];
//         for(int i = 0; i < numOfSlots; i++){
//             newSlotMap[i] = SLOT_UNOCCUPIED;
//         }
//         newRecBuffer.setSlotMap(newSlotMap);

//         // now if there was a previous block, then it's rblock should be set as rec_id.block
//         if(prevBlockNum != -1){
//             // create a RecBuffer object for prevBlockNum
//             // get the header and update the rblock field.
//             // then set the header using setHeader()
//             RecBuffer prevRecBuffer(prevBlockNum);
//             struct HeadInfo prevHead;
//             prevRecBuffer.getHeader(&prevHead);
//             prevHead.rblock = rec_id.block;
//             prevRecBuffer.setHeader(&prevHead);
//         }
//         else{
//             // means it is the 1st block of the relation
//             // update first block field in the relCatEntry to the new block
//             RelCatEntry relCatEntry;
//             RelCacheTable::getRelCatEntry(relId, &relCatEntry);
//             relCatEntry.firstBlk = rec_id.block;
//             RelCacheTable::setRelCatEntry(relId, &relCatEntry);
//         }

//         // update the lastBlk field of the relCatEntry to the new block
//         RelCatEntry relCatEntry;
//         RelCacheTable::getRelCatEntry(relId, &relCatEntry);
//         relCatEntry.lastBlk = rec_id.block;
//         RelCacheTable::setRelCatEntry(relId, &relCatEntry); // is it neccessary because we are setting it up again at the end of this function.
//         RelCacheTable::setRelCatEntry(relId, &relCatEntry);
//     }

//     // Now create a RecBuffer object for rec_id.block
//     // and insert the record into the rec_id.slot using the setRecord()
//     RecBuffer recBuffer(rec_id.block); // i am using the old recBuffer I created initially.
//     recBuffer.setRecord(record, rec_id.slot);

//     /*
//         update the slotmap of the block by making the entry of the slot to which
//         record was inserted as occupied
//         Get the slotMap using getSlotMap() ==> update the rec_id.slot-th index as OCCUPIED ==> set the slotMap using setSlotMap()
//     */
//     struct HeadInfo head;
//     recBuffer.getHeader(&head);
//     unsigned char slotMap[head.numSlots];
//     recBuffer.getSlotMap(slotMap);
//     slotMap[rec_id.slot] = SLOT_OCCUPIED;
//     recBuffer.setSlotMap(slotMap);

//     /*
//         increment the numEntries field in the header of the block to
//         which record was inserted. Then set the header back using BlockBuffer::setHeader()
//     */
//     head.numEntries += 1;
//     recBuffer.setHeader(&head);

//     /*
//         Increment the number of records field in the relation cache entry for the relation.
//     */
//     RelCacheTable::getRelCatEntry(relId, &relCatEntry);
//     relCatEntry.numRecs += 1;
//     RelCacheTable::setRelCatEntry(relId, &relCatEntry);

//     return SUCCESS;
    
// }