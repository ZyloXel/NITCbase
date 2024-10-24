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
        if (attrRec.block == -1 && attrRec.slot != -1)
            break;
        // if there are no more attributes left to check (linearSearch returned {-1,-1})
        //     break;

        RecBuffer changeAttr(attrRec.block);
        changeAttr.getRecord(attrCatEntryRecord, attrRec.slot);
        if (strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName) == 0)
        {
            attrToRenameRecId.block = attrRec.block;
            attrToRenameRecId.slot = attrRec.slot;
            break;
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
int BlockAccess::insert(int relId, Attribute *record) {
    // get the relation catalog entry from relation cache
    // ( use RelCacheTable::getRelCatEntry() of Cache Layer)
    RelCatEntry relCatEntry;
    int ret=RelCacheTable::getRelCatEntry(relId,&relCatEntry);
    if(ret!=SUCCESS){
        return ret;
    }

    int blockNum = relCatEntry.firstBlk;/* first record block of the relation (from the rel-cat entry)*/
    
    // rec_id will be used to store where the new record will be inserted
    RecId rec_id = {-1, -1};

    int numOfSlots = relCatEntry.numSlotsPerBlk/* number of slots per record block */;
    int numOfAttributes = relCatEntry.numAttrs/* number of attributes of the relation */;

    int prevBlockNum = -1/* block number of the last element in the linked list = -1 */;

    /*
        Traversing the linked list of existing record blocks of the relation
        until a free slot is found OR
        until the end of the list is reached
    */
    while (blockNum != -1) {
        // create a RecBuffer object for blockNum (using appropriate constructor!)
        RecBuffer currBlock(blockNum);
        // get header of block(blockNum) using RecBuffer::getHeader() function
        HeadInfo currHeader;
        currBlock.getHeader(&currHeader);
        // get slot map of block(blockNum) using RecBuffer::getSlotMap() function
        unsigned char SlotMap[numOfSlots];
        currBlock.getSlotMap(SlotMap);
        // search for free slot in the block 'blockNum' and store it's rec-id in rec_id
        // (Free slot can be found by iterating over the slot map of the block)
        /* slot map stores SLOT_UNOCCUPIED if slot is free and
           SLOT_OCCUPIED if slot is occupied) */
        for(int i=0;i<numOfSlots;i++){
            if(SlotMap[i]==SLOT_UNOCCUPIED){
                rec_id.slot=i;
                rec_id.block=blockNum;
                break;
            }
        }
        /* if a free slot is found, set rec_id and discontinue the traversal
           of the linked list of record blocks (break from the loop) */
        if(rec_id.block!=-1 && rec_id.slot!=-1){
            break;
        }
        /* otherwise, continue to check the next block by updating the
           block numbers as follows:
              update prevBlockNum = blockNum
              update blockNum = header.rblock (next element in the linked
                                               list of record blocks)
        */
       prevBlockNum=blockNum;
       blockNum=currHeader.rblock;
    }

    //  if no free slot is found in existing record blocks (rec_id = {-1, -1})
    if(rec_id.block==-1 || rec_id.slot==-1){
        // if relation is RELCAT, do not allocate any more blocks
        //     return E_MAXRELATIONS;
        if(relId==RELCAT_RELID){
            return E_MAXRELATIONS;
        }
        // Otherwise,
        // get a new record block (using the appropriate RecBuffer constructor!)
        // get the block number of the newly allocated block
        // (use BlockBuffer::getBlockNum() function)
        // let ret be the return value of getBlockNum() function call
        RecBuffer newBlock;
        ret=newBlock.getBlockNum();
        if (ret == E_DISKFULL) {
            return E_DISKFULL;
        }

        // Assign rec_id.block = new block number(i.e. ret) and rec_id.slot = 0
        rec_id.block=ret;
        rec_id.slot=0;
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
        HeadInfo newBlockHead;
        newBlockHead.blockType=REC;
        newBlockHead.pblock=-1;
        newBlockHead.lblock=prevBlockNum;
        newBlockHead.rblock=-1;
        newBlockHead.numAttrs=numOfAttributes;
        newBlockHead.numEntries=0;
        newBlockHead.numSlots=numOfSlots;
        newBlock.setHeader(&newBlockHead);
        /*
            set block's slot map with all slots marked as free
            (i.e. store SLOT_UNOCCUPIED for all the entries)
            (use RecBuffer::setSlotMap() function)
        */
        unsigned char newBlockSM[numOfSlots];
        for(int i=0;i<numOfSlots;i++){
            newBlockSM[i]=SLOT_UNOCCUPIED;
        }
        newBlock.setSlotMap(newBlockSM);
        // if prevBlockNum != -1
        if(prevBlockNum!=-1)
        {
            // create a RecBuffer object for prevBlockNum
            // get the header of the block prevBlockNum and
            // update the rblock field of the header to the new block
            // number i.e. rec_id.block
            // (use BlockBuffer::setHeader() function)
            RecBuffer prevBlock(prevBlockNum);
            HeadInfo prevBlockHead;
            prevBlock.getHeader(&prevBlockHead);
            prevBlockHead.rblock=rec_id.block;
            prevBlock.setHeader(&prevBlockHead);
        }
        else
        {
            // update first block field in the relation catalog entry to the
            // new block (using RelCacheTable::setRelCatEntry() function)
            relCatEntry.firstBlk=rec_id.block;
            RelCacheTable::setRelCatEntry(relId,&relCatEntry);
        }

        // update last block field in the relation catalog entry to the
        // new block (using RelCacheTable::setRelCatEntry() function)
        relCatEntry.lastBlk=rec_id.block;
        RelCacheTable::setRelCatEntry(relId,&relCatEntry);
        
    }

    // create a RecBuffer object for rec_id.block
    RecBuffer blockInsert(rec_id.block);
    // insert the record into rec_id'th slot using RecBuffer.setRecord())
    blockInsert.setRecord(record,rec_id.slot);

    /* update the slot map of the block by marking entry of the slot to
       which record was inserted as occupied) */
    // (ie store SLOT_OCCUPIED in free_slot'th entry of slot map)
    // (use RecBuffer::getSlotMap() and RecBuffer::setSlotMap() functions)
    unsigned char InsertSM[numOfSlots];
    blockInsert.getSlotMap(InsertSM);
    InsertSM[rec_id.slot]=SLOT_OCCUPIED;
    blockInsert.setSlotMap(InsertSM);

    // increment the numEntries field in the header of the block to
    // which record was inserted
    // (use BlockBuffer::getHeader() and BlockBuffer::setHeader() functions)
    HeadInfo InsertHead;
    blockInsert.getHeader(&InsertHead);
    InsertHead.numEntries++;
    blockInsert.setHeader(&InsertHead);
    // Increment the number of records field in the relation cache entry for
    // the relation. (use RelCacheTable::setRelCatEntry function)
    relCatEntry.numRecs++;
    RelCacheTable::setRelCatEntry(relId,&relCatEntry);

    return SUCCESS;
}