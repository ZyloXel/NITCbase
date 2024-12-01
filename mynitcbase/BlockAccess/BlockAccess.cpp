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
        HeadInfo header;
        
        // get header of block(blockNum) using RecBuffer::getHeader() function
        
        blockBuffer.getHeader(&header);

        // get slot map of block(blockNum) using RecBuffer::getSlotMap() function
        unsigned char slotMap[header.numSlots];

        blockBuffer.getSlotMap(slotMap);

        // search for free slot in the block 'blockNum' and store it's rec-id in rec_id
        // (Free slot can be found by iterating over the slot map of the block)
        /* slot map stores SLOT_UNOCCUPIED if slot is free and
           SLOT_OCCUPIED if slot is occupied) */
        
        for(int i=0;i<header.numSlots;i++){

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
       blockNum = header.rblock;

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

        HeadInfo header;

        header.blockType = REC;
        header.pblock = header.rblock = -1;
        header.lblock = prevBlockNum;
        header.numEntries = 0;
        header.numAttrs = numOfAttributes,header.numSlots = numOfSlots;

        blockBuffer.setHeader(&header);

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

//Stage 8
/*
NOTE: This function will copy the result of the search to the `record` argument.
      The caller should ensure that space is allocated for `record` array
      based on the number of attributes in the relation.
*/
int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
    // Declare a variable called recid to store the searched record
    RecId recId;

    /* search for the record id (recid) corresponding to the attribute with
    attribute name attrName, with value attrval and satisfying the condition op
    using linearSearch() */
    recId=BlockAccess::linearSearch(relId,attrName,attrVal,op);

    // if there's no record satisfying the given condition (recId = {-1, -1})
    //    return E_NOTFOUND;
    if(recId.block==-1 && recId.slot==-1){
        return E_NOTFOUND;
    }

    /* Copy the record with record id (recId) to the record buffer (record)
       For this Instantiate a RecBuffer class object using recId and
       call the appropriate method to fetch the record
    */
   RecBuffer recBuffer(recId.block);
   recBuffer.getRecord(record,recId.slot); //content will be stored in argument record

    return SUCCESS;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE]) {
    // if the relation to delete is either Relation Catalog or Attribute Catalog,
    //     return E_NOTPERMITTED
    // (check if the relation names are either "RELATIONCAT" and "ATTRIBUTECAT".
    // you may use the following constants: RELCAT_NAME and ATTRCAT_NAME)

    if(strcmp(relName,RELCAT_RELNAME) == 0 || strcmp(relName,ATTRCAT_RELNAME) == 0){
        return E_NOTPERMITTED;
    }
        
    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    int relId = RELCAT_RELID;

    RelCacheTable::resetSearchIndex(relId);

    Attribute relNameAttr; // (stores relName as type union Attribute)
    // assign relNameAttr.sVal = relName
    strcpy(relNameAttr.sVal,relName);

    //  linearSearch on the relation catalog for RelName = relNameAttr

    char relcat_attr_relname[ATTR_SIZE] = RELCAT_ATTR_RELNAME;

    RecId recId = linearSearch(relId,relcat_attr_relname,relNameAttr,EQ);

    // if the relation does not exist (linearSearch returned {-1, -1})
    //     return E_RELNOTEXIST
    if(recId.block == -1 || recId.slot == -1)return E_RELNOTEXIST;

    Attribute relCatEntryRecord[RELCAT_NO_ATTRS];

    /* store the relation catalog record corresponding to the relation in
       relCatEntryRecord using RecBuffer.getRecord */
    RecBuffer blockBuffer(recId.block);

    blockBuffer.getRecord(relCatEntryRecord,recId.slot);

    /* get the first record block of the relation (firstBlock) using the
       relation catalog entry record */
    /* get the number of attributes corresponding to the relation (numAttrs)
       using the relation catalog entry record */
    int numAttrs = relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
    int blockNum = relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;

    /*
        Delete all the record blocks of the relation
    */

    HeadInfo blockHeader;

    while(blockNum != -1){
        // for each record block of the relation:
        //     get block header using BlockBuffer.getHeader
        //     get the next block from the header (rblock)
        //     release the block using BlockBuffer.releaseBlock
        //
        //     Hint: to know if we reached the end, check if nextBlock = -1
        RecBuffer blockBuffer(blockNum);
        blockBuffer.getHeader(&blockHeader);
        blockNum = blockHeader.rblock;
        blockBuffer.releaseBlock();

    }


    /***
        Deleting attribute catalog entries corresponding the relation and index
        blocks corresponding to the relation with relName on its attributes
    ***/

    // reset the searchIndex of the attribute catalog

    int numberOfAttributesDeleted = 0;

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    while(true) {
        RecId attrCatRecId;
        // attrCatRecId = linearSearch on attribute catalog for RelName = relNameAttr
        attrCatRecId = linearSearch(ATTRCAT_RELID,relcat_attr_relname,relNameAttr,EQ);

        // if no more attributes to iterate over (attrCatRecId == {-1, -1})
        //     break;
        if(attrCatRecId.block == -1 || attrCatRecId.slot == -1)break;

        numberOfAttributesDeleted++;

        // create a RecBuffer for attrCatRecId.block
        // get the header of the block
        // get the record corresponding to attrCatRecId.slot

        RecBuffer blockBuffer(attrCatRecId.block);

        blockBuffer.getHeader(&blockHeader);

        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

        blockBuffer.getRecord(attrCatRecord,recId.slot);

        // declare variable rootBlock which will be used to store the root
        // block field from the attribute catalog record.
        int rootBlock = attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal;  // (This will be used later to delete any indexes if it exists)

        // Update the Slotmap for the block by setting the slot as SLOT_UNOCCUPIED
        // Hint: use RecBuffer.getSlotMap and RecBuffer.setSlotMap

        unsigned char slotMap[blockHeader.numSlots];

        blockBuffer.getSlotMap(slotMap);
        slotMap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
        blockBuffer.setSlotMap(slotMap);

        blockHeader.numEntries--;
        /* Decrement the numEntries in the header of the block corresponding to
           the attribute catalog entry and then set back the header
           using RecBuffer.setHeader */
        blockBuffer.setHeader(&blockHeader);

        /* If number of entries become 0, releaseBlock is called after fixing
           the linked list.
        */

        if (blockHeader.numEntries == 0) {

            /* Standard Linked List Delete for a Block
               Get the header of the left block and set it's rblock to this
               block's rblock
            */

            // create a RecBuffer for lblock and call appropriate methods
            RecBuffer lBlock(blockHeader.lblock);
            HeadInfo lHeader;
            lBlock.getHeader(&lHeader);

            if (blockHeader.rblock!=-1) {
                /* Get the header of the right block and set it's lblock to
                   this block's lblock */
                // create a RecBuffer for rblock and call appropriate methods-
                RecBuffer rBlock(blockHeader.rblock);
                HeadInfo rHeader;
                rBlock.getHeader(&rHeader);

                lHeader.rblock = blockHeader.rblock;
                rHeader.lblock = blockHeader.lblock;
                
                rBlock.setHeader(&rHeader);

            } else {
                // (the block being released is the "Last Block" of the relation.)
                /* update the Relation Catalog entry's LastBlock field for this
                   relation with the block number of the previous block. */
                Attribute relRecord[blockHeader.numAttrs];
                RecBuffer relBlock(RELCAT_BLOCK);
                relBlock.getRecord(relRecord,recId.slot); //recId.slot contains the slot num for the relation in the relation catalog;
                relRecord[RELCAT_LAST_BLOCK_INDEX].nVal = blockHeader.lblock;
                relBlock.setRecord(relRecord,recId.slot);
                
            }

            // (Since the attribute catalog will never be empty(why?), we do not
            //  need to handle the case of the linked list becoming empty - i.e
            //  every block of the attribute catalog gets released.)

            // call releaseBlock()
            blockBuffer.releaseBlock();
        }

        // (the following part is only relevant once indexing has been implemented)
        // if index exists for the attribute (rootBlock != -1), call bplus destroy
        if (rootBlock != -1) {
            // delete the bplus tree rooted at rootBlock using BPlusTree::bPlusDestroy()
        }

    } 
    /*** Delete the entry corresponding to the relation from relation catalog ***/
    // Fetch the header of Relcat block

    RecBuffer relCatBlock(RELCAT_BLOCK);
    HeadInfo relHead;
    relCatBlock.getHeader(&relHead);

    /* Decrement the numEntries in the header of the block corresponding to the
       relation catalog entry and set it back */
    relHead.numEntries--;

    relCatBlock.setHeader(&relHead);

    /* Get the slotmap in relation catalog, update it by marking the slot as
       free(SLOT_UNOCCUPIED) and set it back. */
    
    unsigned char slotMap[SLOTMAP_SIZE_RELCAT_ATTRCAT];
    relCatBlock.getSlotMap(slotMap);
    slotMap[recId.slot] = SLOT_UNOCCUPIED;
    relCatBlock.setSlotMap(slotMap);

    /*** Updating the Relation Cache Table ***/

    /** Update relation catalog record entry (number of records in relation
        catalog is decreased by 1) **/
    // Get the entry corresponding to relation catalog from the relation
    // cache and update the number of records and set it back
    // (using RelCacheTable::setRelCatEntry() function)

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(RELCAT_RELID,&relCatEntry);
    relCatEntry.numRecs--;
    RelCacheTable::setRelCatEntry(RELCAT_RELID,&relCatEntry);


    /** Update attribute catalog entry (number of records in attribute catalog
        is decreased by numberOfAttributesDeleted) **/
    // i.e., #Records = #Records - numberOfAttributesDeleted

    // Get the entry corresponding to attribute catalog from the relation
    // cache and update the number of records and set it back
    // (using RelCacheTable::setRelCatEntry() function)     

    RelCatEntry attrCatEntry;
    RelCacheTable::getRelCatEntry(ATTRCAT_RELID,&attrCatEntry);
    attrCatEntry.numRecs -= numberOfAttributesDeleted;
    RelCacheTable::setRelCatEntry(ATTRCAT_RELID,&attrCatEntry);

    return SUCCESS;

}

//Stage 9
/*
NOTE: the caller is expected to allocate space for the argument `record` based
      on the size of the relation. This function will only copy the result of
      the projection onto the array pointed to by the argument.
*/
int BlockAccess::project(int relId, Attribute *record) {
    // get the previous search index of the relation relId from the relation
    // cache (use RelCacheTable::getSearchIndex() function)
   RecId prevRecId;
   RelCacheTable::getSearchIndex(relId,&prevRecId);

    // declare block and slot which will be used to store the record id of the
    // slot we need to check.
    int block=-1, slot=-1;

    /* if the current search index record is invalid(i.e. = {-1, -1})
       (this only happens when the caller reset the search index)
    */
    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        // (new project operation. start from beginning)

        // get the first record block of the relation from the relation cache
        // (use RelCacheTable::getRelCatEntry() function of Cache Layer)
        RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(relId,&relCatEntry);

        // block = first record block of the relation
        // slot = 0
        block=relCatEntry.firstBlk;
        slot=0;
    }
    else
    {
        // (a project/search operation is already in progress)

        // block = previous search index's block
        block=prevRecId.block;
        slot=prevRecId.slot+1;
        // slot = previous search index's slot + 1
    }


    // The following code finds the next record of the relation
    /* Start from the record id (block, slot) and iterate over the remaining
       records of the relation */
    while (block != -1)
    {
        // create a RecBuffer object for block (using appropriate constructor!)
        RecBuffer recbuffer(block);

        // get header of the block using RecBuffer::getHeader() function
        HeadInfo head;
        recbuffer.getHeader(&head);
        // get slot map of the block using RecBuffer::getSlotMap() function
        unsigned char slotmap[head.numSlots];
        recbuffer.getSlotMap(slotmap);


        if(slot >= head.numSlots)
        {
            // (no more slots in this block)
            // update block = right block of block
            block=head.rblock;
            // update slot = 0
            slot=0;
            // (NOTE: if this is the last block, rblock would be -1. this would
            //        set block = -1 and fail the loop condition )
        }
        else if (slotmap[slot]==SLOT_UNOCCUPIED/* slot is free */)
        { // (i.e slot-th entry in slotMap contains SLOT_UNOCCUPIED)
            slot++;
            // increment slot
        }
        else {
            // (the next occupied slot / record has been found)
            break;
        }
    }

    if (block == -1){
        // (a record was not found. all records exhausted)
        return E_NOTFOUND;
    }

    // declare nextRecId to store the RecId of the record found
    RecId nextRecId{block, slot};

    // set the search index to nextRecId using RelCacheTable::setSearchIndex
    RelCacheTable::setSearchIndex(relId,&nextRecId);

    /* Copy the record with record id (nextRecId) to the record buffer (record)
       For this Instantiate a RecBuffer class object by passing the recId and
       call the appropriate method to fetch the record
    */
   RecBuffer recBuffer(nextRecId.block);
   recBuffer.getRecord(record,nextRecId.slot);

    return SUCCESS;
}

