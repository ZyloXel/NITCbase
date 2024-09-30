#include "BlockAccess.h"
#include <cstring>
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