#include "OpenRelTable.h"
#include <iostream>
#include <cstring>

void clearList(AttrCacheEntry *head)
{
    for (AttrCacheEntry *it = head, *next; it != nullptr; it = next)
    {
        next = it->next;
        free(it);
    }
}

AttrCacheEntry *createAttrCacheEntryList(int size)
{
    AttrCacheEntry *head = nullptr, *curr = nullptr;
    head = curr = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
    size--;
    while (size--)
    {
        curr->next = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
        curr = curr->next;
    }
    curr->next = nullptr;

    return head;
}
OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];
OpenRelTable::OpenRelTable()
{

    // initialize relCache and attrCache with nullptr
    for (int i = 0; i < MAX_OPEN; ++i)
    {
        RelCacheTable::relCache[i] = nullptr;
        AttrCacheTable::attrCache[i] = nullptr;
    }

    /************ Setting up Relation Cache entries ************/
    // (we need to populate relation cache with entries for the relation catalog
    //  and attribute catalog.)

    // setting up the variables
    RecBuffer relCatBlock(RELCAT_BLOCK);
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    RelCacheEntry *relCacheEntry = nullptr;

    for (int relId = RELCAT_RELID; relId <= ATTRCAT_RELID + 1; relId++)
    {
        relCatBlock.getRecord(relCatRecord, relId);

        relCacheEntry = (RelCacheEntry *)malloc(sizeof(RelCacheEntry));
        RelCacheTable::recordToRelCatEntry(relCatRecord, &(relCacheEntry->relCatEntry));
        relCacheEntry->recId.block = RELCAT_BLOCK;
        relCacheEntry->recId.slot = relId;

        RelCacheTable::relCache[relId] = relCacheEntry;
    }

    /************ Setting up Attribute cache entries ************/
    // (we need to populate attribute cache with entries for the relation catalog
    //  and attribute catalog.)

    // setting up the variables
    RecBuffer attrCatBlock(ATTRCAT_BLOCK);
    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    AttrCacheEntry *attrCacheEntry = nullptr, *head = nullptr;

    for (int relId = RELCAT_RELID, recordId = 0; relId <= ATTRCAT_RELID + 1; relId++)
    {
        int numberOfAttributes = RelCacheTable::relCache[relId]->relCatEntry.numAttrs;
        head = createAttrCacheEntryList(numberOfAttributes);
        attrCacheEntry = head;

        while (numberOfAttributes--)
        {
            attrCatBlock.getRecord(attrCatRecord, recordId);

            AttrCacheTable::recordToAttrCatEntry(
                attrCatRecord,
                &(attrCacheEntry->attrCatEntry));
            attrCacheEntry->recId.slot = recordId++;
            attrCacheEntry->recId.block = ATTRCAT_BLOCK;

            attrCacheEntry = attrCacheEntry->next;
        }

        AttrCacheTable::attrCache[relId] = head;
    }

    RecBuffer relCatBuffer(RELCAT_BLOCK);
    Attribute relCatRecord1[RELCAT_NO_ATTRS];

    HeadInfo relCatHeader;
    relCatBuffer.getHeader(&relCatHeader);

    int relationIndex = -1;
    // char* relationName = "Students";

    for (int index = 2; index < relCatHeader.numEntries + 4; index++)
    {
        relCatBuffer.getRecord(relCatRecord, index);

        if (strcmp(relCatRecord1[RELCAT_REL_NAME_INDEX].sVal,
                   "Students") == 0)
        { // matching the name of the record we want
            relationIndex = index;
        }
    }

    /************ Setting up tableMetaInfo entries ************/

    // in the tableMetaInfo array
    //   set free = false for RELCAT_RELID and ATTRCAT_RELID
    //   set relname for RELCAT_RELID and ATTRCAT_RELID
    for(int i = 0; i < MAX_OPEN; i++) {
        tableMetaInfo[i].free = true;
    }
    tableMetaInfo[RELCAT_RELID].free = false;
    strcpy(tableMetaInfo[RELCAT_RELID].relName, RELCAT_RELNAME);

    tableMetaInfo[ATTRCAT_RELID].free = false;
    strcpy(tableMetaInfo[ATTRCAT_RELID].relName, ATTRCAT_RELNAME);
}

OpenRelTable::~OpenRelTable() {

  // close all open relations (from rel-id = 2 onwards. Why?)
  for (int i = 2; i < MAX_OPEN; ++i) {
    if (!tableMetaInfo[i].free) {
      OpenRelTable::closeRel(i); // we will implement this function later
    }
  }

  // free the memory allocated for rel-id 0 and 1 in the caches
    free(RelCacheTable::relCache[RELCAT_RELID]);
    free(RelCacheTable::relCache[ATTRCAT_RELID]);
}

/* This function will open a relation having name `relName`.
Since we are currently only working with the relation and attribute catalog, we
will just hardcode it. In subsequent stages, we will loop through all the relations
and open the appropriate one.
*/
int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {

  /* traverse through the tableMetaInfo array,
    find the entry in the Open Relation Table corresponding to relName.*/
    for(int i = 0; i < MAX_OPEN; i++) {
        printf("tableMetaInfo[i].relName: %s\n", tableMetaInfo[i].relName);
        if(strcmp(tableMetaInfo[i].relName, relName) == 0) {
            return i;
        }
    }

  // if found return the relation id, else indicate that the relation do not
  // have an entry in the Open Relation Table.
    return E_RELNOTOPEN;
}

int OpenRelTable::getFreeOpenRelTableEntry() {

  /* traverse through the tableMetaInfo array,
    find a free entry in the Open Relation Table.*/
    for(int i = 2; i < MAX_OPEN; i++) {
        if(tableMetaInfo[i].free) {
            return i;
        }
    }

  // if found return the relation id, else return E_CACHEFULL.
    return E_CACHEFULL;
}


int OpenRelTable::closeRel(int relId) {
  if (relId == RELCAT_RELID || relId == ATTRCAT_RELID) {
    return E_NOTPERMITTED;
  }

  if (relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  if (tableMetaInfo[relId].free) {
    return E_RELNOTOPEN;
  }

  // free the memory allocated in the relation and attribute caches which was
  // allocated in the OpenRelTable::openRel() function
    clearList(AttrCacheTable::attrCache[relId]);

  // update `tableMetaInfo` to set `relId` as a free slot
    tableMetaInfo[relId].free = true;
  // update `relCache` and `attrCache` to set the entry at `relId` to nullptr
    RelCacheTable::relCache[relId] = nullptr;
    AttrCacheTable::attrCache[relId] = nullptr;

  return SUCCESS;
}

int OpenRelTable::openRel(char relName[ATTR_SIZE]) {
    // Let relId be used to store the free slot.
    int relId = getRelId(relName);

    // Check if the relation is already open (checked using OpenRelTable::getRelId())
    if (relId != E_RELNOTOPEN) {
        // Return the existing relation ID
        return relId;
    }

    // Find a free slot in the Open Relation Table using OpenRelTable::getFreeOpenRelTableEntry().
    int fslot = getFreeOpenRelTableEntry();

    // If no free slot is found, return E_CACHEFULL indicating the cache is full.
    if (fslot == E_CACHEFULL) {
        return E_CACHEFULL;
    }

    /****** Setting up Relation Cache entry for the relation ******/

    // Create an Attribute object to hold the relation name and copy relName into it.
    Attribute relNameAttribute;
    memcpy(relNameAttribute.sVal, relName, ATTR_SIZE);

    // Reset the search index for the relation catalog (RELCAT_RELID) before calling linearSearch().
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    // Search for the entry with relation name, relName, in the Relation Catalog using BlockAccess::linearSearch().
    RecId relcatRecId = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, relNameAttribute, EQ);

    // If the relation is not found in the catalog, return E_RELNOTEXIST.
    if (relcatRecId.block == -1 && relcatRecId.slot == -1) {
        return E_RELNOTEXIST;
    }

    // Retrieve the record from the block using the record ID (relcatRecId).
    RecBuffer recBuffer(relcatRecId.block);
    Attribute record[RELCAT_NO_ATTRS];
    recBuffer.getRecord(record, relcatRecId.slot);

    // Create a RelCatEntry structure to store the relation catalog entry.
    RelCatEntry relCatEntry;
    RelCacheTable::recordToRelCatEntry(record, &relCatEntry);

    // Allocate memory for a new RelCacheEntry and set up the cache entry in the free slot.
    RelCacheTable::relCache[fslot] = (RelCacheEntry*) malloc(sizeof(RelCacheEntry));
    RelCacheTable::relCache[fslot]->recId = relcatRecId;
    RelCacheTable::relCache[fslot]->relCatEntry = relCatEntry;

    // Initialize a list to cache attribute entries, matching the number of attributes in the relation.
    int numAttrs = relCatEntry.numAttrs;
    AttrCacheEntry* listHead = createAttrCacheEntryList(numAttrs);
    AttrCacheEntry* node = listHead;

    // Reset the search index for the attribute catalog (ATTRCAT_RELID) before performing the search.
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    // Loop to search for each attribute of the relation in the Attribute Catalog and cache them.
    while (true) {
        // Search for attributes associated with the relation name.
        RecId searchRes = BlockAccess::linearSearch(ATTRCAT_RELID, (char*)ATTRCAT_ATTR_RELNAME, relNameAttribute, EQ);

        // If a valid attribute is found, retrieve and cache it.
        if (searchRes.block != -1 && searchRes.slot != -1) {
            Attribute attrcatRecord[ATTRCAT_NO_ATTRS];
            RecBuffer attrRecBuffer(searchRes.block);
            attrRecBuffer.getRecord(attrcatRecord, searchRes.slot);

            AttrCatEntry attrCatEntry;
            AttrCacheTable::recordToAttrCatEntry(attrcatRecord, &attrCatEntry);

            node->recId = searchRes;
            node->attrCatEntry = attrCatEntry;
            node = node->next;
        } else {
            // Break out of the loop when no more attributes are found.
            break;
        }
    }

    // Link the cached attribute list to the corresponding entry in the Attribute Cache Table.
    AttrCacheTable::attrCache[fslot] = listHead;

    // Mark the slot as occupied and set the relation name in the metadata information table.
    OpenRelTable::tableMetaInfo[fslot].free = false;
    memcpy(OpenRelTable::tableMetaInfo[fslot].relName, relCatEntry.relName, ATTR_SIZE);

    // Return the ID of the free slot where the relation was opened.
    return fslot;
}