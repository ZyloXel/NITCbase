#include "OpenRelTable.h"
#include <iostream>
#include <cstring>

void clearList(AttrCacheEntry *head)
{
  for (AttrCacheEntry *it = head, *next; it != nullptr; it = next)
  {
    next = it->next;
    if(it)free(it);
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
    OpenRelTable::tableMetaInfo[i].free = true;
  }

  /************ Setting up Relation Cache entries ************/
  // (we need to populate relation cache with entries for the relation catalog
  //  and attribute catalog.)

  // setting up the variables
  RecBuffer relCatBlock(RELCAT_BLOCK);
  Attribute relCatRecord[RELCAT_NO_ATTRS];

  // loop through rel catalog
  for (int relId = RELCAT_RELID; relId <= ATTRCAT_RELID; relId++)
  {
    // fetch relid th record from relcatalog
    relCatBlock.getRecord(relCatRecord, relId);

    struct RelCacheEntry relCacheEntry;                                           // temp storage for rel cache entry
    RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry); // convert record to cache
    relCacheEntry.recId.block = RELCAT_BLOCK;                                     // Assign blocknum to recID
    relCacheEntry.recId.slot = relId;                                             // Assign slot num to recID

    // allocate memory for cache entry and store them in rel cache
    RelCacheTable::relCache[relId] = (struct RelCacheEntry *)malloc(sizeof(RelCacheEntry));
    *(RelCacheTable::relCache[relId]) = relCacheEntry;                                  // copying constructed entry to cache
    tableMetaInfo[relId].free = false;                                                  // block occupied
    strcpy(tableMetaInfo[relId].relName, relCacheEntry.relCatEntry.relName); // relation name stored
  }

  /************ Setting up Attribute cache entries ************/
  // (we need to populate attribute cache with entries for the relation catalog
  //  and attribute catalog.)

  RecBuffer attrCatBlockRel(ATTRCAT_BLOCK);

    Attribute attrCatRecordRel[ATTRCAT_NO_ATTRS];

    struct AttrCacheEntry* attrCacheEntryRel = new AttrCacheEntry;
    struct AttrCacheEntry* attrCacheEntryRelTemp =  attrCacheEntryRel; // (struct AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    
    for(int i = 0;i < RELCAT_NO_ATTRS; i++){

        attrCatBlockRel.getRecord(attrCatRecordRel,i);

        //this converts the record to a attr catalog entry which is an entity in the struct AttrCacheEntry
        AttrCacheTable::recordToAttrCatEntry(attrCatRecordRel,&(attrCacheEntryRelTemp->attrCatEntry));

        attrCacheEntryRelTemp->recId.block = ATTRCAT_BLOCK;
        attrCacheEntryRelTemp->recId.slot = i;
        attrCacheEntryRelTemp->searchIndex = {-1,-1};

        if(i < RELCAT_NO_ATTRS-1) attrCacheEntryRelTemp->next = new AttrCacheEntry; // here we check if its the last iteration or not if it is we go to else scope and temp->next = nullptr
        else attrCacheEntryRelTemp->next = nullptr;

        attrCacheEntryRelTemp = attrCacheEntryRelTemp->next;
    
    }

    AttrCacheTable::attrCache[RELCAT_RELID] = attrCacheEntryRel; //here since the pointer is already allocated in heap we dont need to malloc again...just set the pointer :)

    /**** setting up Attribute Catalog relation in the Attribute Cache Table ****/
    //same as the above code

    RecBuffer attrCatBlockAttr(ATTRCAT_BLOCK);

    Attribute attrCatRecordAttr[ATTRCAT_NO_ATTRS];

    struct AttrCacheEntry* attrCacheEntryAttr = new AttrCacheEntry;
    struct AttrCacheEntry* attrCacheEntryAttrTemp = attrCacheEntryAttr;

    int startAttrIndex = RELCAT_NO_ATTRS,lastAttrIndex = RELCAT_NO_ATTRS + ATTRCAT_NO_ATTRS;

    for(int i = startAttrIndex;i <= lastAttrIndex;i++){

        attrCatBlockAttr.getRecord(attrCatRecordAttr,i);
        AttrCacheTable::recordToAttrCatEntry(attrCatRecordAttr,&(attrCacheEntryAttrTemp->attrCatEntry));

        attrCacheEntryAttrTemp->recId.block = ATTRCAT_BLOCK;
        attrCacheEntryAttrTemp->recId.slot = i;
        attrCacheEntryAttrTemp->searchIndex = {-1,-1};

        if(i<lastAttrIndex) attrCacheEntryAttrTemp->next = new AttrCacheEntry;
        else attrCacheEntryAttrTemp->next = nullptr;

        attrCacheEntryAttrTemp = attrCacheEntryAttrTemp->next;
    }

    AttrCacheTable::attrCache[ATTRCAT_RELID] = attrCacheEntryAttr; // Store the head of the list in the attribute cache
    /*int numberOfAttributes = RelCacheTable::relCache[relId]->relCatEntry.numAttrs;
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
  
 // Setting up tableMetaInfo entries
  
 //  in the tableMetaInfo array
 //   set free = false for RELCAT_RELID and ATTRCAT_RELID
 //    set relname for RELCAT_RELID and ATTRCAT_RELID
 for(int i = 0; i < MAX_OPEN; i++) {
     tableMetaInfo[i].free = true;
 }
 tableMetaInfo[RELCAT_RELID].free = false;
 strcpy(tableMetaInfo[RELCAT_RELID].relName, RELCAT_RELNAME);
  
 tableMetaInfo[ATTRCAT_RELID].free = false;
 strcpy(tableMetaInfo[ATTRCAT_RELID].relName, ATTRCAT_RELNAME); */
}

//Stage 8
OpenRelTable::~OpenRelTable() {

    for (int i=2;i<MAX_OPEN;i++)
    {
        if(!tableMetaInfo->free)
        {
            // close the relation using openRelTable::closeRel().
            OpenRelTable::closeRel(i);
        }
    }

    /**** Closing the catalog relations in the relation cache ****/

    //releasing the relation cache entry of the attribute catalog

    if (RelCacheTable::relCache[ATTRCAT_RELID]->dirty==true) {

        /* Get the Relation Catalog entry from RelCacheTable::relCache
        Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */

        // declaring an object of RecBuffer class to write back to the buffer
        RelCatEntry relCatEntry=RelCacheTable::relCache[ATTRCAT_RELID]->relCatEntry;
        Attribute relCatRecord[RELCAT_NO_ATTRS];
        RelCacheTable::relCatEntryToRecord(&relCatEntry,relCatRecord);
        RecId recId=RelCacheTable::relCache[ATTRCAT_RELID]->recId;
        RecBuffer relCatBlock(recId.block);

        // Write back to the buffer using relCatBlock.setRecord() with recId.slot
        relCatBlock.setRecord(relCatRecord,recId.slot);
    }
    // free the memory dynamically allocated to this RelCacheEntry
    free(RelCacheTable::relCache[ATTRCAT_RELID]);


    //releasing the relation cache entry of the relation catalog

    if(RelCacheTable::relCache[RELCAT_RELID]->dirty==true) {

        /* Get the Relation Catalog entry from RelCacheTable::relCache
        Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
        RelCatEntry relCatEntry = RelCacheTable::relCache[RELCAT_RELID]->relCatEntry;
        Attribute relCatRecord[RELCAT_NO_ATTRS];
        RelCacheTable::relCatEntryToRecord(&relCatEntry,relCatRecord);

        // declaring an object of RecBuffer class to write back to the buffer
        RecId recId=RelCacheTable::relCache[RELCAT_RELID]->recId;
        RecBuffer relCatBlock(recId.block);

        // Write back to the buffer using relCatBlock.setRecord() with recId.slot
        relCatBlock.setRecord(relCatRecord,recId.slot);
    }
    // free the memory dynamically allocated for this RelCacheEntry
    free(RelCacheTable::relCache[RELCAT_RELID]);


    // free the memory allocated for the attribute cache entries of the
    // relation catalog and the attribute catalog
    for(int i=0;i<2;i++){
      struct AttrCacheEntry *entry=AttrCacheTable::attrCache[i];
      while ((entry!=nullptr)){
        struct AttrCacheEntry *temp=entry;
        entry=entry->next;
        free(temp);
      }      
    }
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE])
{

  /* traverse through the tableMetaInfo array,
    find the entry in the Open Relation Table corresponding to relName.*/
  for (int i = 0; i < MAX_OPEN; i++)
  {
    if (strcmp(tableMetaInfo[i].relName, relName) == 0)
    {
      return i;
    }
  }

  // if found return the relation id, else indicate that the relation do not
  // have an entry in the Open Relation Table.
  return E_RELNOTOPEN;
}

int OpenRelTable::getFreeOpenRelTableEntry()
{

  /* traverse through the tableMetaInfo array,
    find a free entry in the Open Relation Table.*/
  for (int i = 2; i < MAX_OPEN; i++)
  {
    if (tableMetaInfo[i].free)
    {
      return i;
    }
  }

  // if found return the relation id, else return E_CACHEFULL.
  return E_CACHEFULL;
}

int OpenRelTable::closeRel(int relId)
{
  if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)
  {
    return E_NOTPERMITTED;
  }

  if (relId < 0 || relId >= MAX_OPEN)
  {
    return E_OUTOFBOUND;
  }

  if (tableMetaInfo[relId].free)
  {
    return E_RELNOTOPEN;
  }

  // Stage 7
  /****** Releasing the Relation Cache entry of the relation ******/

  if (RelCacheTable::relCache[relId]->dirty == true)
  {

    /* Get the Relation Catalog entry from RelCacheTable::relCache
    Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    RelCatEntry relCatEntry = RelCacheTable::relCache[relId]->relCatEntry;
    RelCacheTable::relCatEntryToRecord(&relCatEntry, relCatRecord);

    // declaring an object of RecBuffer class to write back to the buffer
    RecBuffer relCatBlock(RelCacheTable::relCache[relId]->recId.block);
    relCatBlock.setRecord(relCatRecord, RelCacheTable::relCache[relId]->recId.slot);

    // Write back to the buffer using relCatBlock.setRecord() with recId.slot
  }

  /****** Releasing the Attribute Cache entry of the relation ******/

  // free the memory allocated in the attribute caches which was
  // allocated in the OpenRelTable::openRel() function

  // (because we are not modifying the attribute cache at this stage,
  // write-back is not required. We will do it in subsequent
  // stages when it becomes needed)
  tableMetaInfo[relId].free = true;
  strcpy(tableMetaInfo[relId].relName, "");
  RelCacheTable::relCache[relId] = nullptr;
  AttrCacheTable::attrCache[relId] = nullptr;

  /****** Set the Open Relation Table entry of the relation as free ******/

  // update `metainfo` to set `relId` as a free slot

  return SUCCESS;
}

int OpenRelTable::openRel(char relName[ATTR_SIZE])
{
  // Let relId be used to store the free slot.
  int relId = getRelId(relName);

  // Check if the relation is already open (checked using OpenRelTable::getRelId())
  if (relId != E_RELNOTOPEN)
  {
    // Return the existing relation ID
    return relId;
  }

  // Find a free slot in the Open Relation Table using OpenRelTable::getFreeOpenRelTableEntry().
  int fslot = getFreeOpenRelTableEntry();

  // If no free slot is found, return E_CACHEFULL indicating the cache is full.
  if (fslot == E_CACHEFULL)
  {
    return E_CACHEFULL;
  }

  /****** Setting up Relation Cache entry for the relation ******/

  // Create an Attribute object to hold the relation name and copy relName into it.
  Attribute relNameAttribute;
  memcpy(relNameAttribute.sVal, relName, ATTR_SIZE);

  // Reset the search index for the relation catalog (RELCAT_RELID) before calling linearSearch().
  RelCacheTable::resetSearchIndex(RELCAT_RELID);

  // Search for the entry with relation name, relName, in the Relation Catalog using BlockAccess::linearSearch().
  RecId relcatRecId = BlockAccess::linearSearch(RELCAT_RELID, (char *)RELCAT_ATTR_RELNAME, relNameAttribute, EQ);

  // If the relation is not found in the catalog, return E_RELNOTEXIST.
  if (relcatRecId.block == -1 && relcatRecId.slot == -1)
  {
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
  RelCacheTable::relCache[fslot] = (RelCacheEntry *)malloc(sizeof(RelCacheEntry));
  RelCacheTable::relCache[fslot]->recId = relcatRecId;
  RelCacheTable::relCache[fslot]->relCatEntry = relCatEntry;

  // Initialize a list to cache attribute entries, matching the number of attributes in the relation.
  int numAttrs = relCatEntry.numAttrs;
  AttrCacheEntry *listHead = createAttrCacheEntryList(numAttrs);
  AttrCacheEntry *node = listHead;

  // Reset the search index for the attribute catalog (ATTRCAT_RELID) before performing the search.
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

  // Loop to search for each attribute of the relation in the Attribute Catalog and cache them.
  while (true)
  {
    // Search for attributes associated with the relation name.
    RecId searchRes = BlockAccess::linearSearch(ATTRCAT_RELID, (char *)ATTRCAT_ATTR_RELNAME, relNameAttribute, EQ);

    // If a valid attribute is found, retrieve and cache it.
    if (searchRes.block != -1 && searchRes.slot != -1)
    {
      Attribute attrcatRecord[ATTRCAT_NO_ATTRS];
      RecBuffer attrRecBuffer(searchRes.block);
      attrRecBuffer.getRecord(attrcatRecord, searchRes.slot);

      AttrCatEntry attrCatEntry;
      AttrCacheTable::recordToAttrCatEntry(attrcatRecord, &attrCatEntry);

      node->recId = searchRes;
      node->attrCatEntry = attrCatEntry;
      node = node->next;
    }
    else
    {
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