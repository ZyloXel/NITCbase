#include "OpenRelTable.h"
#include <iostream>
#include <stdlib.h>
#include <cstring>
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

  /**** setting up Relation Catalog relation in the Relation Cache Table****/
  RecBuffer relCatBlock(RELCAT_BLOCK);

  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

  struct RelCacheEntry relCacheEntry;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;

  // allocate this on the heap because we want it to persist outside this function
  RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry *)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;

  /**** setting up Attribute Catalog relation in the Relation Cache Table ****/
  RecBuffer relCatBlock1(RELCAT_BLOCK);

  Attribute relCatRecord1[RELCAT_NO_ATTRS];
  relCatBlock1.getRecord(relCatRecord1, RELCAT_SLOTNUM_FOR_ATTRCAT);

  struct RelCacheEntry relCacheEntry1;
  RelCacheTable::recordToRelCatEntry(relCatRecord1, &relCacheEntry1.relCatEntry);
  relCacheEntry1.recId.block = RELCAT_BLOCK;
  relCacheEntry1.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;

  // allocate this on the heap because we want it to persist outside this function
  RelCacheTable::relCache[ATTRCAT_RELID] = (struct RelCacheEntry *)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[ATTRCAT_RELID]) = relCacheEntry1;

  // set up the relation cache entry for the attribute catalog similarly
  // from the record at RELCAT_SLOTNUM_FOR_ATTRCAT

  // set the value at RelCacheTable::relCache[ATTRCAT_RELID]

  RecBuffer relCatBlock2(RELCAT_BLOCK);

  Attribute relCatRecord2[RELCAT_NO_ATTRS];
  relCatBlock2.getRecord(relCatRecord2, 2);

  struct RelCacheEntry relCacheEntry2;
  RelCacheTable::recordToRelCatEntry(relCatRecord2, &relCacheEntry2.relCatEntry);
  relCacheEntry2.recId.block = RELCAT_BLOCK;
  relCacheEntry2.recId.slot = 2;

  // allocate this on the heap because we want it to persist outside this function
  RelCacheTable::relCache[2] = (struct RelCacheEntry *)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[2]) = relCacheEntry2;

  /************ Setting up Attribute cache entries ************/
  // (we need to populate attribute cache with entries for the relation catalog
  //  and attribute catalog.)

  /**** setting up Relation Catalog relation in the Attribute Cache Table ****/

  RecBuffer attrCatBlock1(ATTRCAT_BLOCK);
  Attribute attrCatRecord1[ATTRCAT_NO_ATTRS];
  struct AttrCacheEntry *attrCacheEntry1;
  struct AttrCacheEntry *attrCacheEntryTemp1 = (struct AttrCacheEntry *)malloc(sizeof(struct AttrCacheEntry));

  for (int ATTRCATRELIDX = 0; ATTRCATRELIDX <= 5; ATTRCATRELIDX++)
  {
    attrCatBlock1.getRecord(attrCatRecord1, ATTRCATRELIDX);
    attrCacheEntryTemp1->next = (struct AttrCacheEntry *)malloc(sizeof(struct AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord1, &(attrCacheEntryTemp1->next->attrCatEntry));
    if (ATTRCATRELIDX == 0)
    {
      attrCacheEntry1 = attrCacheEntryTemp1;
    }
    attrCacheEntryTemp1->next->recId.block = ATTRCAT_BLOCK;
    attrCacheEntryTemp1->next->recId.slot = ATTRCATRELIDX;
    attrCacheEntryTemp1 = attrCacheEntryTemp1->next;
  }
  attrCacheEntryTemp1->next = nullptr;
  AttrCacheTable::attrCache[RELCAT_RELID] = (struct AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
  *(AttrCacheTable::attrCache[RELCAT_RELID]) = *(attrCacheEntry1->next);
  attrCacheEntryTemp1 = attrCacheEntry1;
  attrCacheEntry1 = attrCacheEntry1->next;
  free(attrCacheEntryTemp1);

  RecBuffer attrCatBlock2(ATTRCAT_BLOCK);

  Attribute attrCatRecord2[ATTRCAT_NO_ATTRS];

  // iterate through all the attributes of the relation catalog and create a linked
  // list of AttrCacheEntry (slots 0 to 5)
  // for each of the entries, set
  //    attrCacheEntry.recId.block = ATTRCAT_BLOCK;
  //    attrCacheEntry.recId.slot = i   (0 to 5)
  //    and attrCacheEntry.next appropriately
  // NOTE: allocate each entry dynamically using malloc

  struct AttrCacheEntry *attrCacheEntry2;
  struct AttrCacheEntry *attrCacheEntryTemp2 = (struct AttrCacheEntry *)malloc(sizeof(struct AttrCacheEntry));

  for (int ATTRCATRELIDX = 6; ATTRCATRELIDX <= 11; ATTRCATRELIDX++)
  {
    attrCatBlock2.getRecord(attrCatRecord2, ATTRCATRELIDX);
    attrCacheEntryTemp2->next = (struct AttrCacheEntry *)malloc(sizeof(struct AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord2, &(attrCacheEntryTemp2->next->attrCatEntry));
    if (ATTRCATRELIDX == 6)
    {
      attrCacheEntry2 = attrCacheEntryTemp2;
    }
    attrCacheEntryTemp2->next->recId.block = ATTRCAT_BLOCK;
    attrCacheEntryTemp2->next->recId.slot = ATTRCATRELIDX;
    attrCacheEntryTemp2 = attrCacheEntryTemp2->next;
  }

  // set the next field in the last entry to nullptr
  attrCacheEntryTemp2->next = nullptr;

  AttrCacheTable::attrCache[ATTRCAT_RELID] = (struct AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
  *(AttrCacheTable::attrCache[ATTRCAT_RELID]) = *(attrCacheEntry2->next);
  attrCacheEntryTemp2 = attrCacheEntry2;
  attrCacheEntry2 = attrCacheEntry2->next;
  free(attrCacheEntryTemp2);

  //   // AttrCacheTable::attrCache[RELCAT_RELID] = /* head of the linked list */;

  //   /**** setting up Attribute Catalog relation in the Attribute Cache Table ****/

  //   // set up the attributes of the attribute cache similarly.
  //   // read slots 6-11 from attrCatBlock and initialise recId appropriately

  //   // set the value at AttrCacheTable::attrCache[ATTRCAT_RELID]
  // }

  RecBuffer attrCatBlock_students(ATTRCAT_BLOCK);
  Attribute attrCatRecord_students[ATTRCAT_NO_ATTRS];
  struct AttrCacheEntry *attrCacheEntry3;
  struct AttrCacheEntry *attrCacheEntryTemp3 = (struct AttrCacheEntry *)malloc(sizeof(struct AttrCacheEntry));

  for (int ATTRCATRELIDX = 12; ATTRCATRELIDX <= 15; ATTRCATRELIDX++)
  {
    attrCatBlock_students.getRecord(attrCatRecord_students, ATTRCATRELIDX);
    attrCacheEntryTemp3->next = (struct AttrCacheEntry *)malloc(sizeof(struct AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord_students, &(attrCacheEntryTemp3->next->attrCatEntry));
    if (ATTRCATRELIDX == 12)
    {
      attrCacheEntry3 = attrCacheEntryTemp3;
    }
    attrCacheEntryTemp3->next->recId.block = ATTRCAT_BLOCK;
    attrCacheEntryTemp3->next->recId.slot = ATTRCATRELIDX;
    attrCacheEntryTemp3 = attrCacheEntryTemp3->next;
  }
  attrCacheEntryTemp3->next = nullptr;
  AttrCacheTable::attrCache[2] = (struct AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
  *(AttrCacheTable::attrCache[2]) = *(attrCacheEntry3->next);
  attrCacheEntryTemp3 = attrCacheEntry3;
  attrCacheEntry3 = attrCacheEntry3->next;
  free(attrCacheEntryTemp3);
}

void freeLinkedList(AttrCacheEntry *head)
{
  AttrCacheEntry *current = head;
  AttrCacheEntry *nextNode;

  while (current != nullptr)
  {
    nextNode = current->next; // Save the next node
    free(current);            // Free the current node
    current = nextNode;       // Move to the next node
  }

  head = nullptr; // Optional: set head to NULL
}

OpenRelTable::~OpenRelTable()
{
  // free all the memory that you allocated in the constructor
  for (int i = 0; i < MAX_OPEN; ++i)  //clearing relcache
  {
    if (RelCacheTable::relCache[i] != nullptr)
    {
      free(RelCacheTable::relCache[i]);
    }
  }

  for (int i = 0; i < MAX_OPEN; ++i)  //clearing attrcache
  {
    if (AttrCacheTable::attrCache[i] != nullptr)
    {
      freeLinkedList(AttrCacheTable::attrCache[i]);
    }
  }
}