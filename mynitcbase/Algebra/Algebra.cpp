#include "Algebra.h"

#include <cstring>
#include <stdlib.h>
#include<stdio.h>

// Function to check if a string represents a number
bool isNumber(char* str) {
    int len;
    float ignore;
    
    // sscanf checks if the string can be parsed as a float
    // %f reads a floating-point number, %n records the number of characters processed
    int ret = sscanf(str, " %f %n", &ignore, &len);

    // The string is a valid number if sscanf returns 1 (successful conversion) and
    // len matches the string length (ensures the whole string was a number)
    return ret == 1 && len == strlen(str);
}

/* Function to select all records from a source relation (table) that satisfy a given condition.
 * Parameters:
 * - srcRel: The name of the source relation from which to select records.
 * - targetRel: The name of the target relation to select into (currently ignored).
 * - attr: The name of the attribute to apply the condition on.
 * - op: The operator used for the condition (e.g., EQ for equality).
 * - strVal: The value to compare against, represented as a string.
 */
int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
    // Get the relation ID of the source relation
    int srcRelId = OpenRelTable::getRelId(srcRel);
    printf("srcRelId: %d\n", srcRelId);
    // Return error code if the relation is not open
    if (srcRelId == E_RELNOTOPEN) 
    {
        return srcRelId;    
    }

    // Fetch the attribute catalog entry for the specified attribute in the source relation
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);

    // Return error code if the attribute does not exist
    if (ret == E_ATTRNOTEXIST)
        return ret;

    // Determine the type of the attribute (e.g., NUMBER or STRING)
    int type = attrCatEntry.attrType;
    Attribute attrVal;

    // Convert the string value to the appropriate attribute type
    if (type == NUMBER) {
        // Check if the string value is a valid number
        if (isNumber(strVal))
            attrVal.nVal = atof(strVal); // Convert the string to a floating-point number
        else 
            return E_ATTRTYPEMISMATCH; // Return error if the string is not a valid number
    }
    else if (type == STRING) {
        strcpy(attrVal.sVal, strVal); // Copy the string value
    }

    // Reset the search index for the source relation, starting a fresh search
    RelCacheTable::resetSearchIndex(srcRelId);

    // Retrieve the catalog entry for the relation
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

    // Print the attribute names as table headers
    printf("|");
    for (int i = 0; i < relCatEntry.numAttrs; i++) {
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);

        printf(" %s\t|", attrCatEntry.attrName);
    }
    printf("\n");

    // Perform a linear search to find records matching the condition
    while (true) {
        RecId searchRes = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);

        // Check if a valid record is found
        if (searchRes.block != -1 && searchRes.slot != -1) {
            // Retrieve the record from the found block and slot
            Attribute record[relCatEntry.numAttrs];

            RecBuffer recBuf(searchRes.block);
            recBuf.getRecord(record, searchRes.slot);

            // Print the record's attribute values
            printf("|");
            for (int i = 0; i < relCatEntry.numAttrs; i++) {
                AttrCatEntry attrCatEntry;
                AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
                
                // Print the value based on its type (NUMBER or STRING)
                (attrCatEntry.attrType == NUMBER)
                    ? printf(" %f\t|", record[i].nVal)
                    : printf(" %s\t|", record[i].sVal);
            }
            printf("\n");
        } else {
            // No more matching records found, exit the loop
            break;
        }
    }

    return SUCCESS; // Return success code
}