#ifndef _pfm_h_
#define _pfm_h_

#include "stddef.h"

#include<string>
#include<cstdlib>
#include<cstdio>
#include<stdio.h>
#include<stdlib.h>
#include<fstream>
#include<iostream>

typedef int RC;
typedef unsigned PageNum;

#define PAGE_SIZE 4096

class FileHandle;


class PagedFileManager
{
public:
    static PagedFileManager* instance();                     // Access to the _pf_manager instance

    RC createFile    (const char *fileName);                         // Create a new file
    RC destroyFile   (const char *fileName);                         // Destroy a file
    RC openFile      (const char *fileName, FileHandle &fileHandle); // Open a file
    RC closeFile     (FileHandle &fileHandle);                       // Close a file
    bool fileExists		(const char *filename);

protected:
    PagedFileManager();                                   // Constructor
    ~PagedFileManager();                                  // Destructor

private:
    static PagedFileManager *_pf_manager;
};


class FileHandle
{
public:
    FileHandle();                                                    // Default constructor
    ~FileHandle();                                                   // Destructor

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);       								// Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file

    void setFile(FILE* file);
    void setFileName(std::string fileName);

    FILE* getFile();
    std::string getFileName();

private:
    FILE *file;
    std::string fileName;
 };

 #endif
