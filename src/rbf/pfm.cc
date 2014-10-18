#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
	_pf_manager = NULL;
}


RC PagedFileManager::createFile(const char *fileName)
{
	printf("fileName =%s\n", fileName);

	if( fileName != NULL )
	{
		FILE* file;

		file = fopen( fileName, "rb");
		if( file != NULL )
		{
			fclose( file );
			return -1;
		}

		file = fopen( fileName, "wb");
		if( file != NULL )
		{
			fclose( file );
			return 0;
		}

	}

    return -1;
}

RC PagedFileManager::destroyFile(const char *fileName)
{
	int result = 0;
	if( fileName == NULL )
		result = -1;

	if( ( fileName != NULL ) && ( fileExists( fileName ) != false ) )
		result = remove( fileName );

    return result;
}


RC PagedFileManager::openFile(const char *fileName, FileHandle &fileHandle)
{
	FILE* file;

	// does file exist?

	// file does not exist
	file = fopen( fileName, "rb+" );
	if( file == NULL )
	{
		fclose( file );
		return -1;
	}

	// fileHandle is occupied
	if( fileHandle.getFile() != 0 )
	{
		fclose(file);
		return -1;
	}

	fileHandle.setFile(file);
	std::string strFileName(fileName);

	fileHandle.setFileName(strFileName);

    return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{

	if( fileHandle.getFile() == NULL )
	{
		return -1;
	}

	return fclose( fileHandle.getFile() );
}

bool PagedFileManager::fileExists(const char *filename)
{
	std::ifstream ifile(filename);
	return ifile;
};

FileHandle::FileHandle()
{
	this->file = NULL;
	this->fileName = "";
}


FileHandle::~FileHandle()
{
	this->file = NULL;
	this->fileName = "";
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
	if( pageNum >= getNumberOfPages() )
		return -1;

	if( file == NULL )
		return -1;

	if( fseek(file, pageNum * PAGE_SIZE, SEEK_SET ) != 0 )
		return -1;

	size_t result = fread( data, 1, PAGE_SIZE, file );

	return (result == PAGE_SIZE ? 0 : -1);
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	if( pageNum >= getNumberOfPages() )
		return -1;

	if( file == NULL )
		return -1;

	if( fseek(file, pageNum * PAGE_SIZE, SEEK_SET) != 0 )
		return -1;

	size_t result = fwrite( data, 1, PAGE_SIZE, file);

    return result == PAGE_SIZE ? 0 : -1;
}


RC FileHandle::appendPage(const void *data)
{
	if( file == NULL )
		return -1;

	fseek(file, 0, SEEK_END);

	size_t result = fwrite( data, 1, PAGE_SIZE, file );

    return result == PAGE_SIZE ? 0 : -1;
}


unsigned FileHandle::getNumberOfPages()
{
    if( file == NULL )
    	return 0;

    fseek( file, 0, SEEK_END );
    long size = ftell( file );

    unsigned numOfPages = (unsigned)size / PAGE_SIZE;

    return numOfPages;
}

void FileHandle::setFile(FILE* file)
{
	this->file = file;
}

FILE* FileHandle::getFile()
{
	return this->file;
}

void FileHandle::setFileName(std::string fileName)
{
	this->fileName = fileName;
}
std::string FileHandle::getFileName()
{
	return this->fileName;
}



