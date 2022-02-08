#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "string.h"
#include "iostream"
#include "fstream"

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile() : file(), page(), pageIndex(0) {
}

/* Creates a Db file if success return 0 else if fails return 1 */

int DBFile::Create(const char *f_path, fType f_type, void *startup)
{
    //try opening file if present else create and open
    try
    {
        cout << "Database File creating now\n";
        this->fileName = strdup(f_path);
        this->file.Open(0, this->fileName);
        cout << "Database File created\n";
    }
    catch (...)
    {
        cerr << "Exception raised while creating dbfile\n File path:"<<f_path <<std::endl;
        return 0;
    }
    return 1;
}

/* Loads the table(schema) from the given path to .bin files */
void DBFile::Load(Schema &f_schema, const char *loadpath)
{
    //Load the file
    auto *file1 = fopen(loadpath, "r");
    try
    {
        if (file1 == NULL) //If file is not present at given location throw error
            throw "Error";

        Record record;
        while (record.SuckNextRecord(&f_schema, file1) == 1) //Scan until every record is accessed
        {
            this->Add(record);
        }

        this->file.AddPage(&this->page, pageIndex++);
        cout << "Total number of pages added to file = " << pageIndex << std::endl;
    }
    catch (...)
    {
        cerr<<"Error in loading file"<<std::endl;
        fclose(file1);
    }
}


/* Opens a given file if scucess returns 1 else 0 */ 
int DBFile::Open(const char *f_path)
{
    try
    {
        this->fileName = strdup(f_path);
        this->file.Open(1, this->fileName);
        cout << "File present at path: " << std::string(f_path) << " is now opened. \n";
    }
    catch (...)
    {
        cout << "Failed opening the file at :" << f_path << std::endl;
        return 0;

    }
    return 1;
}

/*Closes a opened DBfile if success returns 1 else 0*/
int DBFile::Close()
{
    try
    {
        ifstream file(this->fileName);
        this->file.Close();

    }
    catch (exception e)
    {
        cerr << e.what();
        return 0;
    }
    return 1;
}


/*Adding a record to DB file*/
void DBFile::Add(Record &rec)
{
    try
    {
         off_t fileSize = this->file.GetLength();
    int pageFull = this->page.Append(&rec);

    if (pageFull == 0)  // which means current page is full
    {
        this->page.EmptyItOut();
        this->page.Append(&rec);
        this->file.AddPage(&this->page, !fileSize ? 0 : fileSize - 1);
        pageIndex++;
    }
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
    
   
}


/* Moving the seeker to the start of file*/
void DBFile::MoveFirst()
{
    try
    {
        if (this->file.GetLength() != 0)
    {
        this->file.GetPage(&this->page, 0); //get first page
    }
    else
        throw "Cannot perform operation move first check whether the data loaded or not\n";
        
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
    
}

/* Get records with no constratints*/
int DBFile::GetNext(Record &nextRecord)
{
    try
    {
        if (!this->page.GetFirst(&nextRecord))
    {
        if (++pageIndex >= this->file.GetLength() - 1)
            return 0;

        this->file.GetPage(&this->page, pageIndex);
        this->page.GetFirst(&nextRecord);
    }
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
    return 1;
}



/* Getting records with a given Constraint */
int DBFile::GetNext(Record &nextRecord, CNF &cnf, Record &literal)
{
    ComparisonEngine compEngine;
    try
    {
         while (this->GetNext(nextRecord))   // made changes corresponding to address of nextRec
        {
        if (compEngine.Compare(&nextRecord, &literal, &cnf))
            return 1;
        }
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
   
    return 0;
}