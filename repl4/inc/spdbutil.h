#ifndef SPDBUTIL_H
#define SPDBUTIL_H

#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>

#define COLUMN_COMPANY_NAME 32  // byte size to store company name
#define COLUMN_MODEL_NAME 128 // byte size to store model name

// get the size of attribute for provided struct
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

#define TABLE_MAX_PAGES 100  // arbitrary

/* struct for InputBuffer */
// small wrapper to interact with getline()
typedef struct {
	char* buffer;
	size_t buffer_length;
	ssize_t input_length;
} InputBuffer;

/* Meta Command Result constants */
typedef enum {
	META_COMMAND_SUCCESS,
	META_COMMAND_UNRECOGNIZED
} MetaCommandResult;

/* different Statement Preperation constants */
typedef enum {
	PREPARE_SUCCESS,
	PREPARE_STRING_TOO_LONG,
	PREPARE_SYNTAX_ERROR,
	PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

/* different exit status */
typedef enum {
	EXECUTE_SUCCESS,
	EXECUTE_TABLE_FULL
} ExecuteResult;

// currently our prepared statement just have only two possible values
typedef enum {
	STATEMENT_INSERT,
	STATEMENT_SELECT
} StatementType;

/**
 * Structure of Row
 */
typedef struct {
	uint32_t sl_no;
	uint32_t year;
	char company[COLUMN_COMPANY_NAME];
	char model[COLUMN_MODEL_NAME];
	float power;
} Row;

/**
 * Structure for Statement
 *
 * have StatementType and Row
 */
typedef struct {
	StatementType type;
	Row row_to_insert;
} Statement;

/**
 * @brief: create new input buffer to perform I/O operation
 * @param: none
 * @return: created new input buffer
 */
InputBuffer* new_input_buffer();

/**
  * @brief: print prompt, "spdb >"
  * @return: nothing
  * @print: prompt
  */
void print_prompt();

/**
 * @brief: read input to created input buffer
 * @param: created input buffer
 */
void read_input(InputBuffer* input_buffer);

/**
 * @brief: handle Meta Commands (the one starting with dots(.))
 * @param: created input buffer
 * @return: return META_COMMAND_UNRECOGNIZED if can't recognize meta command
 */
MetaCommandResult do_meta_command(InputBuffer* input_buffer);

/**
 * @brief: Prepare Statement
 * @param: created input buffer to take input,
 * @param: created statement to store data and query type from input buffer to itself
 *         and other data inside row in statement
 * @return: status code for statement preparation
 */
PrepareResult prepare_statement(InputBuffer* input_buffer, Statement *statement);



extern const uint32_t SLNO_SIZE;
extern const uint32_t YEAR_SIZE;
extern const uint32_t COMPANY_SIZE;
extern const uint32_t MODEL_SIZE;
extern const uint32_t POWER_SIZE;
extern const uint32_t ROW_SIZE;
extern const uint32_t SLNO_OFFSET;
extern const uint32_t YEAR_OFFSET;
extern const uint32_t COMPANY_OFFSET;
extern const uint32_t MODEL_OFFSET;
extern const uint32_t POWER_OFFSET;


/**
 * @brief: serializing row with data provided by source to destination with fixed offset
 * @param: data stored in Row as source
 * @param: destination in which we have to serialize data to store in table
 */
void serialize_row(Row* source, void* destination);

/**
  * @brief: deserialize row
  * @param: source provided from which we have to deserialize stored data to show
  * @param: destination in which to store deserialized data
  */
void deserialize_row(void* source, Row* destination);


extern const uint32_t PAGE_SIZE;
extern const uint32_t ROWS_PER_PAGE;
extern const uint32_t TABLE_MAX_ROWS;

/* Table structure that points to pages and keeps track of how many rows are there */
typedef struct {
	uint32_t num_rows;
	void* pages[TABLE_MAX_PAGES];
} Table;

/**
  * @brief: create page, if not exists and/or provide the offset slot of row in which to store data
  * @param: created table in which to find page to put row or to create new page
  * @param: row number w.r.t. table to find page number and if that page exists with page number
  * @return: location in page in table to store row in the page
  */
void* row_slot(Table* table, uint32_t row_num);

/**
  * @brief: execution of insert statement(query),
  * @param: created table in which to store data provided by user from insert query
  * @param: statement created from the query from input buffer provided by user
  * @return: execution status for whether the table is full not
  */
ExecuteResult execute_insert(Table* table, Statement* statement);

/**
  * @brief: execution of select statement(query),
  * @param: created table in which to store data provided by user from insert query
  * @return: execution status for whether the table is full not
  */
ExecuteResult execute_select(Table* table);

/**
  * @brief: see type of query from provied statement and execute it and provide table
  *         on/from to perform action
  * @param: created table to/from which to perform action
  * @param: created statement from which to store data in table
  * @return: execute status, return from the respective functions of performed operations
  */
ExecuteResult execute_statement(Table *table, Statement* statement);

/**
  * @brief: create a new table,
  * @return: newly created table
  */
Table* new_table();

/**
  * @brief: free the memory of table
  *         iterate through all the pages of table and free them
  * @param: table created
  */
void free_table(Table* table);

/**
  * @brief: print the data formatted accordingly to show the data stored in table in a row
  * @param: row from which to print data
  */
void print_row(Row* row);

/**
  * @brief: free the created input buffer used to take input from user by read_input(),
  *         frees all the buffer which takes input in input buffer created
  * @param: created input buffer to take input
  */
void close_input_buffer(InputBuffer* input_buffer);
#endif
