#include "../inc/spdbutil.h"



/* getting size of inputs for insert query and setting offset to store it */
// we'll have a table like this

/* attr       size               offset

   sl_no       4                 0
   year        4                 4
   company     32                8
   model       128               40
   power       4                 168

   total       172
   */
const uint32_t SLNO_SIZE = size_of_attribute(Row, sl_no);
const uint32_t YEAR_SIZE = size_of_attribute(Row, year);
const uint32_t COMPANY_SIZE = size_of_attribute(Row, company);
const uint32_t MODEL_SIZE = size_of_attribute(Row, model);
const uint32_t POWER_SIZE = size_of_attribute(Row, power);
const uint32_t ROW_SIZE = SLNO_SIZE + YEAR_SIZE + COMPANY_SIZE + MODEL_SIZE + POWER_SIZE;
const uint32_t SLNO_OFFSET = 0;
const uint32_t YEAR_OFFSET = SLNO_SIZE + SLNO_OFFSET;
const uint32_t COMPANY_OFFSET = YEAR_OFFSET + YEAR_SIZE;
const uint32_t MODEL_OFFSET = COMPANY_OFFSET + COMPANY_SIZE;
const uint32_t POWER_OFFSET = MODEL_OFFSET + MODEL_SIZE;



/* setting page_size = 4KB
 * that is most system of same page size of 4KB, that means OS will move our
 * page in and out as whole without breaking them up.
 * Also rows should not cross the page boundaries, since pages are not going to
 * exist side-by-side to each other in memory. */
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = TABLE_MAX_PAGES * ROWS_PER_PAGE;

/**
 * @brief: create new input buffer to perform I/O operation
 * @param: none
 * @return: created new input buffer
 */
InputBuffer* new_input_buffer() {
	InputBuffer* input_buffer = (InputBuffer*)malloc(sizeof(InputBuffer));
	input_buffer->buffer = NULL;
	input_buffer->buffer_length = 0;
	input_buffer->input_length = 0;
	return input_buffer;
}

/**
  * @brief: print prompt, "spdb >"
  * @return: nothing
  * @print: prompt
  */
void print_prompt() {
	printf("spdb > ");
}

/**
 * @brief: read input to created input buffer
 * @param: created input buffer
 */
void read_input(InputBuffer* input_buffer) {
	/* general syntax of getline():
	 * getline(&buffer, &length, stdin);
	 * and prototype for getline() is:
	 * ssize_t getline(char** lineptr, size_t* n, FILE* stream);
	 * &buffer -> address of first character position to store input string, not
	 * the base address of buffer but of the first character of buffer(**)
	 * &length -> address of variable that holds size
	 * stdin -> when stdin is specified, standard input is read, else you can
	 * also use getline() read the file
	 * */
	ssize_t bytes_read =
		getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
	if (bytes_read <= 0) {
		printf("Error reading input\n");
		/* EXIT_FAILURE -> same as exit(1), representing failure
		 * EXIT_SUCCESS -> same as exit(0), representing success
		 * EXIT_SUCCESS, EXIT_FAILURE is std. way as some system actually use 0, 1
		 * for opposite as said above
		 * */
		exit(EXIT_FAILURE);
	}
	// Ignore trailing new line
	input_buffer->input_length = bytes_read - 1;
	input_buffer->buffer[bytes_read - 1] = 0;
}

/**
 * @brief: handle Meta Commands (the one starting with dots(.))
 * @param: created input buffer
 * @return: return META_COMMAND_UNRECOGNIZED if can't recognize meta command
 */
MetaCommandResult do_meta_command(InputBuffer* input_buffer) {
	if (strcmp(input_buffer->buffer, ".exit") == 0) {
		exit(EXIT_SUCCESS);
	}
	else {
		return META_COMMAND_UNRECOGNIZED;
	}
}

/**
 * @brief: Prepare Statement, put data from created input buffer to created Statement,
 *         insert statement type and then data from input buffer to statement->row
 * @param: created input buffer to take input,
 * @param: created statement to store data and query type from input buffer to itself
 *         and other data inside row in statement
 * @return: status code for statement preparation
 */
PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
	/* using strncmp, cause there will be queries after insert, select etc. queries */
	if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
		statement->type = STATEMENT_INSERT;
		/* insert data to different blocks of Row */
		int args_assigned = sscanf(input_buffer->buffer, "insert %d %d %s %s %f",
				&(statement->row_to_insert.sl_no),
				&(statement->row_to_insert.year),
				statement->row_to_insert.company,
				statement->row_to_insert.model,
				&(statement->row_to_insert.power));
		if (args_assigned < 5) {
			return PREPARE_SYNTAX_ERROR;
		}
		return PREPARE_SUCCESS;
	}
	if (strncmp(input_buffer->buffer, "select", 6) == 0) {
		statement->type = STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}
	return PREPARE_UNRECOGNIZED_STATEMENT;
}

/**
 * @brief: serializing row with data provided by source to destination with fixed offset
 * @param: data stored in Row as source
 * @param: destination in which we have to serialize data to store in table
 */
void serialize_row(Row* source, void* destination) {
	memcpy(destination + SLNO_OFFSET, &(source->sl_no), SLNO_SIZE);
	memcpy(destination + YEAR_OFFSET, &(source->year), YEAR_SIZE);
	memcpy(destination + COMPANY_OFFSET, &(source->company), COMPANY_SIZE);
	memcpy(destination + MODEL_OFFSET, &(source->model), MODEL_SIZE);
	memcpy(destination + POWER_OFFSET, &(source->power), POWER_SIZE);
}

/**
  * @brief: deserialize row
  * @param: source provided from which we have to deserialize stored data to show
  * @param: destination in which to store deserialized data
  */
void deserialize_row(void* source, Row* destination) {
	memcpy(&(destination->sl_no), source + SLNO_OFFSET, SLNO_SIZE);
	memcpy(&(destination->year), source + YEAR_OFFSET, YEAR_SIZE);
	memcpy(&(destination->company), source + COMPANY_OFFSET, COMPANY_SIZE);
	memcpy(&(destination->model), source + MODEL_OFFSET, MODEL_SIZE);
	memcpy(&(destination->power), source + POWER_OFFSET, POWER_SIZE);
}

/**
  * @brief: create page, if not exists and/or provide the offset slot of row in which to store data
  * @param: created table in which to find page to put row or to create new page
  * @param: row number w.r.t. table to find page number and if that page exists with page number
  * @return: location in page in table to store row in the page
  */
void* row_slot(Table* table, uint32_t row_num) {
	uint32_t page_num = row_num / ROWS_PER_PAGE;
	void *page = table->pages[page_num];
	if (page == NULL) {
		page = table->pages[page_num] = malloc(PAGE_SIZE);  // 4KB size page
	}
	uint32_t row_offset = row_num % ROWS_PER_PAGE;
	uint32_t byte_offset = row_offset * ROW_SIZE;
	return page + byte_offset;
}

/**
  * @brief: execution of insert statement(query),
  *         check if table is full, if not, serialize row from statement set from InputBuffer
  *         to table with provided page with correct byte offset and return status according to it
  * @param: created table in which to store data provided by user from insert query
  * @param: statement created from the query from input buffer provided by user
  * @return: execution status for whether the table is full not
  */
ExecuteResult execute_insert(Table* table, Statement* statement) {
	if (table->num_rows >= TABLE_MAX_ROWS) {
		return EXECUTE_TABLE_FULL;
	}

	Row* row_to_insert = &(statement->row_to_insert);

	/* source is row_to_insert,
	   destination is table's page with provided byte_offset */
	serialize_row(row_to_insert, row_slot(table, table->num_rows));
	++table->num_rows;  // increasse the num_rows to show that row is now filled
	return EXECUTE_SUCCESS;
}

/**
  * @brief: execution of select statement(query),
  *         deserialize_row from table, to new row instance(of Row struct) and
  *         iterate through number of rows created in table to show full data
  * @param: created table in which to store data provided by user from insert query
  * @return: execution status for whether the table is full not
  */
ExecuteResult execute_select(Table* table) {
	Row row;
	for (uint32_t i = 0; i < table->num_rows; ++i) {
		deserialize_row(row_slot(table,i), &row);
		print_row(&row);
	}
	return EXECUTE_SUCCESS;
}

/**
  * @brief: see type of query from provied statement and execute it and provide table
  *         on/from to perform action
  * @param: created table to/from which to perform action
  * @param: created statement from which to store data in table
  * @return: execute status, return from the respective functions of performed operations
  */
ExecuteResult execute_statement(Table* table, Statement* statement) {
	switch (statement->type) {
		case (STATEMENT_INSERT):
			return execute_insert(table, statement);
		case (STATEMENT_SELECT):
			// return execute_select(table, statement);
			return execute_select(table);
	}
}

/**
  * @brief: create a new table,
  *         new table creation of sizeof of Table struct(having number of rows and pages)
  *         initialize row number and initialization of all the pages a table can have
  * @return: newly created table
  */
Table* new_table() {
	Table* table = malloc(sizeof(Table));
	table->num_rows = 0;
	for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
		table->pages[i] = NULL;
	}
	return table;
}

/**
  * @brief: free the memory of table
  *         iterate through all the pages of table and free them
  * @param: table created
  */
void free_table(Table *table) {
	/* loop till there's are pages are in table */
	for (uint32_t i = 0; table->pages[i]; ++i) {
		free(table->pages[i]);
	}
	free(table);
}

/**
  * @brief: print the data formatted accordingly to show the data stored in table in a row
  *         used by select statement(execute_select()) to show data
  * @param: row from which to print data
  */
void print_row(Row* row) {
	printf("(%d, %d, %s, %s, %f)\n", row->sl_no, row->year, row->company, row->model, row->power);
}

/**
  * @brief: free the created input buffer used to take input from user by read_input(),
  *         frees all the buffer which takes input in input buffer created
  * @param: created input buffer to take input
  */
void close_input_buffer(InputBuffer* input_buffer) {
	free(input_buffer->buffer);
	free(input_buffer);
}
