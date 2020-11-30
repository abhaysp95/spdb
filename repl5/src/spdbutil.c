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
  */
void print_prompt() {
	printf("spdb > ");
}

/**
 * @brief: read input to created input buffer,
 *         read input from stdin for given length into the instance of InputBuffer
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
 */
MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table) {
	if (strcmp(input_buffer->buffer, ".exit") == 0) {
		close_input_buffer(input_buffer);
		db_close(table);
		exit(EXIT_SUCCESS);
	}
	else {
		return META_COMMAND_UNRECOGNIZED;
	}
}

PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement) {
	statement->type = STATEMENT_INSERT;

	char* query_type = strtok(input_buffer->buffer, " ");
	char* sl_no = strtok(NULL, " ");
	char* mfd_year = strtok(NULL, " ");
	char* comp_name = strtok(NULL, " ");
	char* model_name = strtok(NULL, " ");
	char* power = strtok(NULL, " ");

	if ((sl_no == NULL)
			|| (mfd_year == NULL)
			|| (comp_name == NULL)
			|| (model_name == NULL)
			|| (power == NULL)) {
		return PREPARE_SYNTAX_ERROR;
	}

	if ((strlen(comp_name) > COMPANY_SIZE)
			|| (strlen(model_name) > MODEL_SIZE)) {
		return PREPARE_STRING_TOO_LONG;
	}

	statement->row_to_insert.sl_no = atoi(sl_no);
	statement->row_to_insert.year = atoi(mfd_year);
	strcpy(statement->row_to_insert.company, comp_name);
	strcpy(statement->row_to_insert.model, model_name);
	statement->row_to_insert.power = atof(power);  // atof takes char* and returns double
	/*free(query_type);*/
	return PREPARE_SUCCESS;
}

/**
 * @brief: Prepare Statement, put data from created input buffer to created Statement,
 *         insert statement type and then data from input buffer to statement->row
 */
PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
	/* using strncmp, cause there will be queries after insert, select etc. queries */
	if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
		return prepare_insert(input_buffer, statement);
	}
	if (strncmp(input_buffer->buffer, "select", 6) == 0) {
		statement->type = STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}
	return PREPARE_UNRECOGNIZED_STATEMENT;
}

/**
 * @brief: serializing row with data provided by source to destination with fixed offset
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
  */
void* row_slot(Table* table, uint32_t row_num) {
	uint32_t page_num = row_num / ROWS_PER_PAGE;
	void* page = get_page(table->pager, page_num);
	uint32_t row_offset = row_num % ROWS_PER_PAGE;
	uint32_t byte_offset = row_offset * ROW_SIZE;
	return page + byte_offset;
}

/**
  * @brief: execution of insert statement(query),
  *         check if table is full, if not, serialize row from statement set from InputBuffer
  *         to table with provided page with correct byte offset and return status according to it
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
  * @brief: put data from specified file to created table
  *         it calls pager_open(), which will open that database file
  *         and will keep track of its size and sets different attribute for
  *         Table object
  * @return: newly created table
  */
Table* db_open(const char* filename) {
	Pager* pager = pager_open(filename);
	uint32_t num_rows = pager->file_length / ROW_SIZE;
	Table* table = malloc(sizeof(Table));
	table->pager = pager;
	table->num_rows = num_rows;
	return table;
}

/**
  * @brief: opens/creates the database file and keeps track of its length
  *         opens file with read/write access mode and read and write permission
  *         bits, and check if its successful, it then gets the file length with lseek()
  *         with the help of SEEK_END constant as it makes the file offset to length of
  *         file + offset(which is 0 here) and then it declares and initializes the Pager
  *         object and provides appropriate settings for the attibute of Pager's object
  */
Pager* pager_open(const char* filename) {
	/* int open(const char* filename, int flags[, mode_t mode]); */
	int file_desc = open(filename,
			// access modes
			O_RDWR  // open the file for both reading and writing
			// open-time flag
			| O_CREAT,  // the file will be created if not already exist
			// permission bits
			S_IWUSR  // read permission for owner of file(0400)
			| S_IRUSR); // write permission for owner(0200)
	// Refer here for more details:
	// https://www.gnu.org/software/libc/manual/html_node/Opening-and-Closing-Files.html

	if (file_desc == -1) {
		printf("Unable to open file\n");
		exit(EXIT_FAILURE);
	}

	// refer to 'man lseek' for more info
	off_t file_length = lseek(file_desc, 0, SEEK_END);

	Pager* pager = malloc(sizeof(Pager));
	pager->file_descriptor = file_desc;
	pager->file_length = file_length;

	for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
		pager->pages[i] = NULL;
	}
	return pager;
}

/**
  * @brief: fetches the required page and contains the logic of handling cache
  *         miss. Assuming pages are saved one after one and every offset is of
  *         PAGE_SIZE. If the requested page lies outside the bound of file it
  *         should be empty so we'll create a empty page and return it else
  *         simply return the page of required page_num
  */
void* get_page(Pager* pager, uint32_t page_num) {
	if (page_num > TABLE_MAX_ROWS) {
		printf("Trying to access page out of bound\n");
		exit(EXIT_FAILURE);
	}
	if (pager->pages[page_num] == NULL) {
		// cache miss, allocate memory and load data from file
		void* page = malloc(PAGE_SIZE);
		uint32_t num_pages = pager->file_length / PAGE_SIZE;
		// if any parital page missed
		if (pager->file_length % PAGE_SIZE) {
			num_pages++;
		}

		if (page_num <= num_pages) {
			// seek forward to size of page_num * PAGE_SIZE
			lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
			ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
			if (bytes_read == -1) {
				printf("Error reading file: %d\n", errno);
				exit(EXIT_FAILURE);
			}
		}
		pager->pages[page_num] = page;
	}
	return pager->pages[page_num];
}

/**
  * @brief: does following tasks:
  *         calls pager_flush to flush the page cache to disk
  *         closes the database file
  *         frees memory of Pager and Table data structure
  */
void db_close(Table* table) {
	Pager* pager = table->pager;
	uint32_t full_page_num = table->num_rows / ROWS_PER_PAGE;
	for (int i = 0; i < full_page_num; ++i) {
		// not many times this if statement is going to be true so branching
		// will be less
		if (pager->pages[i] == NULL) {
			continue;
		}
		pager_flush(pager, i, PAGE_SIZE);
		free(pager->pages[i]);
		pager->pages[i] = NULL;
	}
	// There maybe partially filled page left at the end of file
	uint32_t num_row_left = table->num_rows % ROWS_PER_PAGE;
	if (num_row_left > 0) {
		uint32_t page_num = full_page_num;
		if (pager->pages[page_num] != NULL) {
			pager_flush(pager, page_num, num_row_left * ROW_SIZE);
			free(pager->pages[page_num]);
			pager->pages[page_num] = NULL;
		}
	}
	int close_result = close(pager->file_descriptor);
	if (close_result == -1) {
		printf("Error closing in db file.\n");
		exit(EXIT_FAILURE);
	}
	for (int i = 0; i < TABLE_MAX_PAGES; ++i) {
		void* page = pager->pages[i];
		if (page) {
			free(page);
			pager->pages[i] = NULL;
		}
	}
	free(pager);
	free(table);
}

/**
  * @brief: writes to db file(disk) from Pager->pages, it takes both page_num
  *         and size as param even though we have fixed PAGE_SIZE because there
  *         could be few rows left as partial page and have to save those to
  *         file too
  */
void pager_flush(Pager* pager, uint32_t page_num, uint32_t size) {
	if (pager->pages[page_num] == NULL) {
		printf("Tried to flush NULL page.\n");
		exit(EXIT_FAILURE);
	}
	off_t offset = lseek(pager->file_descriptor, page_num * size, SEEK_SET);
	if (offset == -1) {
		printf("Error seeking: %d\n", errno);
		exit(EXIT_FAILURE);
	}
	ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);
	if (bytes_written == -1) {
		printf("Error writing: %d\n", errno);
		exit(EXIT_FAILURE);
	}
}

/**
  * @brief: print the data formatted accordingly to show the data stored in table in a row
  *         used by select statement(execute_select()) to show data
  */
void print_row(Row* row) {
	printf("(%d, %d, %s, %s, %f)\n", row->sl_no, row->year, row->company, row->model, row->power);
}

/**
  * @brief: free the created input buffer used to take input from user by read_input(),
  *         frees all the buffer which takes input in input buffer created
  */
void close_input_buffer(InputBuffer* input_buffer) {
	free(input_buffer->buffer);
	free(input_buffer);
}
