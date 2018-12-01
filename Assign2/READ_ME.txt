COMP 310 Assignment 2 README


This is the READ ME for the kv store assignment for COMP 310. 

I attempted an implementation without any structs and relied heavily on int pointers stored at the beginning of memory pods to store bookkeeping data. The resulting pod structure is shown below: 

Due to the implementation choices, I kept track of various locations through global constants like KEYS_PER_POD and DATA_LENGTH_SIZE.  The implementation does not work even for test1. The problem I have traces back to line 227 of a2_lib.c where I attempt to copy the value (or truncated Value, both have same functionality) into the shared memory object. For some reason, there is a memory unalignment error where with copying the key into memory works. (I have verified this behaviour through gdb and Val grind).

The Valgrind error is strange because it suggests there is an invalid write of size 2 at line 227. I do not understand this because to my knowledge I am only dealing with char (1 byte), int (4 byte in Ubuntu) and pointers (8 bytes).

 I have intentionally offset the trunValue by 1 in this submission (on line 227) to show that the value is almost read correctly in that case. When it is offset, the output is offset by 1. When it is not offset, the output is empty.

As a footnote, I would say this assignment has taught me the valuable lesson of relying on structs to make C coding easier.