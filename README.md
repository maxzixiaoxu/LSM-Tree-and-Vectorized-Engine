# LSM-Tree-and-Vectorized-Engine

Project implementation of a LSM-tree Storage Engine and Vectorization Query.
 
## LSM Basic 

Classes implemented: BlockIterator, BlockBuilder, SSTable, SSTableIterator, SSTableBuilder, SortedRun, SortedRunIterator, IteratorHeap, SuperVersion, SuperVersionIterator and Version.

The code is in src/storage/lsm. The architecture of the LSM-tree is as follows:

![image](https://github.com/user-attachments/assets/4986f022-5987-410e-98c2-cf71157b572b)

DBImpl: At the highest level, the DBImpl is the primary component responsible for interacting with users. It references the most recent SuperVersion of the database. It is in lsm.hpp.

SuperVersion: It includes a MemTable, a list of immutable MemTables and the on-disk LSM-tree Version, which together represent the current state of the database. It is in version.hpp.

MemTable: An in-memory ordered data structure. It is flushed to disk when it reaches its capacity. It is in memtable.hpp.

Version: An array of levels, representing the on-disk LSM-tree. It is in version.hpp.

Level: It is composed of one or more sorted runs. It is in level.hpp.

SortedRun: It can be viewed as a sorted key-value array which is divided into several SSTables. It is in level.hpp.

SSTable: It is composed of data blocks, an index, bloom filter and metadata. It is in sst.hpp.

Block: It stores records. It is in block.hpp.

Record Format

Records in the database are structured as (key, seq, type, value) tuple, where seq denotes the timestamp (or sequence number) and type denotes the record type. A record with type=RecordType::Value represents the key-value pair at the timestamp seq. When type equals to RecordType::Deletion, the record indicates that the key has been marked for deletion at the timestamp seq.

(key, seq, type) is called the internal key of record (see storage/lsm/format.hpp) because seq and type are invisible to users. We can define a comparing function to sort them: for two internal keys (key0, seq0, type0) and (key1, seq1, type1), the former is smaller than the latter if and only if key0 < key1 or key0 == key1 && seq0 > seq1. With this comparing function, for (key, seq), the newest record with the same key and a sequence number seq0 <= seq is the first record (key0, seq0) with (key, seq) <= (key0, seq0).

Get

DBImpl::Get(key, seq, &value_str) function is designed to retrieve records from the database. It looks for the first record (key0, seq0, type0) satisfying key0 == key && seq0 <= seq. If the record is a record with type RecordType::Deletion or no such record exists, the function returns false, indicating the requested value is not found. If a record with type RecordType::Value is found, the function returns true and copies the associated value into value_str.

The process of Get is:

Check if MemTable has the record. If not, check if immutable MemTables have the record. It checks from the newest immutable MemTable to the oldest immutable MemTable. It stops once it finds the record. If they do not have the record, proceed to check the on-disk LSM-tree.

Check if Level 0 has the record, then Level 1, 2, and so on. For each level, it performs a binary search to find the SSTable that possibly has the record. Then it queries the bloom filter. If further inquiry is necessary, it performs a binary search on the SSTable's index to find the data block. It reads the data block from the disk, and performs a binary search to find the record.

Put and Delete

DBImpl::Put(key, seq) creates a new record (key, seq, RecordType::Value, value) and DBImpl::Del(key, seq) creates a new record (key, seq, RecordType::Deletion). Once a record is created, it is inserted to the MemTable. For convenience, a lock is employed to ensure that only a single writer can perform operations at any given time.

If the MemTable reaches its capacity, it creates a new superversion and moves the MemTable to the immutable MemTable list and create a new MemTable.

The database has 2 threads to persist the data to disk: the flush thread and the compaction thread. The flush thread is awakened whenever a new immutable MemTable is created. It flushes the immutable MemTables to the first level (Level 0) of the LSM-tree. Every time an immutable Memtable is flushed, the compaction thread is awakened. It acquires new compaction tasks through CompactionPicker::Get. Once it has tasks, it proceeds to execute them and subsequently updates the superversion.

Scan

The database supports range scans. DBImpl::Begin() returns an iterator positioned to the beginning of the data, while DBImpl::Seek(key, seq) returns an iterator positioned to the first record (key0, seq0, type0) satisfying (key, seq) <= (key0, seq0).

Scan operations are performed in a snapshot. When a DBIterator is created, it stores the current sequence number. It can only see the records with sequence number smaller than the stored sequence number.

The architecture of iterators is as follows. BlockIterator is the iterator on data blocks. SSTableIterator is the iterator on SSTables and contains a BlockIterator. SortedRunIterator is the iterator on sorted runs and contains a SSTableIterator. SuperVersionIterator is the iterator on superversions, it contains all the SortedRunIterators and MemTableIterators using IteratorHeap, which maintains the record with the minimum internal key by maintaining iterators in a heap. There is no LevelIterator or VersionIterator because it is inefficient to maintain two IteratorHeaps. The DBIterator operates at the highest level, merging records with the same key and skipping the keys which are marked deleted.

![image](https://github.com/user-attachments/assets/5cf1af07-754a-4c0a-9ce1-9eb978d6f5e8)

The interfaces of Iterator can be found in storage/lsm/iterator.hpp. 

## Vectorized Engine

Classes Implemented: Executor, JoinVecExecutor, and HashJoinVecExecutor.

Volcano-style engines are simple and easy to implement, but they have poor performance due to the large overhead in virtual function calls. While they worked well in the past because disk I/O was the primary bottleneck, they are inefficient on modern CPUs and disks. Most modern query engines either use vectorization or data-centric code generation (just-in-time compilation). Vectorized engines fetch a batch of tuples instead of just one at a time, which amortizes the virtual function call overhead and can leverage SIMD (Single Instruction, Multiple Data) techniques.

In execution/executor.hpp, you can find the interfaces in VecExecutor. The interfaces are:

Init(): Initializes the executor.

Next(): Returns a batch of tuples. If there are no tuples to return, it returns an empty result.

Operators (Executors) are organized as a tree. The system calls the Next() function of the root operator of the tree, then the root operator calls the Next() functions of its children, and so on. The leaf operators of the tree are SeqScanVecExecutor which read tuple data from the storage engine and allocate a buffer to store them in memory. The tuple data is processed and transferred from the leaf to the root, and return to the system. The system calls Next() until it returns empty result.

![image](https://github.com/user-attachments/assets/322241c8-4a63-4e8f-a159-af198d45c427)

Data structure

The batch of tuples is stored in TupleBatch (refer to type/tuple_batch.hpp). TupleBatch has Vectors storing each column (refer to type/vector.hpp and type/vector_buffer.hpp) and a selection vector storing validation bits. The selection vector is used in cases of low selectivity (for example, when 95\% of tuples are valid, we do not need to eliminate invalid ones; instead we just mark them as invalid.). Each Vector has an array of elements. Each element is of type StaticFieldRef (refer to type/static_field.hpp). It is an 8-byte object that can store a 64-bit integer (LogicalType::INT, refer to type/field_type.hpp) or 64-bit float (LogicalType::FLOAT) or a string pointer (LogicalType::STRING). If the Vector stores strings, it stores an array of string pointers and a pointer to an auxlitary buffer (Vector::aux_) which stores actual string data.

![image](https://github.com/user-attachments/assets/984a6629-d7bc-471e-a714-a1c49ccc19c1)

The figure above shows the structure of TupleBatch. The types of tuple are: LogicalType::INT, LogicalType::FLOAT, LogicalType::INT, LogicalType::STRING. The Vector which stores column D has a pointer to an auxlitary buffer storing actual string data.

In TupleBatch, num_tuple_ stores the number of tuples including valid ones and invalid ones, num_valid_tuple_ stores the number of valid tuples, capacity_ stores the maximum number of tuples. Like capacity and size in std::vector, capacity_ can be larger than num_tuple_ and tuples with indices between num_tuple_ and capacity_ - 1 are empty (neither valid nor invalid).

To create a TupleBatch, you need to call TupleBatch::Init. You need to get the tuple type and pass it as std::vector<LogicalType>. If you have an OutputSchema, you can call OutputSchema::GetTypes() to get it. You also need to pass a initial size to the function, you can use max_batch_size_ (the maximum batch size) in VecExecutor, so that it will not need to resize during execution. Here is an example:

```
OutputSchema table0_;
TupleBatch batch;
batch.Init(table0_.GetTypes(), max_batch_size_);
```

After the TupleBatch is created, it is empty, you can use TupleBatch::Append to append tuples. Note that this function deepcopies tuple data, i.e. it copies the string data and creates a new string pointer. So you do not need to worry about the string pointers being invalid. Here is an example:

```
TupleBatch result_;
std::vector<StaticFieldRef> tuple;
...
// Append the tuple in std::vector<StaticFieldRef>
result_.Append(tuple);
std::vector<Vector> v;
...
// Append all tuples in std::vector<Vector>
for (int i = 0; i < tuple_cnt; i++) {
  result_.Append(v, i);
}

```

To access the j-th column of the i-th tuple, you can use TupleBatch::Get(i, j). It returns a StaticFieldRef object, you can use ReadInt, ReadFloat or ReadStringView based on its type (type is not stored in StaticFieldRef, it is stored in other places such as OutputSchema). To assign a value to the j-th column of the i-th tuple, you can use TupleBatch::Set(i, j, value). To get a reference to the i-th tuple, you can use TupleBatch::GetSingleTuple(i), it returns a TupleBatch::SingleTuple, a read-only reference. You can use operator[] to access the j-th column in TupleBatch::SingleTuple, for example GetSingleTuple(i)[j] accesses the j-th column of the i-th tuple.

To iterate over the valid tuples in TupleBatch, you can use: (1) iterate over all the tuples and use TupleBatch::IsValid to check if they are valid, or (2) use TupleBatch::iterator and for(auto :), it only returns valid tuples. It returns TupleBatch::SingleTuple, a read-only reference. Here is an example:

```
// 1.
TupleBatch batch;
for (uint64_t i = 0; i < batch.size(); i++) {
  if (batch.IsValid(i)) {
    // batch.Get(i, j) access the j-th column of i-th tuple.
  }
}

// 2.
TupleBatch batch;
for (auto t : batch) {
  // use t[i] to access the i-th column of the tuple
  // use batch2.Append(t) to append the tuple to another tuple batch.
}

```
Vector has two types: constant and flat. If its type is flat, then it stores a normal array. If its type is constant, then it is a vector in which all the elements are the same. Physically it only stores one element. It is used for constants in the expressions, or nested loop join executors. To create a vector, you need to pass the vector type (VectorType::Flat or VectorType::Constant), the element type (LogicalType::FLOAT, LogicalType::STRING and LogicalType::INT), and the number of elements of the vector. There is no validation information in Vector. It assumes that all the elements in Vector are valid and need to be calculated in expression evaluation.

OutputSchema

Since SQL is a statically-typed language, the types of the output of operators are known. They are stored in OutputSchema (refer to plan/output_schema.hpp) in PlanNode::output_schema_ (refer to plan/plan.hpp). You can use OutputSchema::GetTypes to get types in std::vector<LogicalType>. To get more information, you can use OutputSchema::operator[] or OutputSchema::GetCols to get the OutputColumnData structure, which stores table name, column name, type, etc. You may also need to concatenate two OutputSchemas (e.g in the join executor), you can use OutputSchema::Concat(left, right).

ExprVecExecutor

In vectorized execuction engine, expressions are evaluated in batches, greatly reducing the interpretation overhead. For each expression, we construct an executor called ExprVecExecutor (refer to execution/vec/expr_vexecutor.hpp). ExprVecExecutors are organized as a tree, where the leaf nodes of the tree are input, the root node stores the result into the result Vector. The expression is evaluated from the bottom to the top, and inner nodes (nodes that are not leafs) may need to allocate a buffer to store temporary results. Here is an example shown in the figure below.

![image](https://github.com/user-attachments/assets/462c2e50-9cdc-49e0-b7e5-18a6f5c1af3a)

You can find ExprVecExecutor in execution/vec/expr_vexecutor.hpp. To create an ExprVecExecutor, you need to pass a pointer to Expr, which stores expression information, and a OutputSchema, which stores type information. To evaluate the expression, you need to pass a std::span<Vector> (std::span is similar to std::string_view, but it is used for std::vector or std::array objects) with the same types in the OutputSchema you passed during creation, and the number of tuples (including valid tuples and invalid tuples, i.e. the return value of TupleBatch::size) in the input, and a reference to the result Vector.



