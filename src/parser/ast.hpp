#pragma once

#include <string>
#include <vector>

#include "parser/expr.hpp"
#include "type/field.hpp"

namespace wing {
/**
 *
 * We parse the SQL statement and analysis the type of result (SQL is a strongly
 * typed language).
 *
 * We also bind table name and column name to the corresponding ID in the
 * database.
 *
 * But this data structure cannot be executed directly, we use this to generate
 * execution plan.
 *
 */

/**
 * ResultColumn.
 *
 * The result that SELECT statement returns. It can be either * (e.g. select *
 * from A;) or <Expr>. (e.g. select A.a + 1, A.b from A;)
 *
 *                +- StarResultColumn: This means returns all columns.
 *                |
 * ResultColumn --+
 *                |
 *                +- ExprResultColumn: This column returns a expression
 * containing column references and constants.
 *
 */

enum class ResultColumnType { EXPR = 0, ALL };

struct ResultColumn {
  ResultColumnType type_;
  std::string as_;
  virtual ~ResultColumn() = default;
  ResultColumn(ResultColumnType _type) : type_(_type) {}
  virtual std::string ToString() const = 0;
};

struct ExprResultColumn : public ResultColumn {
  std::unique_ptr<Expr> expr_;
  ExprResultColumn() : ResultColumn(ResultColumnType::EXPR) {}
  std::string ToString() const;
};

struct StarResultColumn : public ResultColumn {
  StarResultColumn() : ResultColumn(ResultColumnType::ALL) {}
  std::string ToString() const;
};

/**
 * OrderByElement
 *
 * Whether the result that SELECT statement returns needs to be sorted and how
 * to sort (ASC/DESC).
 *
 */

struct OrderByElement {
  std::unique_ptr<Expr> expr_;
  bool is_asc_;
  std::string ToString() const;
};

/**
 * ResultTableType
 *
 * Which tables does SELECT statement select from? It can be normal tables (e.g.
 * select * from A;) or join-results (e.g. select * from A INNER JOIN B ON A.a =
 * B.b;) or sub-queries (e.g. select B.b from (select A.a + 1 as b from A) as
 * B;) or values (e.g. select * from (values (2, 3), (4, 5));, or insert into A
 * values (2), (3);)
 *
 *            +- SubqueryTableRef: a select statement.
 *            |
 * TableRef --+- NormalTableRef: ID that denotes a column from table.
 *            |
 *            +- JoinTableRef: Join two tables or subqueries or other joins on
 * some conditions.
 *            |
 *            +- ValuesTableRef: An in-memory temporary table generated by the
 * data in the statement.
 *
 *
 */

enum class TableRefType {
  TABLE = 0,
  JOIN,
  SUBQUERY,
  VALUES,
};

struct TableAs {
  std::string table_name_;
  std::vector<std::string> column_names_;
  std::string ToString() const;
};

struct TableRef {
  TableRefType table_type_;
  std::unique_ptr<TableAs> as_;
  virtual ~TableRef() = default;
  TableRef(TableRefType table_type) : table_type_(table_type) {}
  virtual std::string ToString() const = 0;
};

struct NormalTableRef : public TableRef {
  std::string table_name_;
  NormalTableRef() : TableRef(TableRefType::TABLE) {}
  std::string ToString() const override;
};

struct JoinTableRef : public TableRef {
  std::unique_ptr<TableRef> ch_[2];
  std::unique_ptr<Expr> predicate_;
  JoinTableRef() : TableRef(TableRefType::JOIN) {}
  std::string ToString() const override;
};

struct ValuesTableRef : public TableRef {
  std::vector<Field> values_;
  size_t num_fields_per_tuple_{0};
  ValuesTableRef() : TableRef(TableRefType::VALUES) {}
  std::string ToString() const override;
};

struct SelectStatement;

struct SubqueryTableRef : public TableRef {
  std::unique_ptr<SelectStatement> ch_;
  SubqueryTableRef() : TableRef(TableRefType::SUBQUERY) {}
  std::string ToString() const override;
};

/**
 * Statements.
 *
 * Select: select <ResultColumn-list> from <TableRef-list> where Expr group by
 * <Expr-list> having <Expr> order by <Order By Element-list> limit <Expr>;
 * Insert: insert into <table-name> <TableRef>;
 * Update: update <table-name> set <ColumnUpdate-list> where <Expr>;
 * Create Table: create table <table-name> (<ColumnDescription-list>);
 * Drop Table: drop table <table-name>;
 * Create Index: create index <index-name> on <table-name>(<column-names>);
 * Drop Index: drop index <index-name>;
 *
 */

enum class StatementType {
  SELECT = 0,
  INSERT,
  DELETE,
  UPDATE,
  CREATE_TABLE,
  DROP_TABLE,
  CREATE_INDEX,
  DROP_INDEX,
};

struct Statement {
  StatementType type_;
  virtual ~Statement() = default;
  Statement(StatementType type) : type_(type) {}
  virtual std::string ToString() const = 0;
};

struct SelectStatement : public Statement {
  std::vector<std::unique_ptr<TableRef>> tables_;
  std::vector<std::unique_ptr<ResultColumn>> result_column_;
  std::unique_ptr<Expr> predicate_;
  std::vector<std::unique_ptr<Expr>> group_by_;
  std::unique_ptr<Expr> having_;
  std::vector<std::unique_ptr<OrderByElement>> order_by_;
  std::unique_ptr<Expr> limit_count_;
  std::unique_ptr<Expr> limit_offset_;
  bool is_distinct_;
  SelectStatement() : Statement(StatementType::SELECT) {}
  std::string ToString() const override;
};

struct ColumnDescription {
  std::string column_name_;
  FieldType types_;
  uint32_t size_;
  bool is_primary_key_{false};
  bool is_foreign_key_{false};
  bool is_auto_gen_{false};
  std::string ref_table_name_;
  std::string ref_column_name_;
  std::string ToString() const;
};

struct CreateTableStatement : public Statement {
  std::string table_name_;
  std::vector<ColumnDescription> columns_;
  CreateTableStatement() : Statement(StatementType::CREATE_TABLE) {}
  std::string ToString() const override;
};

struct ColumnUpdate {
  std::string table_name_;
  std::string column_name_;
  std::unique_ptr<Expr> update_value_;
  std::string ToString() const;
};

struct UpdateStatement : public Statement {
  std::string table_name_;
  std::vector<std::unique_ptr<TableRef>> other_tables_;
  std::vector<std::unique_ptr<ColumnUpdate>> updates_;
  std::unique_ptr<Expr> predicate_;
  UpdateStatement() : Statement(StatementType::UPDATE) {}
  std::string ToString() const override;
};

struct DeleteStatement : public Statement {
  std::string table_name_;
  std::unique_ptr<Expr> predicate_;
  DeleteStatement() : Statement(StatementType::DELETE) {}
  std::string ToString() const override;
};

struct InsertStatement : public Statement {
  std::string table_name_;
  std::unique_ptr<TableRef> insert_data_;
  InsertStatement() : Statement(StatementType::INSERT) {}
  std::string ToString() const override;
};

struct DropTableStatement : public Statement {
  std::string table_name_;
  DropTableStatement() : Statement(StatementType::DROP_TABLE) {}
  std::string ToString() const override;
};

struct CreateIndexStatement : public Statement {
  std::string table_name_;
  std::string index_name_;
  std::vector<std::string> indexed_column_names_;
  CreateIndexStatement() : Statement(StatementType::CREATE_INDEX) {}
  std::string ToString() const override;
};

struct DropIndexStatement : public Statement {
  std::string index_name_;
  DropIndexStatement() : Statement(StatementType::DROP_INDEX) {}
  std::string ToString() const override;
};

}  // namespace wing
