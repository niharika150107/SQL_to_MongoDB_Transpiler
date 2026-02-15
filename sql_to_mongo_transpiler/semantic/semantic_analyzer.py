from sql_to_mongo_transpiler.ast.nodes import SelectQuery, LogicalCondition, Comparison,Aggregate

class SemanticError(Exception):
    pass

class SemanticAnalyzer:
    def __init__(self, schema):
        self.schema = schema

    def validate_query(self, ast):
        if isinstance(ast, SelectQuery):
            self.validate_select(ast)
        else:
            raise SemanticError(f"Unsupported query type: {type(ast)}")

    def validate_select(self, node: SelectQuery):
        # 1. Validate Table Exists
        table_name = node.table
        if table_name not in self.schema:
            raise SemanticError(f"Table '{table_name}' does not exist")

        # 2. Validate Columns
        self.validate_columns(node.columns, table_name)

        # 3. Validate WHERE Clause
        if node.where:
            self.validate_condition(node.where, table_name)
        # HAVING requires GROUP BY
        if node.having and not node.group_by:
            raise SemanticError("HAVING clause requires GROUP BY")
        # Validate HAVING condition
        if node.having:
            self.validate_condition(node.having, table_name)
        # 4. Validate GROUP BY
        if node.group_by:
            table_schema = self.schema[table_name]
            # validate group_by columns exist
            for col in node.group_by:
                if col not in table_schema:
                    raise SemanticError(f"Column '{col}' does not exist in table '{table_name}'")
            # validate SELECT columns follow SQL rules
            for col in node.columns:
                if isinstance(col, str):
                    if col not in node.group_by:
                        raise SemanticError(
                                f"Column '{col}' must appear in GROUP BY or be aggregated")

    def validate_columns(self, columns, table_name):
        table_schema = self.schema[table_name]
        
        # Handle 'SELECT *'
        if columns == ['*']:
            return


        # Check for duplicates
        seen = set()
        for col in columns:
            if isinstance(col, str):
                if col in seen:
                    raise SemanticError(f"Duplicate column '{col}' in SELECT list")
                seen.add(col)

            if isinstance(col, str):
                if col not in table_schema:
                    raise SemanticError(f"Column '{col}' does not exist in table '{table_name}'")
            # Aggregate
            elif isinstance(col, Aggregate):
                # COUNT(*) is always valid
                if col.func == "COUNT" and col.column == "*":
                    continue
                # For MIN/MAX/AVG/SUM column must exist
                if col.column not in table_schema:
                    raise SemanticError(f"Column '{col.column}' does not exist in table '{table_name}'"
                )
            else:
                raise SemanticError(f"Invalid column type: {col}")
    def validate_condition(self, node, table_name):
        if isinstance(node, LogicalCondition):
            self.validate_condition(node.left, table_name)
            self.validate_condition(node.right, table_name)
        elif isinstance(node, Comparison):
            self.validate_comparison(node, table_name)

    def validate_comparison(self, node: Comparison, table_name):
        col_name = node.identifier
        table_schema = self.schema[table_name]
        #if identifier is aggregate
        if isinstance(col_name, Aggregate):
            # Just validate literal type
            if not isinstance(node.value, (int, str)):
                raise SemanticError("Invalid HAVING condition value")
            return
        # Check column existence
        if col_name not in table_schema:
            raise SemanticError(f"Column '{col_name}' does not exist in table '{table_name}'")

        # Type checking
        expected_type = table_schema[col_name]
        actual_value = node.value
        # ----- BETWEEN -----
        if node.operator == "BETWEEN":
            if not isinstance(actual_value, tuple) or len(actual_value) != 2:
                raise SemanticError("Invalid BETWEEN syntax")
            lower, upper = actual_value
            if expected_type == 'int':
                if not isinstance(lower, int) or not isinstance(upper, int):
                    raise SemanticError(f"Type mismatch for column '{col_name}'. Expected int.")
            elif expected_type == 'string':
                if not isinstance(lower, str) or not isinstance(upper, str):
                    raise SemanticError(
                            f"Type mismatch for column '{col_name}'. Expected string.")
            return
        # ----- IN -----
        if node.operator == "IN":
            if not isinstance(actual_value, list):
                raise SemanticError("Invalid IN syntax")
            for val in actual_value:
                if expected_type == 'int' and not isinstance(val, int):
                    raise SemanticError(f"Type mismatch for column '{col_name}'. Expected int.")
                if expected_type == 'string' and not isinstance(val, str):
                    raise SemanticError(f"Type mismatch for column '{col_name}'. Expected string.")
            return

        # Determine actual type
        if isinstance(actual_value, int):
            actual_type = 'int'
        elif isinstance(actual_value, str):
            actual_type = 'string'
        else:
            actual_type = 'unknown'

        if expected_type != actual_type:
            raise SemanticError(
                f"Type mismatch for column '{col_name}'. Expected {expected_type} but got {actual_type}."
            )
