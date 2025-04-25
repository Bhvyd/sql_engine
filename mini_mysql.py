import os
import re
import json

class MiniMySQL:
    def __init__(self, data_dir="mini_mysql_data"):
        self.data_dir = data_dir
        self.current_db = None
        
        # Create data directory if it doesn't exist
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
    
    def execute(self, query):
        """Execute a SQL query"""
        query = query.strip().lower()
        
        # CREATE DATABASE
        create_db_match = re.match(r'create database (\w+)', query)
        if create_db_match:
            db_name = create_db_match.group(1)
            return self.create_database(db_name)
        
        # USE DATABASE
        use_db_match = re.match(r'use (\w+)', query)
        if use_db_match:
            db_name = use_db_match.group(1)
            return self.use_database(db_name)
        
        # CREATE TABLE
        create_table_match = re.match(r'create table (\w+) \((.*)\)', query)
        if create_table_match:
            table_name = create_table_match.group(1)
            columns_def = create_table_match.group(2)
            return self.create_table(table_name, columns_def)
        
        # Handle INSERT INTO operation
        insert_match = re.match(r'insert into (\w+) \((.*?)\) values \((.*?)\)', query)
        if insert_match:
            table_name = insert_match.group(1)
            columns = [col.strip() for col in insert_match.group(2).split(',')]
            values_str = insert_match.group(3)
            
            # Handle quoted strings and numeric values
            values = []
            current_value = ""
            in_quotes = False
            for char in values_str:
                if char == "'" or char == '"':
                    in_quotes = not in_quotes
                    current_value += char
                elif char == ',' and not in_quotes:
                    values.append(current_value.strip())
                    current_value = ""
                else:
                    current_value += char
            values.append(current_value.strip())  # Add the last value
            
            return self.insert_into(table_name, columns, values)
            
        # SELECT
        select_match = re.match(r'select (.*?) from (\w+)(?:\s+where\s+(.*?))?(?:\s+order by\s+(.*?))?(?:\s+limit\s+(\d+))?$', query)
        if select_match:
            columns = select_match.group(1).strip()
            table_name = select_match.group(2)
            where_clause = select_match.group(3)
            order_by = select_match.group(4)
            limit = select_match.group(5)
            
            return self.select_from(table_name, columns, where_clause, order_by, limit)
        
        # UPDATE
        update_match = re.match(r'update (\w+) set (.*?)(?:\s+where\s+(.*?))?$', query)
        if update_match:
            table_name = update_match.group(1)
            set_clause = update_match.group(2)
            where_clause = update_match.group(3)
            
            return self.update(table_name, set_clause, where_clause)
        
        # DELETE
        delete_match = re.match(r'delete from (\w+)(?:\s+where\s+(.*?))?$', query)
        if delete_match:
            table_name = delete_match.group(1)
            where_clause = delete_match.group(2)
            
            return self.delete_from(table_name, where_clause)
        
        return "Invalid or unsupported SQL query"
    
    def create_database(self, db_name):
        """Create a new database"""
        db_path = os.path.join(self.data_dir, db_name)
        if os.path.exists(db_path):
            return f"Database '{db_name}' already exists"
        os.makedirs(db_path)
        return f"Database '{db_name}' created successfully"
    
    def use_database(self, db_name):
        """Select a database to use"""
        db_path = os.path.join(self.data_dir, db_name)
        if not os.path.exists(db_path):
            return f"Database '{db_name}' does not exist"
        self.current_db = db_name
        return f"Using database '{db_name}'"
    
    def create_table(self, table_name, columns_def):
        """Create a new table in the current database"""
        if not self.current_db:
            return "No database selected"
        
        table_path = os.path.join(self.data_dir, self.current_db, f"{table_name}.json")
        if os.path.exists(table_path):
            return f"Table '{table_name}' already exists"
        
        # Parse column definitions
        columns = {}
        primary_key = None
        
        for col_def in columns_def.split(','):
            col_def = col_def.strip()
            parts = col_def.split()
            
            if len(parts) < 2:
                return f"Invalid column definition: {col_def}"
            
            col_name = parts[0]
            col_type = parts[1]
            
            # Check for PRIMARY KEY
            if 'primary key' in col_def.lower():
                primary_key = col_name
            
            columns[col_name] = {
                "type": col_type,
                "primary_key": 'primary key' in col_def.lower(),
                "not_null": 'not null' in col_def.lower()
            }
        
        # Create table structure
        table_data = {
            "columns": columns,
            "primary_key": primary_key,
            "rows": []
        }
        
        # Save table structure to file
        with open(table_path, 'w') as f:
            json.dump(table_data, f, indent=2)
        
        return f"Table '{table_name}' created successfully"
    
    def insert_into(self, table_name, columns, values):
        """Insert data into a table"""
        if not self.current_db:
            return "No database selected"
        
        table_path = os.path.join(self.data_dir, self.current_db, f"{table_name}.json")
        if not os.path.exists(table_path):
            return f"Table '{table_name}' does not exist"
        
        # Load table structure
        with open(table_path, 'r') as f:
            table_data = json.load(f)
        
        # Parse values (convert string representations to appropriate types)
        parsed_values = []
        for value in values:
            value = value.strip()
            # Remove quotes from string values
            if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
                parsed_values.append(value[1:-1])
            # Convert numeric values
            elif value.isdigit():
                parsed_values.append(int(value))
            elif re.match(r'^-?\d+(\.\d+)?$', value):
                parsed_values.append(float(value))
            # Handle NULL values
            elif value.lower() == 'null':
                parsed_values.append(None)
            else:
                parsed_values.append(value)
        
        # Create a new row
        new_row = {}
        for col, val in zip(columns, parsed_values):
            new_row[col] = val
        
        # Add default values for missing columns
        for col in table_data["columns"]:
            if col not in new_row:
                new_row[col] = None
        
        # Validate NOT NULL constraints
        for col, col_info in table_data["columns"].items():
            if col_info.get("not_null", False) and new_row.get(col) is None:
                return f"Column '{col}' cannot be NULL"
        
        # Validate PRIMARY KEY constraints
        pk = table_data.get("primary_key")
        if pk and new_row.get(pk) is not None:
            for row in table_data["rows"]:
                if row.get(pk) == new_row.get(pk):
                    return f"Duplicate entry for primary key '{pk}'"
        
        # Add the new row
        table_data["rows"].append(new_row)
        
        # Save updated table
        with open(table_path, 'w') as f:
            json.dump(table_data, f, indent=2)
        
        return f"1 row inserted into table '{table_name}'"
    
    def select_from(self, table_name, columns_str, where_clause=None, order_by=None, limit=None):
        """Select data from a table"""
        if not self.current_db:
            return "No database selected"
        
        table_path = os.path.join(self.data_dir, self.current_db, f"{table_name}.json")
        if not os.path.exists(table_path):
            return f"Table '{table_name}' does not exist"
        
        # Load table data
        with open(table_path, 'r') as f:
            table_data = json.load(f)
        
        # Determine which columns to return
        if columns_str == '*':
            columns_to_return = list(table_data["columns"].keys())
        else:
            columns_to_return = [col.strip() for col in columns_str.split(',')]
            # Verify all requested columns exist
            for col in columns_to_return:
                if col not in table_data["columns"] and col != '*':
                    return f"Unknown column '{col}' in field list"
        
        # Filter rows based on WHERE clause
        filtered_rows = table_data["rows"]
        if where_clause:
            filtered_rows = self._apply_where_clause(filtered_rows, where_clause)
        
        # Apply ORDER BY
        if order_by:
            order_cols = [col.strip() for col in order_by.split(',')]
            for col in reversed(order_cols):
                desc = False
                if ' desc' in col.lower():
                    desc = True
                    col = col.lower().replace(' desc', '').strip()
                
                filtered_rows = sorted(filtered_rows, 
                                       key=lambda row: (row.get(col) is None, row.get(col)), 
                                       reverse=desc)
        
        # Apply LIMIT
        if limit:
            filtered_rows = filtered_rows[:int(limit)]
        
        # Format result for display
        if not filtered_rows:
            return "Empty set"
        
        # Get all column names
        columns_display = columns_to_return
        
        # Calculate column widths
        col_widths = {col: len(col) for col in columns_display}
        for row in filtered_rows:
            for col in columns_display:
                value = row.get(col)
                if value is not None:
                    col_widths[col] = max(col_widths[col], len(str(value)))
        
        # Create header
        header = " | ".join(col.ljust(col_widths[col]) for col in columns_display)
        separator = "-" * len(header)
        
        # Create rows
        formatted_rows = []
        for row in filtered_rows:
            formatted_row = " | ".join(
                str(row.get(col, "NULL")).ljust(col_widths[col]) for col in columns_display
            )
            formatted_rows.append(formatted_row)
        
        # Combine all parts
        result = "\n".join([header, separator] + formatted_rows)
        return result
    
    def update(self, table_name, set_clause, where_clause=None):
        """Update data in a table"""
        if not self.current_db:
            return "No database selected"
        
        table_path = os.path.join(self.data_dir, self.current_db, f"{table_name}.json")
        if not os.path.exists(table_path):
            return f"Table '{table_name}' does not exist"
        
        # Load table data
        with open(table_path, 'r') as f:
            table_data = json.load(f)
        
        # Parse SET clause
        updates = {}
        for item in set_clause.split(','):
            item = item.strip()
            if '=' not in item:
                return f"Invalid SET clause: {item}"
            
            col, value = item.split('=', 1)
            col = col.strip()
            value = value.strip()
            
            # Check if column exists
            if col not in table_data["columns"]:
                return f"Unknown column '{col}' in SET clause"
            
            # Parse value
            if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
                updates[col] = value[1:-1]
            elif value.isdigit():
                updates[col] = int(value)
            elif re.match(r'^-?\d+(\.\d+)?$', value):
                updates[col] = float(value)
            elif value.lower() == 'null':
                updates[col] = None
            else:
                updates[col] = value
        
        # Find rows to update
        if where_clause:
            rows_to_update = self._apply_where_clause(table_data["rows"], where_clause)
        else:
            rows_to_update = table_data["rows"]
        
        # Get indices of rows to update
        indices_to_update = []
        for i, row in enumerate(table_data["rows"]):
            if row in rows_to_update:
                indices_to_update.append(i)
        
        # Update rows
        for idx in indices_to_update:
            for col, value in updates.items():
                table_data["rows"][idx][col] = value
        
        # Save updated table
        with open(table_path, 'w') as f:
            json.dump(table_data, f, indent=2)
        
        return f"{len(indices_to_update)} rows updated in table '{table_name}'"
    
    def delete_from(self, table_name, where_clause=None):
        """Delete data from a table"""
        if not self.current_db:
            return "No database selected"
        
        table_path = os.path.join(self.data_dir, self.current_db, f"{table_name}.json")
        if not os.path.exists(table_path):
            return f"Table '{table_name}' does not exist"
        
        # Load table data
        with open(table_path, 'r') as f:
            table_data = json.load(f)
        
        # Count rows before deletion
        initial_count = len(table_data["rows"])
        
        # Filter rows to keep
        if where_clause:
            rows_to_delete = self._apply_where_clause(table_data["rows"], where_clause)
            table_data["rows"] = [row for row in table_data["rows"] if row not in rows_to_delete]
        else:
            # Delete all rows if no WHERE clause
            table_data["rows"] = []
        
        # Count rows deleted
        rows_deleted = initial_count - len(table_data["rows"])
        
        # Save updated table
        with open(table_path, 'w') as f:
            json.dump(table_data, f, indent=2)
        
        return f"{rows_deleted} rows deleted from table '{table_name}'"
    
    def _apply_where_clause(self, rows, where_clause):
        """Apply a WHERE clause to filter rows"""
        # Simple WHERE clause parser (supports =, <, >, <=, >=, <>, AND, OR)
        filtered_rows = []
        
        # Split by AND and OR (this is a simplistic approach)
        if ' and ' in where_clause.lower():
            conditions = where_clause.split(' and ')
            for row in rows:
                if all(self._evaluate_condition(row, cond) for cond in conditions):
                    filtered_rows.append(row)
        elif ' or ' in where_clause.lower():
            conditions = where_clause.split(' or ')
            for row in rows:
                if any(self._evaluate_condition(row, cond) for cond in conditions):
                    filtered_rows.append(row)
        else:
            # Single condition
            for row in rows:
                if self._evaluate_condition(row, where_clause):
                    filtered_rows.append(row)
        
        return filtered_rows
    
    def _evaluate_condition(self, row, condition):
        """Evaluate a single condition against a row"""
        condition = condition.strip()
        
        # Check for different operators
        for op in ['>=', '<=', '<>', '<', '>', '=']:
            if op in condition:
                col, value = condition.split(op, 1)
                col = col.strip()
                value = value.strip()
                
                # Parse value
                if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
                    parsed_value = value[1:-1]
                elif value.isdigit():
                    parsed_value = int(value)
                elif re.match(r'^-?\d+(\.\d+)?$', value):
                    parsed_value = float(value)
                elif value.lower() == 'null':
                    parsed_value = None
                else:
                    parsed_value = value
                
                # Get row value
                row_value = row.get(col)
                
                # Compare
                if op == '=':
                    return row_value == parsed_value
                elif op == '<>':
                    return row_value != parsed_value
                elif op == '<':
                    return row_value < parsed_value if row_value is not None else False
                elif op == '>':
                    return row_value > parsed_value if row_value is not None else False
                elif op == '<=':
                    return row_value <= parsed_value if row_value is not None else False
                elif op == '>=':
                    return row_value >= parsed_value if row_value is not None else False
        
        return False

# Simple command-line interface
def main():
    db = MiniMySQL()
    print("Mini MySQL Database Engine")
    print("Enter SQL commands (type 'exit' to quit):")
    
    while True:
        command = input("mysql> ")
        command = command.strip()
        
        if command.lower() == 'exit':
            print("Goodbye!")
            break
        
        # Skip empty commands
        if not command:
            continue
        
        # Remove trailing semicolon if present
        if command.endswith(';'):
            command = command[:-1]
        
        # Execute command
        try:
            result = db.execute(command)
            print(result)
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()