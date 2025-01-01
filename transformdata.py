import re
import os
import csv
import json
import sqlite3
import argparse
import tkinter as tk
from tkinter import filedialog, messagebox

# This script acts as a data transformation tool that can convert data between CSV, JSON, and SQLite formats. (and more in the future)
# It provides a simple GUI interface with options to select the input file and output format.
# Optional functionalities will include basic cleaning and validation before conversion using regular expressions.
# Might add an option for simple analytical summary of data before and after conversion.
# But this will require additional study and research on the best way to implement this feature.

# implementing possible functionality to validate data with regular expressions before conversion
# ...it would be embedded into the conversion methods
def validate_value(value, regex):
    return bool(re.fullmatch(regex, value))

def detect_header(csv_reader):
    for i, row in enumerate(csv_reader):
        if all(re.match(r'^[a-zA-Z_]+$', cell) for cell in row):
            return i, row
    raise ValueError("No valid header row detected.")

# csv to sqlite conversion 
def csv_to_sqlite(csv_path, db_path, table):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    with open(csv_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        csv_reader = list(csv_reader)

        header_index, headers = detect_header(csv_reader)
        columns = ', '.join([f'"{header}" TEXT' for header in headers])
        cursor.execute(f'CREATE TABLE IF NOT EXISTS {table} ({columns})')

        rows_to_insert = csv_reader[header_index + 1:]

        if rows_to_insert:
            placeholders = ', '.join(['?' for _ in headers])
            cursor.executemany(f'INSERT INTO {table} VALUES ({placeholders})', rows_to_insert)

    conn.commit()
    conn.close()
    print(f"Data successfully inserted into {db_path}.")

def csv_to_json(csv_path, json_path):
    with open(csv_path, 'r') as csv_file:
        # DictReader will read the csv as a list of dictionaries
        # converting each row into an object with keys from the header row
        # this makes conversion to json very simple
        csv_reader = csv.DictReader(csv_file)
        data = [row for row in csv_reader]

    with open(json_path, 'w') as json_file:
        json.dump(data, json_file, indent=4)

    print(f"CSV successfully converted to {json_path}.")

# json to csv conversion
# reminder: implement json url support
def json_to_csv(json_path, csv_path):
    with open(json_path, 'r') as json_file:
        data = json.load(json_file)

    if not isinstance(data, list):
        raise ValueError("JSON file must contain an array of objects.")

    # we extract keys from the first object to use as headers
    headers = list(data[0].keys())

    with open(csv_path, 'w', newline='') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=headers)
        csv_writer.writeheader()
        for row in data:
            # reminder: issue where key order was not preserved in an earlier version
            # might need to test many different types of data
            ordered_row = {key: row.get(key, None) for key in headers}
            csv_writer.writerow(ordered_row)

    print(f"JSON successfully converted to {csv_path}.")

# json to sqlite conversion
# reminder: implement json url support
def json_to_sqlite(json_path, db_path, table):
    with open(json_path, 'r') as json_file:
        data = json.load(json_file)

    if not isinstance(data, list):
        raise ValueError("JSON file must contain an array of objects.")

    headers = list(data[0].keys())

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # we create a table with columns based on the keys in the json data
    columns = ', '.join([f'"{header}" TEXT' for header in headers])
    cursor.execute(f'CREATE TABLE IF NOT EXISTS {table} ({columns})')

    # reminder: issue where key order was not preserved in an earlier version
    # might need to test many different types of data
    # so far seems to work fine with simple data
    rows_to_insert = [tuple(row.get(header, None) for header in headers) for row in data]

    placeholders = ', '.join(['?' for _ in headers])
    cursor.executemany(f'INSERT INTO {table} VALUES ({placeholders})', rows_to_insert)

    conn.commit()
    conn.close()
    print(f"JSON successfully converted to {db_path}.")

# sqlite to json conversion
def sqlite_to_json(db_path, table, json_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute(f'SELECT * FROM {table}')
    except sqlite3.OperationalError:
        raise ValueError(f"Table '{table}' does not exist in the database.")

    data = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]

    # Similar to DictReader in csv conversion,
    # we create a list of dictionaries where each dictionary represents an object
    # with keys from the sql columns and their corresponding values on each row
    json_data = [dict(zip(headers, row)) for row in data]

    with open(json_path, 'w') as json_file:
        json.dump(json_data, json_file, indent=4)

    conn.close()
    print(f"SQLite successfully converted to {json_path}.")

# sqlite to csv conversion
def sqlite_to_csv(db_path, table, csv_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute(f'SELECT * FROM {table}')
    except sqlite3.OperationalError:
        raise ValueError(f"Table '{table}' does not exist in the database.")
    
    data = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]

    with open(csv_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(headers)
        csv_writer.writerows(data)

    conn.close()
    print(f"SQLite successfully converted to {csv_path}.")

# CLI interface
# will probably allow only simple conversion without any additional features
def cli_interface():
    parser = argparse.ArgumentParser(description="Data Transformer Tool")
    parser.add_argument('--input', required=True, help="Path to input file (CSV/JSON/DB)")
    parser.add_argument('--output', required=True, help="Path to output folder")
    parser.add_argument('--format', required=True, choices=['csv', 'json', 'sqlite'], help="Output format")
    parser.add_argument('--table', required=False, help="Table name (required for SQLite conversions)")
    args = parser.parse_args()

    if args.format == 'sqlite' or args.input.endswith('.db'):
        if not args.table:
            parser.error("--table is required for SQLite conversions")

    if args.input.endswith('.csv') and args.format == 'sqlite':
        csv_to_sqlite(args.input, args.output, args.table)
    elif args.input.endswith('.csv') and args.format == 'json':
        csv_to_json(args.input, args.output)
    elif args.input.endswith('.json') and args.format == 'csv':
        json_to_csv(args.input, args.output)
    elif args.input.endswith('.json') and args.format == 'sqlite':
        json_to_sqlite(args.input, args.output, args.table)
    elif args.input.endswith('.db') and args.format == 'json':
        sqlite_to_json(args.input, args.table, args.output)
    elif args.input.endswith('.db') and args.format == 'csv':
        sqlite_to_csv(args.input, args.table, args.output)
    else:
        parser.error("Unsupported file format or conversion type.")
    
# GUI interface
class DataTransformerApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Clean, Format & Convert")
        self.geometry("500x400")

        # Input File Section
        tk.Label(self, text="Input File:").pack(pady=5)
        self.input_entry = tk.Entry(self, width=30)
        self.input_entry.pack(pady=5)
        tk.Button(self, text="Browse", command=self.load_input).pack(pady=5)

        # Output Format Section
        tk.Label(self, text="Output Format:").pack(pady=5)
        self.format_var = tk.StringVar(value='csv')
        format_menu = tk.OptionMenu(self, self.format_var, 'csv', 'json', 'sqlite', command=self.toggle_table_entry)
        format_menu.pack(pady=5)

        # Table Name Field (for SQLite)
        self.table_frame = tk.Frame(self)
        self.table_label = tk.Label(self.table_frame, text="Table Name (for SQLite):")
        self.table_entry = tk.Entry(self.table_frame, width=50)

        # Start Conversion Button
        tk.Button(self, text="Start Conversion", command=self.start_conversion).pack(pady=20)

    def load_input(self):
        """Prompt the user to select an input file."""
        file_path = filedialog.askopenfilename(filetypes=[("All Files", "*.*")])
        self.input_entry.delete(0, tk.END)
        self.input_entry.insert(0, file_path)

    def toggle_table_entry(self, choice):
        """Show or hide the table name entry based on the selected format."""
        input_path = self.input_entry.get()
        # Showing table entry if output or input is SQLite
        if choice == 'sqlite' or (input_path and input_path.endswith('.db')):
            self.table_frame.pack(pady=5)
            self.table_label.pack(side=tk.LEFT)
            self.table_entry.pack(side=tk.LEFT)
        else:
            self.table_frame.pack_forget()

    def start_conversion(self):
        """Handle the conversion process."""
        input_path = self.input_entry.get()
        format_choice = self.format_var.get()
        table = self.table_entry.get()

        if not input_path:
            messagebox.showerror("Error", "Please specify an input file.")
            return

        # Here I am using the input file name to suggest a default output file name,
        # this seems to be a common practice and is quite useful for the user
        input_filename = os.path.basename(input_path)
        input_name, _ = os.path.splitext(input_filename)
        default_extension = {
            'csv': '.csv',
            'json': '.json',
            'sqlite': '.db'
        }.get(format_choice, '')
        default_filename = f"{input_name}{default_extension}"

        # Prompt for output file path
        output_path = filedialog.asksaveasfilename(
            defaultextension=default_extension,
            initialfile=default_filename,
            filetypes=[("All Files", "*.*")]
        )

        if not output_path:
            return  # allowing the user to cancel the operation

        try:
            if input_path.endswith('.csv') and format_choice == 'sqlite':
                if not table:
                    raise ValueError("Table name is required for SQLite conversion.")
                csv_to_sqlite(input_path, output_path, table)
            elif input_path.endswith('.csv') and format_choice == 'json':
                csv_to_json(input_path, output_path)
            elif input_path.endswith('.json') and format_choice == 'csv':
                json_to_csv(input_path, output_path)
            elif input_path.endswith('.json') and format_choice == 'sqlite':
                if not table:
                    raise ValueError("Table name is required for SQLite conversion.")
                json_to_sqlite(input_path, output_path, table)
            elif input_path.endswith('.db') and format_choice == 'json':
                if not table:
                    raise ValueError("Table name is required for SQLite to JSON conversion.")
                sqlite_to_json(input_path, table, output_path)
            elif input_path.endswith('.db') and format_choice == 'csv':
                if not table:
                    raise ValueError("Table name is required for SQLite to CSV conversion.")
                sqlite_to_csv(input_path, table, output_path)
            else:
                raise ValueError("Unsupported file format or conversion type.")
            messagebox.showinfo("Success", "Conversion completed successfully!")
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred: {str(e)}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        cli_interface()
    else:
        app = DataTransformerApp()
        app.mainloop()