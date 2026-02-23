# SQL to MongoDB Transpiler
---

## Table of Contents

- [Project Overview](#project-overview)  
- [Folder Structure](#folder-structure)  
- [Intended SQL Subset](#intended-sql-subset)  
- [Architecture Flow Diagram](#architecture-flow-diagram)  
- [Technologies Used](#technologies-used)  
- [Work Done So Far](#work-done-so-far)  
- [How to Run](#how-to-run)   

---

##  Project Overview

This project aims to build a **SQL-to-MongoDB transpiler** that converts basic SQL queries into their equivalent MongoDB queries. The current implementation focuses on the **lexical analysis (tokenizer) phase** using the **PLY (Python Lex-Yacc)** library.

---

## Folder Structure
```
sql-to-mongodb-transpiler/
├── ast/
│ └── ast_nodes.py # AST node definitions (for future use)
├── lexer/
│ ├── init.py
│ ├── lexer.py # PLY lexer implementation
│ └── tokens.py # Token definitions & reserved keywords
├── parser/
│ └── parser.py # Placeholder for parser (future)
├── tests/
│ └── test_lexer.py # Unit test for lexer
├── pycache/
├── .gitignore
├── README.md
└── requirements.txt
```
---
## Intended SQL Subset

The transpiler is designed to support the following subset of SQL:

- `SELECT`  
- `FROM`  
- `WHERE`  
- Comparisons (`=`, `!=`, `<`, `>`, `<=`, `>=`)  
- Logical operators: `AND`, `OR`, `NOT`  
- `ORDER BY`  
- `LIMIT`  
- Aggregation functions (e.g., `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`)  

---
## Architecture flow diagram: 
```
+--------------------+
|    SQL Query       |
| (User Input)       |
+--------------------+
          |
          v
+--------------------+
|   Lexical Analyzer |
|   (PLY Lexer)      |
|                    |
| - Keywords         |
| - Identifiers      |
| - Literals         |
| - Operators        |
+--------------------+
          |
          v
+--------------------+
|   Token Stream     |
| (Type, Value,      |
|  Line, Position)   |
+--------------------+
          |
          v
+----------------------------+
|   Syntax Analyzer (Parser) |
|   - SQL Grammar Rules      |
|   - Parse Tree / AST       |
+----------------------------+
          |
          v
+----------------------------+
| Intermediate Code          |
| Generation                 |
| - Logical Query Structure  |
| - Query Representation    |
+----------------------------+
          |
          v
+----------------------------+
| MongoDB Query Translation  |
| - find() / projection      |
| - filter conditions        |
+----------------------------+
          |
          v
+----------------------------+
| MongoDB Query Output       |
+----------------------------+
```
---
##  Technologies Used

- **Python**
- **PLY (Python Lex-Yacc)**
- **sqlparse** (for testing and comparison)
  
---
## Work Done So Far

- Implemented a **lexer** using PLY to tokenize SQL queries.
- Supported SQL keywords: `SELECT`, `FROM`, `WHERE`.
- Identified tokens for:
  - **Identifiers** (table names, column names)
  - **Numeric literals**
  - **String literals**
  - **Relational operators** (`=`, `<`, `>`, `<=`, `>=`, `!=`)
  - **Comma** and **semicolon**
- Implemented **case-insensitive keyword recognition**.
- Added **line number tracking** and **error handling** for illegal characters.
- Developed a **testing script - test_lexer.py** to compare PLY-generated tokens with tokens produced by the **sqlparse** library.

---

## How to Run
Before cloning create venv environment and enter in it.as,
```
python3 -m install venv
source venv/bin/activate
```
1. **Clone the repository:**
```bash
git clone "https://github.com/MANDADI-PRANATHI/SQL_to_MongoDB_Transpiler.git"

cd SQL-to-MongoDB-Transpiler
```
```
pip install -r requirements.txt
pip install ply
```
 To test the lexer implementated until now,in the sql-to-mongodb-transpiler folder
```
python3 main.py
```
