---
# OpenLink MCP Server for ADO\.NET

A lightweight C#-based MCP (Model Context Protocol) server for ADO\.NET . This server is compatible with Virtuoso. Currently, this server has only been successfully tested using the .NET runtimes on Windows and Linux. 

![mcp-client-and-servers|648x499](https://www.openlinksw.com/DAV/www2.openlinksw.com/data/gifs//mcp-client-and-servers-opal-tools-with-dotnet.gif)
---

## Features

- **Get Schemas**: Fetch and list all schema names from the connected database.
- **Get Tables**: Retrieve table information for specific schemas or all schemas.
- **Describe Table**: Generate a detailed description of table structures, including:
  - Column names and data types
  - Nullable attributes
  - Primary and foreign keys
- **Search Tables**: Filter and retrieve tables based on name substrings.
- **Execute Stored Procedures**: When connected to Virtuoso, execute stored procedures and retrieve results.
- **Execute Queries**:
  - JSONL result format: Optimized for structured responses.
  - Markdown table format: Ideal for reporting and visualization.

---

## Prerequisites
1. **NET.8**
   - Check that the project file (`MCP_AdoNet_Server.csproj`) is compatible with your environment by running:
     ```sh
     dotnet run --framework net8.0 --project /path/to/mcp-adonet-server/MCP_AdoNet_Server.csproj
     ```
2. **NET.9**
   - Check that the project file (`MCP_AdoNet_Server.csproj`) is compatible with your environment by running:
     ```sh
     dotnet run --framework net9.0 --project /path/to/mcp-adonet-server/MCP_AdoNet_Server.csproj
     ```
   - If need be, you can also attempt to rebuild `MCP_AdoNet_Server.csproj` by running:
     ```sh
     dotnet clean /path/to/mcp-adonet-server/MCP_AdoNet_Server.csproj
     dotnet build /path/to/mcp-adonet-server/MCP_AdoNet_Server.csproj
     ```

---

## Installation

Clone this repository:
```bash
git clone https://github.com/OpenLinkSoftware/mcp-adonet-server.git  
cd mcp-adonet-server
```
## Environment Variables 

Update your `.env` by overriding these defaults to match your preferences.
```
ADO_URL="HOST=localhost:1111;Database=Demo;UID=demo;PWD=demo"
API_KEY=xxx
```
---

## Configuration

For **Claude Desktop** users:
Add the following to `claude_desktop_config.json`:
1. **NET.8**
   ```json
   {
     "mcpServers": {
       "my_database": {
         "command": "dotnet",
         "args": ["run", "--framework", "net8.0", "--project", "/path/to/mcp-adonet-server/MCP_AdoNet_Server.csproj"],
         "env": {
           "ADO_URL": "HOST=localhost:1111;Database=Demo;UID=demo;PWD=demo",
           "API_KEY": "sk-xxx"
         }
       }
     }
   }
   ```

2. **NET.9**
   ```json
   {
     "mcpServers": {
       "my_database": {
         "command": "dotnet",
         "args": ["run", "--framework", "net9.0", "--project", "/path/to/mcp-adonet-server/MCP_AdoNet_Server.csproj"],
         "env": {
           "ADO_URL": "HOST=localhost:1111;Database=Demo;UID=demo;PWD=demo",
           "API_KEY": "sk-xxx"
         }
       }
     }
   }
   ```
---

## Usage

### Tools Provided

After successful installation, the following tools will be available to MCP client applications.

#### Overview

|name|description|
|---|---|
|`ado_get_schemas`|List database schemas accessible to connected database management system (DBMS).|
|`ado_get_tables`|List tables associated with a selected database schema.|
|`ado_describe_table`|Provide the description of a table associated with a designated database schema. This includes information about column names, data types, null handling, autoincrement, primary key, and foreign keys|
|`ado_filter_table_names`|List tables, based on a substring pattern from the `q` input field, associated with a selected database schema.|
|`ado_query_database`|Execute a SQL query and return results in JSONL format.|
|`ado_execute_query`|Execute a SQL query and return results in JSONL format.|
|`ado_execute_query_md`|Execute a SQL query and return results in Markdown table format.|
|`ado_spasql_query`|Execute a SPASQL query and return results.|
|`ado_sparql_query`|Execute a SPARQL query and return results.|
|`ado_virtuoso_support_ai`|Interact with the Virtuoso Support Assistant/Agent -- a Virtuoso-specific feature for interacting with LLMs|

#### Detailed Description

- **`ado_get_schemas`**
  - Retrieve and return a list of all schema names from the connected database.
  - Input parameters:
    - `url` (string, optional): ADO\.NET URL connection string.
  - Returns a JSON string array of schema names.

- **`ado_get_tables`**
  - Retrieve and return a list containing information about tables in a specified schema. If no schema is provided, uses the connection's default schema.
  - Input parameters:
    - `schema` (string, optional): Database schema to filter tables. Defaults to connection default.
    - `url` (string, optional): ADO\.NET URL connection string.
  - Returns a JSON string containing table information (e.g., `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `TABLE_TYPE`).

- **`ado_filter_table_names`**
  - Filters and returns information about tables whose names contain a specific substring.
  - Input parameters:
    - `q` (string, required): The substring to search for within table names.
    - `schema` (string, optional): Database schema to filter tables. Defaults to connection default.
    - `url` (string, optional): ADO\.NET URL connection string.
  - Returns a JSON string containing information for matching tables.

- **`ado_describe_table`**
  - Retrieve and return detailed information about the columns of a specific table.
  - Input parameters:
    - `schema` (string, required): The database schema name containing the table.
    - `table` (string, required): The name of the table to describe.
    - `url` (string, optional): ADO\.NET URL connection string.
  - Returns a JSON string describing the table's columns (e.g., `COLUMN_NAME`, `TYPE_NAME`, `COLUMN_SIZE`, `IS_NULLABLE`).

- **`ado_query_database`**
  - Execute a standard SQL query and return the results in JSON format.
  - Input parameters:
    - `query` (string, required): The SQL query string to execute.
    - `url` (string, optional): ADO\.NET URL connection string.
  - Returns query results as a JSON string.

- **`ado_query_database_md`**
  - Execute a standard SQL query and return the results formatted as a Markdown table.
  - Input parameters:
    - `query` (string, required): The SQL query string to execute.
    - `url` (string, optional): ADO\.NET URL connection string.
  - Returns query results as a Markdown table string.

- **`ado_query_database_jsonl`**
  - Execute a standard SQL query and return the results in JSON Lines (JSONL) format (one JSON object per line).
  - Input parameters:
    - `query` (string, required): The SQL query string to execute.
    - `url` (string, optional): ADO\.NET URL connection string.
  - Returns query results as a JSONL string.

- **`ado_spasql_query`**
  - Execute a SPASQL (SQL/SPARQL hybrid) query return results. This is a Virtuoso-specific feature.
  - Input parameters:
    - `query` (string, required): The SPASQL query string.
    - `max_rows` (number, optional): Maximum number of rows to return. Defaults to `20`.
    - `timeout` (number, optional): Query timeout in milliseconds. Defaults to `30000`.
    - `url` (string, optional): ADO\.NET URL connection string.
  - Returns the result from the underlying stored procedure call (e.g., ``Demo.demo.execute_spasql_query``).

- **`ado_sparql_query`**
  - Execute a SPARQL query and return results. This is a Virtuoso-specific feature.
  - Input parameters:
    - `query` (string, required): The SPARQL query string.
    - `format` (string, optional): Desired result format. Defaults to `'json'`.
    - `timeout` (number, optional): Query timeout in milliseconds. Defaults to `30000`.
    - `url` (string, optional): ADO\.NET URL connection string.
  - Returns the result from the underlying function call (e.g., `"UB".dba."sparqlQuery"`).

- **`ado_virtuoso_support_ai`**
  - Utilizes a Virtuoso-specific AI Assistant function, passing a prompt and optional API key. This is a Virtuoso-specific feature.
  - Input parameters:
    - `prompt` (string, required): The prompt text for the AI function.
    - `api_key` (string, optional): API key for the AI service. Defaults to `"none"`.
    - `url` (string, optional): ADO\.NET URL connection string.
  - Returns the result from the AI Support Assistant function call (e.g., `DEMO.DBA.OAI_VIRTUOSO_SUPPORT_AI`).

---

## Troubleshooting

For easy troubleshooting:

1. Install the MCP Inspector:
   ```bash
   npm install -g @modelcontextprotocol/inspector
   ```

2. Start the inspector, depending on which .Net version is in use:
   ```bash
   dotnet clean /path/to/mcp-adonet-server/MCP_AdoNet_Server.csproj
   
   npx @modelcontextprotocol/inspector dotnet run --framework net8.0 --project /path/to/mcp-adonet-server/MCP_AdoNet_Server.csproj -e ADO_URL="HOST=localhost:1111;Database=Demo;UID=username;PWD=password" -e API_KEY="sk-xxx-myapikey-xxx"
   ```
   -- or --
   ```
   dotnet clean /path/to/mcp-adonet-server/MCP_AdoNet_Server.csproj
   
   npx @modelcontextprotocol/inspector dotnet run --framework net9.0 --project /path/to/mcp-adonet-server/MCP_AdoNet_Server.csproj -e ADO_URL="HOST=localhost:1111;Database=Demo;UID=username;PWD=password" -e API_KEY="sk-xxx-myapikey-xxx"
   ```

Access the provided URL to troubleshoot server interactions.
