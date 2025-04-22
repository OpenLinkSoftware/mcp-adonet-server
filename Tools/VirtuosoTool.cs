using System.ComponentModel;
using System.Data;
using System.Data.Odbc;
using System.Data.Common;
using System.Text;
using Newtonsoft.Json;
using ModelContextProtocol.Server;
using DotNetEnv;
using ModelContextProtocol.Protocol.Types;


namespace McpNetServer.Tools;

[McpServerToolType]
public sealed class VirtuosoTools
{
    private static readonly string DefaultConnStr;
    private static readonly int DefaultMaxLongData;
    private static readonly string DefaultApiKey;

    static VirtuosoTools()
    {
        // Load environment variables from a .env file located in the application directory.
        Env.TraversePath().Load();

        DefaultConnStr = Env.GetString("ADO_URL", "DSN=VOS;UID=demo;PWD=demo;");
        DefaultMaxLongData = Env.GetInt("MAX_LONG_DATA", 100);
        DefaultApiKey = Env.GetString("API_KEY", "sk-xxx");
    }

    private static string EscapeSql(string value)
    {
        if (value == null)
            return String.Empty;
        return value.Replace("'", "''");
    }

    private static DbConnection GetConnection(string? url)
    {
        var finalUrl = string.IsNullOrWhiteSpace(url) ? DefaultConnStr : url;

        if (string.IsNullOrWhiteSpace(finalUrl))
            throw new InvalidOperationException("ADO_URL is required and was not provided or set in environment.");

        var builder = new OdbcConnectionStringBuilder(finalUrl);

        //if (!string.IsNullOrEmpty(finalUser))
        //    builder.UserID = finalUser;

        //if (!string.IsNullOrEmpty(finalPassword))
        //    builder.Password = finalPassword;

        return new OdbcConnection(builder.ToString());
    }

    [McpServerTool(Name = "ado_get_schemas"),
     Description("Retrieve and return a list of all schema/catalog names from the connected database.")]
    public static async Task<CallToolResponse> AdoGetSchemas(
        [Description("ADO URL")] string? url = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var conn = GetConnection(url);
            await conn.OpenAsync(cancellationToken);

            var schemas = new List<string>();

            var restrictions = new string?[3];
            restrictions[0] = "%";

            var metaData = conn.GetSchema("Tables", restrictions);

            foreach (DataRow row in metaData.Rows)
            {
                schemas.Add(row[0].ToString()!);
            }

            var settings = new JsonSerializerSettings
            {
                Formatting = Formatting.None,
                NullValueHandling = NullValueHandling.Include
            };
            return new CallToolResponse
            {
                IsError = false,
                Content = new List<Content> { new Content { Type = "text", Text = JsonConvert.SerializeObject(schemas, settings) } }
            };

        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CallToolResponse
            {
                IsError = true,
                Content = new List<Content> { new Content { Type = "text", Text = ex.Message } }
            };
        }
    }


    [McpServerTool(Name = "ado_get_tables"),
     Description("Retrieve and return a list containing information about tables in the specified schema.")]
    public static async Task<CallToolResponse> AdoGetTables(
        [Description("Schema name")] string? schema = null,
        [Description("ADO URL")] string? url = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var conn = GetConnection(url);
            await conn.OpenAsync(cancellationToken);

            var restrictions = new string?[3];
            restrictions[0] = schema ?? null; // Catalog
            restrictions[2] = "%";

            var tableSchema = conn.GetSchema("Tables", restrictions);

            var tables = new List<Dictionary<string, string?>>();
            foreach (DataRow row in tableSchema.Rows)
            {
                var table = new Dictionary<string, string?>
                {
                    ["TABLE_CAT"] = row[0].ToString(),
                    ["TABLE_SCHEM"] = row[1].ToString(),
                    ["TABLE_NAME"] = row[2].ToString()
                };
                tables.Add(table);
            }

            var settings = new JsonSerializerSettings
            {
                Formatting = Formatting.None,
                NullValueHandling = NullValueHandling.Include
            };
            return new CallToolResponse
            {
                IsError = false,
                Content = new List<Content> { new Content { Type = "text", Text = JsonConvert.SerializeObject(tables, settings) } }
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CallToolResponse
            {
                IsError = true,
                Content = new List<Content> { new Content { Type = "text", Text = ex.Message } }
            };
        }
    }


    [McpServerTool(Name = "ado_describe_table"),
     Description("Retrieve and return full metadata about a table including columns, primary keys, and foreign keys.")]
    public static async Task<CallToolResponse> AdoDescribeTable(
        [Description("Schema name")] string? schema = null,
        [Description("Table name")] string table = "",
        [Description("ADO URL")] string? url = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var conn = GetConnection(url);
            await conn.OpenAsync(cancellationToken);

            var tableInfo = GetTableInfoAsync(conn, schema, table);

            var settings = new JsonSerializerSettings
            {
                Formatting = Formatting.None,
                NullValueHandling = NullValueHandling.Include
            };
            return new CallToolResponse
            {
                IsError = false,
                Content = new List<Content> { new Content { Type = "text", Text = JsonConvert.SerializeObject(tableInfo, settings) } }
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CallToolResponse
            {
                IsError = true,
                Content = new List<Content> { new Content { Type = "text", Text = ex.Message } }
            };
        }
    }

    [McpServerTool(Name = "ado_filter_table_names"),
     Description("List tables whose names contain the given substring.")]
    public static async Task<CallToolResponse> AdoFilterTableNames(
        [Description("Substring to search")] string q,
        [Description("Schema name")] string? schema = null,
        [Description("ADO URL")] string? url = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(q))
                throw new ArgumentException("Query string cannot be null or empty.", nameof(q));
                
            await using var conn = GetConnection(url);
            await conn.OpenAsync(cancellationToken);

            var restrictions = new string?[3];
            restrictions[0] = schema;
            restrictions[2] = "%";
            var dt = conn.GetSchema("Tables", restrictions);

            var list = new List<Dictionary<string, string?>>();
            foreach (DataRow row in dt.Rows)
            {
                var name = row[2]?.ToString() ?? string.Empty;
                if (name.Contains(q, StringComparison.OrdinalIgnoreCase))
                {
                    list.Add(new Dictionary<string, string?>
                    {
                        ["TABLE_CAT"] = row[0].ToString(),
                        ["TABLE_SCHEM"] = row[1].ToString(),
                        ["TABLE_NAME"] = row[2].ToString()
                    });
                }
            }
            var settings = new JsonSerializerSettings
            {
                Formatting = Formatting.None,
                NullValueHandling = NullValueHandling.Include
            };
            return new CallToolResponse
            {
                IsError = false,
                Content = new List<Content> { new Content { Type = "text", Text = JsonConvert.SerializeObject(list, settings) } }
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CallToolResponse
            {
                IsError = true,
                Content = new List<Content> { new Content { Type = "text", Text = ex.Message } }
            };
        }
    }

    [McpServerTool(Name = "ado_execute_query"),
     Description("Execute a SQL query and return results in JSONL format.")]
    public static async Task<CallToolResponse> AdoExecuteQuery(
        [Description("SQL query")] string query,
        [Description("Max Rows")] int? max_rows = null,
        [Description("ADO URL")] string? url = null,
        CancellationToken cancellationToken = default)
    {
        try {
            var maxRows = max_rows ?? 100;
            await using var conn = GetConnection(url);
            await conn.OpenAsync(cancellationToken);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = query;
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var list = new List<Dictionary<string, object?>>();
            var settings = new JsonSerializerSettings
            {
                Formatting = Formatting.None,
                NullValueHandling = NullValueHandling.Include
            };

            int count = 0;
            while (await reader.ReadAsync(cancellationToken) && count < maxRows)
            {
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var val = reader.GetValue(i);
                    var str = val?.ToString() ?? null;
                    if (str != null && str.Length > DefaultMaxLongData)
                        str = str.Substring(0, DefaultMaxLongData);
                    row[reader.GetName(i)] = str;
                }
                list.Add(row);
                count++;
            }
            var text = JsonConvert.SerializeObject(list, settings);
            return new CallToolResponse
            {
                IsError = false, Content = new List<Content> { new Content { Type = "text", Text = text } }
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CallToolResponse
            {
                IsError = true, Content = new List<Content> { new Content { Type = "text", Text = ex.Message } }
            };
        }
    }

    [McpServerTool(Name = "ado_execute_query_md"),
     Description("Execute a SQL query and return results in Markdown table format.")]
    public static async Task<CallToolResponse> AdoExecuteQueryMd(
        [Description("SQL query")] string query,
        [Description("Max Rows")] int? max_rows = null,
        [Description("ADO URL")] string? url = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var maxRows = max_rows ?? 100;
            await using var conn = GetConnection(url);
            await conn.OpenAsync(cancellationToken);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = query;
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var colNames = Enumerable.Range(0, reader.FieldCount)
                .Select(i => reader.GetName(i))
                .ToList();

            var md = new StringBuilder();
            md.Append("| ").Append(string.Join(" | ", colNames)).AppendLine(" |");
            md.Append("| ").Append(string.Join(" | ", colNames.Select(n => "---"))).AppendLine(" |");

            int count = 0;
            while (await reader.ReadAsync(cancellationToken) && count < maxRows)
            {
                md.Append("| ");
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var val = reader.GetValue(i)?.ToString() ?? string.Empty;
                    if (val.Length > DefaultMaxLongData)
                        val = val.Substring(0, DefaultMaxLongData);
                    md.Append(val).Append(" | ");
                }
                md.AppendLine();
                count++;
            }
            return new CallToolResponse
            {
                IsError = false,
                Content = new List<Content> { new Content { Type = "text", Text = md.ToString() } }
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CallToolResponse
            {
                IsError = true,
                Content = new List<Content> { new Content { Type = "text", Text = ex.Message } }
            };
        }
    }


    [McpServerTool(Name = "ado_query_database"),
     Description("Execute a SQL query and return results in JSONL format.")]
    public static async Task<CallToolResponse> AdoQueryDatabase(
        [Description("SQL query")] string query,
        [Description("ADO URL")] string? url = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(query))
                throw new ArgumentException("Query string cannot be null or empty.", nameof(query));
            await using var conn = GetConnection(url);
            await conn.OpenAsync(cancellationToken);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = query;
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var list = new List<Dictionary<string, object?>>();
            var settings = new JsonSerializerSettings
            {
                Formatting = Formatting.None,
                NullValueHandling = NullValueHandling.Include
            };

            while (await reader.ReadAsync(cancellationToken))
            {
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var val = reader.GetValue(i);
                    var str = val?.ToString() ?? null;
                    if (str != null && str.Length > DefaultMaxLongData)
                        str = str.Substring(0, DefaultMaxLongData);
                    row[reader.GetName(i)] = str;
                }
                list.Add(row);
            }
            var text = JsonConvert.SerializeObject(list, settings);
            return new CallToolResponse
            {
                IsError = false,
                Content = new List<Content> { new Content { Type = "text", Text = text } }
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CallToolResponse
            {
                IsError = true,
                Content = new List<Content> { new Content { Type = "text", Text = ex.Message } }
            };
        }
    }

    [McpServerTool(Name = "ado_spasql_query"),
     Description("Execute a SPASQL query and return results.")]
    public static async Task<CallToolResponse> AdoSpasqlQuery(
        [Description("SPASQL query")] string query,
        [Description("Max Rows")] int? max_rows = null,
        [Description("Timeout ms")] int? timeout = null,
        [Description("ADO URL")] string? url = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(query))
                throw new ArgumentException("Query string cannot be null or empty.", nameof(query));
            var maxRows = max_rows ?? 20;
            var timeoutValue = timeout ?? 300000;

            await using var conn = GetConnection(url);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = conn.CreateCommand();

            cmd.CommandText = $"select Demo.demo.execute_spasql_query('{EscapeSql(query)}', ?, ?) as result";
            cmd.Parameters.Add(maxRows);
            cmd.Parameters.Add(timeoutValue);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            var text = (await reader.ReadAsync(cancellationToken)) ? reader.GetString(0) : string.Empty;

            return new CallToolResponse
            {
                IsError = false,
                Content = new List<Content> { new Content { Type = "text", Text = text } }
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CallToolResponse
            {
                IsError = true,
                Content = new List<Content> { new Content { Type = "text", Text = ex.Message } }
            };
        }
    }

    [McpServerTool(Name = "ado_sparql_query"),
     Description("Execute a SPARQL query and return results.")]
    public static async Task<CallToolResponse> AdoSparqlQuery(
        [Description("SPARQL query")] string query,
        [Description("Result format")] string? format = null,
        [Description("Timeout ms")] int? timeout = null,
        [Description("ADO URL")] string? url = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(query))
                throw new ArgumentException("Query string cannot be null or empty.", nameof(query));

            var formatValue = string.IsNullOrWhiteSpace(format) ? "json" : format;
            var timeoutValue = timeout ?? 300000;

            await using var conn = GetConnection(url);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = conn.CreateCommand();

            cmd.CommandText = $"select \"UB\".dba.\"sparqlQuery\"('{EscapeSql(query)}', ?, ?) as result";
            cmd.Parameters.Add(formatValue);
            cmd.Parameters.Add(timeoutValue);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            var text = (await reader.ReadAsync(cancellationToken)) ? reader.GetString(0) : string.Empty;
            return new CallToolResponse
            {
                IsError = false,
                Content = new List<Content> { new Content { Type = "text", Text = text } }
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CallToolResponse
            {
                IsError = true,
                Content = new List<Content> { new Content { Type = "text", Text = ex.Message } }
            };
        }
    }

    [McpServerTool(Name = "ado_virtuoso_support_ai"),
     Description("Interact with Virtuoso Support AI Agent.")]
    public static async Task<CallToolResponse> AdoVirtuosoSupportAi(
        [Description("Prompt for AI agent")] string prompt,
        [Description("API Key")] string? api_key = null,
        [Description("ADO URL")] string? url = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var key = string.IsNullOrWhiteSpace(api_key) ? DefaultApiKey : api_key;

            await using var conn = GetConnection(url);
            await conn.OpenAsync(cancellationToken);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "select DEMO.DBA.OAI_VIRTUOSO_SUPPORT_AI(?, ?) as result";
            cmd.Parameters.Add(prompt);
            cmd.Parameters.Add(key);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            var text = (await reader.ReadAsync(cancellationToken)) ? reader.GetString(0) : string.Empty;
            return new CallToolResponse
            {
                IsError = false,
                Content = new List<Content> { new Content { Type = "text", Text = text } }
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CallToolResponse
            {
                IsError = true,
                Content = new List<Content> { new Content { Type = "text", Text = ex.Message } }
            };
        }
    }

    [McpServerTool(Name = "ado_sparql_func"),
     Description("Use the SPARQL AI Agent function.")]
    public static async Task<CallToolResponse> AdoSparqlFunc(
        [Description("Prompt for AI function")] string prompt,
        [Description("API Key")] string? api_key = null,
        [Description("ADO URL")] string? url = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var key = string.IsNullOrWhiteSpace(api_key) ? DefaultApiKey : api_key;

            await using var conn = GetConnection(url);
            await conn.OpenAsync(cancellationToken);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "select DEMO.DBA.OAI_SPARQL_FUNC(?, ?) as result";
            cmd.Parameters.Add(prompt);
            cmd.Parameters.Add(key);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            var text = (await reader.ReadAsync(cancellationToken)) ? reader.GetString(0) : string.Empty;
            return new CallToolResponse
            {
                IsError = false,
                Content = new List<Content> { new Content { Type = "text", Text = text } }
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CallToolResponse
            {
                IsError = true,
                Content = new List<Content> { new Content { Type = "text", Text = ex.Message } }
            };
        }
    }


    [McpServerTool(Name = "ado_sparql_get_entity_types"),
     Description(@"This query retrieves all entity types in the RDF graph, along with their labels and comments if available. "
                  +"It filters out blank nodes and ensures that only IRI types are returned. "
                  +"The LIMIT clause is set to 100 to restrict the number of entity types returned. ")]
    public static async Task<CallToolResponse> AdoSparqlGetEntityTypes(
        [Description("ADO URL")] string? url = null,
        CancellationToken cancellationToken = default)
    {
        var query = @"SELECT DISTINCT * FROM (
  SPARQL 
  PREFIX owl: <http://www.w3.org/2002/07/owl#>
  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
  SELECT ?o 
  WHERE {
    GRAPH ?g {
        ?s a ?o .
        
        OPTIONAL {
            ?s rdfs:label ?label . 
            FILTER (LANG(?label) = ""en"" || LANG(?label) = """")
        }
        
        OPTIONAL {
            ?s rdfs:comment ?comment . 
            FILTER (LANG(?comment) = ""en"" || LANG(?comment) = """")
        }
        
        FILTER (isIRI(?o) && !isBlank(?o))
    }
  }
  LIMIT 100
) AS x";
        return await AdoQueryDatabase(query, url, cancellationToken);
    }


    [McpServerTool(Name = "ado_sparql_get_entity_types_detailed"),
     Description(@"This query retrieves all entity types in the RDF graph, along with their labels and comments if available. "
                 + "It filters out blank nodes and ensures that only IRI types are returned. "
                 + "The LIMIT clause is set to 100 to restrict the number of entity types returned.")]
    public static async Task<CallToolResponse> AdoSparqlGetEntityTypesDetailed(
        [Description("ADO URL")] string? url = null,
        CancellationToken cancellationToken = default)
    {
        var query = @"SELECT * FROM (
        SPARQL
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 

        SELECT ?o, (SAMPLE(?label) AS ?label), (SAMPLE(?comment) AS ?comment)
        WHERE {
            GRAPH ?g {
                ?s a ?o .
                OPTIONAL {?o rdfs:label ?label . FILTER (LANG(?label) = ""en"" || LANG(?label) = """")}
                OPTIONAL {?o rdfs:comment ?comment . FILTER (LANG(?comment) = ""en"" || LANG(?comment) = """")}
                FILTER (isIRI(?o) && !isBlank(?o))
            }
        }
        GROUP BY ?o
        ORDER BY ?o
        LIMIT 20
    ) AS results ";
        return await AdoQueryDatabase(query, url, cancellationToken);
    }


    [McpServerTool(Name = "ado_sparql_get_entity_types_samples"),
     Description(@"This query retrieves samples of entities for each type in the RDF graph, along with their labels and counts. "
                + "It groups by entity type and orders the results by sample count in descending order. "
                + "Note: The LIMIT clause is set to 20 to restrict the number of entity types returned.")]
    public static async Task<CallToolResponse> AdoSparqlGetEntityTypesSamples(
        [Description("ADO URL")] string? url = null,
        CancellationToken cancellationToken = default)
    {
        var query = @"SELECT * FROM (
        SPARQL
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
        SELECT (SAMPLE(?s) AS ?sample), ?slabel, (COUNT(*) AS ?sampleCount), (?o AS ?entityType), ?olabel
        WHERE {
            GRAPH ?g {
                ?s a ?o .
                OPTIONAL {?s rdfs:label ?slabel . FILTER (LANG(?slabel) = \""en\"" || LANG(?slabel) = \""\"")}
                FILTER (isIRI(?s) && !isBlank(?s))
                OPTIONAL {?o rdfs:label ?olabel . FILTER (LANG(?olabel) = \""en\"" || LANG(?olabel) = \""\"")}
                FILTER (isIRI(?o) && !isBlank(?o))
            }
        }
        GROUP BY ?slabel ?o ?olabel
        ORDER BY DESC(?sampleCount) ?o ?slabel ?olabel
        LIMIT 20
    ) AS results";
        return await AdoQueryDatabase(query, url, cancellationToken);
    }


    [McpServerTool(Name = "ado_sparql_get_ontologies"),
     Description("This query retrieves all ontologies in the RDF graph, along with their labels and comments if available.")]
    public static async Task<CallToolResponse> AdoSparqlGetOntologies(
        [Description("ADO URL")] string? url = null,
        CancellationToken cancellationToken = default)
    {
        var query = @"SELECT * FROM (
        SPARQL 
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT ?s, ?label, ?comment 
        WHERE {
            GRAPH ?g {
                ?s a owl:Ontology .
            
                OPTIONAL {
                    ?s rdfs:label ?label . 
                    FILTER (LANG(?label) = ""en"" || LANG(?label) = """")
                }
            
                OPTIONAL {
                    ?s rdfs:comment ?comment . 
                    FILTER (LANG(?comment) = ""en"" || LANG(?comment) = """")
                }
            
                FILTER (isIRI(?o) && !isBlank(?o))
            }
        }
        LIMIT 100
    ) AS x";
        return await AdoQueryDatabase(query, url, cancellationToken);
    }

    //==========================
    private static Dictionary<string, object?> GetTableInfoAsync(DbConnection conn, string? schema, string table)
    {
        var columns = new List<Dictionary<string, object?>>();
        var primaryKeys = new List<string>();
        var foreignKeys = new List<Dictionary<string, object?>>();


        var tableSchema = conn.GetSchema("Tables", new[] { schema ?? null, null, table });
        if (tableSchema.Rows.Count == 0)
        {
            return new Dictionary<string, object?>
            {
                ["TABLE_CAT"] = conn.Database,
                ["TABLE_SCHEM"] = schema,
                ["TABLE_NAME"] = table,
                ["columns"] = columns,
                //["primary_keys"] = primaryKeys,
                //["foreign_keys"] = foreignKeys
            };
            // throw new Exception($"Table {table} not found in schema {schema}.");
        }

        var trow = tableSchema.Rows[0];
        var cat = trow[0].ToString();
        var sch = trow[1].ToString();

        var cols = conn.GetSchema("Columns", new[] { cat, sch, table });
        foreach (DataRow row in cols.Rows)
        {
            columns.Add(new Dictionary<string, object?>
            {
                ["name"] = row[3].ToString(), //"COLUMN_NAME"
                ["type"] = row[5].ToString(), //"DATA_TYPE"
                ["column_size"] = row[6], //"CHARACTER_MAXIMUM_LENGTH"
                ["num_prec_radix"] = row[9], //"NUMERIC_PRECISION_RADIX"
                ["nullable"] = Convert.ToInt32(row[10]) == 1, // ToString()?.ToUpper() == "YES", //"IS_NULLABLE" ??
                ["default"] = row[12] //"COLUMN_DEFAULT"
            });
        }
#if PKEY
        var pk = conn.GetSchema("Indexes", new[] { cat, sch, table });
        foreach (DataRow row in pk.Rows)
        {
            if (row["PRIMARY_KEY"].ToString()?.ToUpper() == "TRUE")
                primaryKeys.Add(row[8].ToString()!); //"COLUMN_NAME"
        }
#endif
#if FK_KEYS
        var fk = conn.GetSchema("ForeignKeys", new[] { cat, sch, table });
        foreach (DataRow row in fk.Rows)
        {
            foreignKeys.Add(new Dictionary<string, object?>
            {
                ["name"] = row[11].ToString(), //"FK_NAME"
                ["constrained_columns"] = new List<string> { row[7].ToString()! }, //"FKCOLUMN_NAME"
                ["referred_cat"] = row[0].ToString(),    //"PKTABLE_CAT"
                ["referred_schem"] = row[1].ToString(), //"PKTABLE_SCHEM"
                ["referred_table"] = row[2].ToString(), //"PKTABLE_NAME"
                ["referred_columns"] = new List<string> { row[3].ToString()! } //"PKCOLUMN_NAME"
            });
        }
#endif
        //foreach (var column in columns)
        //    column["primary_key"] = primaryKeys.Contains(column["name"]?.ToString()!);

        return new Dictionary<string, object?>
        {
            ["TABLE_CAT"] = conn.Database,
            ["TABLE_SCHEM"] = schema,
            ["TABLE_NAME"] = table,
            ["columns"] = columns,
            //["primary_keys"] = primaryKeys,
            //["foreign_keys"] = foreignKeys
        };
    }



}
