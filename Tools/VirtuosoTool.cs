using System.ComponentModel;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Newtonsoft.Json;
using ModelContextProtocol.Server;
using DotNetEnv;
using ModelContextProtocol.Protocol.Types;
using OpenLink.Data.VirtuosoNET;
using System.IO;
using Microsoft.Extensions.Logging;



namespace McpNetServer.Tools;

[McpServerToolType]
public sealed class VirtuosoTools
{
    private readonly ILogger<VirtuosoTools> _logger;
    private readonly string DefaultConnStr;
    private readonly int DefaultMaxLongData;
    private readonly string DefaultApiKey;


    public VirtuosoTools(ILogger<VirtuosoTools> logger)
    {
        _logger = logger;

        // Load environment variables from a .env file located in the application directory.
        Env.TraversePath().Load();

        DefaultConnStr = Env.GetString("ADO_URL", "HOST=localhost:1111;Database=Demo;UID=demo;PWD=demo;");
        DefaultMaxLongData = Env.GetInt("MAX_LONG_DATA", 100);
        DefaultApiKey = Env.GetString("API_KEY", "sk-xxx");
    }

    private VirtuosoConnection GetConnection(string? url)
    {
        var finalUrl = string.IsNullOrWhiteSpace(url) ? DefaultConnStr : url;

        if (string.IsNullOrWhiteSpace(finalUrl))
            throw new InvalidOperationException("ADO_URL is required and was not provided or set in environment.");

        var builder = new VirtuosoConnectionStringBuilder(finalUrl);

        //if (!string.IsNullOrEmpty(finalUser))
        //    builder.UserID = finalUser;

        //if (!string.IsNullOrEmpty(finalPassword))
        //    builder.Password = finalPassword;
        //_logger.LogInformation("Connect to => "+ builder.ToString());

        return new VirtuosoConnection(builder.ToString());
    }

    [McpServerTool(Name = "ado_get_schemas"),
     Description("Retrieve and return a list of all schema/catalog names from the connected database.")]
    public async Task<CallToolResponse> AdoGetSchemas(
        [Description("ADO URL")] string? url = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var conn = GetConnection(url);
            await conn.OpenAsync(cancellationToken);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "select distinct name_part(KEY_TABLE,0) AS TABLE_CAT VARCHAR(128) from DB.DBA.SYS_KEYS order by 1";
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var schemas = new List<string>();
            while (await reader.ReadAsync(cancellationToken))
            {
                schemas.Add(reader.GetString(0));
            }

            var settings = new JsonSerializerSettings
            {
                Formatting = Formatting.None,
                NullValueHandling = NullValueHandling.Include
            };
            return new CallToolResponse
            {
                IsError = false,
                Content = [new() { Type = "text", Text = JsonConvert.SerializeObject(schemas, settings) }]
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            //_logger.LogInformation("Error ="+ ex);
            return new CallToolResponse
            {
                IsError = true,
                Content = [new() { Type = "text", Text = ex.Message }]
            };
        }
    }

    
    [McpServerTool(Name = "ado_get_tables"),
     Description("Retrieve and return a list containing information about tables in the specified schema.")]
    public async Task<CallToolResponse> AdoGetTables(
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

            var tableSchema = await conn.GetSchemaAsync("Tables", restrictions, cancellationToken);

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
                Content = [new() { Type = "text", Text = JsonConvert.SerializeObject(tables, settings) }]
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CallToolResponse
            {
                IsError = true,
                Content = [new() { Type = "text", Text = ex.Message }]
            };
        }
    }


    [McpServerTool(Name = "ado_describe_table"),
     Description("Retrieve and return full metadata about a table including columns, primary keys, and foreign keys.")]
    public async Task<CallToolResponse> AdoDescribeTable(
        [Description("Schema name")] string? schema = null,
        [Description("Table name")] string table = "",
        [Description("ADO URL")] string? url = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var conn = GetConnection(url);
            await conn.OpenAsync(cancellationToken);

            var tableInfo = await GetTableInfoAsync(conn, schema, table, cancellationToken);

            var settings = new JsonSerializerSettings
            {
                Formatting = Formatting.None,
                NullValueHandling = NullValueHandling.Include
            };
            return new CallToolResponse
            {
                IsError = false,
                Content = [new Content { Type = "text", Text = JsonConvert.SerializeObject(tableInfo, settings) }]
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CallToolResponse
            {
                IsError = true,
                Content = [new Content { Type = "text", Text = ex.Message }]
            };
        }
    }

    [McpServerTool(Name = "ado_filter_table_names"),
     Description("List tables whose names contain the given substring.")]
    public async Task<CallToolResponse> AdoFilterTableNames(
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
            var dt = await conn.GetSchemaAsync("Tables", restrictions, cancellationToken);

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
                Content = [new Content { Type = "text", Text = JsonConvert.SerializeObject(list, settings) }]
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CallToolResponse
            {
                IsError = true,
                Content = [new Content { Type = "text", Text = ex.Message }]
            };
        }
    }

    [McpServerTool(Name = "ado_execute_query"),
     Description("Execute a SQL query and return results in JSONL format.")]
    public async Task<CallToolResponse> AdoExecuteQuery(
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
                IsError = false, Content = [new Content { Type = "text", Text = text }]
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CallToolResponse
            {
                IsError = true, Content = [new Content { Type = "text", Text = ex.Message }]
            };
        }
    }

    [McpServerTool(Name = "ado_execute_query_md"),
     Description("Execute a SQL query and return results in Markdown table format.")]
    public async Task<CallToolResponse> AdoExecuteQueryMd(
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
                Content = [new Content { Type = "text", Text = md.ToString() }]
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CallToolResponse
            {
                IsError = true,
                Content = [new Content { Type = "text", Text = ex.Message }]
            };
        }
    }


    [McpServerTool(Name = "ado_query_database"),
     Description("Execute a SQL query and return results in JSONL format.")]
    public async Task<CallToolResponse> AdoQueryDatabase(
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
                Content = [new Content { Type = "text", Text = text }]
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CallToolResponse
            {
                IsError = true,
                Content = [new Content { Type = "text", Text = ex.Message }]
            };
        }
    }

    [McpServerTool(Name = "ado_spasql_query"),
     Description("Execute a SPASQL query and return results.")]
    public async Task<CallToolResponse> AdoSpasqlQuery(
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

            cmd.CommandText = $"select Demo.demo.execute_spasql_query(?, ?, ?) as result";
            cmd.Parameters.Add(new VirtuosoParameter{ParameterName="@query", Value=query, DbType=DbType.AnsiString });
            cmd.Parameters.Add(new VirtuosoParameter("@maxrows", maxRows ));
            cmd.Parameters.Add(new VirtuosoParameter("@timeout", timeoutValue ));

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            var text = (await reader.ReadAsync(cancellationToken)) ? reader.GetString(0) : string.Empty;

            return new CallToolResponse
            {
                IsError = false,
                Content = [new Content { Type = "text", Text = text }]
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CallToolResponse
            {
                IsError = true,
                Content = [new Content { Type = "text", Text = ex.Message }]
            };
        }
    }

    [McpServerTool(Name = "ado_virtuoso_support_ai"),
     Description("Interact with Virtuoso Support AI Agent.")]
    public async Task<CallToolResponse> AdoVirtuosoSupportAi(
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
            cmd.Parameters.Add(new VirtuosoParameter("@prompt", prompt ));
            cmd.Parameters.Add(new VirtuosoParameter("@key", key ));

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            var text = (await reader.ReadAsync(cancellationToken)) ? reader.GetString(0) : string.Empty;
            return new CallToolResponse
            {
                IsError = false,
                Content = [new Content { Type = "text", Text = text }]
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CallToolResponse
            {
                IsError = true,
                Content = [new() { Type = "text", Text = ex.Message }]
            };
        }
    }

    [McpServerTool(Name = "ado_sparql_func"),
     Description("Use the SPARQL AI Agent function.")]
    public async Task<CallToolResponse> AdoSparqlFunc(
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
            cmd.Parameters.Add(new VirtuosoParameter("@prompt", prompt));
            cmd.Parameters.Add(new VirtuosoParameter("@key", key));

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            var text = (await reader.ReadAsync(cancellationToken)) ? reader.GetString(0) : string.Empty;
            return new CallToolResponse
            {
                IsError = false,
                Content = [new Content { Type = "text", Text = text }]
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CallToolResponse
            {
                IsError = true,
                Content = [new() { Type = "text", Text = ex.Message }]
            };
        }
    }


    private async Task<CallToolResponse> ExecuteQuery(
        string query, string? graph, int? max_rows = null, string? url = null,
        CancellationToken cancellationToken = default)
    {
        try {
            var maxRows = max_rows ?? 100;
            await using var conn = GetConnection(url);
            await conn.OpenAsync(cancellationToken);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = query;
            if (graph != null)
                cmd.Parameters.Add(new VirtuosoParameter("@graph", graph));
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
                IsError = false, Content = [new Content { Type = "text", Text = text }]
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new CallToolResponse
            {
                IsError = true, Content = [new Content { Type = "text", Text = ex.Message }]
            };
        }
    }

    [McpServerTool(Name = "ado_sparql_list_entity_types"),
     Description(@"This query retrieves all entity types in the RDF graph, along with their labels and comments if available. "
                  + "It filters out blank nodes and ensures that only IRI types are returned. "
                  + "The LIMIT clause is set to 100 to restrict the number of entity types returned. ")]
    public async Task<CallToolResponse> AdoSparqlListEntityTypes(
        [Description("Graph Name")] string? graph = null,
        [Description("ADO URL")] string? url = null,
        CancellationToken cancellationToken = default)
    {
        var graph_clause = graph != null ? "GRAPH `iri(??)`" : "GRAPH ?g";
        var query = $@"SELECT DISTINCT * FROM (
  SPARQL 
  PREFIX owl: <http://www.w3.org/2002/07/owl#>
  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
  SELECT ?o 
  WHERE {{
     {graph_clause} {{
        ?s a ?o .
        
        OPTIONAL {{
            ?s rdfs:label ?label . 
            FILTER (LANG(?label) = ""en"" || LANG(?label) = """")
        }}
        
        OPTIONAL {{
            ?s rdfs:comment ?comment . 
            FILTER (LANG(?comment) = ""en"" || LANG(?comment) = """")
        }}
        
        FILTER (isIRI(?o) && !isBlank(?o))
    }}
  }}
  LIMIT 100
) AS x";
        return await ExecuteQuery(query, graph, 100, url, cancellationToken);
    }


    [McpServerTool(Name = "ado_sparql_list_entity_types_detailed"),
     Description(@"This query retrieves all entity types in the RDF graph, along with their labels and comments if available. "
                 + "It filters out blank nodes and ensures that only IRI types are returned. "
                 + "The LIMIT clause is set to 100 to restrict the number of entity types returned.")]
    public async Task<CallToolResponse> AdoSparqlListEntityTypesDetailed(
        [Description("Graph Name")] string? graph = null,
        [Description("ADO URL")] string? url = null,
        CancellationToken cancellationToken = default)
    {
        var graph_clause = graph != null ? "GRAPH `iri(??)`" : "GRAPH ?g";
        var query = $@"SELECT * FROM (
        SPARQL
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 

        SELECT ?o, (SAMPLE(?label) AS ?label), (SAMPLE(?comment) AS ?comment)
        WHERE {{
            {graph_clause} {{
                ?s a ?o .
                OPTIONAL {{?o rdfs:label ?label . FILTER (LANG(?label) = ""en"" || LANG(?label) = """")}}
                OPTIONAL {{?o rdfs:comment ?comment . FILTER (LANG(?comment) = ""en"" || LANG(?comment) = """")}}
                FILTER (isIRI(?o) && !isBlank(?o))
            }}
        }}
        GROUP BY ?o
        ORDER BY ?o
        LIMIT 20
    ) AS results ";
        return await ExecuteQuery(query, graph, 100, url, cancellationToken);
    }


    [McpServerTool(Name = "ado_sparql_list_entity_types_samples"),
     Description(@"This query retrieves samples of entities for each type in the RDF graph, along with their labels and counts. "
                + "It groups by entity type and orders the results by sample count in descending order. "
                + "Note: The LIMIT clause is set to 20 to restrict the number of entity types returned.")]
    public async Task<CallToolResponse> AdoSparqlListEntityTypesSamples(
        [Description("Graph Name")] string? graph = null,
        [Description("ADO URL")] string? url = null,
        CancellationToken cancellationToken = default)
    {
        var graph_clause = graph != null ? "GRAPH `iri(??)`" : "GRAPH ?g";
        var query = $@"SELECT * FROM (
        SPARQL
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
        SELECT (SAMPLE(?s) AS ?sample), ?slabel, (COUNT(*) AS ?sampleCount), (?o AS ?entityType), ?olabel
        WHERE {{
            {graph_clause} {{
                ?s a ?o .
                OPTIONAL {{?s rdfs:label ?slabel . FILTER (LANG(?slabel) = ""en"" || LANG(?slabel) = """")}}
                FILTER (isIRI(?s) && !isBlank(?s))
                OPTIONAL {{?o rdfs:label ?olabel . FILTER (LANG(?olabel) = ""en"" || LANG(?olabel) = """")}}
                FILTER (isIRI(?o) && !isBlank(?o))
            }}
        }}
        GROUP BY ?slabel ?o ?olabel
        ORDER BY DESC(?sampleCount) ?o ?slabel ?olabel
        LIMIT 20
    ) AS results";
        return await ExecuteQuery(query, graph, 100, url, cancellationToken);
    }


    [McpServerTool(Name = "ado_sparql_list_ontologies"),
     Description("This query retrieves all ontologies in the RDF graph, along with their labels and comments if available.")]
    public async Task<CallToolResponse> AdoSparqlListOntologies(
        [Description("Graph Name")] string? graph = null,
        [Description("ADO URL")] string? url = null,
        CancellationToken cancellationToken = default)
    {
        var graph_clause = graph != null ? "GRAPH `iri(??)`" : "GRAPH ?g";
        var query = $@"SELECT * FROM (
        SPARQL 
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT ?s, ?label, ?comment 
        WHERE {{
            {graph_clause} {{
                ?s a owl:Ontology .
            
                OPTIONAL {{
                    ?s rdfs:label ?label . 
                    FILTER (LANG(?label) = ""en"" || LANG(?label) = """")
                }}
            
                OPTIONAL {{
                    ?s rdfs:comment ?comment . 
                    FILTER (LANG(?comment) = ""en"" || LANG(?comment) = """")
                }}
            
                FILTER (isIRI(?o) && !isBlank(?o))
            }}
        }}
        LIMIT 100
    ) AS x";
        return await ExecuteQuery(query, graph, 100, url, cancellationToken);
    }

    //==========================
    private async Task<Dictionary<string, object?>> GetTableInfoAsync(DbConnection conn, string? schema, string table, CancellationToken cancellationToken = default)
    {
        var columns = new List<Dictionary<string, object?>>();
        var primaryKeys = new List<string>();
        var foreignKeys = new List<Dictionary<string, object?>>();

        var tableSchema = await conn.GetSchemaAsync("Tables", [schema ?? null, null, table], cancellationToken);
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

        var cols = await conn.GetSchemaAsync("Columns", [cat, sch, table, null], cancellationToken);
        foreach (DataRow row in cols.Rows)
        {
            columns.Add(new Dictionary<string, object?>
            {
                ["name"] = row[3].ToString(), //"COLUMN_NAME"
                ["type"] = row[5].ToString(), //"DATA_TYPE"
                ["column_size"] = row[6], //"CHARACTER_MAXIMUM_LENGTH"
                ["num_prec_radix"] = row[9], //"NUMERIC_PRECISION_RADIX"
                ["nullable"] = Convert.ToInt32(row[10]) != 1, 
                ["default"] = row[12] //"COLUMN_DEFAULT"
            });
        }

        var pk = await conn.GetSchemaAsync("PRIMARYKEYS", [cat, sch, table], cancellationToken);
        foreach (DataRow row in pk.Rows)
        {
             primaryKeys.Add(row[3].ToString() ?? ""); //"COLUMN_NAME"
        }

        var fk = await conn.GetSchemaAsync("ForeignKeys", [cat, sch, table], cancellationToken);
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

        foreach (var column in columns)
            column["primary_key"] = primaryKeys.Contains(column["name"]?.ToString()!);

        return new Dictionary<string, object?>
        {
            ["TABLE_CAT"] = conn.Database,
            ["TABLE_SCHEM"] = schema,
            ["TABLE_NAME"] = table,
            ["columns"] = columns,
            ["primary_keys"] = primaryKeys,
            ["foreign_keys"] = foreignKeys
        };
    }



}
