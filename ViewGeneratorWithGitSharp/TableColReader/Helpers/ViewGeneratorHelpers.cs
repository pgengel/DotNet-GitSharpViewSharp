using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace ViewGeneratorWithGitSharp.TableColReader.Helpers
{
  public class ViewGeneratorHelpers
  {
    private readonly SqlConnection _sqlConnection;

    public ViewGeneratorHelpers(SqlConnection sqlConnection)
    {
      _sqlConnection = sqlConnection;
    }

    private List<TableSchemaName> GetTableSchemaName(string top)
    {
      List<TableSchemaName> tableSchemaName = new List<TableSchemaName>();

      SqlCommand sqlCommand = new SqlCommand(
        $"SELECT TOP {top} SCHM.name, TAB.name " +
        $"FROM sys.tables TAB " +
        $"JOIN sys.schemas SCHM " +
        $" ON TAB.schema_id = SCHM.schema_id ", _sqlConnection);

      using (SqlDataReader readTableSchema = sqlCommand.ExecuteReader())
      {
        while (readTableSchema.Read())
        {
          tableSchemaName.Add(new TableSchemaName { Name = readTableSchema[1].ToString(), Schema = readTableSchema[0].ToString() });
        }
      }
      return tableSchemaName;
    }

    private List<TableColSchemaName> GetSchemaTableColName(List<TableSchemaName> tableSchemaNames)
    {
      List<string> columnNames = new List<string>();
      List<TableColSchemaName> tableColSchemaNames = new List<TableColSchemaName>();

      foreach (var tableSchemaName in tableSchemaNames)
      {
        SqlCommand sqlGetTableColNames = new SqlCommand(
          $"SELECT name, column_id " +
          $"FROM sys.columns " +
          $"WHERE OBJECT_ID = OBJECT_ID(\'{tableSchemaName.Schema}.{tableSchemaName.Name}\') " +
          $"ORDER BY column_id;", _sqlConnection);

        using (SqlDataReader readColumnNames = sqlGetTableColNames.ExecuteReader())
        {
          while (readColumnNames.Read())
          {
            columnNames.Add(readColumnNames[0].ToString());
          }
        }
        tableColSchemaNames.Add(new TableColSchemaName { Schema = tableSchemaName.Schema, Name = tableSchemaName.Name, ColumnName = columnNames });
      }
      return tableColSchemaNames;
    }

    private List<string> ViewBuilder(List<string> piiData, List<TableColSchemaName> tableColSchemaNames)
    {
      StringBuilder view = new StringBuilder();

      foreach (var tableColSchemaName in tableColSchemaNames)
      {


        var tableNameWithOutPrefix = tableColSchemaName.Name.Replace("tb_", "");
        view.Append(
          $"--###########Geneerated by the PII Service############## \r\n" +
          $"--Table Schema : {tableColSchemaName.Schema} \r\n" +
          $"--Table Schema : {tableColSchemaName.Schema}" +
          $"--Table Name : {tableColSchemaName.Name}" +
          $"--Date Generated : {DateTime.Now} \r\n" +
          $"--######################################################\r\n" +
          $"SET ANSI_PADDING            ON\r\nSET ANSI_WARNINGS           ON\r\nSET ARITHABORT              ON\r\nSET CONCAT_NULL_YIELDS_NULL ON\r\nSET ANSI_NULLS              ON\r\nSET QUOTED_IDENTIFIER       ON\r\nSET NUMERIC_ROUNDABORT      OFF\r\nGO\r\n\r\nSET NOCOUNT ON\r\nGO\r\n\r\n--drop view if already exists\r\nIF EXISTS(SELECT 1 FROM sys.views WHERE object_id = OBJECT_ID(\'PII.vw_{tableColSchemaName.Schema}_{tableNameWithOutPrefix}\'))\r\n  DROP VIEW PII.vw_{tableColSchemaName.Schema}_{tableNameWithOutPrefix};\r\nGO\r\n" +
          $"CREATE VIEW PII.vw_{tableColSchemaName.Schema}_{tableNameWithOutPrefix}\r\nAS\r\nSELECT "
        );

        foreach (var column in tableColSchemaName.ColumnName)
        {
          view.Append(piiData.Contains(column.ToUpper()) ? $"'*****' AS {column}" : $"{column} AS {column}");
          if (!column.Equals(tableColSchemaName.ColumnName.Last()))
          {
            view.Append($",\r\n");
          }
        }

        view.Append(
          $"\r\nFROM {tableColSchemaName.Schema}.{tableColSchemaName.Name}" +
          $" " +
          $"\r\nGO\r\n\r\nEXEC sp_Version @dboObject = \'PII.vw_{tableColSchemaName.Schema}_{tableNameWithOutPrefix}.VIW\', @Version = 0, @ExpectedHash = \'\'\r\nGO\r\n"
        );
      }

    }

    public void GetView()
    {

      var d = GetTableSchemaName("10");
      var r = GetSchemaTableColName(d);
      ViewBuilder(new List<string>{"", ""}, r);
    }
  }
}
