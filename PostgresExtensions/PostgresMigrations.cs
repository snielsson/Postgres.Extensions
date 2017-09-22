// Copyright (c) 2017 Stig Schmidt Nielsson. 
// Published under the DO WHAT THE F*CK YOU WANT TO PUBLIC LICENSE TERMS AND CONDITIONS.
// 0. You just DO WHAT THE F*CK YOU WANT TO.
// 1. You do not hold ANYBODY but YOURSELF liable for anything that happens or goes wrong with your use of the work.
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Npgsql;
namespace PostgresExtensions {
  public static class PostgresMigrations {
    public static string UpExtension { get; set; } = "up.sql";
    public static string DownExtension { get; set; } = "down.sql";
    public static string MigrationsDir { get; set; } = "migrations";

    public static int CreateMigrationsTable(this NpgsqlConnectionStringBuilder @this) {
      using (var connection = @this.OpenPostgres()) {
        var result = connection.ExecuteNonQuery(MigrationRow.CreateTableSql.Value);
        return result;
      }
    }

    public static MigrationRow GetCurrentMigration(this NpgsqlConnectionStringBuilder @this) {
      using (var connection = @this.OpenPostgres()) {
        return connection.ExecuteQuerySingleOrDefault($"SELECT * FROM {MigrationRow.MigrationsTableName} ORDER BY id DESC LIMIT 1;", MigrationRow.MapAll);
      }
    }

    public static MigrationRow MigrateTo(this NpgsqlConnectionStringBuilder @this, string targetMigration, DateTime now) {
      var currentMigration = GetCurrentMigration(@this);
      if (currentMigration == null || string.Compare(currentMigration.DatabaseMigrationState, targetMigration, StringComparison.OrdinalIgnoreCase) < 0) {
        return MigrateUp(@this, currentMigration?.DatabaseMigrationState, targetMigration, now);
      }
      if (string.Compare(currentMigration.DatabaseMigrationState, targetMigration, StringComparison.OrdinalIgnoreCase) > 0) {
        return MigrateDown(@this, currentMigration.DatabaseMigrationState, targetMigration, now);
      }
      return null;
    }

    public static MigrationRow MigrateToLatest(this NpgsqlConnectionStringBuilder @this, DateTime? now) {
      var currentMigrationState = GetCurrentMigration(@this)?.DatabaseMigrationState;
      var lastUpMigrationFile = GetUpMigrationFiles().Last();
      var latestMigrationState = GetMigrationName(lastUpMigrationFile);
      if (currentMigrationState != null && string.Compare(latestMigrationState, currentMigrationState, StringComparison.OrdinalIgnoreCase) >= 0) return null;
      return MigrateUp(@this, currentMigrationState, latestMigrationState, now ?? DateTime.UtcNow);
    }

    private static MigrationRow MigrateDown(this NpgsqlConnectionStringBuilder @this, string from, string to, DateTime now) {
      var allFiles = GetFiles(MigrationsDir, "*." + DownExtension.TrimStart('.')).OrderBy(x => x).ToArray();
      var files = new List<string>();
      string resultMigrationName = null;
      for (var i = allFiles.Length - 1; i >= 0; i--) {
        var name = GetMigrationName(allFiles[i]);
        if (string.Compare(name, from, StringComparison.OrdinalIgnoreCase) > 0) continue;
        if (string.Compare(name, to, StringComparison.OrdinalIgnoreCase) < 0) break;
        resultMigrationName = i >= 1 ? GetMigrationName(allFiles[i - 1]) : null;
        files.Add(allFiles[i]);
        if (resultMigrationName == null || resultMigrationName == to) break;
      }
      if (files.Count == 0) return null;
      if (resultMigrationName !=null && resultMigrationName != to) throw new Exception($"Target down migration '{to}' not found.");
      return RunMigration(@this, files.ToArray(), MigrationDirection.Down, resultMigrationName, now);
    }

    private static MigrationRow MigrateUp(this NpgsqlConnectionStringBuilder @this, string from, string to, DateTime now) {
      var files = new List<string>();
      foreach (var file in GetUpMigrationFiles()) {
        var name = GetMigrationName(file);
        if (from != null && string.Compare(name, from, StringComparison.OrdinalIgnoreCase) <= 0) continue;
        if (string.Compare(name, to, StringComparison.OrdinalIgnoreCase) > 0) break;
        files.Add(file);
      }
      if (files.Count == 0) return null;
      if (GetMigrationName(files.Last()) != to) throw new Exception($"Target up migration '{to}' not found.");  
      return RunMigration(@this, files.ToArray(), MigrationDirection.Up, GetMigrationName(files.Last()), now);
    }

    public static MigrationRow RunMigration(this NpgsqlConnectionStringBuilder @this, string[] files, MigrationDirection direction, string resultMigrationState, DateTime now) {
      var migrationSql = CreateMigrationSql(files, now);
      var rollbackSql = CreateRollbackSql(files, now);
      PostgresServer.Backup(@this);
      using (var connection = @this.OpenPostgres()) {
        connection.ExecuteNonQuery(migrationSql);
      }
      var schemaFile = PostgresServer.DumpSchema(@this, @this.Password);
      var schema = File.ReadAllText(schemaFile);
        var migration = new MigrationRow {
        Created = now,
        Direction = direction.ToString(),
        DatabaseMigrationState = resultMigrationState,
        Sql = migrationSql,
        RollbackSql = rollbackSql,
        Schema = schema
      };
      using (var connection = @this.OpenPostgres()) migration.Insert(connection).Wait();
      File.Delete(schemaFile);
      return migration;
    }

    private static IEnumerable<string> GetFiles(string dir, string pattern) {
      return Directory.EnumerateFiles(dir, pattern, SearchOption.TopDirectoryOnly).ToArray();
    }

    private static IEnumerable<string> GetUpMigrationFiles() {
      return GetFiles(MigrationsDir, "*." + UpExtension.TrimStart('.'))
        .OrderBy(x => x)
        .ToArray();
    }

    private static string CreateMigrationSql(string[] filePaths, DateTime creationTime) {
      var sql = new StringBuilder();
      sql
        .Append($"-- Running {filePaths.Length} migration file(s):\n--  ")
        .AppendLine(string.Join("\n--  ", filePaths.Select(Path.GetFileName)))
        .AppendLine("START TRANSACTION;");
      for (var i = 0; i < filePaths.Length; i++) {
        var filePath = filePaths[i];
        sql
          .AppendLine("---------------------------------------------------------")
          .AppendLine($"-- Migration file {i + 1} of {filePaths.Length}: " + Path.GetFileName(filePath))
          .AppendLine("---------------------------------------------------------")
          .AppendLine(File.ReadAllText(filePath));
      }
      sql.AppendLine("COMMIT TRANSACTION;");
      return sql.ToString();
    }

    private static string CreateRollbackSql(string[] filePaths, DateTime creationTime) {
      var sql = new StringBuilder();
      sql
        .Append($"-- Running {filePaths.Length} rollback file(s):\n--  ")
        .AppendLine(string.Join("\n--  ", filePaths))
        .AppendLine("START TRANSACTION;");
      for (var i = filePaths.Length - 1; i >= 0; i--) {
        var filePath = filePaths[i];
        sql
          .AppendLine("---------------------------------------------------------")
          .AppendLine($"-- Rollback file {i + 1} of {filePaths.Length}: " + filePath)
          .AppendLine("---------------------------------------------------------")
          .AppendLine(File.ReadAllText(filePath));
      }
      sql.AppendLine("COMMIT TRANSACTION;");
      return sql.ToString();
    }

    private static string GetRollbackMigrationFilePath(string migrationFilePath, MigrationDirection direction, string upExtension, string downExtension) {
      if (direction == MigrationDirection.None) throw new ArgumentException("direction is None.");
      var dir = Path.GetDirectoryName(migrationFilePath);
      var name = GetMigrationName(migrationFilePath);
      var extension = direction == MigrationDirection.Down ? upExtension : downExtension;
      var result = Path.Combine(dir, name + "." + extension.TrimStart('.'));
      return result;
    }

    public static string GetMigrationName(string filepath) {
      var filename = Path.GetFileName(filepath);
      var indexOfFirstDot = filename.IndexOf('.');
      if (indexOfFirstDot == -1) return filepath;
      var result = filename.Substring(0, indexOfFirstDot);
      return result;
    }
  }
}