// Copyright (c) 2017 Stig Schmidt Nielsson. 
// Published under the DO WHAT THE F*CK YOU WANT TO PUBLIC LICENSE TERMS AND CONDITIONS.
// 0. You just DO WHAT THE F*CK YOU WANT TO.
// 1. You do not hold ANYBODY but YOURSELF liable for anything that happens or goes wrong with your use of the work.
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
namespace PostgresExtensions {
  public static class PostgresExtensions {
    private static string _defaultAdminDatabase = "postgres";
    private static string _defaultAdminUser = "postgres";
    private static string _defaultAdminPassword;
    private static string _defaultDatabase;
    private static string _defaultUser;
    private static string _defaultPassword;
    public static Regex ValidPostgresIdentifierRegex { get; set; } = new Regex(@"^[_a-z][_a-z0-9]*$", RegexOptions.Compiled);

    public static string DefaultAdminDatabase {
      get => _defaultAdminDatabase;
      set => _defaultAdminDatabase = value.AssertIsSafePostgresIdentifier();
    }

    public static string DefaultAdminUser {
      get => _defaultAdminUser;
      set => _defaultAdminUser = value.AssertIsSafePostgresIdentifier();
    }

    public static string DefaultAdminPassword {
      get => _defaultAdminPassword;
      set => _defaultAdminPassword = value.AssertIsSafePostgresIdentifier();
    }

    public static string DefaultDatabase {
      get => _defaultDatabase;
      set => _defaultDatabase = value.AssertIsSafePostgresIdentifier();
    }

    public static string DefaultUser {
      get => _defaultUser;
      set => _defaultUser = value.AssertIsSafePostgresIdentifier();
    }

    public static string DefaultPassword {
      get => _defaultPassword;
      set => _defaultPassword = value.AssertIsSafePostgresIdentifier();
    }

    public static string AssertIsSafePostgresIdentifier(this string @this) {
      if (!ValidPostgresIdentifierRegex.IsMatch(@this))
        throw new ArgumentException($"'{@this}' must match the regular expression {ValidPostgresIdentifierRegex}");
      return @this;
    }

    public static Task<NpgsqlConnection> OpenPostgresAsync(this NpgsqlConnectionStringBuilder @this, CancellationToken cancellationToken = default(CancellationToken)) {
      return @this.ConnectionString.OpenPostgresAsync(cancellationToken);
    }

    public static NpgsqlConnection OpenPostgres(this NpgsqlConnectionStringBuilder @this) {
      return @this.ConnectionString.OpenPostgres();
    }

    public static async Task<NpgsqlConnection> OpenPostgresAsync(this string @this,
      CancellationToken cancellationToken = default(CancellationToken)) {
      var connection = new NpgsqlConnection(@this);
      await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
      return connection;
    }

    public static NpgsqlConnection OpenPostgres(this string @this) {
      var connection = new NpgsqlConnection(@this);
      connection.Open();
      return connection;
    }

    public static async Task<bool> TableExistsAsync(this NpgsqlConnection @this, string tableName) {
      tableName = tableName.AssertIsSafePostgresIdentifier();
      var sql = $"SELECT 1 from pg_class WHERE relname='{tableName}';";
      return await (await @this.ExecuteReaderAsync(sql).ConfigureAwait(false)).ReadAsync().ConfigureAwait(false);
    }

    public static DbDataReader ExecuteReader(this NpgsqlConnection @this, string sql,
      Action<NpgsqlParameterCollection> addParameters = null) {
      using (var cmd = new NpgsqlCommand()) {
        cmd.Connection = @this;
        cmd.CommandText = sql;
        addParameters?.Invoke(cmd.Parameters);
        return cmd.ExecuteReader();
      }
    }

    public static async Task<DbDataReader> ExecuteReaderAsync(this NpgsqlConnection @this, string sql,
      Action<NpgsqlParameterCollection> addParameters = null,
      CancellationToken cancellationToken = default(CancellationToken)) {
      using (var cmd = new NpgsqlCommand()) {
        cmd.Connection = @this;
        cmd.CommandText = sql;
        addParameters?.Invoke(cmd.Parameters);
        return await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
      }
    }

    public static object ExecuteScalar(this NpgsqlConnection @this, string sql, Action<NpgsqlParameterCollection> addParameters = null) {
      using (var cmd = new NpgsqlCommand()) {
        cmd.Connection = @this;
        cmd.CommandText = sql;
        addParameters?.Invoke(cmd.Parameters);
        return cmd.ExecuteScalar();
      }
    }

    public static async Task<object> ExecuteScalarAsync(this NpgsqlConnection @this, string sql,
      Action<NpgsqlParameterCollection> addParameters = null,
      CancellationToken cancellationToken = default(CancellationToken)) {
      using (var cmd = new NpgsqlCommand()) {
        cmd.Connection = @this;
        cmd.CommandText = sql;
        addParameters?.Invoke(cmd.Parameters);
        return await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
      }
    }

    public static int ExecuteNonQuery(this NpgsqlConnection @this, string sql, Action<NpgsqlParameterCollection> addParameters = null) {
      using (var cmd = new NpgsqlCommand()) {
        cmd.Connection = @this;
        cmd.CommandText = sql;
        addParameters?.Invoke(cmd.Parameters);
        try {
          return cmd.ExecuteNonQuery();
        }
        catch (PostgresException) {
          using (var rollback = new NpgsqlCommand("ROLLBACK;", @this)) rollback.ExecuteNonQuery();
          throw;
        }
      }
    }

    public static async Task<int> ExecuteNonQueryAsync(this NpgsqlConnection @this, string sql,
      Action<NpgsqlParameterCollection> addParameters = null,
      CancellationToken cancellationToken = default(CancellationToken)) {
      using (var cmd = new NpgsqlCommand()) {
        cmd.Connection = @this;
        cmd.CommandText = sql;
        addParameters?.Invoke(cmd.Parameters);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
      }
    }

    public static IEnumerable<T> ExecuteQuery<T>(this NpgsqlConnection @this, string sql, Func<DbDataReader, T> mapper,
      Action<NpgsqlParameterCollection> addParameters = null) {
      using (var rdr = @this.ExecuteReader(sql, addParameters)) {
        while (rdr.Read()) yield return mapper(rdr);
      }
    }

    public static async Task<List<T>> ExecuteQueryAsync<T>(this NpgsqlConnection @this, string sql, Func<DbDataReader, T> mapper,
      Action<NpgsqlParameterCollection> addParameters = null,
      CancellationToken cancellationToken = default(CancellationToken)) {
      using (var rdr = await @this.ExecuteReaderAsync(sql, addParameters, cancellationToken).ConfigureAwait(false)) {
        var results = new List<T>();
        while (await rdr.ReadAsync(cancellationToken).ConfigureAwait(false)) results.Add(mapper(rdr));
        return results;
      }
    }

    public static T ExecuteQuerySingle<T>(this NpgsqlConnection @this, string sql, Func<DbDataReader, T> mapper,
      Action<NpgsqlParameterCollection> addParameters = null) {
      using (var rdr = @this.ExecuteReader(sql, addParameters)) {
        if (!rdr.Read()) throw new NpgsqlException("Reader returned no result.");
        return mapper(rdr);
      }
    }

    public static async Task<T> ExecuteQuerySingleAsync<T>(this NpgsqlConnection @this, string sql, Func<DbDataReader, T> mapper,
      Action<NpgsqlParameterCollection> addParameters = null,
      CancellationToken cancellationToken = default(CancellationToken)) {
      using (var rdr = await @this.ExecuteReaderAsync(sql, addParameters, cancellationToken).ConfigureAwait(false)) {
        if (!await rdr.ReadAsync(cancellationToken).ConfigureAwait(false)) throw new NpgsqlException("Reader returned no result.");
        return mapper(rdr);
      }
    }

    public static T ExecuteQuerySingleOrDefault<T>(this NpgsqlConnection @this, string sql, Func<DbDataReader, T> mapper,
      Action<NpgsqlParameterCollection> addParameters = null) {
      using (var rdr = @this.ExecuteReader(sql, addParameters)) {
        if (!rdr.Read()) return default(T);
        return mapper(rdr);
      }
    }

    public static async Task<T> ExecuteQuerySingleOrDefaultAsync<T>(this NpgsqlConnection @this, string sql, Func<DbDataReader, T> mapper,
      Action<NpgsqlParameterCollection> addParameters = null,
      CancellationToken cancellationToken = default(CancellationToken)) {
      using (var rdr = await @this.ExecuteReaderAsync(sql, addParameters, cancellationToken)) {
        if (!await rdr.ReadAsync(cancellationToken)) return default(T);
        return mapper(rdr);
      }
    }

    //NEXT: test parameterized instead of dynamic sql:
    public static int CreateRole(this NpgsqlConnection @this, string databasePassword, string databaseRole) {
      AssertIsSafePostgresIdentifier(databaseRole);
      var sql = $@"CREATE ROLE {databaseRole} LOGIN PASSWORD '{databasePassword}'";
      return ExecuteNonQuery(@this, sql);
    }

    public static int CreateDatabase(this NpgsqlConnection @this, string databaseName, string ownerDatabaseRole) {
      AssertIsSafePostgresIdentifier(databaseName);
      AssertIsSafePostgresIdentifier(ownerDatabaseRole);
      var sql = $@"CREATE DATABASE {databaseName} OWNER {ownerDatabaseRole};";
      return ExecuteNonQuery(@this, sql);
    }

    public static bool DatabaseExists(this NpgsqlConnection @this, string databaseName) {
      AssertIsSafePostgresIdentifier(databaseName);
      var sql = $"SELECT 1 from pg_database WHERE datname='{databaseName}';";
      using (var rdr = @this.ExecuteReader(sql)) {
        return rdr.Read();
      }
    }

    public static bool RoleExists(this NpgsqlConnection @this, string databaseRole) {
      AssertIsSafePostgresIdentifier(databaseRole);
      var sql = $"SELECT 1 FROM pg_roles WHERE rolname = '{databaseRole}';";
      using (var rdr = @this.ExecuteReader(sql))
      {
        return rdr.Read();
      }
    }

    public static bool TableExists(this NpgsqlConnection @this, string tableName) {
      AssertIsSafePostgresIdentifier(tableName);
      var sql = $"SELECT 1 from pg_class WHERE relname='{tableName}';";
      using (var rdr = @this.ExecuteReader(sql))
      {
        return rdr.Read();
      }
    }

    public static void DisconnectAllClientsFromDatabase(this NpgsqlConnection @this, string databaseName) {
      AssertIsSafePostgresIdentifier(databaseName);
      var sql = $@" 
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = '{databaseName}'
                AND pid <> pg_backend_pid();";
      @this.ExecuteNonQuery(sql);
    }

    public static void DropDatabaseIfExists(this NpgsqlConnection @this, string databaseName) {
      databaseName = databaseName.AssertIsSafePostgresIdentifier();
      @this.DisconnectAllClientsFromDatabase(databaseName);
      var sql = $@"DROP DATABASE IF EXISTS {databaseName};";
      @this.ExecuteNonQuery(sql);
    }

    public static void DropRoleIfExists(this NpgsqlConnection @this, string databaseRole) {
      databaseRole = databaseRole.AssertIsSafePostgresIdentifier();
      var sql = $@"DROP ROLE IF EXISTS {databaseRole};";
      @this.ExecuteNonQuery(sql);
    }

    public static void ExecuteQuery<T>(this NpgsqlConnection @this, string @select) {
      throw new NotImplementedException();
    }
  }
}