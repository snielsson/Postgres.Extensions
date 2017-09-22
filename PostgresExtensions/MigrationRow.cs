// Copyright (c) 2017 Stig Schmidt Nielsson. 
// Published under the DO WHAT THE F*CK YOU WANT TO PUBLIC LICENSE TERMS AND CONDITIONS.
// 0. You just DO WHAT THE F*CK YOU WANT TO.
// 1. You do not hold ANYBODY but YOURSELF liable for anything that happens or goes wrong with your use of the work.
using System;
using System.Data;
using System.Threading.Tasks;
using Npgsql;
namespace PostgresExtensions {
  public class MigrationRow {
    private static string _migrationsTableName;
    public static readonly Lazy<string> CreateTableSql = new Lazy<string>(() => $@"
      CREATE TABLE {MigrationsTableName} (
          Id bigserial primary key not null,
          Direction text not null,
          Created timestamp not null,
          DatabaseMigrationState text not null,
          Schema text not null,
          Sql text not null,
          RollbackSql text not null
      );");

    public static readonly Lazy<string> InsertSql = new Lazy<string>(() => $@"
      INSERT INTO {MigrationsTableName} (Direction, Created, DatabaseMigrationState, Schema, Sql, RollbackSql)
      VALUES(@Direction, @Created, @DatabaseMigrationState, @Schema, @Sql, @Rollbacksql) RETURNING Id");
    public long Id { get; set; }
    public string Direction { get; set; }
    public DateTime Created { get; set; }
    public string DatabaseMigrationState { get; set; }
    public string Schema { get; set; }
    public string Description { get; set; }
    public string Sql { get; set; }
    public string RollbackSql { get; set; }
    public static string MigrationsTableName {
      get => _migrationsTableName ?? (_migrationsTableName = "migrations");
      set {
        if (_migrationsTableName != null)
          throw new NpgsqlException($"PostgresMigrations table name already set to {_migrationsTableName}, either explicitly or implicity when calling the getter the first time.");
        _migrationsTableName = value;
      }
    }

    public async Task<long> Insert(NpgsqlConnection connection) {
      return Id = await connection.ExecuteNonQueryAsync(InsertSql.Value, x => {
        x.AddWithValue("@Direction", Direction);
        x.AddWithValue("@Created", Created);
        x.AddWithValue("@DatabaseMigrationState", DatabaseMigrationState);
        x.AddWithValue("@Schema", Schema); //TODO: get current schema dumped into here.
        x.AddWithValue("@Sql", Sql);
        x.AddWithValue("@RollbackSql", RollbackSql);
      }).ConfigureAwait(false);
    }

    public override string ToString() {
      return DatabaseMigrationState + ", created " + Created.ToString("s");
    }

    public static MigrationRow MapAll(IDataReader rdr) {
      return new MigrationRow {
        Id = rdr.GetInt64(0),
        Direction = rdr.GetString(1),
        Created = rdr.GetDateTime(2),
        DatabaseMigrationState = rdr.GetString(3),
        Schema = rdr.GetString(4),
        Sql = rdr.GetString(5),
        RollbackSql = rdr.GetString(6)
      };
    }
  }
}