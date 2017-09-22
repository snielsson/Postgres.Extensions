// Copyright (c) 2017 Stig Schmidt Nielsson. 
// Published under the DO WHAT THE F*CK YOU WANT TO PUBLIC LICENSE TERMS AND CONDITIONS.
// 0. You just DO WHAT THE F*CK YOU WANT TO.
// 1. You do not hold ANYBODY but YOURSELF liable for anything that happens or goes wrong with your use of the work.
using System;
using System.Linq;
using Npgsql;
using Shouldly;
using Xunit;
using Xunit.Abstractions;
namespace PostgresExtensions.Tests {
  public class PostgresServiceTests {
    public PostgresServiceTests(ITestOutputHelper log) {
      _log = log;
    }
    private readonly ITestOutputHelper _log;
    private NpgsqlConnection CreateAdminConnection() {
      return new NpgsqlConnectionStringBuilder {
        Database = PostgresTestSettings.AdminDatabase,
        Host = "localhost",
        Port = 5432,
        Username = PostgresTestSettings.AdminUsername,
        Password = PostgresTestSettings.AdminPassword
      }.OpenPostgres();
    }
    private NpgsqlConnection CreateConnection(string databaseName) {
      return CreateConnectionStringBuilder(databaseName).OpenPostgres();
    }
    private NpgsqlConnectionStringBuilder CreateConnectionStringBuilder(string databaseName){
      return new NpgsqlConnectionStringBuilder {
      Database = databaseName,
      Host = "localhost",
      Port = 5432,
      Username = databaseName,
      Password = databaseName
    };
  }

    private void CreateEmptyDatabase(NpgsqlConnection adminConnection, string name) {
      adminConnection.DropDatabaseIfExists(name);
      adminConnection.DropRoleIfExists(name);
      adminConnection.CreateRole(name, name);
      adminConnection.CreateDatabase(name, name);
    }

    [Fact]
    public void MigrationsWorks() {
      var name = "t1".ToLower();
      PostgresMigrations.MigrationsDir = "TestData/TestMigrations/Failing";
      using (var adminConnection = CreateAdminConnection()) {
        CreateEmptyDatabase(adminConnection, name);
        var csb = CreateConnectionStringBuilder(name);
        using (var connection = CreateConnection(name)) {
          csb.CreateMigrationsTable();

          csb.MigrateTo("migration_2", DateTime.UtcNow).DatabaseMigrationState.ShouldBe("migration_2");
          csb.GetCurrentMigration().DatabaseMigrationState.ShouldBe("migration_2");
          connection.TableExists("table1").ShouldBe(true);
          connection.TableExists("table2").ShouldBe(true);
          connection.TableExists("table3").ShouldBe(false);
          var rows = connection.ExecuteQuery($"select * from {MigrationRow.MigrationsTableName}", MigrationRow.MapAll).ToArray();
          rows.Length.ShouldBe(1);
          rows[0].DatabaseMigrationState.ShouldBe("migration_2");

          csb.MigrateTo("migration_1", DateTime.UtcNow).DatabaseMigrationState.ShouldBe("migration_1");
          csb.GetCurrentMigration().DatabaseMigrationState.ShouldBe("migration_1");
          connection.TableExists("table1").ShouldBe(true);
          connection.TableExists("table2").ShouldBe(false);
          connection.TableExists("table3").ShouldBe(false);
          rows = connection.ExecuteQuery($"select * from {MigrationRow.MigrationsTableName}", MigrationRow.MapAll).ToArray();
          rows.Length.ShouldBe(2);
          rows[1].DatabaseMigrationState.ShouldBe("migration_1");

          csb.MigrateTo("migration_3", DateTime.UtcNow).DatabaseMigrationState.ShouldBe("migration_3");
          csb.GetCurrentMigration().DatabaseMigrationState.ShouldBe("migration_3");
          connection.TableExists("table1").ShouldBe(true);
          connection.TableExists("table2").ShouldBe(true);
          connection.TableExists("table3").ShouldBe(true);
          rows = connection.ExecuteQuery($"select * from {MigrationRow.MigrationsTableName}", MigrationRow.MapAll).ToArray();
          rows.Length.ShouldBe(3);
          rows[2].DatabaseMigrationState.ShouldBe("migration_3");

          csb.MigrateTo("migration_3", DateTime.UtcNow).ShouldBeNull("Expected no migrations to run, because already at migration_3");
          var ex = Should.Throw<Exception>(() => csb.MigrateTo("migration_4_which_does_not_exist", DateTime.UtcNow));
          ex.Message.ShouldBe("Target up migration 'migration_4_which_does_not_exist' not found.");

          connection.TableExists("table4").ShouldBe(false);
          var postgresException = Should.Throw<PostgresException>(() => csb.MigrateTo("migration_4-failing", DateTime.UtcNow));
          _log.WriteLine(postgresException.ToString());
          postgresException.Message.ShouldContain("42P07: relation \"table3\" already exists");
          connection.TableExists("table4").ShouldBe(false, "No table4 because migration failed.");
          rows = connection.ExecuteQuery($"select * from {MigrationRow.MigrationsTableName}", MigrationRow.MapAll).ToArray();
          rows.Length.ShouldBe(3);
          rows[2].DatabaseMigrationState.ShouldBe("migration_3", "Should still be at migration state migration_3");
        }
      }
    }

    [Fact]
    public void CanCreateAndDeleteDatabase() {
      var name = "t2".ToLower();
      using (var adminConnection = CreateAdminConnection()) {
        adminConnection.DropDatabaseIfExists(name);
        adminConnection.DropRoleIfExists(name);
        adminConnection.DatabaseExists(name).ShouldBe(false);
        adminConnection.RoleExists(name).ShouldBe(false);
        adminConnection.CreateRole(name, name);
        adminConnection.CreateDatabase(name, name);
        adminConnection.DatabaseExists(name).ShouldBe(true);
        adminConnection.RoleExists(name).ShouldBe(true);
        try {
          adminConnection.CreateRole(name, name);
        }
        catch (PostgresException ex) {
          ex.Message.ShouldBe($@"42710: role ""{name}"" already exists");
        }
        try {
          adminConnection.CreateDatabase(name, name);
        }
        catch (PostgresException ex) {
          ex.Message.ShouldBe($@"42P04: database ""{name}"" already exists");
        }
        adminConnection.DropDatabaseIfExists(name);
        adminConnection.DropRoleIfExists(name);
        adminConnection.DatabaseExists(name).ShouldBe(false);
        adminConnection.RoleExists(name).ShouldBe(false);
      }
    }

    [Fact]
    public void CannotCreateTwoMigrationsTables() {
      var name = "t3".ToLower();
      using (var adminConnection = CreateAdminConnection()) {
        CreateEmptyDatabase(adminConnection, name);
        var csb = CreateConnectionStringBuilder(name);
        csb.CreateMigrationsTable();
        var ex = Should.Throw<PostgresException>(() => csb.CreateMigrationsTable());
        ex.Message.ShouldBe($"42P07: relation \"{MigrationRow.MigrationsTableName}\" already exists");
      }
    }

    [Fact]
    public void CanRunMigrationsToLatest() {
      var name = "t4".ToLower();
      using (var adminConnection = CreateAdminConnection()) {
        PostgresMigrations.MigrationsDir = "TestData/TestMigrations/Working";
        CreateEmptyDatabase(adminConnection, name);
        var csb = CreateConnectionStringBuilder(name);
        using (var connection = CreateConnection(name)) {
          csb.CreateMigrationsTable();
          csb.GetCurrentMigration().ShouldBeNull();
          csb.MigrateToLatest(DateTime.UtcNow).ShouldNotBeNull();
          csb.GetCurrentMigration().DatabaseMigrationState.ShouldBe("migration_3");
          csb.MigrateToLatest(DateTime.UtcNow).ShouldBeNull();
          csb.GetCurrentMigration().DatabaseMigrationState.ShouldBe("migration_3");
          var rows = connection.ExecuteQuery($"select * from {MigrationRow.MigrationsTableName}", MigrationRow.MapAll).ToArray();
          rows.Length.ShouldBe(1);
          rows[0].DatabaseMigrationState.ShouldBe("migration_3");
        }
      }
    }

    [Fact]
    public void GetMigrationNameFromFilepathWorks() {
      PostgresMigrations.GetMigrationName("xyz").ShouldBe("xyz");
      PostgresMigrations.GetMigrationName(".xyz").ShouldBe("");
      PostgresMigrations.GetMigrationName(".x.y.z").ShouldBe("");
      PostgresMigrations.GetMigrationName("x.y.z").ShouldBe("x");
      PostgresMigrations.GetMigrationName("c:\\somedir\\x.y.z").ShouldBe("x");
    }
  }
}