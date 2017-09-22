// Copyright (c) 2017 Stig Schmidt Nielsson. 
// Published under the DO WHAT THE F*CK YOU WANT TO PUBLIC LICENSE TERMS AND CONDITIONS.
// 0. You just DO WHAT THE F*CK YOU WANT TO.
// 1. You do not hold ANYBODY but YOURSELF liable for anything that happens or goes wrong with your use of the work.
using System;
using System.Diagnostics;
using System.IO;
using Npgsql;
namespace PostgresExtensions {
  public static class PostgresServer {
    public static string PostgresBinDir = "C:\\Program Files\\PostgreSQL\\9.6\\bin";
    public static string BackupDir = "backup";
    public static Process Psql(string args, string password) {
      var processStartInfo = new ProcessStartInfo {
        UseShellExecute = false,
        CreateNoWindow = true,
        FileName = Path.Combine(PostgresBinDir, "psql.exe"),
        RedirectStandardError = true,
        RedirectStandardOutput = true,
        RedirectStandardInput = true,
        Arguments = args
      };
      processStartInfo.Environment["PGPASSWORD"] = password;
      return Process.Start(processStartInfo);
    }

    public static Process PgDump(string args, string password) {
      var processStartInfo = new ProcessStartInfo {
        UseShellExecute = false,
        CreateNoWindow = true,
        FileName = Path.Combine(PostgresBinDir, "pg_dump.exe"),
        RedirectStandardError = true,
        RedirectStandardOutput = true,
        RedirectStandardInput = true,
        Arguments = args
      };
      processStartInfo.Environment["PGPASSWORD"] = password;
      return Process.Start(processStartInfo);
    }

    private static Process PgRestore(string args, string password) {
      var processStartInfo = new ProcessStartInfo {
        UseShellExecute = false,
        CreateNoWindow = true,
        FileName = Path.Combine(PostgresBinDir, "pg_restore.exe"),
        RedirectStandardError = true,
        RedirectStandardOutput = true,
        RedirectStandardInput = true,
        Arguments = args
      };
      processStartInfo.Environment["PGPASSWORD"] = password;
      return Process.Start(processStartInfo);
    }

    public static string DumpSchema(NpgsqlConnectionStringBuilder connection, string outputdir = null, string outputfile = null, string extraArgs = "") {
      outputfile = outputfile ?? connection.Database + "_" + DateTime.Now.ToString("yyyyMMdd-HHmmss-fff") + ".schema.pg_dump";
      return Backup(connection, outputdir, outputfile, extraArgs + " -s");
    }

    public static string Backup(NpgsqlConnectionStringBuilder connection, string outputdir = null, string outputfile = null, string extraArgs = ""){ //} "--format=custom") {
      outputdir = outputdir ?? BackupDir;
      Directory.CreateDirectory(outputdir);
      outputfile = outputfile ?? connection.Database + "_" + DateTime.Now.ToString("yyyyMMdd-HHmmss-fff") + ".pg_dump";
      var filePath = Path.Combine(outputdir, outputfile);
      var args = $"--dbname={connection.Database} --host={connection.Host} --port={connection.Port} --username={connection.Username} --no-password {extraArgs} --file={filePath}";
      var process = PgDump(args, connection.Password);
      process.WaitForExit();
      if (process.ExitCode != 0) throw new InvalidOperationException($"Backup failed executing:\n{process.StartInfo.FileName} {process.StartInfo.Arguments}\npgdump exitcode={process.ExitCode}\n{process.StandardOutput.ReadToEnd()}\n{process.StandardError.ReadToEnd()}");
      return filePath;
    }

    public static bool Restore(string connectionString, string backupFile) {
      return Restore(new NpgsqlConnectionStringBuilder(connectionString), backupFile);
    }
    public static bool Restore(NpgsqlConnectionStringBuilder connection, string backupFile) {
      var args = $"-h{connection.Host} -p{connection.Port} -U{connection.Username} --no-password --dbname={connection.Database} --create {backupFile}";
      var process = PgRestore(args, connection.Password);
      process.WaitForExit();
      if (process.ExitCode != 0) throw new InvalidOperationException("Restore failed: pg_restore exitcode=" + process.ExitCode + "\n" + process.StandardOutput.ReadToEnd() + "\n" + process.StandardError.ReadToEnd());
      return true;
    }
  }
}