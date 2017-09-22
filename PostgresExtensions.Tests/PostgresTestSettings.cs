// Copyright (c) 2017 Stig Schmidt Nielsson. 
// Published under the DO WHAT THE F*CK YOU WANT TO PUBLIC LICENSE TERMS AND CONDITIONS.
// 0. You just DO WHAT THE F*CK YOU WANT TO.
// 1. You do not hold ANYBODY but YOURSELF liable for anything that happens or goes wrong with your use of the work.
using System;
namespace PostgresExtensions.Tests {
  public static class PostgresTestSettings {
    public const string AdminDatabase = "postgres";
    public const string AdminUsername = "postgres";
    public static string _adminPassword = "wof13akb";
    public static string AdminPassword {
      get {
        if (string.IsNullOrEmpty(_adminPassword)) throw new Exception("Please set the Admin password for the Postgres server in PostgresTestSettings.cs");
        return _adminPassword;
      }
      set => _adminPassword = value;
    }
  }
}