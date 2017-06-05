using System;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using Sitecore.Collections;
using Sitecore.Configuration;
using Sitecore.Data.DataProviders.Sql;
using Sitecore.Data.SqlServer;
using Sitecore.Diagnostics;

namespace Sitecore.Data.LocalDataProvider
{
    public class SqlServerLocalDataApi : SqlServerDataApi
    {
        private readonly Regex _matchDb = new Regex(@"Database=(?<db>\w+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private string _database;
        public override string ConnectionString
        {
            get
            {
                return base.ConnectionString;
            }
            set
            {
                base.ConnectionString = value;
                _database = GetDbName();
            }
        }
        public SqlServerLocalDataApi(string connectionString) : base(connectionString)
        {
            _database = GetDbName(connectionString);
        }
        protected virtual string CalculateHash(string input)
        {
            using (MD5 md5 = MD5.Create())
            {
                byte[] inputBytes = Encoding.Unicode.GetBytes(input);
                byte[] hash = md5.ComputeHash(inputBytes);
                StringBuilder sb = new StringBuilder();
                foreach (byte t in hash)
                {
                    sb.Append(t.ToString("x2"));
                }
                return sb.ToString();
            }
        }
        protected virtual string MakeQueryText(string sql, object[] parameters)
        {
            StringBuilder builder = new StringBuilder();
            builder.Append(_database);
            builder.AppendLine();
            builder.Append(Format(sql));
            builder.AppendLine();
            int index = 0;
            while (index < parameters.Length - 1)
            {
                builder.AppendFormat("@{0}='{1}'", parameters[index], parameters[index + 1]);
                index += 2;
            }
            return builder.ToString();
        }
        protected virtual string GetLocalFileName(string hash)
        {
            string folder = $"{AppDomain.CurrentDomain.BaseDirectory}{Settings.DataFolder.TrimStart('/')}\\cache\\";
            if (!Directory.Exists(folder))
                Directory.CreateDirectory(folder);
            return $"{folder}{_database}-{hash}.dat";
        }
        public override DataProviderReader CreateReader(string sql, params object[] parameters)
        {
            DataProviderCommand command = null;
            try
            {
                if (!IsNeedCaching(sql, parameters))
                    return base.CreateReader(sql, parameters);
                var queryText = MakeQueryText(sql, parameters);
                var file = GetLocalFileName(CalculateHash(queryText));
                if (File.Exists(file))
                {
                    command = new LocalDataProviderCommand(file);
                    return new DataProviderReader(command);
                }
                return Factory.GetRetryer().Execute(() =>
                {
                    command = CreateCommand(sql, parameters);
                    return new LocalDataProviderReader(command, file, queryText);
                });

            }
            catch (Exception exception)
            {
                Log.Error(exception.Message, exception, typeof(SqlServerLocalDataApi));
                command?.Dispose();
                if (exception is SqlException)
                    throw;
                return base.CreateReader(sql, parameters);
            }
        }
        private string GetDbName(string connectionString = null)
        {
            Match match = _matchDb.Match(connectionString ?? ConnectionString);
            if (match.Success)
                return match.Groups["db"].Value.ToLower();
            return null;
        }
        public bool IsNeedCaching(string sql, params object[] parameters)
        {
            bool enable = Settings.GetBoolSetting("LocalData.Enable", true);
            if (!enable)
                return false;
            string allowedDatabases = Settings.GetSetting("LocalData.Allow.Databases", "*");
            if (allowedDatabases != "*")
            {
                string[] database = allowedDatabases.ToLower()
                    .Split(new[] {','}, StringSplitOptions.RemoveEmptyEntries);
                if (_database == null || database.Length <= 0)
                    return false;
                bool isMatchDb = database.Any(dbName => _database == dbName);
                if (!isMatchDb)
                    return false;
            }
            string excludeQuerys = Settings.GetSetting("LocalData.Exclude.Querys", "EventQueue");
            string[] querys = excludeQuerys.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
            return !querys.Any(sql.Contains);
        }
    }
}
