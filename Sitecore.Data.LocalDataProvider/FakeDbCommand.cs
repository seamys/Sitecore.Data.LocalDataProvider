using System;
using System.Data;

namespace Sitecore.Data.LocalDataProvider
{
    /// <summary>
    /// 
    /// </summary>
    public class FakeDbCommand : IDbCommand
    {
        public void Dispose() { }

        public void Prepare() { }

        public void Cancel() { }

        public IDbDataParameter CreateParameter()
        {
            throw new NotImplementedException();
        }

        public int ExecuteNonQuery()
        {
            return 0;
        }

        public IDataReader ExecuteReader()
        {
            throw new NotImplementedException();
        }

        public IDataReader ExecuteReader(CommandBehavior behavior)
        {
            throw new NotImplementedException();
        }

        public object ExecuteScalar()
        {
            return null;
        }

        public IDbConnection Connection { get; set; }
        public IDbTransaction Transaction { get; set; }
        public string CommandText { get; set; }
        public int CommandTimeout { get; set; }
        public CommandType CommandType { get; set; }
        public IDataParameterCollection Parameters { get; }
        public UpdateRowSource UpdatedRowSource { get; set; }
    }
}