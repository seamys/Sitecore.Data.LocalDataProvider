using System.Data;
using Sitecore.Data.DataProviders.Sql;

namespace Sitecore.Data.LocalDataProvider.UnitTest
{
    public class LocalDataCommandMock : DataProviderCommand
    {
        protected DataTable Table;
        public LocalDataCommandMock(DataTable table) : base(new FakeDbCommand(), false)
        {
            Table = table;
        }

        public override IDataReader ExecuteReader()
        {
            return Table.CreateDataReader();
        }
    }
}