using System.Data;
using Sitecore.Data.DataProviders.Sql;

namespace Sitecore.Data.LocalDataProvider
{
    public class LocalDataProviderCommand : DataProviderCommand
    {
        private readonly string _file;
        public LocalDataProviderCommand(string file) : base(new FakeDbCommand(), false)
        {
            _file = file;
        }
        public override IDataReader ExecuteReader()
        {
            return new StreamDataReader(_file);
        }
    }
}
