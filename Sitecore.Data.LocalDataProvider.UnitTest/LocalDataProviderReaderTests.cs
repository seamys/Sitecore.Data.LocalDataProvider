using System;
using System.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Sitecore.Data.LocalDataProvider.UnitTest
{
    [TestClass()]
    public class LocalDataProviderReaderTests
    {
        protected LocalDataProviderReader Reader;
        protected SqlServerLocalDataApi SqlServerApi;
        protected string Datafile;

        [TestInitialize]
        public void DataStreamProviderReaderTest()
        {
            Datafile = $"{AppDomain.CurrentDomain.BaseDirectory}\\data.dat";
            var table = new DataTable("test");
            // Create two columns, ID and Name.
            DataColumn idColumn = table.Columns.Add("Int", typeof(int));
            table.Columns.Add("String", typeof(string));
            table.Columns.Add("DateTime", typeof(DateTime));
            table.Columns.Add("Bool", typeof(bool));
            table.Columns.Add("Guid", typeof(Guid));
            table.Columns.Add("Long", typeof(long));
            table.Columns.Add("Byte[]", typeof(byte[]));
            // Set the ID column as the primary key column.
            table.PrimaryKey = new[] { idColumn };
            table.Rows.Add(1, "Mary", DateTime.Now, true, Guid.NewGuid(), 1L, new byte[2]);
            table.Rows.Add(2, "Andy", DateTime.Now, true, Guid.NewGuid(), 1L, new byte[2]);
            table.Rows.Add(3, "Peter", DateTime.Now, true, Guid.NewGuid(), 1L, new byte[2]);
            table.Rows.Add(4, "Russ", DateTime.Now, true, Guid.NewGuid(), 1L, new byte[2]);
            table.Rows.Add(5, null, null, true, null, null, null);
            var command = new LocalDataCommandMock(table);
            Reader = new LocalDataProviderReader(command, Datafile,"query text example");
        }

        [TestMethod()]
        public void ReadTest()
        {
            Assert.IsTrue(Reader.Read());
            Assert.AreEqual(7, Reader.InnerReader.FieldCount);
            Assert.AreEqual(1, (int)Reader.InnerReader[0]);

            int i = 0;
            while (Reader.Read() && i < 7)
            {
                i++;
            }
            Assert.AreEqual(4, i);
            Reader.Dispose();

            var localReader = new StreamDataReader(Datafile);
            Assert.IsTrue(localReader.Read());
            Assert.AreEqual(7, localReader.FieldCount);
            Assert.AreEqual(1, (int)localReader[0]);
            Assert.AreEqual("Mary", (string)localReader[1]);

            i = 0;
            while (localReader.Read() && i < 7)
            {
                i++;
            }
            Assert.AreEqual(4, i);
            Reader.Dispose();
        }
    }
}