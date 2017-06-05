using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Threading;
using Sitecore.Collections;
using Sitecore.Configuration;
using Sitecore.Data.DataProviders;
using Sitecore.Data.DataProviders.Sql;
using Sitecore.Data.DataProviders.SqlServer;
using Sitecore.Data.Eventing;
using Sitecore.Data.SqlServer;
using Sitecore.Diagnostics;
using Sitecore.Globalization;

namespace Sitecore.Data.LocalDataProvider
{
    public class SqlServerLocalDataProvider : SqlDataProvider
    {
        private readonly LockSet _blobSetLocks = new LockSet();

        protected TimeSpan CommandTimeout { get; set; } = Settings.DefaultSQLTimeout;

        public virtual int BlobChunkSize => 1029120;

        private string ConnectionString => Api.ConnectionString;

        public SqlServerLocalDataProvider(SqlDataApi api) : base(api) { }

        public override bool BlobStreamExists(Guid blobId, CallContext context)
        {
            Assert.ArgumentNotNull(context, "context");
            using (DataProviderReader reader = this.Api.CreateReader(" SELECT COUNT(*) FROM {0}Blobs{1} WHERE {0}BlobId{1} = {2}blobId{3}", (object)"@blobId", (object)blobId))
            {
                if (reader.Read())
                    return this.Api.GetInt(0, reader) > 0;
            }
            return false;
        }

        public override bool DeleteItem(ItemDefinition itemDefinition, CallContext context)
        {
            ID parentId = this.GetParentID(itemDefinition, context);
            string sql = "DELETE FROM {0}Items{1}\r\n                  WHERE {0}ID{1} = {2}itemId{3}\r\n\r\n                  DELETE FROM {0}SharedFields{1}\r\n                  WHERE {0}ItemId{1} = {2}itemId{3}\r\n\r\n                  DELETE FROM {0}UnversionedFields{1}\r\n                  WHERE {0}ItemId{1} = {2}itemId{3}\r\n\r\n                  DELETE FROM {0}VersionedFields{1}\r\n                  WHERE {0}ItemId{1} = {2}itemId{3}";
            this.DescendantsLock.AcquireReaderLock(-1);
            try
            {
                Factory.GetRetryer().ExecuteNoResult(() =>
                {
                    using (DataProviderTransaction transaction = this.Api.CreateTransaction())
                    {
                        this.Api.Execute(sql, (object)"itemId", (object)itemDefinition.ID);
                        transaction.Complete();
                    }
                });
                this.Descendants_ItemDeleted(itemDefinition.ID);
            }
            finally
            {
                this.DescendantsLock.ReleaseReaderLock();
            }
            this.RemovePrefetchDataFromCache(itemDefinition.ID);
            if ((object)parentId != null)
                this.RemovePrefetchDataFromCache(parentId);
            if (itemDefinition.TemplateID == TemplateIDs.Language)
                this.Languages = null;
            return true;
        }

        public override Stream GetBlobStream(Guid blobId, CallContext context)
        {
            Assert.ArgumentNotNull(context, "context");
            long blobSize = this.GetBlobSize(blobId);
            if (blobSize < 0L)
                return null;
            return this.OpenBlobStream(blobId, blobSize);
        }

        public override bool SetBlobStream(Stream stream, Guid blobId, CallContext context)
        {
            Assert.ArgumentNotNull(stream, "stream");
            Assert.ArgumentNotNull(context, "context");
            lock (this._blobSetLocks.GetLock(blobId))
            {
                this.Api.Execute("DELETE FROM {0}Blobs{1} WHERE {0}BlobId{1} = {2}blobId{3}", (object)"@blobId", (object)blobId);
                string cmdText = " INSERT INTO [Blobs]( [Id], [BlobId], [Index], [Created], [Data] ) VALUES(   NewId(), @blobId, @index, @created, @data)";
                DateTime utcNow = DateTime.UtcNow;
                int num = 0;
                if (stream.CanSeek)
                    stream.Seek(0L, SeekOrigin.Begin);
                int blobChunkSize = this.BlobChunkSize;
                byte[] buffer = new byte[blobChunkSize];
                int size = stream.Read(buffer, 0, blobChunkSize);
                while (size > 0)
                {
                    using (SqlConnection connection = new SqlConnection(this.ConnectionString))
                    {
                        connection.Open();
                        SqlCommand sqlCommand =
                            new SqlCommand(cmdText, connection)
                            {
                                CommandTimeout = (int)this.CommandTimeout.TotalSeconds
                            };
                        sqlCommand.Parameters.AddWithValue("@blobId", blobId);
                        sqlCommand.Parameters.AddWithValue("@index", num);
                        sqlCommand.Parameters.AddWithValue("@created", utcNow);
                        sqlCommand.Parameters.Add("@data", SqlDbType.Image, size).Value = buffer;
                        sqlCommand.ExecuteNonQuery();
                    }
                    size = stream.Read(buffer, 0, blobChunkSize);
                    ++num;
                }
            }
            return true;
        }

        public override Sitecore.Eventing.EventQueue GetEventQueue()
        {
            return new SqlServerEventQueue(this.Api, this.Database);
        }

        protected override void CleanupCyclicDependences()
        {
            this.Api.Execute(" declare @x bigint set @x = 0 DECLARE @item TABLE(ID uniqueidentifier,parentID uniqueidentifier) INSERT INTO @item (ID,parentID)   SELECT  {0}ID{1},{0}ParentID{1} FROM {0}Items{1}  DECLARE @temp TABLE(ID uniqueidentifier) WHILE (SELECT count(id) FROM @item ) <> @x begin set @x = (SELECT count(id) FROM @item ) delete from @temp; insert into @temp (ID)   SELECT  id FROM @item where parentID  = {2}nullId{3} update @item SET Parentid ={2}nullId{3} where Parentid  in (select id from @temp) delete from @item where  id  in (select id from @temp) end UPDATE {0}Items{1} SET {0}Parentid{1} = {2}nullId{3} where {0}ID{1}  in (select id from @item) ; DELETE from {0}Items{1} where {0}ID{1} in (select id from @item)", (object)"nullId", (object)ID.Null);
        }

        protected override void LoadChildIds(string condition, object[] parameters, SafeDictionary<ID, PrefetchData> prefetchData)
        {
        }

        protected override void LoadItemDefinitions(string condition, object[] parameters, SafeDictionary<ID, PrefetchData> prefetchData)
        {
            try
            {
                string sql = "SELECT [ItemId], [Order], [Version], [Language], [Name], [Value], [FieldId], [MasterID], [ParentID]\r\n                     FROM (\r\n                        SELECT [Id] as [ItemId], 0 as [Order], 0 as [Version], '' as [Language], [Name], '' as [Value], [TemplateID] as [FieldId], [MasterID], [ParentID]\r\n                        FROM [Items]\r\n\r\n                        UNION ALL                          \r\n                        SELECT [ParentId] as [ItemId], 1 as [Order], 0 as [Version], '' as [Language], NULL as [Name], '', NULL, NULL, [Id]\r\n                        FROM [Items] \r\n\r\n                        UNION ALL \r\n                        SELECT [ItemId], 2 as [Order], 0 AS [Version], '' as [Language], NULL as [Name], [Value], [FieldId], NULL, NULL\r\n                        FROM [SharedFields] \r\n\r\n                        UNION ALL \r\n                        SELECT [ItemId], 2 as [Order], 0 AS [Version],       [Language], NULL as [Name], [Value], [FieldId], NULL, NULL\r\n                        FROM [UnversionedFields] \r\n\r\n                        UNION ALL \r\n                        SELECT [ItemId], 2 as [Order],      [Version],       [Language], NULL as [Name], [Value], [FieldId], NULL, NULL \r\n                        FROM [VersionedFields]\r\n                     ) as temp " + (" WHERE {0}ItemId{1} IN (SELECT {0}ID{1} FROM {0}Items{1} WITH (nolock) " + condition + ")") + " \r\n                     ORDER BY [ItemId], [Order] ASC, [Language] DESC, [Version] DESC";
                LanguageCollection languages = this.GetLanguages();
                PrefetchData prefetchData1 = null;
                bool flag1 = false;
                bool flag2 = false;
                ID id1 = ID.Null;
                bool flag3 = false;
                int num1 = 5;
                while (true)
                {
                    try
                    {
                        using (DataProviderReader reader = this.Api.CreateReader(sql, parameters))
                        {
                            while (reader.Read())
                            {
                                flag3 = true;
                                ID id2 = this.Api.GetId(0, reader);
                                int num2 = this.Api.GetInt(1, reader);
                                if (num2 == 0)
                                {
                                    string itemName = this.Api.GetString(4, reader);
                                    ID id3 = this.Api.GetId(6, reader);
                                    ID id4 = this.Api.GetId(7, reader);
                                    ID id5 = this.Api.GetId(8, reader);
                                    flag1 = false;
                                    id1 = id2;
                                    if (prefetchData.ContainsKey(id2))
                                    {
                                        prefetchData1 = prefetchData[id2];
                                    }
                                    else
                                    {
                                        prefetchData1 = new PrefetchData(new ItemDefinition(id2, itemName, id3, id4), id5);
                                        prefetchData[id2] = prefetchData1;
                                    }
                                }
                                else if (id2 != id1 || prefetchData1 == null)
                                {
                                    if (!flag2)
                                    {
                                        Log.Error("Failed to get item information", this);
                                        flag2 = true;
                                    }
                                }
                                else if (num2 == 1)
                                {
                                    ID id3 = this.Api.GetId(8, reader);
                                    prefetchData1.AddChildId(id3);
                                }
                                else
                                {
                                    if (!flag1)
                                    {
                                        prefetchData1.InitializeFieldLists(languages);
                                        flag1 = true;
                                    }
                                    ID id3 = this.Api.GetId(6, reader);
                                    int version = this.Api.GetInt(2, reader);
                                    string language = this.Api.GetString(3, reader);
                                    string str = this.Api.GetString(5, reader);
                                    prefetchData1.AddField(language, version, id3, str);
                                }
                            }
                            break;
                        }
                    }
                    catch (SqlException ex)
                    {
                        if (!flag3 && ex.Number == 1205 && num1 > 0)
                        {
                            --num1;
                            Thread.Sleep(200);
                        }
                        else
                            throw;
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error("Failed to load item data", ex, this);
                throw;
            }
        }

        protected override void LoadItemFields(string itemCondition, string fieldCondition, object[] parameters, SafeDictionary<ID, PrefetchData> prefetchData)
        {
        }

        protected override void RemoveFields(ID itemId, Language language, Sitecore.Data.Version version)
        {
            this.Api.Execute("DELETE FROM {0}SharedFields{1}\r\n                  WHERE {0}ItemId{1} = {2}itemId{3}\r\n                \r\n                  DELETE FROM {0}UnversionedFields{1}\r\n                  WHERE {0}ItemId{1} = {2}itemId{3}\r\n                  AND {0}Language{1} = {2}language{3}\r\n                \r\n                  DELETE FROM {0}VersionedFields{1}\r\n                  WHERE {0}ItemId{1} = {2}itemId{3}\r\n                  AND {0}Language{1} = {2}language{3}\r\n                  AND {0}Version{1} = {2}version{3}", (object)"itemId", (object)itemId, (object)"language", (object)language, (object)"version", (object)version);
        }

        private long GetBlobSize(Guid blobId)
        {
            using (SqlConnection connection = this.OpenConnection())
            {
                SqlCommand sqlCommand =
                    new SqlCommand(" SELECT SUM(DATALENGTH([Data])) FROM [Blobs] WHERE [BlobId] = @blobId",
                        connection)
                    { CommandTimeout = (int)this.CommandTimeout.TotalSeconds };
                sqlCommand.Parameters.AddWithValue("@blobId", blobId);
                using (SqlDataReader reader = sqlCommand.ExecuteReader(CommandBehavior.CloseConnection))
                {
                    if (!reader.Read() || reader.IsDBNull(0))
                        return -1;
                    return SqlServerHelper.GetLong(reader, 0);
                }
            }
        }

        private Stream OpenBlobStream(Guid blobId, long blobSize)
        {
            string cmdText = "SELECT [Data]\r\n                     FROM [Blobs]\r\n                     WHERE [BlobId] = @blobId\r\n                     ORDER BY [Index]";
            SqlConnection connection = this.OpenConnection();
            try
            {
                SqlCommand sqlCommand =
                    new SqlCommand(cmdText, connection) { CommandTimeout = (int)this.CommandTimeout.TotalSeconds };
                sqlCommand.Parameters.AddWithValue("@blobId", blobId);
                SqlDataReader reader = sqlCommand.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.CloseConnection);
                try
                {
                    return new SqlServerStream(reader, blobSize);
                }
                catch (Exception ex)
                {
                    reader.Close();
                    Log.Error("Error reading blob stream (blob id: " + blobId + ")", ex, this);
                }
            }
            catch (Exception ex)
            {
                connection.Close();
                Log.Error("Error reading blob stream (blob id: " + blobId + ")", ex, this);
            }
            return null;
        }

        private SqlConnection OpenConnection()
        {
            SqlConnection sqlConnection = new SqlConnection(this.ConnectionString);
            sqlConnection.Open();
            return sqlConnection;
        }
    }
}
