using System;
using System.Collections;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Threading;
using Sitecore.Collections;
using Sitecore.Configuration;
using Sitecore.Data.DataProviders;
using Sitecore.Data.DataProviders.Sql;
using Sitecore.Data.DataProviders.Sql.FastQuery;
using Sitecore.Data.DataProviders.SqlServer;
using Sitecore.Data.Eventing;
using Sitecore.Data.SqlServer;
using Sitecore.Diagnostics;
using Sitecore.Eventing;
using Sitecore.Globalization;
using Sitecore.Workflows;

namespace Sitecore.Data.LocalDataProvider
{
    /// <summary>SQL Server based data provider.</summary>
    public class SqlServerLocalDataProvider : SqlDataProvider
    {
        /// <summary>The _blob set locks.</summary>
        private readonly LockSet _blobSetLocks = new LockSet();

        /// <summary>
        ///     Initializes a new instance of the <see cref="T:Sitecore.Data.SqlServer.SqlServerLocalDataProvider" /> class.
        /// </summary>
        /// <param name="api">SqlDataApi.</param>
        public SqlServerLocalDataProvider(SqlDataApi api) : base(api) { }

        /// <summary>Gets or sets the command timeout.</summary>
        /// <value>The command timeout.</value>
        protected TimeSpan CommandTimeout { get; set; } = Settings.DefaultSQLTimeout;

        /// <summary>
        ///     Gets the size of the individual chunks making up a BLOB.
        /// </summary>
        /// <value>The size of the BLOB chunk.</value>
        public virtual int BlobChunkSize => 1029120;

        /// <summary>Gets the connection string.</summary>
        /// <value>The connection string.</value>
        private string ConnectionString
        {
            get
            {
                var api = Api as SqlServerLocalDataApi;
                if (api == null)
                    return string.Empty;
                return api.ConnectionString;
            }
        }

        /// <summary>
        ///     Determines whether the specified field contains a BLOB value.
        /// </summary>
        /// <param name="blobId">The BLOB id.</param>
        /// <param name="context">The context.</param>
        /// <returns>
        ///     <c>true</c> if the specified item definition has children; otherwise, <c>false</c>.
        /// </returns>
        public override bool BlobStreamExists(Guid blobId, CallContext context)
        {
            Assert.ArgumentNotNull(context, "context");
            using (var reader = Api.CreateReader(" SELECT COUNT(*) FROM {0}Blobs{1} WHERE {0}BlobId{1} = {2}blobId{3}",
                (object)"@blobId", (object)blobId))
            {
                if (reader.Read())
                    return Api.GetInt(0, reader) > 0;
            }
            return false;
        }

        /// <summary>Removes unused blob fields</summary>
        /// <param name="context">The context.</param>
        protected override void CleanupBlobs(CallContext context)
        {
            try
            {
                Factory.GetRetryer().ExecuteNoResult(() =>
                {
                    using (var transaction = Api.CreateTransaction())
                    {
                        Api.Execute(
                            "\r\nWITH {0}BlobFields{1} ({0}FieldId{1})\r\nAS\r\n(  SELECT\r\n    {0}SharedFields{1}.{0}ItemId{1}\r\n  FROM\r\n    {0}SharedFields{1}\r\n  WHERE\r\n    {0}SharedFields{1}.{0}FieldId{1} = {2}BlobID{3}\r\n    AND {0}SharedFields{1}.{0}Value{1} = 1\r\n  UNION\r\n  SELECT\r\n    {0}VersionedFields{1}.{0}ItemId{1}\r\n  FROM\r\n    {0}VersionedFields{1}\r\n  WHERE\r\n    {0}VersionedFields{1}.{0}FieldId{1} = {2}BlobID{3}\r\n    AND {0}VersionedFields{1}.{0}Value{1} = 1\r\n  UNION\r\n  SELECT\r\n    {0}UnversionedFields{1}.{0}ItemId{1}\r\n  FROM\r\n    {0}UnversionedFields{1}\r\n  WHERE\r\n    {0}UnversionedFields{1}.{0}FieldId{1} = {2}BlobID{3}\r\n    AND {0}UnversionedFields{1}.{0}Value{1} = 1\r\n  UNION\r\n  SELECT\r\n    {0}ArchivedFields{1}.{0}ArchivalId{1}\r\n  FROM\r\n    {0}ArchivedFields{1}\r\n  WHERE\r\n    {0}ArchivedFields{1}.{0}FieldId{1} = {2}BlobID{3}\r\n    AND {0}ArchivedFields{1}.{0}Value{1} = 1\r\n),\r\n\r\n{0}ExistingBlobs{1} ({0}BlobId{1})\r\nAS\r\n(  SELECT\r\n    {0}Blobs{1}.{0}BlobId{1}\r\n  FROM\r\n    {0}Blobs{1}\r\n    JOIN {0}SharedFields{1}\r\n      ON '{{' + CONVERT(NVARCHAR(MAX), {0}Blobs{1}.{0}BlobId{1}) + '}}' = {0}SharedFields{1}.{0}Value{1}\r\n    JOIN {0}BlobFields{1}\r\n      ON {0}SharedFields{1}.{0}FieldId{1} = {0}BlobFields{1}.{0}FieldId{1}\r\n  UNION\r\n  SELECT\r\n    {0}Blobs{1}.{0}BlobId{1}\r\n  FROM\r\n    {0}Blobs{1}\r\n    JOIN {0}SharedFields{1}\r\n      ON CONVERT(NVARCHAR(MAX), {0}Blobs{1}.{0}BlobId{1}) = {0}SharedFields{1}.{0}Value{1}\r\n    JOIN {0}BlobFields{1}\r\n      ON {0}SharedFields{1}.{0}FieldId{1} = {0}BlobFields{1}.{0}FieldId{1}\r\n  UNION\r\n  SELECT\r\n    {0}Blobs{1}.{0}BlobId{1}\r\n  FROM\r\n    {0}Blobs{1}\r\n    JOIN {0}VersionedFields{1}\r\n      ON '{{' + CONVERT(NVARCHAR(MAX), {0}Blobs{1}.{0}BlobId{1}) + '}}' = {0}VersionedFields{1}.{0}Value{1}\r\n    JOIN {0}BlobFields{1}\r\n      ON {0}VersionedFields{1}.{0}FieldId{1} = {0}BlobFields{1}.{0}FieldId{1}\r\n  UNION\r\n  SELECT\r\n    {0}Blobs{1}.{0}BlobId{1}\r\n  FROM\r\n    {0}Blobs{1}\r\n    JOIN {0}VersionedFields{1}\r\n      ON CONVERT(NVARCHAR(MAX), {0}Blobs{1}.{0}BlobId{1}) = {0}VersionedFields{1}.{0}Value{1}\r\n    JOIN {0}BlobFields{1}\r\n      ON {0}VersionedFields{1}.{0}FieldId{1} = {0}BlobFields{1}.{0}FieldId{1}\r\n  UNION\r\n  SELECT\r\n    {0}Blobs{1}.{0}BlobId{1}\r\n  FROM\r\n    {0}Blobs{1}\r\n    JOIN {0}UnversionedFields{1}\r\n      ON '{{' + CONVERT(NVARCHAR(MAX), {0}Blobs{1}.{0}BlobId{1}) + '}}' = {0}UnversionedFields{1}.{0}Value{1}\r\n    JOIN {0}BlobFields{1}\r\n      ON {0}UnversionedFields{1}.{0}FieldId{1} = {0}BlobFields{1}.{0}FieldId{1}\r\n  UNION\r\n  SELECT\r\n    {0}Blobs{1}.{0}BlobId{1}\r\n  FROM\r\n    {0}Blobs{1}\r\n    JOIN {0}UnversionedFields{1}\r\n      ON CONVERT(NVARCHAR(MAX), {0}Blobs{1}.{0}BlobId{1}) = {0}UnversionedFields{1}.{0}Value{1}\r\n    JOIN {0}BlobFields{1}\r\n      ON {0}UnversionedFields{1}.{0}FieldId{1} = {0}BlobFields{1}.{0}FieldId{1}\r\n  UNION\r\n  SELECT\r\n    {0}Blobs{1}.{0}BlobId{1}\r\n  FROM\r\n    {0}Blobs{1}\r\n    JOIN {0}ArchivedFields{1}\r\n      ON '{{' + CONVERT(NVARCHAR(MAX), {0}Blobs{1}.{0}BlobId{1}) + '}}' = {0}ArchivedFields{1}.{0}Value{1}\r\n    JOIN {0}BlobFields{1}\r\n      ON {0}ArchivedFields{1}.{0}FieldId{1} = {0}BlobFields{1}.{0}FieldId{1}\r\n  UNION\r\n  SELECT\r\n    {0}Blobs{1}.{0}BlobId{1}\r\n  FROM\r\n    {0}Blobs{1}\r\n    JOIN {0}ArchivedFields{1}\r\n      ON CONVERT(NVARCHAR(MAX), {0}Blobs{1}.{0}BlobId{1}) = {0}ArchivedFields{1}.{0}Value{1}\r\n    JOIN {0}BlobFields{1}\r\n      ON {0}ArchivedFields{1}.{0}FieldId{1} = {0}BlobFields{1}.{0}FieldId{1})\r\n\r\nDELETE FROM {0}Blobs{1}\r\nWHERE NOT EXISTS\r\n(  SELECT NULL\r\n  FROM {0}ExistingBlobs{1}\r\n  WHERE {0}ExistingBlobs{1}.{0}BlobId{1} = {0}Blobs{1}.{0}BlobId{1})\r\n",
                            (object)"BlobID", (object)TemplateFieldIDs.Blob);
                        transaction.Complete();
                    }
                });
            }
            catch (Exception ex)
            {
                Log.Error(ex.ToString(), this);
            }
        }

        /// <summary>Deletes an item.</summary>
        /// <param name="itemDefinition">The item definition.</param>
        /// <param name="context">The context.</param>
        /// <returns>The delete item.</returns>
        public override bool DeleteItem(ItemDefinition itemDefinition, CallContext context)
        {
            var parentId = GetParentID(itemDefinition, context);
            var sql =
                "DELETE FROM {0}Items{1}\r\n                  WHERE {0}ID{1} = {2}itemId{3}\r\n\r\n                  DELETE FROM {0}SharedFields{1}\r\n                  WHERE {0}ItemId{1} = {2}itemId{3}\r\n\r\n                  DELETE FROM {0}UnversionedFields{1}\r\n                  WHERE {0}ItemId{1} = {2}itemId{3}\r\n\r\n                  DELETE FROM {0}VersionedFields{1}\r\n                  WHERE {0}ItemId{1} = {2}itemId{3}";
            DescendantsLock.AcquireReaderLock(-1);
            try
            {
                Factory.GetRetryer().ExecuteNoResult(() =>
                {
                    using (var transaction = Api.CreateTransaction())
                    {
                        Api.Execute(sql, (object)"itemId", (object)itemDefinition.ID);
                        transaction.Complete();
                    }
                });
                Descendants_ItemDeleted(itemDefinition.ID);
            }
            finally
            {
                DescendantsLock.ReleaseReaderLock();
            }
            RemovePrefetchDataFromCache(itemDefinition.ID);
            if ((object)parentId != null)
                RemovePrefetchDataFromCache(parentId);
            if (itemDefinition.TemplateID == TemplateIDs.Language)
                Languages = null;
            return true;
        }

        /// <summary>Gets the BLOB stream associated with a field.</summary>
        /// <param name="blobId">The BLOB id.</param>
        /// <param name="context">The call context.</param>
        /// <returns>The stream.</returns>
        public override Stream GetBlobStream(Guid blobId, CallContext context)
        {
            Assert.ArgumentNotNull(context, "context");
            var blobSize = GetBlobSize(blobId);
            if (blobSize < 0L)
                return null;
            return OpenBlobStream(blobId, blobSize);
        }

        /// <summary>
        ///     Gets all items that are in a specific workflow state taking into account SQL Server engine specific:
        ///     <para>The query executed with NoLock statement, since results would be re-tested by GetItem further on.</para>
        ///     <para><see cref="F:Sitecore.FieldIDs.WorkflowState" /> is set not as param to enable SQL Filtered indexes.</para>
        /// </summary>
        /// <param name="info">The workflow info.</param>
        /// <param name="context">The context.</param>
        /// <returns>Arrray of items that are in requested workflow state.</returns>
        public override DataUri[] GetItemsInWorkflowState(WorkflowInfo info, CallContext context)
        {
            using (var reader =
                Api.CreateReader(
                    "SELECT TOP ({2}maxVersionsToLoad{3}) {0}ItemId{1}, {0}Language{1}, {0}Version{1}\r\n          FROM {0}VersionedFields{1} WITH (NOLOCK) \r\n          WHERE {0}FieldId{1}={4}" +
                    FieldIDs.WorkflowState.ToGuid().ToString("D") +
                    "{4} \r\n          AND {0}Value{1}= {2}workflowStateFieldValue{3}\r\n          ORDER BY {0}Updated{1} desc",
                    (object)"maxVersionsToLoad", (object)Settings.Workbox.SingleWorkflowStateVersionLoadThreshold,
                    (object)"workflowStateFieldValue", (object)info.StateID))
            {
                var arrayList = new ArrayList();
                while (reader.Read())
                {
                    var id = Api.GetId(0, reader);
                    var language = Api.GetLanguage(1, reader);
                    var version = Api.GetVersion(2, reader);
                    arrayList.Add(new DataUri(id, language, version));
                }
                return arrayList.ToArray(typeof(DataUri)) as DataUri[];
            }
        }

        /// <summary>Sets the BLOB stream associated with a field.</summary>
        /// <param name="stream">The stream.</param>
        /// <param name="blobId">The BLOB id.</param>
        /// <param name="context">The call context.</param>
        /// <returns>The set blob stream.</returns>
        public override bool SetBlobStream(Stream stream, Guid blobId, CallContext context)
        {
            Assert.ArgumentNotNull(stream, "stream");
            Assert.ArgumentNotNull(context, "context");
            lock (_blobSetLocks.GetLock(blobId))
            {
                Api.Execute("DELETE FROM {0}Blobs{1} WHERE {0}BlobId{1} = {2}blobId{3}", (object)"@blobId",
                    (object)blobId);
                var cmdText =
                    " INSERT INTO [Blobs]( [Id], [BlobId], [Index], [Created], [Data] ) VALUES(   NewId(), @blobId, @index, @created, @data)";
                var utcNow = DateTime.UtcNow;
                var num = 0;
                if (stream.CanSeek)
                    stream.Seek(0L, SeekOrigin.Begin);
                var blobChunkSize = BlobChunkSize;
                var buffer = new byte[blobChunkSize];
                var size = stream.Read(buffer, 0, blobChunkSize);
                while (size > 0)
                {
                    using (var connection = new SqlConnection(ConnectionString))
                    {
                        connection.Open();
                        var sqlCommand =
                            new SqlCommand(cmdText, connection) { CommandTimeout = (int)CommandTimeout.TotalSeconds };
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

        /// <summary>Gets the event queue driver.</summary>
        /// <returns>The event queue driver.</returns>
        public override EventQueue GetEventQueue()
        {
            return new SqlServerEventQueue(Api, Database);
        }

        /// <summary>
        ///     Cleanups the cyclic dependences. The items couldn't be removed by  CleanupOrphans because always have a parent but
        ///     is not participated in item tree.
        /// </summary>
        protected override void CleanupCyclicDependences()
        {
            Api.Execute(
                "\r\nWITH Tree ({0}ID{1}, {0}ParentID{1})\r\nAS\r\n(\r\n  SELECT\r\n    {0}Items{1}.{0}ID{1},\r\n    {0}Items{1}.{0}ParentID{1}\r\n  FROM\r\n    {0}Items{1}\r\n  WHERE\r\n    {0}Items{1}.{0}ParentID{1} = {2}nullId{3}\r\n  UNION ALL\r\n  SELECT\r\n    {0}Items{1}.{0}ID{1},\r\n    {0}Items{1}.{0}ParentID{1}\r\n  FROM\r\n    {0}Items{1}\r\n    JOIN {0}Tree{1} {0}t{1}\r\n      ON {0}Items{1}.{0}ParentID{1} = {0}t{1}.{0}ID{1}\r\n)\r\n\r\nDELETE FROM {0}Items{1} WHERE {0}Items{1}.{0}ID{1} NOT IN (SELECT {0}t{1}.{0}ID{1} FROM {0}Tree{1} {0}t{1})\r\n",
                (object)"nullId", (object)ID.Null);
        }

        /// <summary>Creates the SQL translator.</summary>
        /// <returns>The SQL translator.</returns>
        protected override QueryToSqlTranslator CreateSqlTranslator()
        {
            return new SqlServerQueryToSqlTranslator(Api);
        }

        /// <summary>Loads the child ids.</summary>
        /// <param name="condition">The condition.</param>
        /// <param name="parameters">The parameters.</param>
        /// <param name="prefetchData">The working set.</param>
        protected override void LoadChildIds(string condition, object[] parameters,
            SafeDictionary<ID, PrefetchData> prefetchData)
        {
        }

        /// <summary>Loads the item definitions.</summary>
        /// <param name="condition">The condition.</param>
        /// <param name="parameters">The parameters.</param>
        /// <param name="prefetchData">The working set.</param>
        /// <exception cref="T:System.Exception"><c>Exception</c>.</exception>
        [Obsolete("Use ExecuteLoadItemDefinitionsSql instead.")]
        protected override void LoadItemDefinitions(string condition, object[] parameters,
            SafeDictionary<ID, PrefetchData> prefetchData)
        {
            try
            {
                ExecuteLoadItemDefinitionsSql(
                    "SELECT [ItemId], [Order], [Version], [Language], [Name], [Value], [FieldId], [MasterID], [ParentID]\r\n                     FROM (\r\n                        SELECT [Id] as [ItemId], 0 as [Order], 0 as [Version], '' as [Language], [Name], '' as [Value], [TemplateID] as [FieldId], [MasterID], [ParentID]\r\n                        FROM [Items]\r\n\r\n                        UNION ALL                          \r\n                        SELECT [ParentId] as [ItemId], 1 as [Order], 0 as [Version], '' as [Language], NULL as [Name], '', NULL, NULL, [Id]\r\n                        FROM [Items] \r\n\r\n                        UNION ALL \r\n                        SELECT [ItemId], 2 as [Order], 0 AS [Version], '' as [Language], NULL as [Name], [Value], [FieldId], NULL, NULL\r\n                        FROM [SharedFields] \r\n\r\n                        UNION ALL \r\n                        SELECT [ItemId], 2 as [Order], 0 AS [Version],       [Language], NULL as [Name], [Value], [FieldId], NULL, NULL\r\n                        FROM [UnversionedFields] \r\n\r\n                        UNION ALL \r\n                        SELECT [ItemId], 2 as [Order],      [Version],       [Language], NULL as [Name], [Value], [FieldId], NULL, NULL \r\n                        FROM [VersionedFields]\r\n                     ) as temp " +
                    " WHERE {0}ItemId{1} IN (SELECT {0}ID{1} FROM {0}Items{1} WITH (nolock) " + condition + ")" +
                    " \r\n                     ORDER BY [ItemId], [Order] ASC, [Language] DESC, [Version] DESC",
                    parameters, prefetchData);
            }
            catch (Exception ex)
            {
                Log.Error("Failed to load item data", ex, this);
                throw;
            }
        }

        /// <summary>Gets an SQL to load initial item definitions.</summary>
        /// <param name="sql">SQL-fragment.</param>
        /// <returns>Initial item definitions SQL.</returns>
        protected override string GetLoadInitialItemDefinitionsSql(string sql)
        {
            return
                "\r\nDECLARE @children TABLE (ID UNIQUEIDENTIFIER PRIMARY KEY (ID))\r\nINSERT INTO @children (ID)\r\nSELECT [ID]\r\nFROM [Items] WITH (NOLOCK)\r\nWHERE " +
                sql +
                "\r\n\r\nSELECT\r\n  [Id] AS [ItemId],\r\n  0 AS [Order],\r\n  0 AS [Version],\r\n  '' AS [Language],\r\n  [Name] AS [Name],\r\n  '' AS [Value],\r\n  [TemplateID] AS [FieldId],\r\n  [MasterID],\r\n  [ParentID],\r\n  [Created]\r\nFROM\r\n  [Items]\r\nWHERE\r\n  [Id] IN (SELECT [ID] FROM @children)\r\nUNION ALL\r\nSELECT\r\n  [ParentId] AS [ItemId],\r\n  1 AS [Order],\r\n  0 AS [Version],\r\n  '' AS [Language],\r\n  NULL AS [Name],\r\n  '' AS [Value],\r\n  NULL,\r\n  NULL,\r\n  [Id],\r\n  NULL AS [Created]\r\nFROM [Items]\r\nWHERE\r\n  [ParentId] IN (SELECT [ID] FROM @children)\r\nUNION ALL\r\nSELECT\r\n  [ItemId],\r\n  2 AS [Order],\r\n  0 AS [Version],\r\n  '' AS [Language],\r\n  NULL AS [Name],\r\n  [Value],\r\n  [FieldId],\r\n  NULL,\r\n  NULL,\r\n  NULL AS [Created]\r\nFROM [SharedFields]\r\nWHERE\r\n  [ItemId] IN (SELECT [ID] FROM @children)\r\nUNION ALL\r\nSELECT\r\n  [ItemId],\r\n  2 AS [Order],\r\n  0 AS [Version],\r\n  [Language],\r\n  NULL AS [Name],\r\n  [Value],\r\n  [FieldId],\r\n  NULL,\r\n  NULL,\r\n  NULL AS [Created]\r\nFROM [UnversionedFields]\r\nWHERE [ItemId] IN (SELECT [ID] FROM @children)\r\nUNION ALL\r\nSELECT\r\n  [ItemId],\r\n  2 AS [Order],\r\n  [Version],\r\n  [Language],\r\n  NULL AS [Name],\r\n  [Value],\r\n  [FieldId],\r\n  NULL,\r\n  NULL,\r\n  NULL AS [Created]\r\nFROM [VersionedFields]\r\nWHERE\r\n  [ItemId] IN (SELECT [ID] FROM @children)\r\nORDER BY\r\n  [ItemId],\r\n  [Order] ASC,\r\n  [Language] DESC,\r\n  [Version] DESC";
        }

        /// <summary>Returns an SQL to load child items definitions.</summary>
        /// <returns>Child items definitions SQL.</returns>
        protected override string GetLoadChildItemsDefinitionsSql()
        {
            return GetLoadInitialItemDefinitionsSql("{0}ParentID{1} = {2}itemId{3}");
        }

        /// <summary>Returns an SQL to load item definitions.</summary>
        /// <returns>Item definitions SQL.</returns>
        protected override string GetLoadItemDefinitionsSql()
        {
            return
                "\r\nSELECT\r\n  [ItemId],\r\n  [Order],\r\n  [Version],\r\n  [Language],\r\n  [Name],\r\n  [Value],\r\n  [FieldId],\r\n  [MasterID],\r\n  [ParentID],\r\n  [Created]\r\nFROM\r\n( SELECT\r\n    [Id] AS [ItemId],\r\n    0 AS [Order],\r\n    0 AS [Version],\r\n    '' AS [Language],\r\n    [Name],\r\n    '' AS [Value],\r\n    [TemplateID] AS [FieldId],\r\n    [MasterID],\r\n    [ParentID],\r\n    [Created]\r\n  FROM [Items]\r\n  UNION ALL\r\n  SELECT\r\n    [ParentId] AS [ItemId],\r\n    1 AS [Order],\r\n    0 AS [Version],\r\n    '' AS [Language],\r\n    NULL AS [Name],\r\n    '',\r\n    NULL,\r\n    NULL,\r\n    [Id],\r\n    NULL\r\n  FROM [Items]\r\n  UNION ALL\r\n  SELECT\r\n    [ItemId],\r\n    2 AS [Order],\r\n    0 AS [Version],\r\n    '' AS [Language],\r\n    NULL AS [Name],\r\n    [Value],\r\n    [FieldId],\r\n    NULL,\r\n    NULL,\r\n    NULL\r\n  FROM [SharedFields]\r\n  UNION ALL\r\n  SELECT\r\n    [ItemId],\r\n    2 AS [Order],\r\n    0 AS [Version],\r\n    [Language],\r\n    NULL AS [Name],\r\n    [Value],\r\n    [FieldId],\r\n    NULL,\r\n    NULL,\r\n    NULL\r\n  FROM [UnversionedFields]\r\n  UNION ALL\r\n  SELECT\r\n    [ItemId],\r\n    2 AS [Order],\r\n    [Version],\r\n    [Language],\r\n    NULL AS [Name],\r\n    [Value],\r\n    [FieldId],\r\n    NULL,\r\n    NULL,\r\n    NULL\r\n  FROM [VersionedFields]) AS temp\r\n  WHERE {0}ItemId{1} IN (SELECT {0}ID{1} FROM {0}Items{1} WITH (NOLOCK) WHERE {0}ID{1} = {2}itemId{3})\r\n  ORDER BY [ItemId], [Order] ASC, [Language] DESC, [Version] DESC";
        }

        /// <summary>Executes item definitions SQL.</summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <param name="prefetchData"></param>
        protected override void ExecuteLoadItemDefinitionsSql(string sql, object[] parameters,
            SafeDictionary<ID, PrefetchData> prefetchData)
        {
            var languages = GetLanguages();
            PrefetchData prefetchData1 = null;
            var flag1 = false;
            var flag2 = false;
            var id1 = ID.Null;
            var flag3 = false;
            var num1 = 5;
            while (true)
                try
                {
                    using (var reader = Api.CreateReader(sql, parameters))
                    {
                        while (reader.Read())
                        {
                            flag3 = true;
                            var id2 = Api.GetId(0, reader);
                            var num2 = Api.GetInt(1, reader);
                            if (num2 == 0)
                            {
                                var itemName = Api.GetString(4, reader);
                                var id3 = Api.GetId(6, reader);
                                var id4 = Api.GetId(7, reader);
                                var id5 = Api.GetId(8, reader);
                                var dateTime = Api.GetDateTime(9, reader);
                                flag1 = false;
                                id1 = id2;
                                if (prefetchData.ContainsKey(id2))
                                {
                                    prefetchData1 = prefetchData[id2];
                                }
                                else
                                {
                                    prefetchData1 =
                                        new PrefetchData(new ItemDefinition(id2, itemName, id3, id4, dateTime), id5);
                                    prefetchData[id2] = prefetchData1;
                                }
                            }
                            else if (id2 != id1 || prefetchData1 == null)
                            {
                                if (!flag2)
                                {
                                    Log.Error(
                                        $"Failed to get item information, ItemID: {((object)id2 == null ? (object)"NULL" : (object)id2.ToString())}",
                                        this);
                                    flag2 = true;
                                }
                            }
                            else if (num2 == 1)
                            {
                                var id3 = Api.GetId(8, reader);
                                prefetchData1.AddChildId(id3);
                            }
                            else
                            {
                                if (!flag1)
                                {
                                    prefetchData1.InitializeFieldLists(languages);
                                    flag1 = true;
                                }
                                var id3 = Api.GetId(6, reader);
                                var version = Api.GetInt(2, reader);
                                var language = Api.GetString(3, reader);
                                var str = Api.GetString(5, reader);
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
                    {
                        throw;
                    }
                }
        }

        /// <summary>Loads the item fields.</summary>
        /// <param name="itemCondition">The item condition.</param>
        /// <param name="fieldCondition">The field condition.</param>
        /// <param name="parameters">The parameters.</param>
        /// <param name="prefetchData">The working set.</param>
        protected override void LoadItemFields(string itemCondition, string fieldCondition, object[] parameters,
            SafeDictionary<ID, PrefetchData> prefetchData)
        {
        }

        /// <summary>Rebuilds the descendants table.</summary>
        protected override void RebuildDescendants()
        {
            DescendantsLock.AcquireWriterLock(-1);
            try
            {
                Factory.GetRetryer().ExecuteNoResult(() =>
                {
                    using (var transaction = Api.CreateTransaction())
                    {
                        Api.Execute(
                            "\r\nDECLARE @descendants TABLE (\r\n  {0}Ancestor{1} {0}uniqueidentifier{1} NOT NULL,\r\n  {0}Descendant{1} {0}uniqueidentifier{1} NOT NULL,\r\n  PRIMARY KEY({0}Ancestor{1}, {0}Descendant{1})\r\n);\r\n\r\nWITH TempSet({0}Ancestor{1}, {0}Descendant{1}) AS\r\n(\r\n  SELECT\r\n    {0}Items{1}.{0}ParentID{1},\r\n    {0}Items{1}.{0}ID{1}\r\n  FROM\r\n    {0}Items{1}\r\n  UNION ALL\r\n  SELECT\r\n    {0}Items{1}.{0}ParentID{1},\r\n    {0}TempSet{1}.{0}Descendant{1}\r\n  FROM\r\n    {0}Items{1} JOIN {0}TempSet{1}\r\n      ON {0}TempSet{1}.{0}Ancestor{1} = {0}Items{1}.{0}ID{1}\r\n)\r\nINSERT INTO @descendants({0}Ancestor{1}, {0}Descendant{1})\r\nSELECT\r\n  {0}TempSet{1}.{0}Ancestor{1},\r\n  {0}TempSet{1}.{0}Descendant{1}\r\nFROM\r\n  {0}TempSet{1}\r\nOPTION (MAXRECURSION 32767)\r\nMERGE {0}Descendants{1} AS {0}Target{1}\r\nUSING @descendants AS {0}Source{1}\r\nON (\r\n  {0}Target{1}.{0}Ancestor{1} = {0}Source{1}.{0}Ancestor{1}\r\n  AND {0}Target{1}.{0}Descendant{1} = {0}Source{1}.{0}Descendant{1}\r\n)\r\nWHEN NOT MATCHED BY TARGET THEN\r\n  INSERT ({0}ID{1}, {0}Ancestor{1}, {0}Descendant{1})\r\n  VALUES (NEWID(), {0}Source{1}.{0}Ancestor{1}, {0}Source{1}.{0}Descendant{1})\r\nWHEN NOT MATCHED BY SOURCE THEN\r\n  DELETE;\r\n");
                        transaction.Complete();
                    }
                });
            }
            catch (Exception ex)
            {
                Log.Error(ex.ToString(), this);
            }
            finally
            {
                DescendantsShouldBeUpdated = false;
                DescendantsLock.ReleaseWriterLock();
            }
            RebuildThread = null;
        }

        /// <summary>Removes the fields.</summary>
        /// <param name="itemId">The item id.</param>
        /// <param name="language">The language.</param>
        /// <param name="version">The version.</param>
        protected override void RemoveFields(ID itemId, Language language, Version version)
        {
            Api.Execute(
                "DELETE FROM {0}SharedFields{1}\r\n                  WHERE {0}ItemId{1} = {2}itemId{3}\r\n                \r\n                  DELETE FROM {0}UnversionedFields{1}\r\n                  WHERE {0}ItemId{1} = {2}itemId{3}\r\n                  AND {0}Language{1} = {2}language{3}\r\n                \r\n                  DELETE FROM {0}VersionedFields{1}\r\n                  WHERE {0}ItemId{1} = {2}itemId{3}\r\n                  AND {0}Language{1} = {2}language{3}\r\n                  AND {0}Version{1} = {2}version{3}",
                (object)"itemId", (object)itemId, (object)"language", (object)language, (object)"version",
                (object)version);
        }

        /// <summary>Gets the SQL which checks if BLOB should be deleted.</summary>
        /// <returns>SQL which checks if BLOB should be deleted.</returns>
        protected override string GetCheckIfBlobShouldBeDeletedSql()
        {
            return
                "\r\nIF EXISTS (SELECT NULL FROM {0}SharedFields{1} WITH (NOLOCK) WHERE {0}SharedFields{1}.{0}Value{1} LIKE {2}blobId{3})\r\nBEGIN\r\n  SELECT 1\r\nEND\r\nELSE IF EXISTS (SELECT NULL FROM {0}VersionedFields{1} WITH (NOLOCK) WHERE {0}VersionedFields{1}.{0}Value{1} LIKE {2}blobId{3})\r\nBEGIN\r\n  SELECT 1\r\nEND\r\nELSE IF EXISTS (SELECT NULL FROM {0}ArchivedFields{1} WITH (NOLOCK) WHERE {0}ArchivedFields{1}.{0}Value{1} LIKE {2}blobId{3})\r\nBEGIN\r\n  SELECT 1\r\nEND\r\nELSE IF EXISTS (SELECT NULL FROM {0}UnversionedFields{1} WITH (NOLOCK) WHERE {0}UnversionedFields{1}.{0}Value{1} LIKE {2}blobId{3})\r\nBEGIN\r\n  SELECT 1\r\nEND";
        }

        /// <summary>Gets the size of a BLOB.</summary>
        /// <param name="blobId">The BLOB id.</param>
        /// <returns>The get blob size.</returns>
        private long GetBlobSize(Guid blobId)
        {
            using (var connection = OpenConnection())
            {
                var sqlCommand = new SqlCommand(" SELECT SUM(DATALENGTH([Data])) FROM [Blobs] WHERE [BlobId] = @blobId",
                    connection)
                { CommandTimeout = (int)CommandTimeout.TotalSeconds };
                sqlCommand.Parameters.AddWithValue("@blobId", blobId);
                using (var reader = sqlCommand.ExecuteReader(CommandBehavior.CloseConnection))
                {
                    if (!reader.Read() || reader.IsDBNull(0))
                        return -1;
                    return SqlServerHelper.GetLong(reader, 0);
                }
            }
        }

        /// <summary>Opens the BLOB stream.</summary>
        /// <param name="blobId">The BLOB id.</param>
        /// <param name="blobSize">Size of the BLOB.</param>
        /// <returns>Stream object.</returns>
        private Stream OpenBlobStream(Guid blobId, long blobSize)
        {
            var cmdText =
                "SELECT [Data]\r\n                     FROM [Blobs]\r\n                     WHERE [BlobId] = @blobId\r\n                     ORDER BY [Index]";
            var connection = OpenConnection();
            try
            {
                var sqlCommand =
                    new SqlCommand(cmdText, connection) { CommandTimeout = (int)CommandTimeout.TotalSeconds };
                sqlCommand.Parameters.AddWithValue("@blobId", blobId);
                var reader =
                    sqlCommand.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.CloseConnection);
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

        /// <summary>Opens a connection to the database.</summary>
        /// <returns>SqlConnection object.</returns>
        private SqlConnection OpenConnection()
        {
            var sqlConnection = new SqlConnection(ConnectionString);
            sqlConnection.Open();
            return sqlConnection;
        }
    }
}