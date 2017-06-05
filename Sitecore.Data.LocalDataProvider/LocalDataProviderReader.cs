using System;
using System.IO;
using Sitecore.Data.DataProviders.Sql;
using Sitecore.Diagnostics;

namespace Sitecore.Data.LocalDataProvider
{
    public class LocalDataProviderReader : DataProviderReader
    {
        private readonly FileStream _stream;
        public LocalDataProviderReader(DataProviderCommand command, string file, string queryText) : base(command)
        {
            _stream = new FileStream(file, FileMode.OpenOrCreate);
            SetFileMark();
            _stream.Write(queryText);
            _stream.Write(InnerReader.FieldCount);
            for (int i = 0; i < InnerReader.FieldCount; i++)
            {
                Type type = InnerReader.GetFieldType(i);
                byte[] buffer = new byte[1];
                if (!StreamExtension.TypeMap.ContainsKey(type))
                    throw new Exception($"Can't read the type:{type}");
                buffer[0] = StreamExtension.TypeMap[type];
                string name = InnerReader.GetName(i);
                _stream.Write(buffer, 0, 1);
                _stream.Write(name);
            }
        }
        public void SetFileMark()
        {
            _stream.Write(StreamExtension.FileMark);
        }
        public override bool Read()
        {
            var flag = InnerReader.Read();
            if (flag)
                FillStream();
            return flag;
        }
        public virtual void FillStream()
        {
            _stream.Write(InnerReader.FieldCount);
            for (int i = 0; i < InnerReader.FieldCount; i++)
            {
                if (InnerReader.IsDBNull(i))
                {
                    _stream.Write(new byte[] { 0 }, 0, 1);
                    continue;
                }
                _stream.Write(new byte[] { 1 }, 0, 1);
                Type type = InnerReader[i].GetType();
                if (typeof(Guid) == type)
                {
                    Guid guid = InnerReader.GetGuid(i);
                    _stream.Write(guid);
                    continue;
                }
                if (typeof(int) == type)
                {
                    int val = InnerReader.GetInt32(i);
                    _stream.Write(val);
                    continue;
                }
                if (typeof(DateTime) == type)
                {
                    DateTime val = InnerReader.GetDateTime(i);
                    _stream.Write(val);
                    continue;
                }
                if (typeof(byte[]) == type)
                {
                    byte[] bytes = (byte[])InnerReader[i];
                    _stream.Write(bytes, true);
                    continue;
                }
                if (typeof(string) == type)
                {
                    string val = InnerReader.GetString(i);
                    _stream.Write(val);
                    continue;
                }
                if (typeof(long) == type)
                {
                    long val = InnerReader.GetInt64(i);
                    _stream.Write(val);
                    continue;

                }
                if (typeof(bool) == type)
                {
                    bool val = InnerReader.GetBoolean(i);
                    _stream.Write(val);
                    continue;
                }
                Log.Error($"Can't read data type:{type.FullName} form IDataReader", new Exception(type.FullName));
            }
        }
        public override void Dispose()
        {
            _stream.Close();
            _stream.Dispose();
            base.Dispose();
        }
    }
}