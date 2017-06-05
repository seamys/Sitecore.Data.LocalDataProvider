using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using Sitecore.Diagnostics;

namespace Sitecore.Data.LocalDataProvider
{
    public class StreamDataReader : IDataReader
    {
        private readonly FileStream _stream;
        private object[] _current;
        private readonly Type[] _fieldTypes;
        private readonly string[] _fieldNames;
        public string QueryText { get; }
        private void ThrowFileCantRead(string file)
        {
            throw new Exception($"Can't read file :{file}, It's not data cache file");
        }

        public bool IsSitecoreCacheData()
        {
            int length = _stream.GetInt32();
            if (length == StreamExtension.FileMarkLength)
            {
                _stream.Position = 0;
                string mark = _stream.GetString();
                if (mark == StreamExtension.FileMark)
                    return true;
            }
            return false;
        }

        public StreamDataReader(string file)
        {
            _stream = new FileStream(file, FileMode.OpenOrCreate, FileAccess.Read);
            if (!IsSitecoreCacheData())
                ThrowFileCantRead(file);
            QueryText = _stream.GetString();
            //Skip length,value.
            var maps = StreamExtension.TypeMap;
            //Get table filed length
            FieldCount = _stream.GetInt32();
            _fieldTypes = new Type[FieldCount];
            _fieldNames = new string[FieldCount];
            for (int i = 0; i < FieldCount; i++)
            {
                byte btBype = _stream.GetByte();
                if (!maps.ContainsValue(btBype))
                    ThrowFileCantRead(file);
                _fieldTypes[i] = maps.FirstOrDefault(x => x.Value == btBype).Key;
                _fieldNames[i] = _stream.GetString();
            }
            RecordsAffected = 0;
        }

        public object this[string name]
        {
            get
            {
                int i = GetOrdinal(name);
                return GetValue(i);
            }
        }

        public object this[int i] => GetValue(i);

        public int Depth => 1;

        public int FieldCount { get; }

        public bool IsClosed => _stream.CanRead;

        public int RecordsAffected { get; private set; }

        public void Close()
        {
            _stream.Close();
        }

        public void Dispose()
        {
            _stream.Dispose();
        }

        public bool GetBoolean(int i)
        {
            object obj = GetValue(i);
            return (bool)obj;
        }

        public byte GetByte(int i)
        {
            object obj = GetValue(i);
            return (byte)obj;
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            return Char.Parse(GetValue(i).ToString());
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public IDataReader GetData(int i)
        {
            throw new NotImplementedException();
        }

        public string GetDataTypeName(int i)
        {
            return GetFieldType(i).FullName;
        }

        public DateTime GetDateTime(int i)
        {
            var obj = GetValue(i);
            return (DateTime)obj;
        }

        public decimal GetDecimal(int i)
        {
            var obj = GetValue(i);
            return (decimal)obj;
        }

        public double GetDouble(int i)
        {
            var obj = GetValue(i);
            return (double)obj;
        }

        public Type GetFieldType(int i)
        {
            if (_fieldTypes.Length > i && i >= 0)
                return _fieldTypes[i];
            return null;
        }

        public float GetFloat(int i)
        {
            var obj = GetValue(i);
            return (float)obj;
        }

        public Guid GetGuid(int i)
        {
            var obj = GetValue(i);
            return (Guid)obj;
        }

        public short GetInt16(int i)
        {
            var obj = GetValue(i);
            return (short)obj;
        }

        public int GetInt32(int i)
        {
            var obj = GetValue(i);
            return (int)obj;
        }

        public long GetInt64(int i)
        {
            var obj = GetValue(i);
            return (long)obj;
        }

        public string GetName(int i)
        {
            if (_fieldNames.Length > i && i >= 0)
                return _fieldNames[i];
            return null;
        }

        public int GetOrdinal(string name)
        {
            for (int i = 0; i < _fieldNames.Length; i++)
            {
                if (_fieldNames[i] == name)
                    return i;
            }
            return -1;
        }

        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public string GetString(int i)
        {
            var obj = GetValue(i);
            return (obj ?? "").ToString();
        }

        public object GetValue(int i)
        {
            if (_current != null && _current.Length > i && i >= 0)
                return _current[i];
            return null;
        }

        public int GetValues(object[] values)
        {
            return FieldCount;
        }

        public bool IsDBNull(int i)
        {
            if (_current != null && _current.Length > i)
                return _current[i] == null;
            return true;
        }

        public bool NextResult()
        {
            return Read();
        }

        public bool Read()
        {
            _current = Read(FieldCount);
            RecordsAffected++;
            return _current != null;
        }

        public object[] Read(int length)
        {
            int len = _stream.GetInt32();
            if (len != length)
                return null;
            object[] list = new object[length];
            for (int i = 0; i < length; i++)
            {
                byte isNull = _stream.GetByte();
                if (isNull == 0)
                {
                    list[i] = null;
                    continue;
                }
                Type type = GetFieldType(i);
                if (type == typeof(byte[]))
                {
                    list[i] = _stream.GetBytes();
                    continue;
                }
                if (type == typeof(string))
                {
                    list[i] = _stream.GetString();
                    continue;
                }
                if (type == typeof(int))
                {
                    list[i] = _stream.GetInt32();
                    continue;
                }
                if (type == typeof(long))
                {
                    list[i] = _stream.GetInt64();
                    continue;
                }
                if (type == typeof(DateTime))
                {
                    list[i] = _stream.GetDateTime();
                    continue;
                }
                if (type == typeof(Guid))
                {
                    list[i] = _stream.GetGuid();
                    continue;
                }
                if (type == typeof(bool))
                {
                    list[i] = _stream.GetBoolean();
                    continue;
                }
                Log.Error(type.FullName, new Exception("Can't match the type form byte."));
            }
            return list;
        }
    }
}