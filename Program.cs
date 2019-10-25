using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using NPoco;
using NPoco.RowMappers;

namespace NpocoTuples {
    class Program {
        static void Main(string[] args) {
            var config = new ConfigurationBuilder()
                .AddJsonFile(Path.Join(Environment.CurrentDirectory, "appsettings.json"))
                .Build();
            var connectionString = config.GetConnectionString("mssql");
            
            var db = new Database(connectionString, DatabaseType.SqlServer2012, SqlClientFactory.Instance);
            MappingFactory.RowMappers.Insert(0, () => new ValueTupleRowMapper(new MapperCollection()));


            // var test = db.Single<string>("select 'test' as str");
            // Console.WriteLine($"Value: {test}");

            var sql = "select 1, 'hello', 16 union all select 2, 'goodbye', 32 union all select 3, null, null";
            foreach (var (x, y, b) in db.Fetch<(int x, string y, byte? b)>(sql)) {
                Console.WriteLine($"Value: ({x}, {y}, {b})");
            }

            var sql2 = "select * from (values ('a', 97), ('b', 98), ('c', 99), ('d', 100)) ascii (chr, [asc])";
            foreach (var (chr, asc) in db.Fetch<(char chr, byte asc)>(sql2)) {
                Console.WriteLine($"chr({asc}) = '{chr}';");
            }
        }
    }

    class ValueTupleRowMapper : IRowMapper
    {
        private Func<DbDataReader, object> mapper;
        private MapperCollection mappers;
        private static Cache<(Type, MapperCollection), Func<DbDataReader, object>> cache
             = new Cache<(Type, MapperCollection), Func<DbDataReader, object>>();

        public ValueTupleRowMapper(MapperCollection mappers) {
            this.mappers = mappers;
        }

        public void Init(DbDataReader dataReader, PocoData pocoData) {
            mapper = GetRowMapper(pocoData.Type, this.mappers);
        }

        public object Map(DbDataReader dataReader, RowMapperContext context) {
            return mapper(dataReader);
        }

        public static bool IsValueTuple(Type type) {
            if (!type.IsGenericType)
                return false;
                
            var baseType = type.GetGenericTypeDefinition();
            return (
                baseType == typeof(ValueTuple<>) || 
                baseType == typeof(ValueTuple<,>) || 
                baseType == typeof(ValueTuple<,,>) || 
                baseType == typeof(ValueTuple<,,,>) || 
                baseType == typeof(ValueTuple<,,,,>) || 
                baseType == typeof(ValueTuple<,,,,,>) || 
                baseType == typeof(ValueTuple<,,,,,,,>)
            );
        }

        public bool ShouldMap(PocoData pocoData) {
            return IsValueTuple(pocoData.Type);
        }

        private static Func<DbDataReader, object> GetRowMapper(Type type, MapperCollection mappers) {
            return cache.Get((type, mappers), () => CreateRowMapper(type, mappers));
        }

        private static Func<DbDataReader, object> CreateRowMapper(Type type, MapperCollection mappers) {
            var argTypes = type.GetGenericArguments();
            var ctor = type.GetConstructor(argTypes);
            var reader = Expression.Parameter(typeof(DbDataReader), "reader");
            var getValue = typeof(DbDataReader).GetMethod("GetValue")!;
            var isDBNull = typeof(DbDataReader).GetMethod("IsDBNull")!;

            var expr = Expression.Lambda(
                Expression.Convert(
                    Expression.New(ctor, argTypes.Select((argType, i) => {
                        // reader.IsDBNull(i) ? null : converter(reader.GetValue(i))
                        // reader.IsDBNull(i) ? (T)null : converter(reader.GetValue(i))
                        return Expression.Condition(
                            Expression.Call(reader, isDBNull, new [] { Expression.Constant(i) }),
                            Expression.Convert(Expression.Constant(null), argType),
                            Expression.Convert(
                                Expression.Invoke(
                                    Expression.Constant(
                                        MappingHelper.GetConverter(mappers, null, null, argType)
                                    ),
                                    new[] {
                                        Expression.Call(reader, getValue, new[] { Expression.Constant(i) } )
                                    }
                                ),
                                argType
                            )
                        );
                    })),
                    typeof(object)
                ),
                new [] { reader }
            );
            return (Func<DbDataReader, object>)expr.Compile();
        }
    }
}
