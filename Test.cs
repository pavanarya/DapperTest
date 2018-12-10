using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Dapper;
using DapperExtensions;
using DapperExtensions.Mapper;
using HiretechAdmin.DatabaseServices.IRepository;
using HiretechAdmin.Utility;
using Elasticsearch.Net;
using MongoDB.Driver;
using Nest;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace HiretechAdmin.DatabaseServices
{
    public enum RnDCommandType
    {
        Text = 1,
        StoredProcedure = 2
    }

    public abstract class SailsBaseRepository : IBaseRepository
    {
        private readonly IIXLogger _logger;
        //private readonly string _i9ConnectionString = ConfigurationManager.ConnectionStrings["connectionString_I9"].ConnectionString;
        //private readonly string _wageverifyConnectionString = ConfigurationManager.ConnectionStrings["connectionString_Wageverify"].ConnectionString;
        //private readonly string _authDbConnectionString = ConfigurationManager.ConnectionStrings["connectionString_AuthDB"].ConnectionString;
        //private readonly string _ucmConnectionString = ConfigurationManager.ConnectionStrings["connectionString_UCM"].ConnectionString;


        public SailsBaseRepository(IIXLogger logger)
        {
            _logger = logger;
        }


        public IMongoDatabase MongoDbConnection()
        {
            try
            {
                var mongoConnectionString = ConfigurationManager.ConnectionStrings["mongoConnection"].ConnectionString;
                var client = new MongoClient(mongoConnectionString);
                var database = client.GetDatabase("I9");
                return database;
            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message + "------" + ex.StackTrace);
                throw;
            }
        }

        public ElasticLowLevelClient ElasticConnection()
        {
            try
            {
                var uris = new[]
                {
                    new Uri(ConfigurationManager.ConnectionStrings["elasticConnection"].ConnectionString)
                };

                var connectionPool = new SniffingConnectionPool(uris);
                var settings = new ConnectionConfiguration(connectionPool);

                var lowlevelClient = new ElasticLowLevelClient(settings);
                return lowlevelClient;
            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message + "------" + ex.StackTrace);
                throw;
            }
        }

        public ElasticClient EsClient()
        {
            var nodes = new[]
            {
                new Uri(ConfigurationManager.ConnectionStrings["elasticConnection"].ConnectionString)
            };

            var connectionPool = new StaticConnectionPool(nodes);
            var connectionSettings = new ConnectionSettings(connectionPool);

            var elasticClient = new ElasticClient(connectionSettings);
            return elasticClient;
        }

        public async Task<T> UpsertAsync<T>(T entity, Enum product) where T : class
        {
            try
            {
                if (entity == null) return null;

                return await WithConnection(async c =>
                {
                    var t = typeof(T);
                    var mapper = DapperExtensions.DapperExtensions.GetMap<T>();

                    int id = 0;
                    foreach (var prop in mapper.Properties)
                    {
                        if (prop.KeyType == KeyType.Identity)
                        {
                            var pro = t.GetProperty(prop.Name);
                            if (pro != null)
                            {
                                id = Convert.ToInt32(pro.GetValue(entity, null));
                                break;
                            }
                        }
                    }

                    if (id > 0)
                    {
                       var entityInDb =  await GetAsync<T>(id, product);
                        if (entityInDb != null)
                        {
                            await UpdateAsync(entity, product);
                            return entity;
                        }
                        else
                        {
                            return await InsertAsync(entity, product);
                        }
                    }
                    else
                    {
                        return await InsertAsync(entity, product);
                    }
                    
                }, product);
            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message + "------" + ex.StackTrace);
                throw;
            }
        }


        public async Task<T> InsertAsync<T>(T entity, Enum product) where T : class
        {
            try
            {
                if (entity == null) return null;

                var assemblies = new List<Assembly> { Assembly.GetAssembly(typeof(T)) };
                DapperAsyncExtensions.SetMappingAssemblies(assemblies);

                var t = typeof(T);
                var proModifiedDtTime = t.GetProperty("ModifiedOn");
                if (proModifiedDtTime != null) proModifiedDtTime.SetValue(entity, DateTime.Now, null);

                var proCreatedDtTime = t.GetProperty("CreatedOn");
                if (proCreatedDtTime != null) proCreatedDtTime.SetValue(entity, DateTime.Now, null);

                return await WithConnection(async c =>
                {
                    var id = await c.InsertAsync(entity);
                    return await GetAsync<T>(id,product);
                }, product);
            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message + "------" + ex.StackTrace);
                throw;
            }
        }

        public async Task<IEnumerable<decimal>> InsertMultipleAsync<T>(List<T> entities, Enum product)
            where T : class
        {
            try
            {
                if (!entities.Any()) return null;
                var assemblies = new List<Assembly> { Assembly.GetAssembly(typeof(T)) };
                DapperAsyncExtensions.SetMappingAssemblies(assemblies);

                var t = typeof(T);
                var mapper = DapperExtensions.DapperExtensions.GetMap<T>();

                var tableName = mapper.TableName;

                var query = "insert into " + tableName + " (##columns)  values (##values);SELECT SCOPE_IDENTITY();";

                var columns = new List<string>();
                var values = new List<string>();

                for (var i = 0; i < entities.Count; i++)
                {
                    foreach (var prop in mapper.Properties)
                    {
                        if (prop.KeyType == KeyType.Identity) continue;
                        columns.Add(prop.ColumnName);
                        values.Add("@" + prop.Name + i);
                    }

                    query = query.Replace("##columns", string.Join(",", columns))
                        .Replace("##values", string.Join(",", values));
                }

                var p = new DynamicParameters();
                for (var i = 0; i < entities.Count; i++)
                    foreach (var prop in mapper.Properties)
                    {
                        if (prop.KeyType == KeyType.Identity) continue;
                        columns.Add(prop.ColumnName);
                        values.Add(prop.Name + i);

                        var pro = t.GetProperty(prop.Name);
                        if (pro != null) p.Add(prop.Name + i, pro.GetValue(entities[i], null));
                    }


                return await WithConnection(async c =>
                {
                    var aa = await c.QueryMultipleAsync(query, p, commandType: CommandType.Text);
                    var identityColumns = await aa.ReadAsync<decimal>();
                    return identityColumns;
                }, product);
            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message + "------" + ex.StackTrace);
                throw;
            }
        }


        public async Task<bool> UpdateAsync<T>(T entity, Enum product) where T : class
        {
            try
            {
                if (entity == null) return false;

                var assemblies = new List<Assembly> { Assembly.GetAssembly(typeof(T)) };
                DapperAsyncExtensions.SetMappingAssemblies(assemblies);

                var t = typeof(T);
                var pro = t.GetProperty("ModifiedOn");
                if (pro != null) pro.SetValue(entity, DateTime.Now, null);

                return await WithConnection(async c =>
                {
                    await c.UpdateAsync(entity);
                    return true;
                }, product);

            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message + "------" + ex.StackTrace);
                throw;
            }
        }

        public async Task<bool> UpdateMultipleAsync<T>(List<T> entities, Enum product) where T : class
        {
            try
            {
                if (!entities.Any()) return false;

                var mapper = DapperExtensions.DapperExtensions.GetMap<T>();

                var tableName = mapper.TableName;
                var keyColumn = mapper.Properties.SingleOrDefault(i => i.KeyType == KeyType.Identity);

                var keyColumnName = keyColumn != null ? keyColumn.ColumnName : "";

                var query = "update " + tableName + " set ##columns where " + keyColumnName + " = @" + keyColumnName;

                var columns = new List<string>();
                foreach (var prop in mapper.Properties)
                {
                    if (prop.KeyType == KeyType.Identity) continue;
                    columns.Add(prop.ColumnName + "= @" + prop.Name);
                }

                query = query.Replace("##columns", string.Join(",", columns));

                var parameterList = from entity in entities select new DynamicParameters(entity);


                return await WithConnection(async c =>
                {
                    await c.ExecuteAsync(query, parameterList);
                    return true;
                }, product);
            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message + "------" + ex.StackTrace);
                throw;
            }
        }

        public async Task<bool> DeleteAsync<T>(T entity, Enum product) where T : class
        {
            try
            {
                if (entity == null) return false;

                var assemblies = new List<Assembly> { Assembly.GetAssembly(typeof(T)) };
                DapperAsyncExtensions.SetMappingAssemblies(assemblies);


                return await WithConnection(async c =>
                {
                    await c.DeleteAsync(entity);
                    return true;
                }, product);
            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message + "------" + ex.StackTrace);
                throw;
            }
        }

        public async Task<T> GetAsync<T>(long id, Enum product) where T : class
        {
            var assemblies = new List<Assembly> { Assembly.GetAssembly(typeof(T)) };
            DapperAsyncExtensions.SetMappingAssemblies(assemblies);

            return await WithConnection(async c => await c.GetAsync<T>(id), product);

        }

        public async Task<T> GetAsync<T>(object parameters, Enum product) where T : class
        {
            var assemblies = new List<Assembly> { Assembly.GetAssembly(typeof(T)) };
            DapperAsyncExtensions.SetMappingAssemblies(assemblies);

            string query = "Select * from [{tablename}] ";
            query = query.Replace("{tablename}", typeof(T).Name);

            //var whereclause = parameters.GetType().GetProperties().Select(prop => prop.Name + "=" + "@" + prop.Name).ToList();

            List<string> whereclause = new List<string>();
            foreach (var prop in parameters.GetType().GetProperties())
            {
                var propValue = prop.GetValue(parameters, null);

                if (propValue == null)
                {
                    whereclause.Add(prop.Name + " is null");
                }
                else
                {
                    whereclause.Add(prop.Name + "=" + "@" + prop.Name);
                }
            }
            if (whereclause.Any())
            {
                query = query + " where " + string.Join(" and ", whereclause);
            }

            return await WithConnection(async c => await c.QueryFirstOrDefaultAsync<T>(query, parameters, commandType: CommandType.Text), product);
        }


        public async Task<IEnumerable<T>> GetListAsync<T>(object parameters, Enum product) where T : class
        {
            var assemblies = new List<Assembly> { Assembly.GetAssembly(typeof(T)) };
            DapperAsyncExtensions.SetMappingAssemblies(assemblies);

            string query = "Select * from [{tablename}] ";
            query = query.Replace("{tablename}", typeof(T).Name);

            List<string> whereclause = new List<string>();
            foreach (var prop in parameters.GetType().GetProperties())
            {
                var propValue = prop.GetValue(parameters, null);

                if (propValue == null)
                {
                    whereclause.Add(prop.Name + " is null");
                }
                else
                {
                    whereclause.Add(prop.Name + "=" + "@" + prop.Name);
                }
            }
            if (whereclause.Any())
            {
                query = query + " where " + string.Join(" and ", whereclause);
            }

            return await WithConnection(async c => await c.QueryAsync<T>(query,parameters, commandType: CommandType.Text), product);
        }

        public async Task<SqlMapper.GridReader> GetMultipleDataAsync(string query, object parameters,
            Enum product)
        {
            SqlMapper.GridReader data;

            return await WithConnection(async c =>
            {
                data = await c.QueryMultipleAsync(query, parameters, commandType: CommandType.StoredProcedure);
                return data;
            }, product);

        }

        //This method the order of select clauses in proc and the entity should be same
        public async Task<T> GetMultipleDataAsync<T>(string query, object parameters, RnDCommandType operationType,
            Enum product)
            where T : class
        {
            var returnResult = Activator.CreateInstance(typeof(T));




            return await WithConnection(async con =>
            {
                SqlMapper.GridReader data;

                if (operationType == RnDCommandType.StoredProcedure)
                    data = await con.QueryMultipleAsync(query, parameters, commandType: CommandType.StoredProcedure);
                else
                    data = await con.QueryMultipleAsync(query, parameters, commandType: CommandType.Text);

                foreach (var eachProperty in typeof(T).GetProperties())
                {

                    var attribute = eachProperty.GetCustomAttributes(typeof(DescriptionAttribute), true)
                        .FirstOrDefault();
                    if (attribute != null && ((DescriptionAttribute)attribute).Description == "ignoreMapping")
                    {
                        continue;
                    }

                    var type = eachProperty.PropertyType;
                    var isListType = false;
                    if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>))
                    {
                        type = type.GetGenericArguments()[0];

                        isListType = true;
                    }

                    var resultData = data.Read(type).ToList();

                    try
                    {
                        if (resultData.Any())
                            eachProperty.SetValue(returnResult,
                                isListType ? ConvertList(resultData, eachProperty.PropertyType) : resultData[0],
                                null);
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(ex.Message + "------" + ex.StackTrace);
                    }
                }
                return (T)returnResult;
            }, product);


        }

        public async Task<IEnumerable<T>> GetListAsync<T>(Enum product) where T : class
        {
            var assemblies = new List<Assembly> { Assembly.GetAssembly(typeof(T)) };
            DapperAsyncExtensions.SetMappingAssemblies(assemblies);

            return await WithConnection(async c => await c.GetListAsync<T>(), product);

        }

        public abstract string GetConnectionString(Enum product);

        public async Task<T> WithConnection<T>(Func<IDbConnection, Task<T>> getData, Enum product)
        {
            try
            {
                string connectionString = GetConnectionString(product);

                using (var connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();
                    return await getData(connection);
                }
            }
            catch (TimeoutException ex)
            {
                throw new TimeoutException("Timeout Exception", ex);
            }
            catch (SqlException ex)
            {
                throw new DBConcurrencyException("Sql Exception", ex);
            }
        }

        public async Task<IEnumerable<T>> GetAsync<T>(string procedureName, object parameters,
            RnDCommandType operationType, Enum product)
        {
            try
            {
                return await WithConnection(async c =>
                {
                    // map the result from stored procedure to Employee data model
                    if (operationType == RnDCommandType.StoredProcedure)
                    {
                        var results = await c.QueryAsync<T>(procedureName, parameters,
                            commandType: CommandType.StoredProcedure);
                        return results;
                    }
                    else
                    {
                        var results = await c.QueryAsync<T>(procedureName, parameters, commandType: CommandType.Text);
                        return results;
                    }
                }, product);
            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message + "------" + ex.StackTrace);
                throw;
            }
        }


        private async Task DapperMultiMapper<T>(string procedureName, Type[] types, Func<object[], T> map, object parameters,
            RnDCommandType operationType,string splitOn, Enum product)
        {
            try
            {
                await WithConnection(async c =>
                {
                    // map the result from stored procedure to Employee data model
                    if (operationType == RnDCommandType.StoredProcedure)
                    {
                        var results = await c.QueryAsync(procedureName,types,map, parameters,
                            commandType: CommandType.StoredProcedure, splitOn: splitOn);
                        return results;
                    }
                    else
                    {
                        var results = await c.QueryAsync(procedureName, types, map, parameters, commandType: CommandType.Text, splitOn: splitOn);
                        return results;
                    }
                }, product);
            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message + "------" + ex.StackTrace);
                throw;
            }
        }

        public async Task GetComplexDataUsingMapAsync<T>(string query, object parameters,
            RnDCommandType operationType, string breakOn,Type[] types, Func<object[], T> map, Enum product)
        {
            try
            {
                await DapperMultiMapper(query, types, map, parameters, RnDCommandType.Text, breakOn, product);
            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message + "------" + ex.StackTrace);
                throw;
            }
        }


        // This should be used when we dont have any data to return from query
        public async Task<int> ExecuteAsync(string procedureName, object parameters, RnDCommandType operationType,
            Enum product)
        {
            try
            {
                return await WithConnection(async c =>
                {
                    if (operationType == RnDCommandType.StoredProcedure)
                    {
                        var results = await c.ExecuteAsync(procedureName, parameters,
                            commandType: CommandType.StoredProcedure);
                        return results;
                    }
                    else
                    {
                        var results = await c.ExecuteAsync(procedureName, parameters, commandType: CommandType.Text);
                        return results;
                    }
                }, product);
            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message + "------" + ex.StackTrace);
                throw;
            }
        }

        private static object ConvertList(List<object> value, Type type)
        {
            var list = (IList)Activator.CreateInstance(type);
            foreach (var item in value)
                list.Add(item);
            return list;
        }
    }
}
