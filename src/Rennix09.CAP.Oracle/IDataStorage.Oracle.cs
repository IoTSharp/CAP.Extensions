// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DotNetCore.CAP;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Monitoring;
using DotNetCore.CAP.Persistence;
using DotNetCore.CAP.Serialization;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Options;
using Oracle.ManagedDataAccess.Client;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

namespace Rennix09.CAP.Oracle
{
    public class OracleDataStorage : IDataStorage
    {
        private readonly IOptions<OracleOptions> _options;
        private readonly IOptions<CapOptions> _capOptions;
        private readonly IStorageInitializer _initializer;
        private readonly string _pubName;
        private readonly string _recName;
        private readonly string _lockName;
        private readonly ISerializer _serializer;
        private readonly ISnowflakeId _snowflakeId;

        public OracleDataStorage(
            IOptions<OracleOptions> options,
            IOptions<CapOptions> capOptions,
            IStorageInitializer initializer,
             ISerializer serializer, 
        ISnowflakeId snowflakeId)
        {
            _options = options;
            _capOptions = capOptions;
            _initializer = initializer;
            _pubName = initializer.GetPublishedTableName();
            _recName = initializer.GetReceivedTableName();
            _serializer = serializer;
            _snowflakeId = snowflakeId;
            _lockName = initializer.GetLockTableName();
        }

        public async Task ChangePublishStateToDelayedAsync(string[] ids)
        {
            var sql =$@"UPDATE ""{_pubName}"" SET ""Retries"" = :P_Retries,""ExpiresAt"" = :P_ExpiresAt,""StatusName"" = :P_StatusName WHERE ""Id"" = :P_Id";
            using var connection = new OracleConnection(_options.Value.ConnectionString);
            using var tran = connection.BeginTransaction();
            try
            {
                foreach (var id in ids)
                {
                    object[] sqlParams = { new OracleParameter(":P_Id", id) };
                    await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams).ConfigureAwait(false);
                }
                await tran.CommitAsync();
            }
            catch (Exception)
            {
                await tran.RollbackAsync();
                throw;
            }
        }

        public async Task ChangePublishStateAsync(MediumMessage message, StatusName state) =>
         await ChangeMessageStateAsync(_pubName, message, state);

        public async Task ChangeReceiveStateAsync(MediumMessage message, StatusName state) =>
            await ChangeMessageStateAsync(_recName, message, state);

        public async Task<MediumMessage> StoreMessageAsync(string name, Message content, object dbTransaction = null)
        {
            var message = new MediumMessage
            {
                DbId = content.GetId(),
                Origin = content,
                Content = System.Text.Json.JsonSerializer.Serialize(content),
                Added = DateTime.Now,
                ExpiresAt = null,
                Retries = 0
            };

            var sql = $"INSERT INTO \"{_pubName}\"(\"Id\",\"Version\",\"Name\",\"Content\",\"Retries\",\"Added\",\"ExpiresAt\",\"StatusName\")" +
                      $" VALUES(:P_Id,'{_options.Value.Version}',:P_Name,:P_Content,:P_Retries,:P_Added,:P_ExpiresAt,:P_StatusName)";

            object[] sqlParams =
            {
                new OracleParameter(":P_Id", message.DbId),
                new OracleParameter(":P_Name", name),
                new OracleParameter(":P_Content", message.Content),
                new OracleParameter(":P_Retries", message.Retries),
                new OracleParameter(":P_Added", message.Added),
                new OracleParameter(":P_ExpiresAt", message.ExpiresAt.HasValue ? (object)message.ExpiresAt.Value : DBNull.Value),
                new OracleParameter(":P_StatusName", nameof(StatusName.Scheduled)),
            };

            if (dbTransaction == null)
            {
                using var connection = new OracleConnection(_options.Value.ConnectionString);
             await   connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams).ConfigureAwait(false);
            }
            else
            {
                var dbTrans = dbTransaction as IDbTransaction;
                if (dbTrans == null && dbTransaction is IDbContextTransaction dbContextTrans)
                {
                    dbTrans = dbContextTrans.GetDbTransaction();
                }

                var conn = (DbConnection)dbTrans?.Connection;
                await conn.ExecuteNonQueryAsync(sql, sqlParams: sqlParams).ConfigureAwait(false);
            }

            return message;
        }

        public Task StoreReceivedExceptionMessageAsync(string name, string group, string content)
        {
            object[] sqlParams =
            {
                new OracleParameter(":P_Id", _snowflakeId.NextId().ToString()),
                new OracleParameter(":P_Name", name),
                new OracleParameter(":P_Group", group),
                new OracleParameter(":P_Content", content),
                new OracleParameter(":P_Retries", _capOptions.Value.FailedRetryCount),
                new OracleParameter(":P_Added", DateTime.Now),
                new OracleParameter(":P_ExpiresAt", DateTime.Now.AddDays(15)),
                new OracleParameter(":P_StatusName", nameof(StatusName.Failed))
            };

            StoreReceivedMessage(sqlParams, DateTime.Now, DateTime.Now.AddDays(15));
            return Task.CompletedTask;
        }

        public Task<MediumMessage> StoreReceivedMessageAsync(string name, string group, Message message)
        {
            var mdMessage = new MediumMessage
            {
                DbId = _snowflakeId.NextId().ToString(),
                Origin = message,
                Added = DateTime.Now,
                ExpiresAt = null,
                Retries = 0
            };

            object[] sqlParams =
            {
                new OracleParameter(":P_Id", mdMessage.DbId),
                new OracleParameter(":P_Name", name),
                new OracleParameter(":P_Group", group),
                new OracleParameter(":P_Content", System.Text.Json.JsonSerializer.Serialize(mdMessage.Origin)),
                new OracleParameter(":P_Retries", mdMessage.Retries),
                new OracleParameter(":P_Added", mdMessage.Added),
                new OracleParameter(":P_ExpiresAt", mdMessage.ExpiresAt.HasValue ? (object) mdMessage.ExpiresAt.Value : DBNull.Value),
                new OracleParameter(":P_StatusName", nameof(StatusName.Scheduled))
            };

            StoreReceivedMessage(sqlParams, mdMessage.Added, mdMessage.ExpiresAt);
            return  Task.FromResult( mdMessage);
        }

        public async Task<int> DeleteExpiresAsync(string table, DateTime timeout, int batchCount = 1000, CancellationToken token = default)
        {
            using var connection = new OracleConnection(_options.Value.ConnectionString);
   
            var sql = $@"DELETE FROM ""{table}"" WHERE ""ExpiresAt"" < :timeout AND ROWNUM <= :batchCount";
            return await  connection.ExecuteNonQueryAsync(sql, null, new OracleParameter(":timeout", timeout), new OracleParameter(":batchCount", batchCount)).ConfigureAwait(false);
        }

        public async Task<IEnumerable<MediumMessage>> GetPublishedMessagesOfNeedRetry() =>
            await GetMessagesOfNeedRetryAsync(_pubName);

        public async Task<IEnumerable<MediumMessage>> GetReceivedMessagesOfNeedRetry() =>
            await GetMessagesOfNeedRetryAsync(_recName);

        public IMonitoringApi GetMonitoringApi()
        {
            return new OracleMonitoringApi(_options, _initializer);
        }

        private async Task ChangeMessageStateAsync(string tableName, MediumMessage message, StatusName state, object dbTransaction=null)
        {
            var sql =
                $@"UPDATE ""{tableName}"" SET ""Retries"" = :P_Retries,""ExpiresAt"" = :P_ExpiresAt,""StatusName"" = :P_StatusName WHERE ""Id"" = :P_Id";

            object[] sqlParams =
            {
                new OracleParameter(":P_Retries", message.Retries),
                new OracleParameter(":P_ExpiresAt", message.ExpiresAt),
                new OracleParameter(":P_StatusName", state.ToString("G")),
                new OracleParameter(":P_Id", message.DbId)
            };
            if (dbTransaction == null)
            {
                using var connection = new OracleConnection(_options.Value.ConnectionString);
                await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams).ConfigureAwait(false);
            }
            else
            {
                var dbTrans = dbTransaction as IDbTransaction;
                if (dbTrans == null && dbTransaction is IDbContextTransaction dbContextTrans)
                {
                    dbTrans = dbContextTrans.GetDbTransaction();
                }

                var conn = dbTrans?.Connection as DbConnection;
                await conn.ExecuteNonQueryAsync(sql, sqlParams: sqlParams).ConfigureAwait(false);
            }
        }

        private void StoreReceivedMessage(object[] sqlParams, DateTime added, DateTime? expiresAt)
        {
            var sql = $@"INSERT INTO ""{ _recName}"" (""Id"",""Version"",""Name"",""Group"",""Content"",""Retries"",""Added"",""ExpiresAt"",""StatusName"")
                      VALUES(:P_Id,'{_options.Value.Version}',:P_Name,:P_Group,:P_Content,:P_Retries,:P_Added,:P_ExpiresAt,:P_StatusName)";

            using var connection = new OracleConnection(_options.Value.ConnectionString);
            _ = connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams);
        }

        private async Task<IEnumerable<MediumMessage>> GetMessagesOfNeedRetryAsync(string tableName)
        {
            var fourMinAgo = DateTime.Now.AddMinutes(-4);
            var sql =
                $"SELECT \"Id\",\"Content\",\"Retries\",\"Added\" FROM \"{tableName}\" WHERE \"Retries\"<{_capOptions.Value.FailedRetryCount} " +
                $"AND \"Version\"='{_capOptions.Value.Version}' AND \"Added\"<:P_FourMinAgo AND (\"StatusName\" = '{StatusName.Failed}' OR \"StatusName\" = '{StatusName.Scheduled}') AND ROWNUM <= 200";

            using var connection = new OracleConnection(_options.Value.ConnectionString);
            var result = await connection.ExecuteReaderAsync(sql, reader =>
            {
                var messages = new List<MediumMessage>();
                while (reader.Read())
                {
                    messages.Add(new MediumMessage
                    {
                        DbId = reader.GetInt64(0).ToString(),
                        Origin = System.Text.Json.JsonSerializer.Deserialize<Message>(reader.GetString(1)),
                        Retries = reader.GetInt32(2),
                        Added = reader.GetDateTime(3)
                    });
                }

                return Task.FromResult(messages);
            },null, new OracleParameter(":P_FourMinAgo", fourMinAgo)).ConfigureAwait(false);

            return await Task.FromResult(result);
        }

        public async Task ChangePublishStateAsync(MediumMessage message, StatusName state, object transaction)
        {
            await ChangeMessageStateAsync(_pubName, message, state,transaction);
        }

        public    async Task  ScheduleMessagesOfDelayedAsync(Func<object, IEnumerable<MediumMessage>, Task> scheduleTask, CancellationToken token)
        {
            var sql =
           $"SELECT Id,Content,Retries,Added,ExpiresAt FROM {_pubName} WITH (UPDLOCK,READPAST) WHERE Version=:P_Version " +
           $"AND ((ExpiresAt< :P_TwoMinutesLater AND StatusName = '{StatusName.Delayed}') OR (ExpiresAt< :P_OneMinutesAgo AND StatusName = '{StatusName.Queued}'))";

            object[] sqlParams =
            {
            new OracleParameter(":P_Version", _capOptions.Value.Version),
            new OracleParameter(":P_TwoMinutesLater", DateTime.Now.AddMinutes(2)),
            new OracleParameter(":P_OneMinutesAgo", DateTime.Now.AddMinutes(-1)),
        };

            using var connection = new OracleConnection(_options.Value.ConnectionString);
            await connection.OpenAsync(token);
            using var transaction = connection.BeginTransaction();
            var messageList =await connection.ExecuteReaderAsync(sql, reader =>
            {
                var messages = new List<MediumMessage>();
                while (reader.Read())
                {
                    messages.Add(new MediumMessage
                    {
                        DbId = reader.GetInt64(0).ToString(),
                        Origin = _serializer.Deserialize(reader.GetString(1))!,
                        Retries = reader.GetInt32(2),
                        Added = reader.GetDateTime(3),
                        ExpiresAt = reader.GetDateTime(4)
                    });
                }
                return Task.FromResult(messages);
            }, transaction, sqlParams);
            await scheduleTask(transaction, messageList);
            await transaction.CommitAsync(token);
        }

        public async Task<bool> AcquireLockAsync(string key, TimeSpan ttl, string instance,
               CancellationToken token = default)
        {
            var sql =
                $"UPDATE `{_lockName}` SET `Instance`=:Instance,`LastLockTime`=:LastLockTime WHERE `Key`=:Key AND `LastLockTime` < :TTL;";
            var connection = new OracleConnection(_options.Value.ConnectionString);
            await using var _ = connection.ConfigureAwait(false);
            object[] sqlParams =
            {
            new OracleParameter(":Instance", instance),
            new OracleParameter(":LastLockTime", DateTime.Now),
            new OracleParameter(":Key", key),
            new OracleParameter(":TTL", DateTime.Now.Subtract(ttl))
        };
            var opResult = await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams).ConfigureAwait(false);
            return opResult > 0;
        }

        public async Task ReleaseLockAsync(string key, string instance, CancellationToken token = default)
        {
            var sql =
                $"UPDATE `{_lockName}` SET `Instance`='',`LastLockTime`=:LastLockTime WHERE `Key`=:Key AND `Instance`=:Instance;";
            var connection = new OracleConnection(_options.Value.ConnectionString);
            await using var _ = connection.ConfigureAwait(false);
            object[] sqlParams =
            {
            new OracleParameter(":Instance", instance),
            new OracleParameter(":LastLockTime", DateTime.MinValue),
            new OracleParameter(":Key", key)
        };
            await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams).ConfigureAwait(false);
        }

        public async Task RenewLockAsync(string key, TimeSpan ttl, string instance, CancellationToken token = default)
        {
            var sql =
                $"UPDATE `{_lockName}` SET `LastLockTime`= date_add(`LastLockTime`, interval {ttl.TotalSeconds} second) WHERE `Key`=:Key AND `Instance`=:Instance;";
            var connection = new OracleConnection(_options.Value.ConnectionString);
            await using var _ = connection.ConfigureAwait(false);
            object[] sqlParams =
            {
            new OracleParameter(":Instance", instance),
            new OracleParameter(":Key", key)
        };
            await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams).ConfigureAwait(false);
        }
 
           

        public Task<IEnumerable<MediumMessage>> GetPublishedMessagesOfNeedRetry(TimeSpan lookbackSeconds)
        {
            return GetMessagesOfNeedRetryAsync(_pubName, lookbackSeconds);
        }

        public Task<IEnumerable<MediumMessage>> GetReceivedMessagesOfNeedRetry(TimeSpan lookbackSeconds)
        {
            return GetMessagesOfNeedRetryAsync(_recName, lookbackSeconds);
        }
         private async Task StoreReceivedMessage(object[] sqlParams)
        {
            var sql =
                $@"INSERT INTO `{_recName}`(`Id`,`Version`,`Name`,`Group`,`Content`,`Retries`,`Added`,`ExpiresAt`,`StatusName`) " +
                $"VALUES(:Id,'{_options.Value.Version}',:Name,:Group,:Content,:Retries,:Added,:ExpiresAt,:StatusName);";

            var connection = new OracleConnection(_options.Value.ConnectionString);
            await using var _ = connection.ConfigureAwait(false);
            await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams).ConfigureAwait(false);
        }

        private async Task<IEnumerable<MediumMessage>> GetMessagesOfNeedRetryAsync(string tableName, TimeSpan lookbackSeconds)
        {
            var fourMinAgo = DateTime.Now.Subtract(lookbackSeconds);
            var sql =
                $"SELECT `Id`,`Content`,`Retries`,`Added` FROM `{tableName}` WHERE `Retries`<:Retries " +
                $"AND `Version`=:Version AND `Added`<:Added AND `StatusName` IN ('{StatusName.Failed}','{StatusName.Scheduled}') LIMIT 200;";

            object[] sqlParams =
            {
            new OracleParameter(":Retries", _capOptions.Value.FailedRetryCount),
            new OracleParameter(":Version", _capOptions.Value.Version),
            new OracleParameter(":Added", fourMinAgo)
        };

            var connection = new OracleConnection(_options.Value.ConnectionString);
            await using var _ = connection.ConfigureAwait(false);
            var result = await connection.ExecuteReaderAsync(sql, async reader =>
            {
                var messages = new List<MediumMessage>();
                while (await reader.ReadAsync().ConfigureAwait(false))
                {
                    messages.Add(new MediumMessage
                    {
                        DbId = reader.GetInt64(0).ToString(),
                        Origin = _serializer.Deserialize(reader.GetString(1))!,
                        Retries = reader.GetInt32(2),
                        Added = reader.GetDateTime(3)
                    });
                }

                return messages;
            }, sqlParams: sqlParams).ConfigureAwait(false);

            return result;
        }
    }
}
