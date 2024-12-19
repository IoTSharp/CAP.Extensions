// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DotNetCore.CAP;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Monitoring;
using DotNetCore.CAP.Persistence;
using DotNetCore.CAP.Serialization;
using LiteDB;
using Microsoft.Extensions.Options;

namespace MaiKeBing.CAP.LiteDB
{
    internal class LiteDBStorage : IDataStorage
    {
        private readonly IOptions<CapOptions> _capOptions;
        private readonly  LiteDBOptions  _ldboption;
        private readonly ISnowflakeId _snowflakeId;
        private readonly ISerializer _serializer;
        LiteDatabase _lite;
        public LiteDBStorage(IOptions<CapOptions> capOptions, IOptions<LiteDBOptions> ldboption,ISerializer serializer, ISnowflakeId snowflakeId)
        {
            _capOptions = capOptions;
            _ldboption = ldboption.Value;
            _snowflakeId = snowflakeId;
            _serializer = serializer;
            _lite = new LiteDatabase(_ldboption.ConnectionString);
            PublishedMessages = _lite.GetCollection<LiteDBMessage>(nameof(PublishedMessages));
            PublishedMessages.EnsureIndex( l => l.DbId,true);
            ReceivedMessages = _lite.GetCollection<LiteDBMessage>(nameof(ReceivedMessages));
            ReceivedMessages.EnsureIndex(l => l.DbId, true);
        }

        public static ILiteCollection< LiteDBMessage> PublishedMessages { get; private set; } 

        public static ILiteCollection<  LiteDBMessage> ReceivedMessages { get; private set; }  

        public Task ChangePublishStateAsync(MediumMessage message, StatusName state)
        {
            var msg = PublishedMessages.FindOne(l => l.DbId == message.DbId);
            msg.StatusName = state;
            msg.ExpiresAt = message.ExpiresAt;
            PublishedMessages.Update(msg);
            return Task.CompletedTask;
        }

        public Task ChangeReceiveStateAsync(MediumMessage message, StatusName state)
        {
            var msg = ReceivedMessages.FindOne(l => l.DbId == message.DbId);
            msg.StatusName = state;
            msg.ExpiresAt = message.ExpiresAt;
            ReceivedMessages.Update(msg);
            return Task.CompletedTask;
        }

        public MediumMessage StoreMessage(string name, Message content, object dbTransaction = null)
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

            PublishedMessages.Insert(new LiteDBMessage()
            {
                DbId = message.DbId,
                Name = name,
                Content = message.Content,
                Retries = message.Retries,
                Added = message.Added,
                ExpiresAt = message.ExpiresAt,
                StatusName = StatusName.Scheduled
            });
            return message;
        }

        public void StoreReceivedExceptionMessage(string name, string group, string content)
        {
            var id =    _snowflakeId.NextId().ToString();

            ReceivedMessages.Insert( new LiteDBMessage
            {
                 DbId = id,
                Group = group,
                Name = name,
                Content = content,
                Retries = _capOptions.Value.FailedRetryCount,
                Added = DateTime.Now,
                ExpiresAt = DateTime.Now.AddDays(15),
                StatusName = StatusName.Failed
            });
        }

        public MediumMessage StoreReceivedMessage(string name, string @group, Message message)
        {
            var mdMessage = new MediumMessage
            {
                DbId = _snowflakeId.NextId().ToString(),
                Origin = message,
                Added = DateTime.Now,
                ExpiresAt = null,
                Retries = 0
            };

            ReceivedMessages.Insert ( new LiteDBMessage(mdMessage.Origin)
            {
                 DbId = mdMessage.DbId,
                Group = group,
                Name = name,
                Content =_serializer.Serialize(mdMessage.Origin),
                Retries = mdMessage.Retries,
                Added = mdMessage.Added,
                ExpiresAt = mdMessage.ExpiresAt,
                StatusName = StatusName.Scheduled
            });
            return mdMessage;
        }

        public Task<int> DeleteExpiresAsync(string table, DateTime timeout, int batchCount = 1000, CancellationToken token = default)
        {
            var removed = 0;
            if (table == nameof(PublishedMessages))
            {
                removed = PublishedMessages.DeleteMany(x => x.ExpiresAt < timeout);
               
            }
            else
            {
                removed = ReceivedMessages.DeleteMany(x => x.ExpiresAt < timeout);
                
            }
            return Task.FromResult(removed);
        }

        public Task<IEnumerable<MediumMessage>> GetPublishedMessagesOfNeedRetry()
        {
            var ret = PublishedMessages
                .Find(x => x.Retries < _capOptions.Value.FailedRetryCount
                            && x.Added < DateTime.Now.AddSeconds(-10)
                            && (x.StatusName == StatusName.Scheduled || x.StatusName == StatusName.Failed))
                .Take(200)
                .Select(x => (MediumMessage)x);

            foreach (var message in ret)
            {
                message.Origin =  System.Text.Json.JsonSerializer.Deserialize<Message>(message.Content);
            }

            return Task.FromResult(ret);
        }

        public Task<IEnumerable<MediumMessage>> GetReceivedMessagesOfNeedRetry()
        {
            var ret = ReceivedMessages
                 .Find(x => x.Retries < _capOptions.Value.FailedRetryCount
                             && x.Added < DateTime.Now.AddSeconds(-10)
                             && (x.StatusName == StatusName.Scheduled || x.StatusName == StatusName.Failed))
                 .Take(200)
                 .Select(x => (MediumMessage)x);

            foreach (var message in ret)
            {
                message.Origin = System.Text.Json.JsonSerializer.Deserialize<Message>(message.Content);
            }

            return Task.FromResult(ret);
        }

        public IMonitoringApi GetMonitoringApi()
        {
            return new LiteDBMonitoringApi();
        }

        public Task<bool> AcquireLockAsync(string key, TimeSpan ttl, string instance, CancellationToken token = default)
        {
            return Task.FromResult(true);
        }

        public Task ReleaseLockAsync(string key, string instance, CancellationToken token = default)
        {
            return Task.FromResult(true);
        }

        public Task RenewLockAsync(string key, TimeSpan ttl, string instance, CancellationToken token = default)
        {
           return Task.CompletedTask;
        }

        public Task ChangePublishStateToDelayedAsync(string[] ids)
        {
            foreach (var id in ids)
            {
                var msg = PublishedMessages.FindById(id);
                msg.StatusName = StatusName.Delayed;
                PublishedMessages.Update(msg);
            }

            return Task.CompletedTask;
        }

        public Task ChangePublishStateAsync(MediumMessage message, StatusName state, object transaction = null)
        {
            var msg = PublishedMessages.FindById(message.DbId);
                msg.StatusName = state;
            msg.ExpiresAt = message.ExpiresAt;
            msg.Content = _serializer.Serialize(message.Origin);
            PublishedMessages.Update(msg);  
            return Task.CompletedTask;
        }

        public Task<MediumMessage> StoreMessageAsync(string name, Message content, object transaction = null)
        {
            var message = new MediumMessage
            {
                DbId = content.GetId(),
                Origin = content,
                Content = _serializer.Serialize(content),
                Added = DateTime.Now,
                ExpiresAt = null,
                Retries = 0
            };

           var msg= new LiteDBMessage
            {
                DbId = message.DbId,
                Name = name,
                Origin = message.Origin,
                Content = message.Content,
                Retries = message.Retries,
                Added = message.Added,
                ExpiresAt = message.ExpiresAt,
                StatusName = StatusName.Scheduled
            };
            PublishedMessages.Upsert(msg);
            return Task.FromResult(message);
        }

        public Task StoreReceivedExceptionMessageAsync(string name, string group, string content)
        {
            var id = _snowflakeId.NextId().ToString();

          var msg= new LiteDBMessage()
          {
                DbId = id,
                Group = group,
                Origin = null!,
                Name = name,
                Content = content,
                Retries = _capOptions.Value.FailedRetryCount,
                Added = DateTime.Now,
                ExpiresAt = DateTime.Now.AddSeconds(_capOptions.Value.FailedMessageExpiredAfter),
                StatusName = StatusName.Failed
            };
            PublishedMessages.Upsert(msg);
            return Task.CompletedTask;
        }

        public Task<MediumMessage> StoreReceivedMessageAsync(string name, string group, Message content)
        {
            var mdMessage = new MediumMessage
            {
                DbId = _snowflakeId.NextId().ToString(),
                Origin =   content,
                Added = DateTime.Now,
                ExpiresAt = null,
                Retries = 0
            };

           var msg= new LiteDBMessage()
           {
                DbId = mdMessage.DbId,
                Origin = mdMessage.Origin,
                Group = group,
                Name = name,
                Content = _serializer.Serialize(mdMessage.Origin),
                Retries = mdMessage.Retries,
                Added = mdMessage.Added,
                ExpiresAt = mdMessage.ExpiresAt,
                StatusName = StatusName.Scheduled
            };
            PublishedMessages.Upsert(msg);
            return Task.FromResult(mdMessage);
        }

        public Task<IEnumerable<MediumMessage>> GetPublishedMessagesOfNeedRetry(TimeSpan lookbackSeconds)
        {
            IEnumerable<MediumMessage> result = PublishedMessages.Find(x => x.Retries < _capOptions.Value.FailedRetryCount
            && x.Added < DateTime.Now.Subtract(lookbackSeconds)
                          && (x.StatusName == StatusName.Scheduled || x.StatusName == StatusName.Failed))
              .Take(200)
              .Select(x => (MediumMessage)x).ToList();
            return Task.FromResult(result);
        }

        public Task ScheduleMessagesOfDelayedAsync(Func<object, IEnumerable<MediumMessage>, Task> scheduleTask, CancellationToken token = default)
        {
            var result = PublishedMessages.Find(x =>
              (x.StatusName == StatusName.Delayed && x.ExpiresAt < DateTime.Now.AddMinutes(2))
              || (x.StatusName == StatusName.Queued && x.ExpiresAt < DateTime.Now.AddMinutes(-1)))
          .Select(x => (MediumMessage)x);

            return scheduleTask(null!, result);
        }

        public Task<IEnumerable<MediumMessage>> GetReceivedMessagesOfNeedRetry(TimeSpan lookbackSeconds)
        {
            IEnumerable<MediumMessage> result = PublishedMessages.Find(x => x.Retries < _capOptions.Value.FailedRetryCount
                             && x.Added < DateTime.Now.Subtract(lookbackSeconds)
                             && (x.StatusName == StatusName.Scheduled || x.StatusName == StatusName.Failed))
                 .Take(200)
                 .Select(x => (MediumMessage)x).ToList();

            return Task.FromResult(result);
        }
    }
}