// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System.Threading;
using System.Threading.Tasks;
using DotNetCore.CAP.Persistence;

namespace MaiKeBing.CAP.LiteDB
{
    internal class LiteDBStorageInitializer : IStorageInitializer
    {
        public string GetLockTableName()
        {
            return nameof(LiteDBStorage.PublishedMessages);
        }

        public string GetPublishedTableName()
        {
            return nameof(LiteDBStorage.PublishedMessages);
        }

        public string GetReceivedTableName()
        {
            return nameof(LiteDBStorage.ReceivedMessages);
        }

        public Task InitializeAsync(CancellationToken cancellationToken)
        {
             return Task.CompletedTask;
        }
    }
}