using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Services.Storage.EpochManager;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Core.Data;

namespace EventStore.Core.Tests.Services.ElectionsService
{
    public class FakeEpochManager: IEpochManager
    {
        private Dictionary<int, EpochRecord> _epochs;

        public void SetEpochs(Epoch[] epochs)
        {
            _epochs = new Dictionary<int, EpochRecord>();
            var prevEpochNumber = -1;
            foreach(var e in epochs)
            {
                _epochs.Add(e.EpochNumber, new EpochRecord(e.EpochPosition, e.EpochNumber, e.EpochId, prevEpochNumber, DateTime.Now));
                prevEpochNumber = e.EpochNumber;
            }
        }

        public int LastEpochNumber { get { return -1; } }

        public void Init()
        {
        }

        public EpochRecord GetLastEpoch()
        {
            return null;
        }

        public EpochRecord[] GetLastEpochs(int maxCount)
        {
            throw new NotImplementedException();
        }

        public EpochRecord GetEpoch(int epochNumber, bool throwIfNotFound)
        {
            if(_epochs == null) return null;
            return _epochs[epochNumber];
        }

        public EpochRecord GetEpochWithAllEpochs(int epochNumber, bool throwIfNotFound)
        {
            throw new NotImplementedException();
        }

        public bool IsCorrectEpochAt(long epochPosition, int epochNumber, Guid epochId)
        {
            return true;
        }

        public void WriteNewEpoch()
        {
            throw new NotImplementedException();
        }

        public void SetLastEpoch(EpochRecord epoch)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<EpochRecord> GetCachedEpochs()
        {
            throw new NotImplementedException();
        }
    }
}