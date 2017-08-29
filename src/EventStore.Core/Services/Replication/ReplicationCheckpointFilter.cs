using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Common.Log;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.TimerService;
using EventStore.Core.TransactionLog.Checkpoint;

namespace EventStore.Core.Services.Replication
{
    public class ReplicationCheckpointFilter : IHandle<ReplicationMessage.ReplicationAckCheckTick>
    {
        public ILogger Log = LogManager.GetLoggerFor<ReplicationCheckpointFilter>();
        private readonly IPublisher _outputBus;
        private readonly IPublisher _publisher;
        private readonly IEnvelope _busEnvelope;
        private readonly ICheckpoint _replicationCheckpoint;
        private readonly TimeSpan TimeoutPeriod = new TimeSpan(1000);

        private SortedDictionary<long, List<Message>> _messages;

        public ReplicationCheckpointFilter(IPublisher outputBus, IPublisher publisher, ICheckpoint replicationCheckpoint)
        {
            _outputBus = outputBus;
            _publisher = publisher;
            _replicationCheckpoint = replicationCheckpoint;
            _busEnvelope = new PublishEnvelope(_publisher);
            _messages = new SortedDictionary<long, List<Message>>();
            _publisher.Publish(TimerMessage.Schedule.Create(TimeoutPeriod, _busEnvelope, new ReplicationMessage.ReplicationAckCheckTick()));
        }

        public void Handle(StorageMessage.EventCommitted message)
        {
            Log.Debug("Handling event committed at {0}", message.CommitPosition);
            var replChk = _replicationCheckpoint.ReadNonFlushed();
            if(message.CommitPosition > replChk)
            {
                Log.Debug("New event has not been replicated yet. R: {0}, C: {1}", replChk, message.CommitPosition);
                Enqueue(message, message.CommitPosition);
            }
            else
            {
                _outputBus.Publish(message);
            }
        }

        public void Handle(ReplicationMessage.ReplicationAckCheckTick message)
        {
            HandleMessages();
            _publisher.Publish(TimerMessage.Schedule.Create(TimeoutPeriod, _busEnvelope, message));
        }

        private void Enqueue(Message message, long commitPosition)
        {
            if(_messages.ContainsKey(commitPosition))
            {
                _messages[commitPosition].Add(message);
            }
            else
            {
                _messages.Add(commitPosition, new List<Message>{ message });
            }
        }

        private void HandleMessages()
        {
            var replChk = _replicationCheckpoint.ReadNonFlushed();
            var messagesToHandle = _messages.Where(x=>x.Key <= replChk).ToList();
            foreach(var m in messagesToHandle)
            {
                Log.Debug("Handle messages at {0}. Got {1} events", m.Key, m.Value.Count());
                foreach(var i in m.Value)
                {
                    Log.Debug("Publishing message");
                    _outputBus.Publish(i);
                }
                _messages.Remove(m.Key);
            }
        }
    }
}