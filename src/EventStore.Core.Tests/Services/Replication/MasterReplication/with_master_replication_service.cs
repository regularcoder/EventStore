using System;
using System.Net;
using System.IO;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Services.Replication;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Tests.Bus.Helpers;
using EventStore.Core.Tests.Services.ElectionsService;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Core.Services.Transport.Tcp;
using EventStore.Core.Authentication;
using EventStore.Core.Tests.Services.Transport.Tcp;
using EventStore.Core.Tests.Authentication;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using EventStore.Common.Utils;
using NUnit.Framework;

namespace EventStore.Core.Tests.Services.Replication.MasterReplication
{
    public abstract class with_master_replication_service : SpecificationWithDirectoryPerTestFixture
    {
        protected MasterReplicationService Service;

        protected Guid MasterId = Guid.NewGuid();

        protected TFChunkDb Db;
        protected TFChunkWriter Writer;

        protected int ClusterSize = 3;
        protected FakeEpochManager EpochManager;
        protected InMemoryBus Publisher;
        protected InMemoryBus TcpSendPublisher;

        protected TestHandler<Message> PublisherConsumer = new TestHandler<Message>();
        protected TestHandler<Message> TcpSendPublisherConsumer = new TestHandler<Message>();

        [OneTimeSetUp]
        public void SetUp()
        {
            CreateDb();
            EpochManager = new FakeEpochManager();

            Publisher = new InMemoryBus("publisher");
            Publisher.Subscribe(PublisherConsumer);
            TcpSendPublisher = new InMemoryBus("tcpSendPublisher");
            TcpSendPublisher.Subscribe(TcpSendPublisherConsumer);

            Service = new MasterReplicationService(Publisher, MasterId, Db, TcpSendPublisher, EpochManager, ClusterSize);

            When();
        }

        private void CreateDb()
        {
            string dbPath = Path.Combine(PathName, string.Format("mini-node-db-{0}", Guid.NewGuid()));

            var writerCheckpoint = new InMemoryCheckpoint();
            var chaserCheckpoint = new InMemoryCheckpoint();
            var replicationCheckpoint = new InMemoryCheckpoint(-1);
            Db = new TFChunkDb(new TFChunkDbConfig(dbPath,
                                                   new VersionedPatternFileNamingStrategy(dbPath, "chunk-"),
                                                   TFConsts.ChunkSize,
                                                   0,
                                                   writerCheckpoint,
                                                   chaserCheckpoint,
                                                   new InMemoryCheckpoint(-1),
                                                   new InMemoryCheckpoint(-1),
                                                   replicationCheckpoint,
                                                   inMemDb: true));

            Db.Open();

            Writer = new TFChunkWriter(Db);
            Writer.Open();

            WriteTestScenario();

            Writer.Close();
            Writer = null;

            writerCheckpoint.Flush();
            chaserCheckpoint.Write(writerCheckpoint.Read());
            chaserCheckpoint.Flush();
        }

        public abstract void When();
        public abstract void WriteTestScenario();

        protected EventRecord WriteSingleEvent(string eventStreamId, long eventNumber, string data)
        {
            var prepare = LogRecord.SingleWrite(Db.Config.WriterCheckpoint.ReadNonFlushed(),
                                                Guid.NewGuid(),
                                                Guid.NewGuid(),
                                                eventStreamId,
                                                eventNumber - 1,
                                                "test-type",
                                                Helper.UTF8NoBom.GetBytes(data),
                                                null,
                                                DateTime.Now);
            long pos;
            Assert.IsTrue(Writer.Write(prepare, out pos));
            var commit = LogRecord.Commit(Db.Config.WriterCheckpoint.ReadNonFlushed(), prepare.CorrelationId, prepare.LogPosition, eventNumber);
            Assert.IsTrue(Writer.Write(commit, out pos));

            var eventRecord = new EventRecord(eventNumber, prepare);
            return eventRecord;
        }
    }

    public class TestReplicationSubscription
    {
        private int _connectionPendingSendBytesThreshold = 10 * 1024 * 1024;

        public Guid MasterId;
        public TcpConnectionManager Connection;
        public IPEndPoint Endpoint;

        public TestReplicationSubscription(IPEndPoint endpoint)
        {
            Endpoint = endpoint;

            var dummyConnection = new DummyTcpConnection();
            var authProvider = new InternalAuthenticationProvider(new Core.Helpers.IODispatcher(InMemoryBus.CreateTest(), new NoopEnvelope()), new StubPasswordHashAlgorithm(), 1);
            Connection = new TcpConnectionManager(
                Guid.NewGuid().ToString(), TcpServiceType.External, new InternalTcpDispatcher(),
                InMemoryBus.CreateTest(), dummyConnection, InMemoryBus.CreateTest(), authProvider,
                TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10), (man, err) => { }, _connectionPendingSendBytesThreshold , true);
        }

        public ReplicationMessage.ReplicaSubscriptionRequest CreateSubscriptionRequest(Guid masterId, Guid subscriptionId, long subscribePosition, Guid chunkId, Epoch[] lastEpochs)
        {
            return new ReplicationMessage.ReplicaSubscriptionRequest(Guid.NewGuid(), new FakeEnvelope(),
                Connection, subscribePosition, chunkId, lastEpochs, Endpoint, masterId, subscriptionId, true);
        }
    }
}