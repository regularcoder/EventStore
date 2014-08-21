﻿using System;
using System.Net;
using System.Text;
using System.Threading;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;

namespace CompetingPlayground
{
    class Program
    {
        private const string Stream = "stream";
        private const string SubName = "subscription3";
        static void Main(string[] args)
        {
            BasicTest();
        }

        private static void BasicTest()
        {
            var endpoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 1113);
            using (var connection = EventStoreConnection.Create(endpoint, "foo"))
            {
                connection.ConnectAsync().Wait();
                CreateSubscription(connection, SubName);
                var sub = ConnectToSubscription(connection, "sub1");
                var sub2 = ConnectToSubscription(connection, "sub2");
                WriteEvents(connection);
                
                Thread.Sleep(5000);
                sub.Stop(TimeSpan.FromSeconds(5));
                WriteEvents(connection);
                sub2.Stop(TimeSpan.FromSeconds(5));
                Thread.Sleep(TimeSpan.FromSeconds(5));
                //DeleteSubscription(connection, SubName);
            }
        }

        
        private static EventStorePersistentSubscription ConnectToSubscription(IEventStoreConnection connection, string name)
        {
            return connection.ConnectToPersistentSubscription(SubName, Stream,
                (sub, ev) =>
                {
                    //Thread.Sleep(1000);
                    Console.WriteLine(name + "received: " + ev.OriginalEventNumber);
                },
                (sub, ev, ex) => Console.WriteLine(name + "sub dropped " + ev),
                bufferSize: 12, autoAck: true);
        }

        private static void WriteEvents(IEventStoreConnection connection)
        {
            for (int i = 0; i < 1000; i++)
            {
                connection.AppendToStreamAsync(Stream, ExpectedVersion.Any,
                    new EventData(Guid.NewGuid(), "test", true, Encoding.UTF8.GetBytes("{'foo' : 'bar'}"), new byte[0])).Wait();
            }
        }

        private static void DeleteSubscription(IEventStoreConnection connection, string name)
        {
            try
            {
                connection.DeletePersistentSubscriptionAsync(Stream, name, new UserCredentials("admin", "changeit")).Wait();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Unable to delete : " + ex);
            }
        }


        private static void CreateSubscription(IEventStoreConnection connection, string name)
        {
            try
            {
                connection.CreatePersistentSubscriptionAsync(Stream, name, true,
                    new UserCredentials("admin", "changeit")).Wait();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Unable to create : " + ex);
            }
        }
    }
}