using System;
using System.Diagnostics;

namespace Buffy
{
    class Program
    {
        static void Main(string[] args)
        {
            // The number of clients to simulate.
            const int NumClients = 10;

            // The number of trials to run.
            const int NumTrials = 10000;

            // Used to randomly request buffers with a fixed set of sizes.
            var rand = new Random();
            var messageSizes = new[] { 31, 60, 63, 121, 250, 501, 1001 };

            // Configure the pool.
            var pool = new BufferPool(pageSize: 1024, blockSize: 32);

            // Each client needs to cache a buffer.
            var clients = new ArraySegment<byte>[NumClients];

            for (var trial = 0; trial < NumTrials; trial++)
            {
                // Get the buffers.
                for (var clientIndex = 0; clientIndex < NumClients; clientIndex++)
                {
                    var messageSizeIndex = rand.Next(messageSizes.Length);
                    var buffer = pool.GetBuffer(messageSizes[messageSizeIndex]);
                    Debug.Assert(buffer.Count == messageSizes[messageSizeIndex]);
                    for(var i = 0; i < buffer.Count; i++)
                    {
                        buffer.Array[i + buffer.Offset] = 1;
                    }
                    clients[clientIndex] = buffer;
                }

                // Release the buffers.
                for (var clientIndex = 0; clientIndex < NumClients; clientIndex++)
                {
                    // Simulate some clients hanging onto their buffer.
                    var release = rand.Next(10000);
                    if(release > -1) // adjust odds of not releasing here
                    {
                        pool.ReleaseBuffer(clients[clientIndex]);
                    }
                }
            }
            pool.PrintMetrics(Console.Out);
        }
    }
}