using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace Buffy
{
    /// <summary>
    /// A <see cref="BufferPool"/> is an object which serves reusable buffers to clients.
    /// </summary>
    /// <remarks>
    /// A client can obtain a buffer of a desired size from the pool and release it back into the pool when it is 
    /// finished with it. 
    /// 
    /// Buffers are <see cref="ArraySegment{T}"/> structs which are backed by an array managed by this class.
    /// 
    /// When a buffer is released back into the pool it is cleared, and a reference to the buffer is maintained in a free list.
    /// This prevents the buffer from being collected by the garbage collector. The next time a client requests a buffer of
    /// that size, the previously released buffer will be made available to the client and removed from the free list.
    /// 
    /// Free lists are only maintained for sizes that are multiples of the block size. Portions of the underlying array are
    /// allocated in terms of these sizes, and are referred to as segments. If a client requests a buffer that maps to a
    /// given segment size, and the free list for that segment size has an available buffer, then that buffer is removed
    /// from the free list and its size is compared to the requested buffer size. If these sizes are the same then that
    /// buffer is returned to the client. If the sizes differ, then a new <see cref="ArraySegment{T}"/> of the requested
    /// size is created in place of the old buffer. It is backed by the same segment of the array, but an allocation for
    /// the buffer does take place.
    /// 
    /// The performance characteristics of this class can be controlled by 2 factors: the page size and the block size. The
    /// page size affects the number of array resize operations that need to be performed. The block size affects how many
    /// free lists need to be maintained. Both of these factors determine how much space is "wasted", both in terms of
    /// unallocated space within the underlying buffer, and in terms of unused space within the buffers that are served. The
    /// page size must be evenly divisible by the block size, and must be a power of 2.
    /// 
    /// The block size also affects how many <see cref="ArraySegment{T}"/> allocations must be made by the pool. If clients
    /// </remarks>
    public class BufferPool
    {
        /// <summary>
        /// The page size controls the array re-sizing behavior. Array sizes are always a multiple of the page size.
        /// </summary>
        /// <devdoc>Must be a power of 2.</devdoc>
        private readonly int pageSize;
        
        /// <summary>
        /// The minimum buffer size that can be created.
        /// </summary>
        /// <devdoc>Must evenly divide the page size.</devdoc>
        private readonly int blockSize;

        /// <summary>
        /// The raw byte array that backs all of the buffers.
        /// </summary>
        private byte[] rawBytes;

        /// <summary>
        /// An object used to lock on <see cref="rawBytes"/>.
        /// </summary>
        /// <devdoc>
        /// <see cref="rawBytes"/> isn't safe to lock on since a reference to it may be obtained via the array segments.
        /// </devdoc>
        private readonly object rawBytesLock = new object();

        /// <summary>
        /// The index of the first free element in the buffer, which marks the beginning of unallocated space.
        /// </summary>
        /// <devdoc>
        /// This value increases monotonically.
        /// </devdoc>
        private int freeIndex;

        /// <summary>
        /// Free lists, indexed by buffer size. These contain any <see cref="ArraySegment{T}"/> objects that have been
        /// released and are ready for reuse.
        /// </summary>
        private Dictionary<int, Queue<ArraySegment<byte>>> freeLists = new Dictionary<int, Queue<ArraySegment<byte>>>();

        /// <summary>
        /// Sizes of any buffers in the free lists.
        /// </summary>
        /// <devdoc>Used to quickly determine if a buffer of a given size is available.</devdoc>
        private HashSet<int> freeSizes = new HashSet<int>();

        /// <summary>
        /// Initializes a buffer pool with the given allocation size.
        /// </summary>
        /// <param name="pageSize">
        /// The size in bytes for the initial allocated space. Any further allocations will be in multiples of this 
        /// value. Must be a power of 2.
        /// </param>
        /// <param name="blockSize">
        /// The block size, which dictates the size that buffers will be rounded up to. Must be a power of 2.
        /// </param>
        public BufferPool(int pageSize, int blockSize)
        {
            if (blockSize == 0 || (blockSize & (blockSize - 1)) != 0 )
            {
                throw new ArgumentException($"{nameof(blockSize)} must be a power of 2.");
            }
            if (pageSize < blockSize || (pageSize % blockSize) != 0)
            {
                throw new ArgumentException($"{nameof(pageSize)} must be divisible by {nameof(blockSize)}.");
            }
            this.pageSize = pageSize;
            this.blockSize = blockSize;
            rawBytes = new byte[pageSize];
            freeIndex = 0;
        }

        /// <summary>
        /// Gets a buffer of the given size.
        /// </summary>
        /// <param name="bufferSize">The size of the buffer to retrieve.</param>
        /// <returns>
        /// An <see cref="ArraySegment{T}"/> of <see cref="byte"/>s with <paramref name="bufferSize"/> elements.
        /// </returns>
        /// <remarks>This method is thread-safe.</remarks>
        public ArraySegment<byte> GetBuffer(int bufferSize)
        {
            ArraySegment<byte> buffer;

            // Round up to the nearest multiple of blockSize.
            var segmentSize = GetSegmentSize(bufferSize);

            if (freeSizes.Contains(segmentSize))
            {
                // A buffer of the requested size already exists, so use it.
                var freeList = freeLists[segmentSize];
                lock (freeList)
                {
                    buffer = freeList.Dequeue();
                    if (buffer.Count != bufferSize)
                    {
                        buffer = new ArraySegment<byte>(rawBytes, buffer.Offset, bufferSize);
                        buffersRecreated++;
                    }
                    if (freeList.Count == 0)
                    {
                        // If this is the last buffer of this size, remove the size from freeSizes.
                        freeSizes.Remove(segmentSize);
                    }
                }
            }
            else
            {
                // We don't have a buffer of the requested size available, so create a new one.
                lock (rawBytesLock)
                {
                    // First make sure there is enough space. Resize our array if there isn't.
                    var requiredBytes = bufferSize - (rawBytes.Length - freeIndex + 1);
                    if (requiredBytes > 0)
                    {
                        var pages = requiredBytes / pageSize + ((requiredBytes % pageSize == 0) ? 0 : 1);
                        int newSize = rawBytes.Length + (pages * pageSize);
                        Array.Resize(ref rawBytes, newSize);
                        resizes++;
                    }

                    // Create the new buffer (make sure the freeIndex is updated to reflect this!)
                    buffer = new ArraySegment<byte>(rawBytes, freeIndex, bufferSize);
                    freeIndex += segmentSize;
                    buffersCreated++;
                }
            }
            Interlocked.Increment(ref buffersRequested);
            return buffer;
        }

        /// <summary>
        /// Releases the given buffer, returning it to the pool.
        /// </summary>
        /// <param name="buffer">The buffer to return to the pool.</param>
        /// <remarks>
        /// Once a buffer is released it is cleared, and should not be used again by the releasing client.
        /// </remarks>
        /// <remarks>This method is thread-safe.</remarks>
        public void ReleaseBuffer(ArraySegment<byte> buffer)
        {
            var segmentSize = GetSegmentSize(buffer.Count);

            // See if we need to create a new free list to hold the released buffer.
            if (!freeLists.ContainsKey(segmentSize))
            {
                freeLists[segmentSize] = new Queue<ArraySegment<byte>>();
            }

            // Clear the contents of the buffer before adding to the free list.
            Array.Clear(buffer.Array, buffer.Offset, buffer.Count);

            // Put the buffer in the free list, and indicate that a buffer of its size is available.
            var freeList = freeLists[segmentSize];
            lock(freeList)
            {
                freeList.Enqueue(buffer);
                freeSizes.Add(segmentSize);
                buffersReleased++;
            }
        }

        /// <summary>
        /// Gets the segment size for the given buffer size.
        /// </summary>
        /// <param name="bufferSize">The requested buffer size, in bytes.</param>
        /// <returns>
        /// The segment size, which is at least <paramref name="bufferSize"/>, and is a multiple of 
        /// <see cref="blockSize"/>.
        /// </returns>
        private int GetSegmentSize(int bufferSize)
        {
            return (bufferSize + (blockSize - 1)) & (-blockSize);
        }

        #region Metrics

        private int buffersCreated;
        private int buffersRecreated;
        private int buffersRequested;
        private int buffersReleased;
        private int resizes;

        internal void PrintMetrics(TextWriter writer)
        {
            writer.WriteLine($"Page Size: {pageSize}");
            writer.WriteLine($"Block Size: {blockSize}");
            writer.WriteLine($"Free Lists: {freeLists.Count}");
            writer.WriteLine($"Size: {rawBytes.Length} bytes");
            writer.WriteLine($"Resizes: {resizes}");
            writer.WriteLine($"Created: {buffersCreated}");
            writer.WriteLine($"Re-created: {buffersRecreated}");
            writer.WriteLine($"Requested: {buffersRequested}");
            writer.WriteLine($"Released: {buffersReleased}");
            writer.WriteLine("Reuse: {0:p}", ((double)(buffersRequested - (buffersCreated + buffersRecreated))) / buffersRequested);
        }

        #endregion
    }
}
