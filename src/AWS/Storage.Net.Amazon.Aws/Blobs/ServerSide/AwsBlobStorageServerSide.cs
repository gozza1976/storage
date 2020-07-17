using Storage.Net.Blobs;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Amazon;
using Amazon.S3;
using Amazon.Runtime;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using System.Threading.Tasks;
using System.Threading;
using Storage.Net.Streaming;
using NetBox.Extensions;
using System.Net;

namespace Storage.Net.Amazon.Aws.Blobs
{
   class AwsBlobStorageServerSide : AwsS3BlobStorage
   {
      public AwsBlobStorageServerSide(string bucketName, string region) :base(bucketName, region) {}


      public override  async Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options = null, CancellationToken cancellationToken = default)
      {
         if(options == null)
            options = new ListOptions();

         GenericValidation.CheckBlobPrefix(options.FilePrefix);

         AmazonS3Client client = await GetClientAsync().ConfigureAwait(false);

         IReadOnlyCollection<Blob> blobs;
         
         using(var browser = new AwsS3DirectoryBrowserServerSide(client, _bucketName))
         {
            blobs = await browser.ListAsync(options, cancellationToken).ConfigureAwait(false);
         }

         if(options.IncludeAttributes)
         {
            foreach(IEnumerable<Blob> page in blobs.Where(b => !b.IsFolder).Chunk(ListChunkSize))
            {
               await Converter.AppendMetadataAsync(client, _bucketName, page, cancellationToken).ConfigureAwait(false);
            }
         }

         return blobs;
      }


   }
}
