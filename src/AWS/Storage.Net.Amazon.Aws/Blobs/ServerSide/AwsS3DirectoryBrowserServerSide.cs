using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using NetBox.Async;
using Storage.Net.Blobs;

namespace Storage.Net.Amazon.Aws.Blobs
{
   class AwsS3DirectoryBrowserServerSide : AwsS3DirectoryBrowser
   {
      private readonly AmazonS3Client _client;
      private readonly string _bucketName;
      private readonly AsyncLimiter _limiter = new AsyncLimiter(10);

      public AwsS3DirectoryBrowserServerSide(AmazonS3Client client, string bucketName) : base(client, bucketName)
      {
         _client = client;
         _bucketName = bucketName;
      }

      internal override async Task ListFolderAsync(List<Blob> container, string path, ListOptions options, CancellationToken cancellationToken)
      {
         var request = new ListObjectsV2Request()
         {
            BucketName = _bucketName,
            Prefix = FormatFolderPrefix(path),
            Delimiter = "/"   //this tells S3 not to go into the folder recursively
         };

         // if we have provided a file prefix, then append it on.
         // as S3 can perform the filtering at source
         if (!string.IsNullOrEmpty(options.FilePrefix))
         {
            request.Prefix += options.FilePrefix;
         }

         var folderContainer = new List<Blob>();

         while(options.MaxResults == null || (container.Count < options.MaxResults))
         {
            ListObjectsV2Response response;

            using(await _limiter.AcquireOneAsync().ConfigureAwait(false))
            {
               response = await _client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);
            }

            folderContainer.AddRange(response.ToBlobs(options));

            if(response.NextContinuationToken == null)
               break;

            request.ContinuationToken = response.NextContinuationToken;
         }

         container.AddRange(folderContainer);

         if(options.Recurse)
         {
            List<Blob> folders = folderContainer.Where(b => b.Kind == BlobItemKind.Folder).ToList();

            await Task.WhenAll(folders.Select(f => ListFolderAsync(container, f.FullPath, options, cancellationToken))).ConfigureAwait(false);
         }
      }

   }
}