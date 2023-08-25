require('dotenv').config();
const { BlobServiceClient } = require('@azure/storage-blob');
const csvParser = require('csv-parser');
const stream = require('stream');
const fs = require('fs');

const AZURE_STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING;
const CONTAINER_NAME = 'main';
const BLOB_NAME = 'data/lou_malnatis/dw/Snap/Snap_Sales/part-00000-tid-2098782748904002653-cd5aed95-21f1-4fae-bc70-e4e1d3e73e35-42-1-c000.csv';
const OUTPUT_FILE = 'output.json';

async function readCSVFromBlob() {
  const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_STORAGE_CONNECTION_STRING);
  const containerClient = blobServiceClient.getContainerClient(CONTAINER_NAME);
  const blobClient = containerClient.getBlobClient(BLOB_NAME);
  const downloadBlockBlobResponse = await blobClient.download();
  const readableStream = downloadBlockBlobResponse.readableStreamBody;

  // Open a writable stream to the output file
  const writeStream = fs.createWriteStream(OUTPUT_FILE);

  // Start writing to the file
  writeStream.write('[');

  // Converting the readableStream to a stream that can be parsed by csv-parser
  const readStream = stream.Readable.from(readableStream);
  let firstRow = true;
  readStream.pipe(csvParser())
    .on('data', (row) => {
      if (!firstRow) {
        writeStream.write(',');
      }
      writeStream.write(JSON.stringify(row)); // Write the JSON object for each row
      firstRow = false;
    })
    .on('end', () => {
      writeStream.write(']');
      writeStream.end();
      console.log('CSV file successfully processed and written to', OUTPUT_FILE);
    });
}

readCSVFromBlob().catch((err) => console.error(err));
