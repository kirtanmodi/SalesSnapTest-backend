require('dotenv').config();
const { BlobServiceClient } = require('@azure/storage-blob');
const csvParser = require('csv-parser');
const stream = require('stream');
const fs = require('fs');

const AZURE_STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING;
const CONTAINER_NAME = 'main';
const BLOB_NAME = 'data/lou_malnatis/dw/Snap/Snap_Sales/part-00000-tid-4461000225816458826-bd64164a-1025-45e7-a336-83b5c8bce192-10018-1-c000.csv';
const BLOB_PAYROLL = `data/lou_malnatis/dw/Snap/Snap_Payroll/part-00000-tid-125739318678499795-9e0332c3-1304-4e59-9a82-ee93d37e22d6-14502-1-c000.csv`
const BLOB_PRODUCT = `data/lou_malnatis/dw/Snap/Snap_Product/part-00000-tid-6051312648026986729-f9e7ce36-4088-4d5f-9ba4-d795b135e392-10014-1-c000.csv`
const OUTPUT_FILE = 'product.json';

async function readCSVFromBlob() {
  const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_STORAGE_CONNECTION_STRING);
  const containerClient = blobServiceClient.getContainerClient(CONTAINER_NAME);
  const blobClient = containerClient.getBlobClient(BLOB_PRODUCT);
  const downloadBlockBlobResponse = await blobClient.download();
  const readableStream = downloadBlockBlobResponse.readableStreamBody;

  
  const writeStream = fs.createWriteStream(OUTPUT_FILE);

  
  writeStream.write('[');

  
  const readStream = stream.Readable.from(readableStream);
  let firstRow = true;
  readStream.pipe(csvParser())
    .on('data', (row) => {
      if (!firstRow) {
        writeStream.write(',');
      }
      writeStream.write(JSON.stringify(row)); 
      firstRow = false;
    })
    .on('end', () => {
      writeStream.write(']');
      writeStream.end();
      console.log('CSV file successfully processed and written to', OUTPUT_FILE);
    });
}

readCSVFromBlob().catch((err) => console.error(err));
